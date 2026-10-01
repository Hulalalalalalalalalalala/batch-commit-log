// Package log implements a single-writer, append-only batch commit log.
//
// Records are grouped into batches. A batch is staged with Append and becomes
// atomically visible with Commit, which assigns it a sequence number starting
// at 1 in commit order. Committed batches are stored in rolling segment files;
// reopening a log directory recovers the committed prefix and discards
// incomplete tail frames.
package log

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"sync"
)

// Public error values.
var (
	// ErrNotCommitted is returned when reading a sequence that has no
	// committed batch.
	ErrNotCommitted = errors.New("log: sequence not committed")
	// ErrCorruptSegment is returned when a segment contains damage in the
	// middle of otherwise complete data: a bad length, checksum, magic or a
	// break in sequence continuity. The directory is left untouched.
	ErrCorruptSegment = errors.New("log: corrupt segment")
	// ErrUnknownBatch is returned when Commit receives a batch that was not
	// staged by this Log.
	ErrUnknownBatch = errors.New("log: unknown batch")
)

const (
	magic      = "BCL1"
	headerSize = 4 + 1 + 4 // magic, frame type, body length
	crcSize    = 4
	segNameFmt = "%010d.log"
)

const (
	frameBatch  = byte('B')
	frameCommit = byte('C')
)

var segNameRe = regexp.MustCompile(`^(\d{10})\.log$`)

// Options configures a Log.
type Options struct {
	// SegmentBytes is the soft size limit at which a new segment is started
	// before the next commit. A value <= 0 disables rolling.
	SegmentBytes int
	// Sync makes Commit flush the batch and its commit marker to stable
	// storage before returning. When false, writes may remain merged in the
	// OS page cache; data confirmed by Commit is not lost on a normal process
	// exit.
	Sync bool
}

// Batch is a staged or replayed set of records. Sequence is 0 until the batch
// is committed.
type Batch struct {
	ID       uint64
	Records  [][]byte
	Sequence uint64
}

// Segment describes one segment file.
type Segment struct {
	// File is the path of the segment file.
	File string
	// FirstSeq and LastSeq are the sequence numbers of the first and last
	// committed batches in the segment.
	FirstSeq uint64
	LastSeq  uint64
	// Bytes is the current size of the segment file.
	Bytes int64
}

type stagedBatch struct {
	id      uint64
	records [][]byte
	seq     uint64 // 0 while uncommitted
}

type location struct {
	seg    int // segment index, 1-based
	offset int64
	bytes  int64 // on-disk size of the batch frame
}

type segmentMeta struct {
	index    int
	file     string
	firstSeq uint64
	lastSeq  uint64
	size     int64
}

type frameEntry struct {
	seq uint64
	id  uint64
	loc location
}

type recoveredSegment struct {
	meta      *segmentMeta
	entries   []frameEntry
	validSize int64 // committed prefix length
	torn      bool  // file has bytes beyond validSize that must be dropped
}

// Log is an append-only batch commit log. It supports a single writer;
// concurrent appends are not serialised.
type Log struct {
	dir  string
	opts Options

	mu       sync.Mutex // guards the in-memory index and the write cursor
	segments []*segmentMeta
	locs     map[uint64]location
	lastSeq  uint64

	active    *os.File
	activeIdx int // 1-based index of active; 0 when no segment file exists

	nextID  uint64
	pending map[uint64]*stagedBatch
}

// Open opens the log directory, creating it if necessary. It replays segment
// files and recovers the committed prefix: an incomplete trailing frame in the
// last segment is truncated, while damage in the middle of complete data
// yields ErrCorruptSegment without modifying the directory.
func Open(dir string, opts Options) (*Log, error) {
	if dir == "" {
		return nil, errors.New("log: empty directory path")
	}
	info, err := os.Stat(dir)
	switch {
	case err == nil:
		if !info.IsDir() {
			return nil, fmt.Errorf("log: %s is not a directory", dir)
		}
	case errors.Is(err, os.ErrNotExist):
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	default:
		return nil, err
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, e := range entries {
		if !e.IsDir() && segNameRe.MatchString(e.Name()) {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)

	l := &Log{
		dir:     dir,
		opts:    opts,
		locs:    make(map[uint64]location),
		pending: make(map[uint64]*stagedBatch),
	}

	expectedSeq := uint64(1)
	for i, name := range names {
		index := i + 1
		var numbered int
		fmt.Sscanf(segNameRe.FindStringSubmatch(name)[1], "%d", &numbered)
		if numbered != index {
			return nil, fmt.Errorf("%w: segment %s is out of order", ErrCorruptSegment, name)
		}

		path := filepath.Join(dir, name)
		rec, err := scanSegment(path, index, &expectedSeq, i == len(names)-1)
		if err != nil {
			return nil, err
		}

		// A trailing segment with no committed batch (a torn transaction or a
		// file created but never written) is removed so the next commit starts
		// a clean segment 1..N.
		if len(rec.entries) == 0 {
			if i != len(names)-1 {
				return nil, fmt.Errorf("%w: empty non-final segment %s", ErrCorruptSegment, name)
			}
			if err := os.Remove(path); err != nil {
				return nil, err
			}
			if l.opts.Sync {
				if err := syncDir(dir); err != nil {
					return nil, err
				}
			}
			continue
		}
		if rec.torn {
			if err := truncateTail(path, rec.validSize, l.opts.Sync); err != nil {
				return nil, err
			}
		}

		l.segments = append(l.segments, rec.meta)
		for _, e := range rec.entries {
			l.locs[e.seq] = e.loc
			l.lastSeq = e.seq
		}
	}

	if len(l.segments) > 0 {
		last := l.segments[len(l.segments)-1]
		f, err := os.OpenFile(last.file, os.O_RDWR|os.O_APPEND, 0o644)
		if err != nil {
			return nil, err
		}
		l.active = f
		l.activeIdx = last.index
	}

	return l, nil
}

// scanSegment replays one segment file. expectedSeq is advanced past every
// committed batch found. Damage in complete data is ErrCorruptSegment; bytes
// past the last complete transaction in the final segment are reported as a
// torn tail instead.
func scanSegment(path string, index int, expectedSeq *uint64, lastSegment bool) (*recoveredSegment, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	fileSize := st.Size()

	rec := &recoveredSegment{meta: &segmentMeta{index: index, file: path}}

	var pos int64
	var (
		pendingID    uint64
		pendingStart int64
		pendingSize  int64
		havePending  bool
	)

	corrupt := func(format string, args ...any) error {
		return fmt.Errorf("%w: %s", ErrCorruptSegment, fmt.Sprintf(format, args...))
	}
	// tornTail rolls back the trailing transaction to the given byte offset;
	// permitted only in the final segment.
	tornTail := func(valid int64) (*recoveredSegment, error) {
		if !lastSegment {
			return nil, corrupt("incomplete frame in non-final segment %s", path)
		}
		if havePending {
			// The trailing transaction (batch frame, possibly followed by a
			// partial commit marker) was never committed; drop all of it.
			valid = pendingStart
		}
		rec.torn = true
		rec.validSize = valid
		rec.meta.size = valid
		return rec, nil
	}

	buf := make([]byte, 0, 4096)
	for pos < fileSize {
		if fileSize-pos < headerSize+crcSize {
			return tornTail(pos)
		}
		header := make([]byte, headerSize)
		if _, err := io.ReadFull(f, header); err != nil {
			return nil, err
		}
		bodyLen := int64(binary.BigEndian.Uint32(header[5:headerSize]))
		frameTotal := headerSize + bodyLen + crcSize

		// A frame that runs past end-of-file is an incomplete tail write.
		// This is checked before magic/CRC: torn bytes typically carry a
		// garbage length that extends beyond the file.
		if pos+frameTotal > fileSize {
			return tornTail(pos)
		}
		if string(header[0:4]) != magic {
			return nil, corrupt("bad magic in %s at offset %d", path, pos)
		}
		frameType := header[4]
		buf = buf[:0]
		if bodyLen+crcSize > int64(cap(buf)) {
			buf = make([]byte, bodyLen+crcSize)
		} else {
			buf = buf[:bodyLen+crcSize]
		}
		if _, err := io.ReadFull(f, buf); err != nil {
			return nil, err
		}
		body := buf[:bodyLen]
		wantCRC := binary.BigEndian.Uint32(buf[bodyLen : bodyLen+crcSize])
		if crc32.ChecksumIEEE(append(header, body...)) != wantCRC {
			return nil, corrupt("checksum mismatch in %s at offset %d", path, pos)
		}

		switch frameType {
		case frameBatch:
			id, _, perr := parseBatchBody(body)
			if perr != nil {
				return nil, corrupt("%v in %s at offset %d", perr, path, pos)
			}
			if havePending {
				return nil, corrupt("batch frame without commit marker in %s at offset %d", path, pos)
			}
			havePending = true
			pendingID = id
			pendingStart = pos
			pendingSize = frameTotal
		case frameCommit:
			seq, id, perr := parseCommitBody(body)
			if perr != nil {
				return nil, corrupt("%v in %s at offset %d", perr, path, pos)
			}
			if !havePending || id != pendingID {
				return nil, corrupt("commit marker does not match batch in %s at offset %d", path, pos)
			}
			if seq != *expectedSeq {
				return nil, corrupt("sequence discontinuity in %s: want %d got %d", path, *expectedSeq, seq)
			}
			if rec.meta.firstSeq == 0 {
				rec.meta.firstSeq = seq
			}
			rec.meta.lastSeq = seq
			rec.entries = append(rec.entries, frameEntry{
				seq: seq,
				id:  id,
				loc: location{seg: index, offset: pendingStart, bytes: pendingSize},
			})
			*expectedSeq = seq + 1
			havePending = false
		default:
			return nil, corrupt("unknown frame type %d in %s at offset %d", frameType, path, pos)
		}

		pos += frameTotal
	}

	if havePending {
		// A complete batch frame without its commit marker is a transaction
		// torn between its two writes; roll the whole batch back.
		return tornTail(pendingStart)
	}

	rec.validSize = fileSize
	rec.meta.size = fileSize
	return rec, nil
}

func truncateTail(path string, size int64, sync bool) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	if err := f.Truncate(size); err != nil {
		f.Close()
		return err
	}
	if sync {
		if err := f.Sync(); err != nil {
			f.Close()
			return err
		}
	}
	return f.Close()
}

func parseBatchBody(body []byte) (id uint64, records [][]byte, err error) {
	if len(body) < 12 {
		return 0, nil, errors.New("short batch frame")
	}
	id = binary.BigEndian.Uint64(body[0:8])
	n := int(binary.BigEndian.Uint32(body[8:12]))
	p := 12
	records = make([][]byte, 0, n)
	for i := 0; i < n; i++ {
		if p+4 > len(body) {
			return 0, nil, errors.New("truncated record length")
		}
		rl := int(binary.BigEndian.Uint32(body[p : p+4]))
		p += 4
		if p+rl > len(body) {
			return 0, nil, errors.New("truncated record")
		}
		rec := make([]byte, rl)
		copy(rec, body[p:p+rl])
		records = append(records, rec)
		p += rl
	}
	if p != len(body) {
		return 0, nil, errors.New("trailing bytes in batch frame")
	}
	return id, records, nil
}

func parseCommitBody(body []byte) (seq, id uint64, err error) {
	if len(body) != 16 {
		return 0, 0, errors.New("bad commit frame")
	}
	return binary.BigEndian.Uint64(body[0:8]), binary.BigEndian.Uint64(body[8:16]), nil
}

func segmentName(index int) string {
	return fmt.Sprintf(segNameFmt, index)
}

// Append stages a batch of opaque records. It returns a Batch whose Sequence
// is 0 until Commit succeeds.
func (l *Log) Append(records [][]byte) (Batch, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	bodyLen := 12
	for i, r := range records {
		if len(r) > 0xffffffff {
			return Batch{}, fmt.Errorf("log: record %d too large", i)
		}
		bodyLen += 4 + len(r)
	}
	if bodyLen > 0xffffffff {
		return Batch{}, errors.New("log: batch too large")
	}

	l.nextID++
	id := l.nextID
	copied := make([][]byte, len(records))
	for i, r := range records {
		cp := make([]byte, len(r))
		copy(cp, r)
		copied[i] = cp
	}
	l.pending[id] = &stagedBatch{id: id, records: copied}
	return Batch{ID: id, Records: copied, Sequence: 0}, nil
}

// Commit makes a staged batch durable and returns its sequence number, counted
// from 1 in commit order. All records of one Append become visible atomically.
// Committing the same Batch again returns its original sequence without
// writing anything.
func (l *Log) Commit(b Batch) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	sb, ok := l.pending[b.ID]
	if !ok {
		return 0, ErrUnknownBatch
	}
	if sb.seq != 0 {
		return sb.seq, nil
	}

	if err := l.rollIfNeeded(); err != nil {
		return 0, err
	}

	seq := l.lastSeq + 1
	buf, err := encodeFrames(sb.id, seq, sb.records)
	if err != nil {
		return 0, err
	}

	seg := l.segments[len(l.segments)-1]
	committedSize := seg.size
	n, err := l.active.Write(buf)
	if err == nil && n != len(buf) {
		err = io.ErrShortWrite
	}
	if err == nil && l.opts.Sync {
		err = l.active.Sync()
	}
	if err != nil {
		// Roll back the unconfirmed transaction so a later commit cannot land
		// behind a torn frame; this mirrors what Open would do after a crash.
		if rerr := l.active.Truncate(committedSize); rerr != nil {
			return 0, fmt.Errorf("log: commit failed (%v); rollback also failed: %w", err, rerr)
		}
		return 0, err
	}

	seg.size += int64(n)
	if seg.firstSeq == 0 {
		seg.firstSeq = seq
	}
	seg.lastSeq = seq
	l.lastSeq = seq
	l.locs[seq] = location{seg: seg.index, offset: committedSize, bytes: batchFrameBytes(sb.records)}
	sb.seq = seq
	return seq, nil
}

// Read returns the records of one committed batch.
func (l *Log) Read(seq uint64) ([][]byte, error) {
	_, records, err := l.readCommitted(seq)
	return records, err
}

func (l *Log) readCommitted(seq uint64) (uint64, [][]byte, error) {
	l.mu.Lock()
	loc, ok := l.locs[seq]
	l.mu.Unlock()
	if !ok {
		return 0, nil, ErrNotCommitted
	}

	f, err := os.Open(filepath.Join(l.dir, segmentName(loc.seg)))
	if err != nil {
		return 0, nil, err
	}
	defer f.Close()

	buf := make([]byte, loc.bytes)
	if _, err := io.ReadFull(io.NewSectionReader(f, loc.offset, loc.bytes), buf); err != nil {
		return 0, nil, err
	}
	body, ok := verifyFrame(buf, frameBatch)
	if !ok {
		return 0, nil, ErrCorruptSegment
	}
	id, records, err := parseBatchBody(body)
	if err != nil {
		return 0, nil, ErrCorruptSegment
	}
	return id, records, nil
}

// Scan replays committed batches in sequence order, starting with the first
// batch whose sequence is >= from, and invokes fn for each. If fn returns an
// error, scanning stops and that error is returned.
func (l *Log) Scan(from uint64, fn func(Batch) error) error {
	l.mu.Lock()
	last := l.lastSeq
	l.mu.Unlock()

	for seq := from; seq <= last; seq++ {
		id, records, err := l.readCommitted(seq)
		if err != nil {
			return err
		}
		if err := fn(Batch{ID: id, Records: records, Sequence: seq}); err != nil {
			return err
		}
	}
	return nil
}

// Segments reports the sequence range and size of each segment.
func (l *Log) Segments() []Segment {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]Segment, 0, len(l.segments))
	for _, s := range l.segments {
		out = append(out, Segment{
			File:     s.file,
			FirstSeq: s.firstSeq,
			LastSeq:  s.lastSeq,
			Bytes:    s.size,
		})
	}
	return out
}

// Close releases the write handle.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.active != nil {
		err := l.active.Close()
		l.active = nil
		return err
	}
	return nil
}

// rollIfNeeded opens the active segment, starting a new one when the current
// segment has reached SegmentBytes.
func (l *Log) rollIfNeeded() error {
	if l.active != nil {
		cur := l.segments[len(l.segments)-1]
		if l.opts.SegmentBytes <= 0 || cur.size < int64(l.opts.SegmentBytes) {
			return nil
		}
		if err := l.active.Close(); err != nil {
			return err
		}
		l.active = nil
	}

	idx := l.activeIdx + 1
	path := filepath.Join(l.dir, segmentName(idx))
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	if l.opts.Sync {
		if err := syncDir(l.dir); err != nil {
			f.Close()
			return err
		}
	}
	l.active = f
	l.activeIdx = idx
	l.segments = append(l.segments, &segmentMeta{index: idx, file: path})
	return nil
}

func encodeFrames(id, seq uint64, records [][]byte) ([]byte, error) {
	body := make([]byte, 12, 12)
	binary.BigEndian.PutUint64(body[0:8], id)
	binary.BigEndian.PutUint32(body[8:12], uint32(len(records)))
	for _, r := range records {
		var lenBuf [4]byte
		binary.BigEndian.PutUint32(lenBuf[:], uint32(len(r)))
		body = append(body, lenBuf[:]...)
		body = append(body, r...)
	}
	if len(body) > 0xffffffff {
		return nil, errors.New("log: batch too large")
	}

	var commitBody [16]byte
	binary.BigEndian.PutUint64(commitBody[0:8], seq)
	binary.BigEndian.PutUint64(commitBody[8:16], id)

	buf := appendFrame(nil, frameBatch, body)
	buf = appendFrame(buf, frameCommit, commitBody[:])
	return buf, nil
}

func appendFrame(dst []byte, frameType byte, body []byte) []byte {
	var header [headerSize]byte
	copy(header[0:4], magic)
	header[4] = frameType
	binary.BigEndian.PutUint32(header[5:headerSize], uint32(len(body)))
	dst = append(dst, header[:]...)
	dst = append(dst, body...)
	crc := crc32.ChecksumIEEE(append(header[:], body...))
	var crcBuf [crcSize]byte
	binary.BigEndian.PutUint32(crcBuf[:], crc)
	return append(dst, crcBuf[:]...)
}

func verifyFrame(buf []byte, wantType byte) (body []byte, ok bool) {
	if len(buf) < headerSize+crcSize {
		return nil, false
	}
	if string(buf[0:4]) != magic || buf[4] != wantType {
		return nil, false
	}
	bodyLen := int(binary.BigEndian.Uint32(buf[5:headerSize]))
	if headerSize+bodyLen+crcSize != len(buf) {
		return nil, false
	}
	body = buf[headerSize : headerSize+bodyLen]
	wantCRC := binary.BigEndian.Uint32(buf[len(buf)-crcSize:])
	if crc32.ChecksumIEEE(buf[:len(buf)-crcSize]) != wantCRC {
		return nil, false
	}
	return body, true
}

func batchFrameBytes(records [][]byte) int64 {
	size := int64(headerSize + 12 + crcSize)
	for _, r := range records {
		size += int64(4 + len(r))
	}
	return size
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
