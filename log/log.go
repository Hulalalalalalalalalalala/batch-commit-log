// Package log implements an append-only, batch-commit segmented log.
//
// A writer stages a batch of records with Append, which reserves a
// strictly increasing sequence number, and publishes it atomically with
// Commit. Readers only ever see committed batches.
package log

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

var (
	// ErrNotCommitted is returned when reading a sequence that has been
	// staged with Append but not yet committed.
	ErrNotCommitted = errors.New("log: batch is staged but not committed")
	// ErrCorruptSegment is returned when a segment file is truncated or
	// tampered with.
	ErrCorruptSegment = errors.New("log: segment file is corrupt")
	// ErrUnknownBatch is returned for sequence 0, sequences that were
	// never reserved, and batches that were already committed.
	ErrUnknownBatch = errors.New("log: unknown batch")
	// ErrInvalidOptions is returned when Options are not usable, e.g. a
	// non-positive segment capacity.
	ErrInvalidOptions = errors.New("log: invalid options")
)

// Options configures a Log.
type Options struct {
	// SegmentBytes is the capacity of one segment file in bytes. Once a
	// non-empty segment would grow past this capacity the log rolls to a
	// new segment. Must be positive.
	SegmentBytes int
	// Sync makes Commit fsync the segment file before returning, so a
	// returned commit is durable.
	Sync bool
}

// Segment describes one segment file: the minimum and maximum committed
// sequence numbers stored in it.
type Segment struct {
	FirstSeq uint64
	LastSeq  uint64
}

// Batch is a handle to a group of records. Append returns a staged
// batch; Scan delivers committed batches.
type Batch struct {
	seq     uint64
	records [][]byte
}

// Seq returns the sequence number reserved for this batch.
func (b Batch) Seq() uint64 { return b.seq }

// Records returns the batch's records as opaque bytes.
func (b Batch) Records() [][]byte {
	out := make([][]byte, len(b.records))
	copy(out, b.records)
	return out
}

type location struct {
	segment int
	offset  int64
}

type segmentState struct {
	path    string
	minSeq  uint64
	maxSeq  uint64
	batches int
}

// Log is an append-only batch commit log. Single writer; concurrent
// appends are not serialised.
type Log struct {
	dir        string
	opts       Options
	nextSeq    uint64
	staged     map[uint64][][]byte
	committed  map[uint64]location
	segments   []*segmentState
	active     *os.File
	activeSize int64
	nextSegIdx int
}

// Open opens the log in dir, creating the directory if it does not
// exist, and validates every segment file.
func Open(dir string, opts Options) (*Log, error) {
	if opts.SegmentBytes <= 0 {
		return nil, ErrInvalidOptions
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var indices []int
	nameByIdx := map[int]string{}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		idx, ok := parseSegmentName(e.Name())
		if !ok {
			continue
		}
		indices = append(indices, idx)
		nameByIdx[idx] = e.Name()
	}
	sort.Ints(indices)

	l := &Log{
		dir:        dir,
		opts:       opts,
		nextSeq:    1,
		staged:     make(map[uint64][][]byte),
		committed:  make(map[uint64]location),
		nextSegIdx: 1,
	}
	for _, idx := range indices {
		path := filepath.Join(dir, nameByIdx[idx])
		info, err := os.Stat(path)
		if err != nil {
			return nil, err
		}
		if info.Size() == 0 {
			// Leftover from a crash between creating the file and
			// writing the first entry; it holds no data.
			if err := os.Remove(path); err != nil {
				return nil, err
			}
			continue
		}
		if err := l.loadSegment(path); err != nil {
			return nil, err
		}
		if idx >= l.nextSegIdx {
			l.nextSegIdx = idx + 1
		}
	}
	if n := len(l.segments); n > 0 {
		last := l.segments[n-1]
		f, err := os.OpenFile(last.path, os.O_WRONLY, 0o644)
		if err != nil {
			return nil, err
		}
		info, err := f.Stat()
		if err != nil {
			f.Close()
			return nil, err
		}
		l.active = f
		l.activeSize = info.Size()
	}
	return l, nil
}

// Close closes the active segment file.
func (l *Log) Close() error {
	if l.active != nil {
		err := l.active.Close()
		l.active = nil
		return err
	}
	return nil
}

// Append stages a batch of records, reserving the next sequence number
// for it. The batch becomes readable only after Commit.
func (l *Log) Append(records [][]byte) (Batch, error) {
	cp := make([][]byte, len(records))
	for i, r := range records {
		b := make([]byte, len(r))
		copy(b, r)
		cp[i] = b
	}
	seq := l.nextSeq
	l.nextSeq++
	l.staged[seq] = cp
	return Batch{seq: seq, records: cp}, nil
}

// Commit writes a staged batch to the log, making it atomically visible
// to readers, and returns the sequence reserved at Append time.
// Committing the same batch twice returns ErrUnknownBatch.
func (l *Log) Commit(b Batch) (uint64, error) {
	records, ok := l.staged[b.seq]
	if !ok {
		return 0, ErrUnknownBatch
	}
	entry := encodeEntry(b.seq, records)
	if err := l.ensureActive(len(entry)); err != nil {
		return 0, err
	}
	off := l.activeSize
	if _, err := l.active.WriteAt(entry, off); err != nil {
		return 0, err
	}
	if l.opts.Sync {
		if err := l.active.Sync(); err != nil {
			return 0, err
		}
	}
	l.activeSize += int64(len(entry))
	seg := l.segments[len(l.segments)-1]
	if seg.batches == 0 || b.seq < seg.minSeq {
		seg.minSeq = b.seq
	}
	if seg.batches == 0 || b.seq > seg.maxSeq {
		seg.maxSeq = b.seq
	}
	seg.batches++
	l.committed[b.seq] = location{segment: len(l.segments) - 1, offset: off}
	delete(l.staged, b.seq)
	return b.seq, nil
}

// Read returns the records of one committed batch. A staged but
// uncommitted sequence yields ErrNotCommitted; sequence 0 and sequences
// outside the reserved range yield ErrUnknownBatch.
func (l *Log) Read(seq uint64) ([][]byte, error) {
	if seq == 0 {
		return nil, ErrUnknownBatch
	}
	if _, ok := l.staged[seq]; ok {
		return nil, ErrNotCommitted
	}
	loc, ok := l.committed[seq]
	if !ok {
		return nil, ErrUnknownBatch
	}
	return l.readAt(loc)
}

// Scan replays committed batches with sequence >= from, in increasing
// sequence order, invoking fn for each. A non-nil error from fn stops
// the scan and is returned.
func (l *Log) Scan(from uint64, fn func(Batch) error) error {
	seqs := make([]uint64, 0, len(l.committed))
	for s := range l.committed {
		if s >= from {
			seqs = append(seqs, s)
		}
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	for _, s := range seqs {
		records, err := l.readAt(l.committed[s])
		if err != nil {
			return err
		}
		if err := fn(Batch{seq: s, records: records}); err != nil {
			return err
		}
	}
	return nil
}

// Segments lists the segment files that hold committed batches, in
// order, each with its first and last sequence.
func (l *Log) Segments() []Segment {
	out := make([]Segment, 0, len(l.segments))
	for _, s := range l.segments {
		if s.batches == 0 {
			continue
		}
		out = append(out, Segment{FirstSeq: s.minSeq, LastSeq: s.maxSeq})
	}
	return out
}

func (l *Log) readAt(loc location) ([][]byte, error) {
	seg := l.segments[loc.segment]
	f, err := os.Open(seg.path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	_, records, _, err := readEntry(f, loc.offset, info.Size())
	if err != nil {
		return nil, ErrCorruptSegment
	}
	return records, nil
}

// ensureActive makes sure there is an active segment with room for need
// more bytes, rolling to a new segment file when the current one would
// exceed its capacity.
func (l *Log) ensureActive(need int) error {
	if l.active != nil && (l.activeSize == 0 || l.activeSize+int64(need) <= int64(l.opts.SegmentBytes)) {
		return nil
	}
	if l.active != nil {
		l.active.Close()
		l.active = nil
	}
	idx := l.nextSegIdx
	l.nextSegIdx++
	path := filepath.Join(l.dir, segmentFileName(idx))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	l.active = f
	l.activeSize = 0
	l.segments = append(l.segments, &segmentState{path: path})
	return nil
}

func (l *Log) loadSegment(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return err
	}
	size := info.Size()
	seg := &segmentState{path: path}
	segIdx := len(l.segments)
	var off int64
	for off < size {
		seq, _, next, err := readEntry(f, off, size)
		if err != nil {
			return ErrCorruptSegment
		}
		if seg.batches == 0 || seq < seg.minSeq {
			seg.minSeq = seq
		}
		if seg.batches == 0 || seq > seg.maxSeq {
			seg.maxSeq = seq
		}
		seg.batches++
		l.committed[seq] = location{segment: segIdx, offset: off}
		if seq >= l.nextSeq {
			l.nextSeq = seq + 1
		}
		off = next
	}
	l.segments = append(l.segments, seg)
	return nil
}

// Entry layout on disk:
//
//	[payloadLen uint32][crc32(payload) uint32][payload]
//	payload: [seq uint64][recordCount uint32][recordLen uint32, record]...
func encodeEntry(seq uint64, records [][]byte) []byte {
	size := 12
	for _, r := range records {
		size += 4 + len(r)
	}
	buf := make([]byte, 8+size)
	binary.BigEndian.PutUint32(buf[0:4], uint32(size))
	payload := buf[8:]
	binary.BigEndian.PutUint64(payload[0:8], seq)
	binary.BigEndian.PutUint32(payload[8:12], uint32(len(records)))
	off := 12
	for _, r := range records {
		binary.BigEndian.PutUint32(payload[off:off+4], uint32(len(r)))
		copy(payload[off+4:], r)
		off += 4 + len(r)
	}
	binary.BigEndian.PutUint32(buf[4:8], crc32.ChecksumIEEE(payload))
	return buf
}

// readEntry decodes the entry at off in f, where size is the file size.
// It returns the batch sequence, its records, and the offset of the
// next entry.
func readEntry(f *os.File, off, size int64) (uint64, [][]byte, int64, error) {
	if size-off < 8 {
		return 0, nil, 0, ErrCorruptSegment
	}
	var hdr [8]byte
	if _, err := f.ReadAt(hdr[:], off); err != nil {
		return 0, nil, 0, ErrCorruptSegment
	}
	length := int64(binary.BigEndian.Uint32(hdr[0:4]))
	wantCRC := binary.BigEndian.Uint32(hdr[4:8])
	if length > size-off-8 {
		return 0, nil, 0, ErrCorruptSegment
	}
	payload := make([]byte, length)
	if _, err := f.ReadAt(payload, off+8); err != nil {
		return 0, nil, 0, ErrCorruptSegment
	}
	if crc32.ChecksumIEEE(payload) != wantCRC {
		return 0, nil, 0, ErrCorruptSegment
	}
	seq, records, err := parsePayload(payload)
	if err != nil {
		return 0, nil, 0, err
	}
	return seq, records, off + 8 + length, nil
}

func parsePayload(payload []byte) (uint64, [][]byte, error) {
	if len(payload) < 12 {
		return 0, nil, ErrCorruptSegment
	}
	seq := binary.BigEndian.Uint64(payload[0:8])
	count := int(binary.BigEndian.Uint32(payload[8:12]))
	// Every record needs at least a 4-byte length prefix.
	if count > (len(payload)-12)/4 {
		return 0, nil, ErrCorruptSegment
	}
	records := make([][]byte, 0, count)
	off := 12
	for i := 0; i < count; i++ {
		n := int(binary.BigEndian.Uint32(payload[off : off+4]))
		off += 4
		if n > len(payload)-off {
			return 0, nil, ErrCorruptSegment
		}
		rec := make([]byte, n)
		copy(rec, payload[off:off+n])
		off += n
		records = append(records, rec)
	}
	if off != len(payload) {
		return 0, nil, ErrCorruptSegment
	}
	return seq, records, nil
}

const (
	segmentPrefix = "segment-"
	segmentSuffix = ".log"
)

func segmentFileName(idx int) string {
	return fmt.Sprintf("%s%06d%s", segmentPrefix, idx, segmentSuffix)
}

func parseSegmentName(name string) (int, bool) {
	if !strings.HasPrefix(name, segmentPrefix) || !strings.HasSuffix(name, segmentSuffix) {
		return 0, false
	}
	num := name[len(segmentPrefix) : len(name)-len(segmentSuffix)]
	if num == "" {
		return 0, false
	}
	for _, c := range num {
		if c < '0' || c > '9' {
			return 0, false
		}
	}
	n, err := strconv.Atoi(num)
	if err != nil {
		return 0, false
	}
	return n, true
}
