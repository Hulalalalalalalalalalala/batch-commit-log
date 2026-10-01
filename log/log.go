// Package log implements an append-only batch commit log.
//
// Records are grouped into batches. A staged batch becomes visible
// atomically when Commit succeeds: committed batches are assigned dense,
// gap-free sequence numbers starting at 1, in commit order.
//
// The log is stored in length-delimited, checksummed segment files. On
// Open the committed prefix is recovered and an incomplete trailing frame
// is truncated; corruption anywhere before that prefix is reported.
//
// See the repository README for the public interface contract.
package log

import (
	"bytes"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// Sentinel errors. Callers should compare with errors.Is.
var (
	// ErrNotCommitted is returned when reading a sequence that has no
	// committed batch.
	ErrNotCommitted = errors.New("log: sequence is not committed")

	// ErrCorruptSegment is returned when Open encounters a length,
	// checksum or sequence-continuity failure in the middle of stored
	// data. The directory is left unchanged.
	ErrCorruptSegment = errors.New("log: corrupt segment")

	// ErrUnknownBatch is returned when Commit is handed a Batch that was
	// not staged by Append on this Log.
	ErrUnknownBatch = errors.New("log: unknown batch")
)

var errCorrupt = errors.New("corrupt frame")

const (
	segSuffix   = ".seg"
	segNameFmt  = "%020d" + segSuffix
	segNameBase = 10
	segNameBits = 64

	// Frame layout:
	//   magic    [8]  frameMagic
	//   plen     uint32 big-endian payload length
	//   payload  [plen]
	//   crc      uint32 big-endian CRC-32/IEEE over payload
	magicSize  = 8
	headerSize = 4 // uint32 payload length
	crcSize    = 4 // uint32 CRC-32/IEEE over payload
	prefixSize = magicSize + headerSize
	// payload: uint64 seq, uint32 record count, then per record
	// uint32 length followed by the record bytes.
	minPayloadSize = 8 + 4

	dirPerm  = 0o755
	filePerm = 0o644
)

// frameMagic marks the start of every frame. It lets recovery tell a torn
// trailing write apart from a damaged length field followed by valid
// frames, even when records contain arbitrary bytes.
var frameMagic = [magicSize]byte{'B', 'C', 'L', 'F', 'R', 'M', '0', '1'}

// Options configures a Log opened with Open.
type Options struct {
	// SegmentBytes is the target size at which a new segment file is
	// started before writing the next batch. A batch is never split
	// across segments. Values <= 0 disable rolling.
	SegmentBytes int

	// Sync makes Commit flush the batch (and its recovery metadata) to
	// durable storage before returning. When false, writes may be
	// coalesced by the OS, but data written before a normal process
	// exit is not lost.
	Sync bool
}

// Batch is a set of opaque records staged by Append, or a committed batch
// handed to Scan callbacks. Sequence is 0 until the batch is committed;
// batches reconstructed from storage have ID 0 and cannot be recommitted.
type Batch struct {
	ID       uint64
	Records  [][]byte
	Sequence uint64
}

// Segment describes one on-disk segment file.
type Segment struct {
	File     string
	FirstSeq uint64
	LastSeq  uint64
	Bytes    int64
}

// Log is an open batch commit log. It is single-writer: the methods are
// not safe for concurrent use.
type Log struct {
	dir  string
	opts Options

	segs   []*segment
	active *segment
	f      *os.File // active segment, opened O_APPEND

	// index maps a committed sequence to its frame location.
	index map[uint64]location

	staged  map[uint64]*stagedBatch
	nextID  uint64
	lastSeq uint64
	nextSeg uint64
}

type segment struct {
	num      uint64
	name     string
	firstSeq uint64
	lastSeq  uint64
	bytes    int64
}

type location struct {
	seg int
	off int64
}

type frameLoc struct {
	seq uint64
	off int64
}

type stagedBatch struct {
	records [][]byte
	seq     uint64
}

// Open opens the log directory, creating it if necessary. Existing
// segment files are recovered: the valid committed prefix is loaded and
// an incomplete trailing frame is truncated. Corruption before the tail
// makes Open return ErrCorruptSegment without modifying the directory.
func Open(dir string, opts Options) (*Log, error) {
	if dir == "" {
		return nil, errors.New("log: empty directory path")
	}
	if err := os.MkdirAll(dir, dirPerm); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	type segFile struct {
		num  uint64
		name string
	}
	var files []segFile
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasSuffix(name, segSuffix) {
			continue
		}
		stem := strings.TrimSuffix(name, segSuffix)
		num, err := strconv.ParseUint(stem, segNameBase, segNameBits)
		if err != nil || num == 0 {
			continue
		}
		files = append(files, segFile{num: num, name: name})
	}
	sort.Slice(files, func(i, j int) bool { return files[i].num < files[j].num })

	l := &Log{
		dir:     dir,
		opts:    opts,
		index:   make(map[uint64]location),
		staged:  make(map[uint64]*stagedBatch),
		nextID:  1,
		nextSeg: 1,
	}

	expected := uint64(1)
	var tailSeg *segment
	var tailValid int64
	for i, sf := range files {
		path := filepath.Join(dir, sf.name)
		f, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		first, last, valid, locs, incomplete, rerr := recoverSegment(f, expected)
		f.Close()
		if rerr != nil {
			return nil, ErrCorruptSegment
		}
		if incomplete && i != len(files)-1 {
			// Only the final segment may hold an unfinished tail.
			return nil, ErrCorruptSegment
		}
		seg := &segment{
			num:      sf.num,
			name:     sf.name,
			firstSeq: first,
			lastSeq:  last,
			bytes:    valid,
		}
		l.segs = append(l.segs, seg)
		for _, fl := range locs {
			l.index[fl.seq] = location{seg: i, off: fl.off}
			expected = fl.seq + 1
		}
		l.lastSeq = expected - 1
		if sf.num >= l.nextSeg {
			l.nextSeg = sf.num + 1
		}
		if incomplete {
			tailSeg = seg
			tailValid = valid
		}
	}

	if tailSeg != nil {
		if err := truncateSegment(filepath.Join(dir, tailSeg.name), tailValid); err != nil {
			return nil, err
		}
	}

	if len(l.segs) > 0 {
		l.active = l.segs[len(l.segs)-1]
		l.f, err = os.OpenFile(
			filepath.Join(dir, l.active.name),
			os.O_RDWR|os.O_APPEND, filePerm)
		if err != nil {
			return nil, err
		}
	}

	return l, nil
}

// Append stages a batch of opaque records and returns a handle used to
// commit it. The records are copied and do not change if the caller
// mutates its slices afterwards.
func (l *Log) Append(records [][]byte) (Batch, error) {
	if l == nil {
		return Batch{}, errors.New("log: Append on nil Log")
	}
	cp := make([][]byte, len(records))
	for i, r := range records {
		cp[i] = append([]byte(nil), r...)
	}
	id := l.nextID
	l.nextID++
	l.staged[id] = &stagedBatch{records: cp}
	return Batch{ID: id, Records: cp}, nil
}

// Commit makes a staged batch durable and returns its sequence number,
// starting at 1 and contiguous in commit order. Committing the same
// Batch again returns the original sequence without writing again. A
// Batch not produced by this Log's Append returns ErrUnknownBatch.
func (l *Log) Commit(b Batch) (uint64, error) {
	if l == nil {
		return 0, errors.New("log: Commit on nil Log")
	}
	st := l.staged[b.ID]
	if st == nil {
		return 0, ErrUnknownBatch
	}
	if st.seq != 0 {
		return st.seq, nil
	}
	seq := l.lastSeq + 1
	frame, err := encodeFrame(seq, st.records)
	if err != nil {
		return 0, err
	}
	seg, off, err := l.writeFrame(frame, seq)
	if err != nil {
		return 0, err
	}
	st.seq = seq
	l.lastSeq = seq
	l.index[seq] = location{seg: seg, off: off}
	return seq, nil
}

// Read returns the records of one committed batch. An uncommitted or
// unknown sequence returns ErrNotCommitted.
func (l *Log) Read(seq uint64) ([][]byte, error) {
	if l == nil {
		return nil, errors.New("log: Read on nil Log")
	}
	loc, ok := l.index[seq]
	if !ok {
		return nil, ErrNotCommitted
	}
	f, err := os.Open(filepath.Join(l.dir, l.segs[loc.seg].name))
	if err != nil {
		return nil, err
	}
	defer f.Close()
	payload, err := readFrameAt(f, loc.off)
	if err != nil {
		return nil, err
	}
	_, records, ok := parsePayload(payload)
	if !ok {
		return nil, ErrCorruptSegment
	}
	return records, nil
}

// Scan replays committed batches in sequence order, starting with the
// first batch whose sequence is >= from (from == 0 starts at 1), and
// invokes fn for each one. If fn returns an error, scanning stops and
// that error is returned.
func (l *Log) Scan(from uint64, fn func(Batch) error) error {
	if l == nil {
		return errors.New("log: Scan on nil Log")
	}
	if fn == nil {
		return errors.New("log: nil Scan callback")
	}
	start := from
	if start == 0 {
		start = 1
	}
	if start > l.lastSeq {
		return nil
	}
	for seq := start; seq <= l.lastSeq; seq++ {
		records, err := l.Read(seq)
		if err != nil {
			return err
		}
		if err := fn(Batch{Records: records, Sequence: seq}); err != nil {
			return err
		}
	}
	return nil
}

// Segments reports the committed range and on-disk size of each segment
// in file order. Empty trailing segments left by recovery are omitted.
func (l *Log) Segments() []Segment {
	if l == nil {
		return nil
	}
	out := make([]Segment, 0, len(l.segs))
	for _, s := range l.segs {
		if s.bytes == 0 {
			continue
		}
		out = append(out, Segment{
			File:     filepath.Join(l.dir, s.name),
			FirstSeq: s.firstSeq,
			LastSeq:  s.lastSeq,
			Bytes:    s.bytes,
		})
	}
	return out
}

// writeFrame appends one encoded frame, starting a new segment when
// needed, and returns the segment index and frame offset after a
// successful write.
func (l *Log) writeFrame(frame []byte, seq uint64) (int, int64, error) {
	if l.f == nil {
		if err := l.startSegment(); err != nil {
			return 0, 0, err
		}
	} else if l.opts.SegmentBytes > 0 && l.active.bytes > 0 &&
		l.active.bytes+int64(len(frame)) > int64(l.opts.SegmentBytes) {
		if err := l.startSegment(); err != nil {
			return 0, 0, err
		}
	}

	off := l.active.bytes
	n, err := l.f.Write(frame)
	if err != nil {
		return 0, 0, err
	}
	if n != len(frame) {
		return 0, 0, io.ErrShortWrite
	}
	if l.opts.Sync {
		if err := l.f.Sync(); err != nil {
			return 0, 0, err
		}
	}

	segIdx := len(l.segs) - 1
	if l.active.firstSeq == 0 {
		l.active.firstSeq = seq
	}
	l.active.lastSeq = seq
	l.active.bytes += int64(len(frame))
	return segIdx, off, nil
}

// startSegment closes the active segment (if any) and creates the next
// numbered segment as the active one.
func (l *Log) startSegment() error {
	if l.f != nil {
		if l.opts.Sync {
			if err := l.f.Sync(); err != nil {
				return err
			}
		}
		if err := l.f.Close(); err != nil {
			return err
		}
		l.f = nil
	}
	name := fmt.Sprintf(segNameFmt, l.nextSeg)
	f, err := os.OpenFile(
		filepath.Join(l.dir, name),
		os.O_RDWR|os.O_CREATE|os.O_EXCL|os.O_APPEND, filePerm)
	if err != nil {
		return err
	}
	seg := &segment{num: l.nextSeg, name: name}
	l.segs = append(l.segs, seg)
	l.active = seg
	l.f = f
	l.nextSeg++
	if l.opts.Sync {
		// Make the new directory entry durable alongside the file.
		if err := syncDir(l.dir); err != nil {
			return err
		}
	}
	return nil
}

func truncateSegment(path string, valid int64) error {
	f, err := os.OpenFile(path, os.O_WRONLY, filePerm)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Truncate(valid)
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// recoverSegment validates one segment against the expected first
// sequence number. It returns the valid-prefix range, frame locations,
// and whether an unfinished trailing frame was found.
//
// A fully contained frame that fails any check is corruption. A frame
// whose declared length runs past end of file is either a torn trailing
// write or a damaged length field: if further complete, valid frames
// follow it, the damage is in the middle and is corruption; otherwise it
// is the tail and gets truncated.
func recoverSegment(f *os.File, expected uint64) (
	firstSeq, lastSeq uint64, valid int64, locs []frameLoc, incomplete bool, err error,
) {
	st, err := f.Stat()
	if err != nil {
		return 0, 0, 0, nil, false, err
	}
	data := make([]byte, st.Size())
	if _, err := io.ReadFull(f, data); err != nil && st.Size() > 0 {
		return 0, 0, 0, nil, false, err
	}

	var off int64
	for off < int64(len(data)) {
		buf := data[off:]
		if len(buf) < prefixSize {
			// Torn header: the writer stopped mid frame at the tail.
			return firstSeq, lastSeq, off, locs, true, nil
		}
		if !bytes.Equal(buf[:magicSize], frameMagic[:]) {
			// Could be damage, or a crash that left fewer than
			// magicSize bytes of the trailing write. Distinguish the
			// same way as an overrun below.
			if chainAfterDamage(buf, expected+1) {
				return 0, 0, 0, nil, false, errCorrupt
			}
			return firstSeq, lastSeq, off, locs, true, nil
		}
		plen := uint64(binaryBigEndianUint32(buf[magicSize:prefixSize]))
		frameLen := prefixSize + int64(plen) + crcSize
		if plen < minPayloadSize || frameLen > int64(len(buf)) {
			if chainAfterDamage(buf, expected+1) {
				return 0, 0, 0, nil, false, errCorrupt
			}
			return firstSeq, lastSeq, off, locs, true, nil
		}

		payload := buf[prefixSize : prefixSize+int64(plen)]
		if crc32.ChecksumIEEE(payload) !=
			binaryBigEndianUint32(buf[prefixSize+int64(plen):frameLen]) {
			return 0, 0, 0, nil, false, errCorrupt
		}
		seq, _, ok := parsePayload(payload)
		if !ok || seq != expected {
			return 0, 0, 0, nil, false, errCorrupt
		}

		if firstSeq == 0 {
			firstSeq = seq
		}
		lastSeq = seq
		locs = append(locs, frameLoc{seq: seq, off: off})
		off += frameLen
		expected++
	}
	return firstSeq, lastSeq, off, locs, false, nil
}

// chainAfterDamage reports whether some offset strictly inside buf starts
// a run of complete, valid frames (beginning at wantSeq) that consumes
// the rest of buf. It searches frame-magic occurrences so that opaque
// record bytes cannot fool the scan.
func chainAfterDamage(buf []byte, wantSeq uint64) bool {
	rest := buf[1:]
	for {
		idx := bytes.Index(rest, frameMagic[:])
		if idx < 0 {
			return false
		}
		cand := rest[idx:]
		if validChain(cand, wantSeq) {
			return true
		}
		rest = rest[idx+1:]
	}
}

// validChain reports whether buf is exactly one or more complete valid
// frames with contiguous sequence numbers starting at wantSeq.
func validChain(buf []byte, wantSeq uint64) bool {
	off := 0
	frames := 0
	for off < len(buf) {
		end, ok := frameEnd(buf[off:], wantSeq)
		if !ok {
			return false
		}
		off += end
		wantSeq++
		frames++
	}
	return frames > 0
}

// frameEnd validates one frame at the start of buf and returns its total
// length. The frame must be fully contained.
func frameEnd(buf []byte, wantSeq uint64) (int, bool) {
	if len(buf) < prefixSize+crcSize+minPayloadSize {
		return 0, false
	}
	if !bytes.Equal(buf[:magicSize], frameMagic[:]) {
		return 0, false
	}
	plen := uint64(binaryBigEndianUint32(buf[magicSize:prefixSize]))
	if plen < minPayloadSize {
		return 0, false
	}
	end := prefixSize + int64(plen) + crcSize
	if end > int64(len(buf)) {
		return 0, false
	}
	payload := buf[prefixSize : prefixSize+int64(plen)]
	if crc32.ChecksumIEEE(payload) !=
		binaryBigEndianUint32(buf[prefixSize+int64(plen):end]) {
		return 0, false
	}
	seq, _, ok := parsePayload(payload)
	if !ok || seq != wantSeq {
		return 0, false
	}
	return int(end), true
}

// readFrameAt reads and validates a single frame starting at off.
func readFrameAt(f *os.File, off int64) ([]byte, error) {
	var prefix [prefixSize]byte
	if _, err := io.ReadFull(io.NewSectionReader(f, off, prefixSize), prefix[:]); err != nil {
		return nil, err
	}
	if !bytes.Equal(prefix[:magicSize], frameMagic[:]) {
		return nil, ErrCorruptSegment
	}
	plen := uint64(binaryBigEndianUint32(prefix[magicSize:]))
	if plen < minPayloadSize {
		return nil, ErrCorruptSegment
	}
	buf := make([]byte, plen+crcSize)
	if _, err := io.ReadFull(io.NewSectionReader(f, off+prefixSize, int64(len(buf))), buf); err != nil {
		return nil, err
	}
	payload := buf[:plen]
	if crc32.ChecksumIEEE(payload) != binaryBigEndianUint32(buf[plen:]) {
		return nil, ErrCorruptSegment
	}
	return payload, nil
}

// encodeFrame builds a frame:
//
//	magic | uint32 payload length | payload | uint32 CRC-32/IEEE(payload)
func encodeFrame(seq uint64, records [][]byte) ([]byte, error) {
	payloadLen := uint64(minPayloadSize)
	for _, r := range records {
		if uint64(len(r)) > uint64(^uint32(0)) {
			return nil, errors.New("log: record too large")
		}
		payloadLen += 4 + uint64(len(r))
	}
	if payloadLen > uint64(^uint32(0)) || uint64(len(records)) > uint64(^uint32(0)) {
		return nil, errors.New("log: batch too large")
	}

	frame := make([]byte, 0, prefixSize+payloadLen+crcSize)
	frame = append(frame, frameMagic[:]...)
	var hdr [headerSize]byte
	putBigEndianUint32(hdr[:], uint32(payloadLen))
	frame = append(frame, hdr[:]...)

	payload := make([]byte, payloadLen)
	putBigEndianUint64(payload[0:8], seq)
	putBigEndianUint32(payload[8:12], uint32(len(records)))
	pos := 12
	for _, r := range records {
		putBigEndianUint32(payload[pos:pos+4], uint32(len(r)))
		pos += 4
		pos += copy(payload[pos:], r)
	}
	frame = append(frame, payload...)

	var crcb [crcSize]byte
	putBigEndianUint32(crcb[:], crc32.ChecksumIEEE(payload))
	frame = append(frame, crcb[:]...)
	return frame, nil
}

// parsePayload parses a frame payload, returning copied records. The
// boolean is false if the payload is structurally invalid.
func parsePayload(p []byte) (uint64, [][]byte, bool) {
	if len(p) < minPayloadSize {
		return 0, nil, false
	}
	seq := binaryBigEndianUint64(p[0:8])
	count := binaryBigEndianUint32(p[8:12])
	pos := minPayloadSize
	records := make([][]byte, 0, count)
	for i := uint32(0); i < count; i++ {
		if pos+4 > len(p) {
			return 0, nil, false
		}
		n := uint64(binaryBigEndianUint32(p[pos : pos+4]))
		pos += 4
		if uint64(pos)+n > uint64(len(p)) {
			return 0, nil, false
		}
		records = append(records, append([]byte(nil), p[pos:pos+int(n)]...))
		pos += int(n)
	}
	if pos != len(p) {
		return 0, nil, false
	}
	return seq, records, true
}
