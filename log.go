// Package log implements an append-only, batch-commit segmented log.
//
// A writer stages a batch of opaque records with Append and publishes it
// atomically with Commit, or publishes several staged batches atomically
// with CommitGroup. Readers only ever see committed batches, in strictly
// increasing sequence order starting at 1. Reads locate each batch
// through a segment-level index kept in a persistent side file, which
// Open adopts incrementally when it is intact and consistent with the
// segment files, and rebuilds from the segment files otherwise, so
// losing the index never loses or invents data.
package log

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

var (
	// ErrNotCommitted is returned when reading a sequence that has been
	// staged with Append but not yet committed.
	ErrNotCommitted = errors.New("log: batch not committed")
	// ErrUnknownBatch is returned for sequence 0, sequences that were
	// never reserved, sequences beyond the reserved range, and batches
	// that have already been committed.
	ErrUnknownBatch = errors.New("log: unknown batch")
	// ErrCorruptSegment is returned when a segment file fails validation
	// while opening or scanning the log.
	ErrCorruptSegment = errors.New("log: corrupt segment")
	// ErrInvalidOptions is returned by Open when the options are not
	// usable, e.g. a non-positive segment capacity.
	ErrInvalidOptions = errors.New("log: invalid options")
	// ErrSyncFailed is returned by Commit and CommitGroup when
	// Options.Sync is set and fsyncing fails. The batches stay staged
	// and keep their reserved sequences; committing them again retries.
	ErrSyncFailed = errors.New("log: sync failed")
)

var errClosed = errors.New("log: closed")

// syncFile fsyncs a file. It is a variable so tests can simulate sync
// failures; it covers both segment and index side files.
var syncFile = func(f *os.File) error { return f.Sync() }

// Options configures a Log opened with Open.
type Options struct {
	// SegmentBytes is the capacity of one segment file in bytes. Once a
	// non-empty segment would exceed it, the log rolls to a new segment.
	// A single batch or group larger than the capacity occupies its own
	// segment. Must be positive.
	SegmentBytes int
	// Sync makes Commit and CommitGroup fsync the segment and then the
	// index side file before returning, so a returned commit is durable.
	Sync bool
}

// Batch is a group of records identified by a single sequence number.
// A Batch returned by Append is staged; after Commit or CommitGroup it
// is readable.
type Batch struct {
	seq     uint64
	records [][]byte
	owner   *Log
}

// Seq returns the sequence number reserved for the batch.
func (b Batch) Seq() uint64 { return b.seq }

// Records returns a copy of the batch's records.
func (b Batch) Records() [][]byte { return copyRecords(b.records) }

// Segment describes one segment file by the committed sequences it holds.
type Segment struct {
	FirstSeq uint64
	LastSeq  uint64
}

// entryRef locates one committed batch body inside a segment file. gen
// is the publication generation of the commit that published the batch;
// every member of a group commit shares one gen, which lets a streaming
// replay snapshot see the whole group or none of it.
type entryRef struct {
	seg    int // segment file number
	off    int // offset of the batch body within the segment
	length int // length of the batch body
	gen    uint64
}

// Log is an append-only batch-commit log stored in a directory of
// segment files plus one index side file. It is safe for concurrent
// use, but the writer side is expected to be a single goroutine (see
// README Limits).
//
// The read and write paths are decoupled: writers take mu exclusively,
// while readers hold it only for the brief index and staged-state
// lookups. Segment file reads, decoding and user callbacks happen
// outside the lock, so a slow reader or a long replay never blocks a
// committing writer.
type Log struct {
	dir  string
	opts Options

	// mu guards the mutable index and write state below. Readers never
	// hold it across file I/O, decoding or callbacks.
	mu         sync.RWMutex
	file       *os.File // current segment, nil until first write
	idxFile    *os.File // index side file, append-only
	fileSize   int
	segIndex   int    // index of the current segment file, 0 = none yet
	curBatches int    // committed batches in the current segment
	curSegCRC  uint32 // CRC32 of the current segment's durable prefix
	nextSeq    uint64
	staged     map[uint64][][]byte
	index      map[uint64]entryRef
	segs       []Segment // segments holding at least one committed batch
	gen        uint64    // publication generation of the last commit
	closed     bool
}

// Open opens the log in dir, creating the directory if needed. The
// segment-level index is adopted from the persistent side file
// whenever that file is intact and consistent with the segments,
// without decoding their entries; otherwise it is rebuilt from the
// segment files. An uncommitted suffix of the last segment — the
// remnant of a crash in the middle of segment writing, segment
// syncing, index writing or index syncing — is replaced by a durable
// hole marker without error and its reserved sequences stay
// permanent holes across reopens; any other inconsistency is
// ErrCorruptSegment.
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
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".seg") {
			continue
		}
		n, err := strconv.Atoi(strings.TrimSuffix(e.Name(), ".seg"))
		if err != nil {
			continue
		}
		indices = append(indices, n)
	}
	sort.Ints(indices)

	l := &Log{
		dir:     dir,
		opts:    opts,
		nextSeq: 1,
		staged:  make(map[uint64][][]byte),
		index:   make(map[uint64]entryRef),
	}

	var frames []indexFrame
	if len(indices) > 0 {
		fs, recovered, ok := l.adoptIndex(indices)
		if !ok {
			fs, err = l.rebuildIndex(indices)
			if err != nil {
				return nil, err
			}
		} else if recovered {
			// Incremental adoption had to repair a crash suffix; make
			// the side file match the repaired segments before the log
			// becomes usable, so a crash at any later instant leaves
			// the two durable files in agreement.
			if err := l.persistIndexImage(fs); err != nil {
				return nil, err
			}
		}
		frames = fs
	}

	// Frames from either path describe exactly the committed set;
	// applying them builds the in-memory index, segment listing and
	// reserved-sequence bookkeeping identically.
	if len(indices) > 0 {
		l.segIndex = indices[len(indices)-1]
		l.applyFrames(frames)

		if err := l.openCurrent(); err != nil {
			return nil, err
		}
	}
	if err := l.openIndexFile(); err != nil {
		return nil, err
	}
	return l, nil
}

// segAdoptState is the running per-segment state while the side file
// is streamed during incremental adoption.
type segAdoptState struct {
	endOff uint64
	crc    uint32
}

// adoptIndex incrementally adopts the index side file: it validates
// the side file bytes, checks every frame against the segment files
// with one streaming CRC pass per segment plus a few bounded header
// reads (record payloads are never decoded), and repairs the last
// segment's uncommitted crash suffix. The boolean returned is false
// on any missing, truncated, wrong-version, malformed, stale or
// contradictory side file, which sends Open down the full rebuild
// path. The second boolean reports whether crash recovery mutated a
// segment, in which case the caller persists a fresh side file.
func (l *Log) adoptIndex(indices []int) (frames []indexFrame, recovered bool, ok bool) {
	raw, err := os.ReadFile(l.indexPath())
	if err != nil || len(raw) == 0 {
		return nil, false, false
	}
	parsed, ok := parseIndex(raw)
	if !ok {
		return nil, false, false
	}
	state, ok := l.verifyWatermark(indices, parsed)
	if !ok {
		return nil, false, false
	}
	frames = parsed

	// Every segment except the last must end exactly where the frames
	// stop covering it. The last segment may additionally carry one
	// uncommitted crash suffix, which the same recovery routine the
	// full rebuild uses truncates and turns into permanent holes.
	last := indices[len(indices)-1]
	for _, n := range indices {
		st, err := os.Stat(l.segmentPath(n))
		if err != nil {
			return nil, false, false
		}
		size := uint64(st.Size())
		var covered uint64
		var baseCRC uint32
		if s := state[n]; s != nil {
			covered = s.endOff
			baseCRC = s.crc
		}
		if size < covered {
			return nil, false, false // side file ahead of the segment
		}
		if size == covered {
			continue
		}
		if n != last {
			return nil, false, false
		}
		hf, err := l.recoverUncommittedSuffix(n, covered, baseCRC)
		if err != nil {
			return nil, false, false
		}
		if hf != nil {
			frames = append(frames, *hf)
		}
		recovered = true
	}
	return frames, recovered, true
}

// verifyWatermark streams the given frames and proves each one against
// the segment files: one streaming CRC pass over the newly covered
// bytes per segment and bounded reads of the entry headers (record
// payloads are never decoded), plus duplicate/hole consistency. It
// returns the running per-segment coverage. ok is false if any frame
// is stale or contradicts the segments.
func (l *Log) verifyWatermark(indices []int, parsed []indexFrame) (map[int]*segAdoptState, bool) {
	known := make(map[int]bool, len(indices))
	for _, n := range indices {
		known[n] = true
	}
	state := make(map[int]*segAdoptState)
	segOf := func(n int) *segAdoptState {
		s := state[n]
		if s == nil {
			s = &segAdoptState{}
			state[n] = s
		}
		return s
	}

	published := make(map[uint64]bool) // sequences a data frame claimed
	var holed []uint64                 // sequences a holes frame claimed
	openSegs := make(map[uint64]*os.File)
	defer func() {
		for _, f := range openSegs {
			f.Close()
		}
	}()

	activeSeg := 0
	for i := range parsed {
		f := &parsed[i]
		n := int(f.seg)
		if !known[n] || n < activeSeg {
			return nil, false
		}
		s := segOf(n)
		if f.endOff <= s.endOff {
			return nil, false
		}
		// Streaming CRC over exactly the byte range the frame newly
		// covers, extending this segment's running digest. No entry is
		// decoded.
		fh := openSegs[f.seg]
		if fh == nil {
			opened, oerr := os.Open(l.segmentPath(n))
			if oerr != nil {
				return nil, false
			}
			fh = opened
			openSegs[f.seg] = fh
		}
		runCRC, err := crcRange(fh, s.endOff, f.endOff, s.crc)
		if err != nil || runCRC != f.segCRC {
			return nil, false
		}
		// Cross-check the frame against the segment's entry header and
		// body layout with bounded reads: magic, member tiling and each
		// body's embedded sequence. Record payloads stay untouched.
		if err := l.verifyFrameLayout(fh, s.endOff, f); err != nil {
			return nil, false
		}
		switch f.kind {
		case kindData:
			for _, m := range f.members {
				if published[m.seq] || containsU64(holed, m.seq) {
					return nil, false
				}
				published[m.seq] = true
			}
		case kindHoles:
			for _, seq := range f.seqs {
				if published[seq] || containsU64(holed, seq) {
					return nil, false
				}
				holed = append(holed, seq)
			}
		}
		s.endOff = f.endOff
		s.crc = f.segCRC
		activeSeg = n
	}
	return state, true
}

// verifyFrameLayout cross-checks one adopted frame against the actual
// bytes of the segment range it covers: the entry magic, the tiling of
// member bodies and every embedded sequence. Reads are bounded to
// headers and sequence fields; records are never decoded.
func (l *Log) verifyFrameLayout(fh *os.File, prevEnd uint64, f *indexFrame) error {
	var head [12]byte
	if _, err := fh.ReadAt(head[:4], int64(prevEnd)); err != nil {
		return err
	}
	var buf [8]byte
	switch string(head[:4]) {
	case string(segmentMagic):
		if f.kind != kindData || len(f.members) != 1 {
			return ErrCorruptSegment
		}
		m := f.members[0]
		if m.off != prevEnd+4 || m.length != f.endOff-prevEnd-8 {
			return ErrCorruptSegment
		}
		if _, err := fh.ReadAt(buf[:], int64(m.off)); err != nil {
			return err
		}
		if binary.LittleEndian.Uint64(buf[:]) != m.seq {
			return ErrCorruptSegment
		}
	case string(groupMagic):
		if f.kind != kindData {
			return ErrCorruptSegment
		}
		if _, err := fh.ReadAt(head[:8], int64(prevEnd)); err != nil {
			return err
		}
		if binary.LittleEndian.Uint32(head[4:8]) != uint32(len(f.members)) {
			return ErrCorruptSegment
		}
		next := prevEnd + 8
		for i := range f.members {
			m := f.members[i]
			if m.off != next {
				return ErrCorruptSegment
			}
			if _, err := fh.ReadAt(buf[:], int64(m.off)); err != nil {
				return err
			}
			if binary.LittleEndian.Uint64(buf[:]) != m.seq {
				return ErrCorruptSegment
			}
			next = m.off + m.length
		}
		if next != f.endOff-4 {
			return ErrCorruptSegment
		}
	case string(holeMagic):
		if f.kind != kindHoles {
			return ErrCorruptSegment
		}
		if _, err := fh.ReadAt(head[:8], int64(prevEnd)); err != nil {
			return err
		}
		count := binary.LittleEndian.Uint32(head[4:8])
		if uint32(len(f.seqs)) != count || f.endOff-prevEnd != uint64(12+8*int(count)) {
			return ErrCorruptSegment
		}
		raw := make([]byte, 8*count)
		if _, err := fh.ReadAt(raw, int64(prevEnd+8)); err != nil {
			return err
		}
		for i, seq := range f.seqs {
			if binary.LittleEndian.Uint64(raw[i*8:]) != seq {
				return ErrCorruptSegment
			}
		}
	default:
		return ErrCorruptSegment
	}
	return nil
}

// rebuildIndex rebuilds the index from the segment files after
// incremental adoption was rejected. Three situations:
//
//   - No usable side file (missing, empty, bad header, wrong version):
//     a purely structural rebuild, where a complete checksum-valid
//     entry defines a commit and only the last segment may end in a
//     torn partial entry.
//   - A usable header whose salvaged frame prefix verifies against the
//     segments: those frames are the commit watermark, so every byte
//     past the covered prefix of the last segment is an uncommitted
//     crash remnant — structurally complete or not — and its reserved
//     sequences become permanent holes.
//   - A usable header whose frames contradict the segments: the
//     structural rebuild decides, returning ErrCorruptSegment where
//     the bytes do not parse.
func (l *Log) rebuildIndex(indices []int) ([]indexFrame, error) {
	var watermark []indexFrame
	usable := false
	if raw, err := os.ReadFile(l.indexPath()); err == nil && len(raw) > 0 {
		frames, consumed, headerOK, truncated := salvageIndex(raw)
		// The salvaged prefix is a usable watermark when the header is
		// valid and the file is either fully valid frames (it may stop
		// at a clean boundary — even at the header, which is exactly how
		// a crash between the segment sync and the first frame byte
		// looks) or was cut off mid-frame by a crash. A frame that is
		// wholly present but fails its checksum is tampering, not a
		// crash: no watermark is trusted and the structural rebuild
		// decides.
		if headerOK && (consumed == len(raw) || truncated) {
			watermark = frames
			usable = true
		}
	}
	if usable {
		if st, ok := l.verifyWatermark(indices, watermark); ok {
			if fs, ok, err := l.buildFromWatermark(indices, watermark, st); err != nil {
				return nil, err
			} else if ok {
				return fs, nil
			}
		}
		// Frames contradict the segments: fall through to the purely
		// structural rebuild, which surfaces genuine tampering.
	}
	return l.rebuildFromScratch(indices)
}

// rebuildFromScratch is the full structural rebuild used when no
// trustworthy side file exists: a complete checksum-valid entry
// defines a commit, and only the last segment may end in one torn
// partial entry. The regenerated frames are persisted as a fresh side
// file image.
func (l *Log) rebuildFromScratch(indices []int) ([]indexFrame, error) {
	var frames []indexFrame
	for i, n := range indices {
		data, err := os.ReadFile(l.segmentPath(n))
		if err != nil {
			return nil, err
		}
		fs, tail, err := walkEntries(data, uint64(n))
		if err != nil {
			return nil, err
		}
		if tail != nil && i != len(indices)-1 {
			// A half-written tail is a crash remnant only in the
			// last segment; anywhere else it is tampering.
			return nil, ErrCorruptSegment
		}
		frames = append(frames, fs...)
		if tail != nil {
			if err := l.recoverTail(n, tail); err != nil {
				return nil, err
			}
			frames = appendHolesFrame(frames, n, tail)
		}
	}
	if err := l.persistIndexImage(frames); err != nil {
		return nil, err
	}
	return frames, nil
}

// buildFromWatermark rebuilds using a frame prefix already proven
// against the segments. Entries inside each covered prefix are
// re-parsed and must reproduce the watermark frames; bytes past the
// covered prefix are tolerated only on the last segment and are
// recovered as an uncommitted suffix. ok is false when the segment
// bytes do not reproduce the watermark (caller then rebuilds from
// scratch).
func (l *Log) buildFromWatermark(indices []int, watermark []indexFrame, st map[int]*segAdoptState) ([]indexFrame, bool, error) {
	var frames []indexFrame
	last := indices[len(indices)-1]
	for _, n := range indices {
		data, err := os.ReadFile(l.segmentPath(n))
		if err != nil {
			return nil, false, err
		}
		s, hasCover := st[n]
		if !hasCover {
			// No watermark frame ends in this segment. Such an earlier
			// segment contradicts the watermark; the last segment can
			// only be a segment the lost commit rolled into, so its
			// whole content is an uncommitted suffix.
			if n != last {
				return nil, false, nil
			}
			hf, err := l.recoverUncommittedSuffix(n, 0, 0)
			if err != nil {
				return nil, false, err
			}
			if hf != nil {
				frames = append(frames, *hf)
			}
			continue
		}
		fs, tail, err := walkEntries(data[:s.endOff], uint64(n))
		if err != nil {
			return nil, false, err
		}
		if tail != nil {
			return nil, false, nil // covered prefix cannot be torn
		}
		frames = append(frames, fs...)
		if uint64(len(data)) == s.endOff {
			continue
		}
		if n != last {
			return nil, false, nil
		}
		hf, err := l.recoverUncommittedSuffix(n, s.endOff, s.crc)
		if err != nil {
			return nil, false, err
		}
		if hf != nil {
			frames = append(frames, *hf)
		}
	}
	// The segment-derived frames must reproduce the watermark exactly;
	// only a trailing holes frame from this recovery is extra.
	if !framePrefixEqual(watermark, frames) {
		return nil, false, nil
	}
	frames = append(append([]indexFrame{}, watermark...), frames[len(watermark):]...)
	if err := l.persistIndexImage(frames); err != nil {
		return nil, false, err
	}
	return frames, true, nil
}

// framePrefixEqual reports whether the first len(watermark) rebuilt
// frames are field-for-field identical to the salvaged watermark —
// same kinds, segment, offsets, lengths, sequences and running CRCs.
// Any trailing rebuilt frame is the holes frame produced by recovery
// and is intentionally not compared here.
func framePrefixEqual(watermark, rebuilt []indexFrame) bool {
	if len(watermark) > len(rebuilt) {
		return false
	}
	for i := range watermark {
		a, b := watermark[i], rebuilt[i]
		if a.kind != b.kind || a.seg != b.seg || a.endOff != b.endOff || a.segCRC != b.segCRC {
			return false
		}
		if len(a.members) != len(b.members) || len(a.seqs) != len(b.seqs) {
			return false
		}
		for j := range a.members {
			if a.members[j] != b.members[j] {
				return false
			}
		}
		for j := range a.seqs {
			if a.seqs[j] != b.seqs[j] {
				return false
			}
		}
	}
	return true
}

// recoverUncommittedSuffix handles bytes of the last segment past the
// commit watermark covered, whose CRC over [0, covered) is baseCRC.
// Those bytes never formed a visible commit, however complete they
// look. Accepted shapes:
//
//   - one or more complete data entries, optionally followed by one
//     torn partial entry: all are truncated and every reserved
//     sequence becomes one permanent hole marker;
//   - exactly one complete hole marker left by a previous recovery
//     whose index image never synced: it is kept and adopted;
//   - nothing: a no-op.
//
// Anything else is ErrCorruptSegment. It returns the holes frame to
// append to the in-memory index when recovery reserved sequences.
func (l *Log) recoverUncommittedSuffix(n int, covered uint64, baseCRC uint32) (*indexFrame, error) {
	data, err := os.ReadFile(l.segmentPath(n))
	if err != nil {
		return nil, err
	}
	if uint64(len(data)) < covered {
		return nil, ErrCorruptSegment
	}
	suf := data[covered:]
	fs, tail, err := walkEntries(suf, 0)
	if err != nil {
		return nil, ErrCorruptSegment
	}
	var dataFrames, holeFrames []indexFrame
	for i := range fs {
		switch fs[i].kind {
		case kindData:
			dataFrames = append(dataFrames, fs[i])
		case kindHoles:
			holeFrames = append(holeFrames, fs[i])
		}
	}

	// A retained marker from an earlier recovery: nothing torn, exactly
	// one hole frame and no uncommitted data entries around it.
	if tail == nil && len(dataFrames) == 0 {
		if len(holeFrames) == 0 {
			return nil, nil
		}
		if len(holeFrames) != 1 || len(fs) != 1 {
			return nil, ErrCorruptSegment
		}
		hf := holeFrames[0]
		// Re-base its relative offsets/running CRC onto the whole segment.
		end := covered + hf.endOff
		crc := crc32.Update(baseCRC, crc32.IEEETable, suf[:hf.endOff])
		return &indexFrame{
			kind: kindHoles, seg: uint64(n), endOff: end, segCRC: crc, seqs: hf.seqs,
		}, nil
	}

	// Every other accepted suffix is discarded wholesale and replaced
	// by one hole marker. A hole marker mixed with uncommitted data or
	// a torn entry is not a state this log produces: treat as corrupt.
	if len(holeFrames) > 0 {
		return nil, ErrCorruptSegment
	}
	var seqs []uint64
	for i := range dataFrames {
		for _, m := range dataFrames[i].members {
			seqs = append(seqs, m.seq)
		}
	}
	if tail != nil {
		seqs = append(seqs, tail.seqs...)
	}
	var keep []uint64
	for _, seq := range seqs {
		if seq > 0 {
			keep = append(keep, seq)
		}
	}
	t := &segmentTail{off: int(covered), seqs: keep, prefixCRC: baseCRC}
	if err := l.recoverTail(n, t); err != nil {
		return nil, err
	}
	if len(keep) == 0 {
		return nil, nil
	}
	marker := encodeHoles(keep)
	return &indexFrame{
		kind:   kindHoles,
		seg:    uint64(n),
		endOff: covered + uint64(len(marker)),
		segCRC: crc32.Update(baseCRC, crc32.IEEETable, marker),
		seqs:   keep,
	}, nil
}

// appendHolesFrame appends the holes frame produced by recovering the
// tail of segment n. Recovery only ever touches the last segment, so
// the frame comes after every rebuilt frame. The frame records the
// exact end offset and running CRC of the synced hole marker.
func appendHolesFrame(frames []indexFrame, n int, tail *segmentTail) []indexFrame {
	var keep []uint64
	for _, seq := range tail.seqs {
		if seq > 0 {
			keep = append(keep, seq)
		}
	}
	if len(keep) == 0 {
		return frames
	}
	marker := encodeHoles(keep)
	endOff := uint64(tail.off) + uint64(len(marker))
	crc := crc32.Update(tail.prefixCRC, crc32.IEEETable, marker)
	return append(frames, indexFrame{
		kind:   kindHoles,
		seg:    uint64(n),
		endOff: endOff,
		segCRC: crc,
		seqs:   keep,
	})
}

// recoverTail truncates segment n at tail.off and, when the remnant
// carried recoverable sequences, appends and syncs a hole marker so
// the reserved sequences stay permanent holes across reopens.
func (l *Log) recoverTail(n int, tail *segmentTail) error {
	path := l.segmentPath(n)
	if err := os.Truncate(path, int64(tail.off)); err != nil {
		return err
	}
	var keep []uint64
	for _, seq := range tail.seqs {
		if seq > 0 {
			keep = append(keep, seq)
		}
	}
	if len(keep) == 0 {
		return nil
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		return err
	}
	if _, err := f.Write(encodeHoles(keep)); err != nil {
		f.Close()
		return err
	}
	if err := syncFile(f); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// applyFrames builds the in-memory index, segment listing and
// sequence bookkeeping from adopted or rebuilt frames. Data frames
// publish batches; holes frames reserve sequences forever. The result
// is identical whichever adoption path produced the frames. l.segIndex
// must already identify the last segment file.
func (l *Log) applyFrames(frames []indexFrame) {
	segBatches := make(map[int][]frameMember)
	var segOrder []int
	for i := range frames {
		f := &frames[i]
		n := int(f.seg)
		switch f.kind {
		case kindData:
			if _, seen := segBatches[n]; !seen {
				segOrder = append(segOrder, n)
			}
			l.gen++
			for _, m := range f.members {
				segBatches[n] = append(segBatches[n], m)
				l.index[m.seq] = entryRef{
					seg:    n,
					off:    int(m.off),
					length: int(m.length),
					gen:    l.gen,
				}
				if m.seq >= l.nextSeq {
					l.nextSeq = m.seq + 1
				}
			}
		case kindHoles:
			for _, seq := range f.seqs {
				if seq == 0 {
					continue
				}
				if _, ok := l.index[seq]; !ok {
					l.staged[seq] = nil
				}
				if seq >= l.nextSeq {
					l.nextSeq = seq + 1
				}
			}
		}
	}
	sort.Ints(segOrder)
	for _, n := range segOrder {
		members := segBatches[n]
		if len(members) == 0 {
			continue
		}
		seg := Segment{FirstSeq: ^uint64(0)}
		for _, m := range members {
			if m.seq < seg.FirstSeq {
				seg.FirstSeq = m.seq
			}
			if m.seq > seg.LastSeq {
				seg.LastSeq = m.seq
			}
		}
		l.segs = append(l.segs, seg)
		if n == l.segIndex {
			l.curBatches = len(members)
		}
	}
	// The running CRC of the current (last) segment is whatever its
	// final frame recorded — a data frame or a recovered holes frame.
	for i := len(frames) - 1; i >= 0; i-- {
		if int(frames[i].seg) == l.segIndex {
			l.curSegCRC = frames[i].segCRC
			break
		}
	}
}

// Append stages a batch of records and reserves the next sequence number
// for it. The batch becomes readable only after Commit or CommitGroup.
func (l *Log) Append(records [][]byte) (Batch, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return Batch{}, errClosed
	}
	recs := copyRecords(records)
	seq := l.nextSeq
	l.nextSeq++
	l.staged[seq] = recs
	return Batch{seq: seq, records: recs, owner: l}, nil
}

// Commit makes a staged batch durable and readable, and returns its
// reserved sequence number. The segment bytes are written and synced
// first; only afterwards is one frame appended and synced to the
// index side file, so the batch becomes visible exactly when that
// frame is durable. Committing a batch that is not staged — including
// one already committed — returns ErrUnknownBatch. With Options.Sync a
// returned commit is durable; if either sync fails Commit returns
// ErrSyncFailed, rolls both files back to their prior sizes, and the
// batch stays staged with its reserved sequence for a retry that
// cannot duplicate the entry.
func (l *Log) Commit(b Batch) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return 0, errClosed
	}
	recs, ok := l.staged[b.seq]
	if !ok || b.owner != l {
		return 0, ErrUnknownBatch
	}
	entry := encodeBatch(b.seq, recs)
	off, err := l.writeEntry(entry)
	if err != nil {
		return 0, err
	}
	frame := indexFrame{
		kind:   kindData,
		seg:    uint64(l.segIndex),
		endOff: uint64(off + len(entry)),
		segCRC: crc32.Update(l.curSegCRC, crc32.IEEETable, entry),
		members: []frameMember{{
			seq:    b.seq,
			off:    uint64(off + len(segmentMagic)),
			length: uint64(len(entry) - len(segmentMagic) - 4),
		}},
	}
	idxOff := l.indexSize()
	if err := l.publish(off, idxOff, frame, encodeFrame(&frame)); err != nil {
		return 0, err
	}
	delete(l.staged, b.seq)
	l.gen++
	l.curSegCRC = frame.segCRC
	l.index[b.seq] = entryRef{
		seg:    l.segIndex,
		off:    off + len(segmentMagic),
		length: len(entry) - len(segmentMagic) - 4,
		gen:    l.gen,
	}
	l.commitSpan(b.seq, b.seq, 1)
	return b.seq, nil
}

// CommitGroup makes several staged batches durable and readable as one
// atomic unit: a single segment write, a single segment fsync, a
// single index frame and a single index fsync, so a crash can never
// leave half the group visible. Each batch keeps the sequence it
// reserved at Append, and the batches become visible in that reserved
// order. If any batch is not staged — including one already committed
// — nothing is written and the error is ErrUnknownBatch. If a sync
// fails, the segment bytes and index frame are both rolled back,
// every batch stays staged with its reserved sequence, the error is
// ErrSyncFailed and retrying the group commits it without duplicating
// entries. Sequences reserved by a torn group stay permanent holes.
func (l *Log) CommitGroup(batches []Batch) ([]uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil, errClosed
	}
	if len(batches) == 0 {
		return nil, nil
	}
	members := make([]groupMember, len(batches))
	seqs := make([]uint64, len(batches))
	seen := make(map[uint64]struct{}, len(batches))
	for i, b := range batches {
		recs, ok := l.staged[b.seq]
		if !ok || b.owner != l {
			return nil, ErrUnknownBatch
		}
		if _, dup := seen[b.seq]; dup {
			return nil, ErrUnknownBatch
		}
		seen[b.seq] = struct{}{}
		members[i] = groupMember{seq: b.seq, records: recs}
		seqs[i] = b.seq
	}
	entry, offs, lens := encodeGroup(members)
	base, err := l.writeEntry(entry)
	if err != nil {
		return nil, err
	}
	fmembers := make([]frameMember, len(members))
	first, last := seqs[0], seqs[0]
	for i, m := range members {
		fmembers[i] = frameMember{
			seq:    m.seq,
			off:    uint64(base + offs[i]),
			length: uint64(lens[i]),
		}
		if m.seq < first {
			first = m.seq
		}
		if m.seq > last {
			last = m.seq
		}
	}
	frame := indexFrame{
		kind:    kindData,
		seg:     uint64(l.segIndex),
		endOff:  uint64(base + len(entry)),
		segCRC:  crc32.Update(l.curSegCRC, crc32.IEEETable, entry),
		members: fmembers,
	}
	idxOff := l.indexSize()
	if err := l.publish(base, idxOff, frame, encodeFrame(&frame)); err != nil {
		return nil, err
	}
	l.gen++
	l.curSegCRC = frame.segCRC
	for i, m := range members {
		delete(l.staged, m.seq)
		l.index[m.seq] = entryRef{seg: l.segIndex, off: base + offs[i], length: lens[i], gen: l.gen}
	}
	l.commitSpan(first, last, len(members))
	return seqs, nil
}

// publish performs the durable publication of an entry already
// written at segOff: with Sync, fsync the segment; then append the
// index frame; with Sync, fsync the index. Any failure rolls both
// files back to their prior sizes (segOff and idxOff) and leaves the
// caller's batches staged; a sync failure reports ErrSyncFailed. The
// caller holds l.mu.
func (l *Log) publish(segOff, idxOff int, frame indexFrame, frameBytes []byte) error {
	if l.opts.Sync {
		if err := syncFile(l.file); err != nil {
			l.rollbackSegment(segOff)
			return fmt.Errorf("%w: %v", ErrSyncFailed, err)
		}
	}
	if _, err := l.idxFile.Write(frameBytes); err != nil {
		l.rollbackSegment(segOff)
		l.rollbackIndex(idxOff)
		return err
	}
	if l.opts.Sync {
		if err := syncFile(l.idxFile); err != nil {
			// The index never durably advertised the entry: roll the
			// synced segment prefix back as well so the retry is the
			// entry's unique publication. If this process dies first,
			// the next open treats the bytes as an uncommitted suffix
			// and turns their reserved sequences into permanent holes.
			l.rollbackIndex(idxOff)
			l.rollbackSegment(segOff)
			return fmt.Errorf("%w: %v", ErrSyncFailed, err)
		}
	}
	return nil
}

// rollbackSegment truncates the current segment back to off.
func (l *Log) rollbackSegment(off int) {
	l.file.Truncate(int64(off))
	l.fileSize = off
	if off == 0 {
		// The entry was the first in a freshly rolled segment; the
		// running prefix is empty again.
		l.curSegCRC = 0
	}
}

// rollbackIndex truncates the side file back to off. The handle is in
// append mode, so the next frame write starts at the new end.
func (l *Log) rollbackIndex(off int) {
	l.idxFile.Truncate(int64(off))
}

// Read returns the records of the committed batch with the given
// sequence, located directly through the segment-level index. A staged
// but uncommitted sequence yields ErrNotCommitted; sequence 0,
// never-reserved sequences, and sequences beyond the reserved range
// yield ErrUnknownBatch.
//
// The lock is held only for the index lookup; the segment file is read
// and the body decoded without it, so a slow read never blocks writers.
func (l *Log) Read(seq uint64) ([][]byte, error) {
	l.mu.RLock()
	if l.closed {
		l.mu.RUnlock()
		return nil, errClosed
	}
	ref, ok := l.index[seq]
	if !ok {
		_, staged := l.staged[seq]
		l.mu.RUnlock()
		if staged {
			return nil, ErrNotCommitted
		}
		return nil, ErrUnknownBatch
	}
	l.mu.RUnlock()
	return l.readRef(seq, ref)
}

// scanSnapshot is a read-consistent visibility boundary: a batch is
// visible only when its publication generation is <= gen and its
// sequence is <= highSeq. The boundary is captured in one brief
// read-lock acquisition, so a group that commits after a replay starts
// (even one filling an earlier reserved hole) is either wholly inside
// the snapshot or wholly outside it.
type scanSnapshot struct {
	gen     uint64
	highSeq uint64
}

// Scan replays committed batches with sequence >= from in increasing
// sequence order, stopping at the first error returned by fn.
//
// The replay is a consistent snapshot: batches committed after the
// replay starts are invisible to it, and a group commit is seen either
// whole or not at all. The replay streams batches straight from the
// segment files one sequence at a time and keeps only the current
// batch in memory; it never loads the whole replay first. Each
// sequence lookup takes the lock briefly, while the file read, decode
// and the fn callback all run outside it, so a long replay never
// blocks writers.
func (l *Log) Scan(from uint64, fn func(Batch) error) error {
	l.mu.RLock()
	if l.closed {
		l.mu.RUnlock()
		return errClosed
	}
	snap := scanSnapshot{gen: l.gen, highSeq: l.nextSeq - 1}
	l.mu.RUnlock()

	if from > snap.highSeq {
		return nil
	}
	for seq := from; ; {
		// One brief read-lock per sequence: only the index metadata is
		// touched while holding it.
		l.mu.RLock()
		ref, ok := l.index[seq]
		l.mu.RUnlock()
		// Skip holes (reserved but uncommitted sequences) and batches
		// published after the snapshot was taken.
		if ok && ref.gen <= snap.gen {
			records, err := l.readRef(seq, ref)
			if err != nil {
				return err
			}
			if err := fn(Batch{seq: seq, records: records, owner: l}); err != nil {
				return err
			}
		}
		if seq == snap.highSeq {
			break
		}
		seq++
	}
	return nil
}

// Segments lists the segments that hold committed batches, in file
// order, each with its first and last committed sequence.
func (l *Log) Segments() []Segment {
	l.mu.RLock()
	defer l.mu.RUnlock()
	out := make([]Segment, len(l.segs))
	copy(out, l.segs)
	return out
}

// Close closes the segment and index files. The log must not be used
// afterwards.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	l.closed = true
	var err error
	if l.file != nil {
		err = l.file.Close()
	}
	if l.idxFile != nil {
		if cerr := l.idxFile.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}
	return err
}

// readRef loads the records of a committed batch straight from its
// segment file. It performs no locking and must be called without l.mu
// held: the segment read and body decode happen entirely on the read
// path, outside the writer lock. The on-disk bytes of an indexed body
// are immutable (only the unindexed suffix of the very last segment is
// ever rewritten, and it never has an index entry), so a ref captured
// under the read lock stays valid afterwards. The returned records are
// freshly decoded, so callers can mutate them without affecting the
// log or later reads.
func (l *Log) readRef(seq uint64, ref entryRef) ([][]byte, error) {
	f, err := os.Open(l.segmentPath(ref.seg))
	if err != nil {
		return nil, err
	}
	defer f.Close()
	body := make([]byte, ref.length)
	if _, err := f.ReadAt(body, int64(ref.off)); err != nil {
		return nil, err
	}
	got, records, err := decodeBody(body)
	if err != nil {
		return nil, err
	}
	if got != seq {
		return nil, ErrCorruptSegment
	}
	return records, nil
}

// commitSpan records n newly committed batches covering sequences
// [first, last] in the segment listing.
func (l *Log) commitSpan(first, last uint64, n int) {
	if l.curBatches == 0 {
		l.segs = append(l.segs, Segment{FirstSeq: first, LastSeq: last})
	} else {
		seg := &l.segs[len(l.segs)-1]
		if first < seg.FirstSeq {
			seg.FirstSeq = first
		}
		if last > seg.LastSeq {
			seg.LastSeq = last
		}
	}
	l.curBatches += n
}

// writeEntry appends one encoded entry to the current segment, rolling
// to a new segment when the current one is non-empty and would exceed
// the configured capacity, and returns the offset the entry was
// written at.
func (l *Log) writeEntry(entry []byte) (int, error) {
	if l.file != nil && l.fileSize > 0 && l.fileSize+len(entry) > l.opts.SegmentBytes {
		if err := l.file.Close(); err != nil {
			return 0, err
		}
		l.file = nil
		l.segIndex++
		l.curBatches = 0
		l.curSegCRC = 0
	}
	if l.file == nil {
		if l.segIndex == 0 {
			l.segIndex = 1
		}
		if err := l.openCurrent(); err != nil {
			return 0, err
		}
	}
	off := l.fileSize
	n, err := l.file.Write(entry)
	if err != nil {
		return 0, err
	}
	l.fileSize += n
	return off, nil
}

func (l *Log) openCurrent() error {
	f, err := os.OpenFile(l.segmentPath(l.segIndex), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return err
	}
	l.file = f
	l.fileSize = int(st.Size())
	return nil
}

// openIndexFile opens the append side file, creating a header-only one
// when the log is still empty, and leaves writes positioned at end.
// With Sync the freshly created header is fsynced (along with the
// directory) before Open returns, so the side file already exists at a
// clean watermark when the first commit writes its segment: a crash
// between that segment sync and the index sync then finds an empty
// watermark rather than a missing file, and the unadvertised commit is
// correctly hidden.
func (l *Log) openIndexFile() error {
	path := l.indexPath()
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return err
	}
	if st.Size() == 0 {
		if _, err := f.Write(encodeIndexShard(nil)); err != nil {
			f.Close()
			return err
		}
		if l.opts.Sync {
			if err := syncFile(f); err != nil {
				f.Close()
				return fmt.Errorf("%w: %v", ErrSyncFailed, err)
			}
			if err := syncDir(l.dir); err != nil {
				f.Close()
				return err
			}
		}
	}
	l.idxFile = f
	return nil
}

// syncDir fsyncs a directory so a newly created or renamed file inside
// it becomes durable.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// persistIndexImage atomically replaces the side file with the header
// followed by the given frames and syncs the replacement and the
// directory, used after a full rebuild or an incremental recovery.
func (l *Log) persistIndexImage(frames []indexFrame) error {
	path := l.indexPath()
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(encodeIndexShard(frames)); err != nil {
		f.Close()
		return err
	}
	if err := syncFile(f); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	d, err := os.Open(l.dir)
	if err != nil {
		return err
	}
	err = d.Sync()
	d.Close()
	return err
}

func (l *Log) indexSize() int {
	st, err := l.idxFile.Stat()
	if err != nil {
		return 0
	}
	return int(st.Size())
}

func (l *Log) segmentPath(index int) string {
	return filepath.Join(l.dir, fmt.Sprintf("%06d.seg", index))
}

func (l *Log) indexPath() string {
	return filepath.Join(l.dir, "index.idx")
}

// crcRange streams bytes [start,end) of fh extending the running CRC
// prev, without loading the segment into memory.
func crcRange(fh *os.File, start, end uint64, prev uint32) (uint32, error) {
	if end < start {
		return 0, ErrCorruptSegment
	}
	if _, err := fh.Seek(int64(start), io.SeekStart); err != nil {
		return 0, err
	}
	run := prev
	buf := make([]byte, 32<<10)
	remaining := int64(end - start)
	for remaining > 0 {
		b := buf
		if int64(len(b)) > remaining {
			b = b[:remaining]
		}
		nr, err := fh.Read(b)
		run = crc32.Update(run, crc32.IEEETable, b[:nr])
		remaining -= int64(nr)
		if err == io.EOF {
			break
		}
		if err != nil {
			return 0, err
		}
	}
	if remaining != 0 {
		return 0, io.ErrUnexpectedEOF
	}
	return run, nil
}

func containsU64(xs []uint64, x uint64) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

func copyRecords(records [][]byte) [][]byte {
	out := make([][]byte, len(records))
	for i, r := range records {
		cp := make([]byte, len(r))
		copy(cp, r)
		out[i] = cp
	}
	return out
}
