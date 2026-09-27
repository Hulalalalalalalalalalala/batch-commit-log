// Package log implements an append-only, batch-commit segmented log.
//
// A writer stages a batch of opaque records with Append and publishes it
// atomically with Commit, or publishes several staged batches as one
// durable group with CommitGroup. Readers only ever see committed
// batches, in strictly increasing sequence order starting at 1, and
// locate them through a segment-level index that Open rebuilds from
// the segment files.
package log

import (
	"errors"
	"fmt"
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
	// ErrSyncFailed is returned by Commit or CommitGroup when
	// Options.Sync is set and fsyncing the segment file fails. The
	// batches stay staged and keep their reserved sequences; committing
	// them again retries.
	ErrSyncFailed = errors.New("log: sync failed")
)

var errClosed = errors.New("log: closed")

// syncFile fsyncs a segment file. It is a variable so tests can
// simulate sync failures.
var syncFile = func(f *os.File) error { return f.Sync() }

// Options configures a Log opened with Open.
type Options struct {
	// SegmentBytes is the capacity of one segment file in bytes. Once a
	// non-empty segment would exceed it, the log rolls to a new segment.
	// A single batch or group larger than the capacity occupies its own
	// segment. Must be positive.
	SegmentBytes int
	// Sync makes Commit and CommitGroup fsync the segment file before
	// returning, so a returned commit is durable.
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

// entryLoc points at one committed batch entry inside a segment file.
// The index built from these locations lets Read and Scan jump straight
// to an entry instead of scanning segments.
type entryLoc struct {
	seg    int
	off    int64
	length int
}

// Log is an append-only batch-commit log stored in a directory of
// segment files. It is safe for concurrent use, but the writer side is
// expected to be a single goroutine (see README Limits).
type Log struct {
	dir  string
	opts Options

	mu         sync.Mutex
	file       *os.File // current segment, nil until first write
	fileSize   int
	segIndex   int // index of the current segment file, 0 = none yet
	curBatches int // committed batches in the current segment
	nextSeq    uint64
	staged     map[uint64][][]byte
	index      map[uint64]entryLoc // committed sequences, rebuilt from segments at Open
	segs       []Segment           // segments holding at least one committed batch
	closed     bool
}

// Open opens the log in dir, creating the directory if needed. Existing
// segments are validated, their committed batches become readable, and
// the read index is rebuilt from the segment files. A half-written tail
// of the last segment — the remnant of a crash mid-write, including an
// incomplete group frame — is discarded without error and its reserved
// sequences stay permanent holes; any other inconsistency is
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
		index:   make(map[uint64]entryLoc),
	}
	for i, n := range indices {
		data, err := os.ReadFile(l.segmentPath(n))
		if err != nil {
			return nil, err
		}
		batches, tail, err := parseSegment(data)
		if err != nil {
			return nil, err
		}
		if tail != nil && i != len(indices)-1 {
			// A half-written tail is a crash remnant only in the
			// last segment; anywhere else it is tampering.
			return nil, ErrCorruptSegment
		}
		l.curBatches = len(batches)
		if len(batches) > 0 {
			seg := Segment{FirstSeq: ^uint64(0)}
			for _, pb := range batches {
				l.index[pb.seq] = entryLoc{seg: n, off: int64(pb.off), length: pb.length}
				if pb.seq < seg.FirstSeq {
					seg.FirstSeq = pb.seq
				}
				if pb.seq > seg.LastSeq {
					seg.LastSeq = pb.seq
				}
			}
			l.segs = append(l.segs, seg)
			if seg.LastSeq >= l.nextSeq {
				l.nextSeq = seg.LastSeq + 1
			}
		}
		if tail != nil {
			// Drop the torn bytes so later appends stay parseable, and
			// keep every reserved sequence as a permanent hole: the
			// crashed batches stay staged forever and their sequences
			// are never reused.
			if err := os.Truncate(l.segmentPath(n), int64(tail.off)); err != nil {
				return nil, err
			}
			for _, seq := range tail.seqs {
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
	if len(indices) > 0 {
		l.segIndex = indices[len(indices)-1]
		if err := l.openCurrent(); err != nil {
			return nil, err
		}
	}
	return l, nil
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
// reserved sequence number. Committing a batch that is not staged —
// including one already committed — returns ErrUnknownBatch. With
// Options.Sync a returned commit is durable; if the fsync fails Commit
// returns ErrSyncFailed and the batch stays staged, keeping its
// reserved sequence for a retry.
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
	loc, err := l.writeBytes(entry)
	if err != nil {
		return 0, err
	}
	if l.opts.Sync {
		if err := syncFile(l.file); err != nil {
			// The batch stays staged and keeps its reserved sequence;
			// a later Commit retries. Drop the un-synced entry so the
			// retry cannot duplicate it on disk.
			l.rollback(loc.off)
			return 0, fmt.Errorf("%w: %v", ErrSyncFailed, err)
		}
	}
	delete(l.staged, b.seq)
	l.index[b.seq] = loc
	l.trackSegment(b.seq, b.seq)
	l.curBatches++
	return b.seq, nil
}

// CommitGroup makes several staged batches durable and readable with
// one combined write and, when Options.Sync is set, a single fsync.
// Every batch keeps the sequence it reserved at Append, and the group
// becomes visible in that sequence order; the returned sequences are
// sorted ascending. The commit is atomic: it either publishes every
// batch in the group or — after a failed sync or a crash — leaves
// every one of them staged, never a subset. If any batch is not staged
// (including one already committed or staged twice) nothing is written
// and ErrUnknownBatch is returned. With Options.Sync a failed fsync
// returns ErrSyncFailed, rolls the written bytes back, and keeps the
// whole group staged so a retry cannot duplicate entries.
func (l *Log) CommitGroup(batches []Batch) ([]uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil, errClosed
	}
	if len(batches) == 0 {
		return nil, nil
	}
	type item struct {
		seq  uint64
		recs [][]byte
	}
	items := make([]item, len(batches))
	seen := make(map[uint64]bool, len(batches))
	for i, b := range batches {
		recs, ok := l.staged[b.seq]
		if !ok || b.owner != l || seen[b.seq] {
			return nil, ErrUnknownBatch
		}
		seen[b.seq] = true
		items[i] = item{seq: b.seq, recs: recs}
	}
	sort.Slice(items, func(i, j int) bool { return items[i].seq < items[j].seq })

	seqs := make([]uint64, len(items))
	entries := make([][]byte, len(items))
	for i, it := range items {
		seqs[i] = it.seq
		entries[i] = encodeBatch(it.seq, it.recs)
	}
	frame := encodeGroup(seqs, entries)
	base, err := l.writeBytes(frame)
	if err != nil {
		return nil, err
	}
	if l.opts.Sync {
		if err := syncFile(l.file); err != nil {
			// Roll the whole frame back: the group stays staged and a
			// retry cannot duplicate any of its entries.
			l.rollback(base.off)
			return nil, fmt.Errorf("%w: %v", ErrSyncFailed, err)
		}
	}
	off := base.off + int64(groupHeaderSize(len(items)))
	for i, it := range items {
		delete(l.staged, it.seq)
		l.index[it.seq] = entryLoc{seg: base.seg, off: off, length: len(entries[i])}
		off += int64(len(entries[i]))
	}
	l.trackSegment(seqs[0], seqs[len(seqs)-1])
	l.curBatches += len(items)
	return seqs, nil
}

// Read returns the records of the committed batch with the given
// sequence, located directly through the segment-level index. A staged
// but uncommitted sequence yields ErrNotCommitted; sequence 0,
// never-reserved sequences, and sequences beyond the reserved range
// yield ErrUnknownBatch.
func (l *Log) Read(seq uint64) ([][]byte, error) {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return nil, errClosed
	}
	loc, ok := l.index[seq]
	if !ok {
		_, staged := l.staged[seq]
		l.mu.Unlock()
		if staged {
			return nil, ErrNotCommitted
		}
		return nil, ErrUnknownBatch
	}
	l.mu.Unlock()
	got, recs, err := l.readEntry(loc)
	if err != nil {
		return nil, err
	}
	if got != seq {
		return nil, ErrCorruptSegment
	}
	return recs, nil
}

// Scan replays committed batches with sequence >= from in increasing
// sequence order, stopping at the first error returned by fn. The
// replay is a consistent snapshot: batches committed after Scan starts
// are invisible to it.
func (l *Log) Scan(from uint64, fn func(Batch) error) error {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return errClosed
	}
	seqs := make([]uint64, 0, len(l.index))
	for seq := range l.index {
		if seq >= from {
			seqs = append(seqs, seq)
		}
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	locs := make([]entryLoc, len(seqs))
	for i, seq := range seqs {
		locs[i] = l.index[seq]
	}
	l.mu.Unlock()
	for _, loc := range locs {
		seq, recs, err := l.readEntry(loc)
		if err != nil {
			return err
		}
		if err := fn(Batch{seq: seq, records: recs, owner: l}); err != nil {
			return err
		}
	}
	return nil
}

// Segments lists the segments that hold committed batches, in file
// order, each with its first and last committed sequence.
func (l *Log) Segments() []Segment {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]Segment, len(l.segs))
	copy(out, l.segs)
	return out
}

// Close closes the current segment file. The log must not be used
// afterwards.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	l.closed = true
	if l.file != nil {
		return l.file.Close()
	}
	return nil
}

// readEntry loads one committed entry straight from its segment using
// an index location; no scan of other segments is involved. Committed
// entries are immutable, so this is safe to call without holding the
// log lock.
func (l *Log) readEntry(loc entryLoc) (uint64, [][]byte, error) {
	f, err := os.Open(l.segmentPath(loc.seg))
	if err != nil {
		return 0, nil, err
	}
	defer f.Close()
	buf := make([]byte, loc.length)
	if _, err := f.ReadAt(buf, loc.off); err != nil {
		return 0, nil, err
	}
	pb, next, tail, err := parseEntry(buf, 0)
	if err != nil {
		return 0, nil, err
	}
	if tail != nil || next != len(buf) {
		return 0, nil, ErrCorruptSegment
	}
	return pb.seq, pb.records, nil
}

// writeBytes appends buf to the current segment with a single Write,
// rolling to a new segment when the current one is non-empty and would
// exceed the configured capacity. It returns where the bytes landed.
func (l *Log) writeBytes(buf []byte) (entryLoc, error) {
	if l.file != nil && l.fileSize > 0 && l.fileSize+len(buf) > l.opts.SegmentBytes {
		if err := l.file.Close(); err != nil {
			return entryLoc{}, err
		}
		l.file = nil
		l.segIndex++
		l.curBatches = 0
	}
	if l.file == nil {
		if l.segIndex == 0 {
			l.segIndex = 1
		}
		if err := l.openCurrent(); err != nil {
			return entryLoc{}, err
		}
	}
	loc := entryLoc{seg: l.segIndex, off: int64(l.fileSize), length: len(buf)}
	if _, err := l.file.Write(buf); err != nil {
		// Drop any partially written bytes so the segment stays a clean
		// sequence of whole entries and frames.
		l.rollback(loc.off)
		return entryLoc{}, err
	}
	l.fileSize += len(buf)
	return loc, nil
}

// rollback drops the un-synced bytes written at or after off in the
// current segment.
func (l *Log) rollback(off int64) {
	l.file.Truncate(off)
	l.fileSize = int(off)
}

// trackSegment records that the current segment now also holds
// committed sequences in [first, last].
func (l *Log) trackSegment(first, last uint64) {
	if l.curBatches == 0 {
		l.segs = append(l.segs, Segment{FirstSeq: first, LastSeq: last})
		return
	}
	seg := &l.segs[len(l.segs)-1]
	if first < seg.FirstSeq {
		seg.FirstSeq = first
	}
	if last > seg.LastSeq {
		seg.LastSeq = last
	}
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

func (l *Log) segmentPath(index int) string {
	return filepath.Join(l.dir, fmt.Sprintf("%06d.seg", index))
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
