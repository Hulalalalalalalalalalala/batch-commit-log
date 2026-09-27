// Package log implements an append-only, batch-commit segmented log.
//
// A writer stages a batch of opaque records with Append and publishes it
// atomically with Commit, or publishes several staged batches atomically
// with CommitGroup. Readers only ever see committed batches, in strictly
// increasing sequence order starting at 1. Reads locate each batch
// through a segment-level index that Open rebuilds from the segment
// files, so losing the index never loses data.
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
	// ErrSyncFailed is returned by Commit and CommitGroup when
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

// entryRef locates one committed batch body inside a segment file. The
// index is rebuilt from the segment files every time the log opens, so
// its loss or invalidation is always safe. gen is the publication
// generation of the commit that published the batch; every member of a
// group commit shares one gen, which lets a streaming replay snapshot
// see the whole group or none of it.
type entryRef struct {
	seg    int // segment file number
	off    int // offset of the batch body within the segment
	length int // length of the batch body
	gen    uint64
}

// Log is an append-only batch-commit log stored in a directory of
// segment files. It is safe for concurrent use, but the writer side is
// expected to be a single goroutine (see README Limits).
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
	fileSize   int
	segIndex   int // index of the current segment file, 0 = none yet
	curBatches int // committed batches in the current segment
	nextSeq    uint64
	staged     map[uint64][][]byte
	index      map[uint64]entryRef
	segs       []Segment // segments holding at least one committed batch
	gen        uint64    // publication generation of the last commit
	closed     bool
}

// Open opens the log in dir, creating the directory if needed. Existing
// segments are validated and their committed batches become readable,
// and the segment-level index is rebuilt from them. A half-written tail
// of the last segment — the remnant of a crash mid-write — is replaced
// by a durable hole marker without error and its reserved sequences stay
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
	for i, n := range indices {
		data, err := os.ReadFile(l.segmentPath(n))
		if err != nil {
			return nil, err
		}
		batches, holes, tail, err := parseSegment(data)
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
				l.index[pb.seq] = entryRef{seg: n, off: pb.off, length: pb.length}
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
			// Replace the torn entry with a durable hole marker so
			// its reserved sequences stay permanent holes: the
			// crashed batches stay staged forever and their
			// sequences are never reused, even across reopens.
			if err := l.rewriteTail(n, tail.off, tail.seqs); err != nil {
				return nil, err
			}
			holes = append(holes, tail.seqs...)
		}
		for _, seq := range holes {
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
	off, err := l.writeEntry(entry)
	if err != nil {
		return 0, err
	}
	if l.opts.Sync {
		if err := syncFile(l.file); err != nil {
			// The batch stays staged and keeps its reserved sequence;
			// a later Commit retries. Drop the un-synced entry so the
			// retry cannot duplicate it on disk.
			l.file.Truncate(int64(off))
			l.fileSize = off
			return 0, fmt.Errorf("%w: %v", ErrSyncFailed, err)
		}
	}
	delete(l.staged, b.seq)
	l.gen++
	l.index[b.seq] = entryRef{seg: l.segIndex, off: off + len(segmentMagic), length: len(entry) - len(segmentMagic) - 4, gen: l.gen}
	l.commitSpan(b.seq, b.seq, 1)
	return b.seq, nil
}

// CommitGroup makes several staged batches durable and readable as one
// atomic unit: a single write and, with Options.Sync, a single fsync
// covering the whole group. Each batch keeps the sequence it reserved at
// Append, and the batches become visible in that reserved order — the
// merge never reorders them. If any batch is not staged — including one
// already committed — nothing is written and the error is
// ErrUnknownBatch. If the fsync fails, the written bytes are rolled back
// and every batch stays staged with its reserved sequence; the error is
// ErrSyncFailed and retrying the group commits it without duplicating
// entries. A crash can never leave half of the group visible: the group
// is either wholly durable or wholly staged, and sequences reserved by a
// torn group stay permanent holes.
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
	if l.opts.Sync {
		if err := syncFile(l.file); err != nil {
			// Every batch stays staged and keeps its reserved
			// sequence; a later CommitGroup retries. Drop the
			// un-synced group entry so the retry cannot duplicate it.
			l.file.Truncate(int64(base))
			l.fileSize = base
			return nil, fmt.Errorf("%w: %v", ErrSyncFailed, err)
		}
	}
	first, last := seqs[0], seqs[0]
	l.gen++
	for i, m := range members {
		delete(l.staged, m.seq)
		l.index[m.seq] = entryRef{seg: l.segIndex, off: base + offs[i], length: lens[i], gen: l.gen}
		if m.seq < first {
			first = m.seq
		}
		if m.seq > last {
			last = m.seq
		}
	}
	l.commitSpan(first, last, len(members))
	return seqs, nil
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

// readRef loads the records of a committed batch straight from its
// segment file. It performs no locking and must be called without l.mu
// held: the segment read and body decode happen entirely on the read
// path, outside the writer lock. The on-disk bytes are immutable once
// committed (only the torn tail of the very last segment is ever
// rewritten, and that tail never has an index entry), so a ref captured
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

// rewriteTail drops a torn tail from segment n and, when the torn entry
// carried recoverable sequences, replaces it with a synced hole marker
// so the reserved sequences stay permanent holes across reopens.
func (l *Log) rewriteTail(n, off int, seqs []uint64) error {
	path := l.segmentPath(n)
	if err := os.Truncate(path, int64(off)); err != nil {
		return err
	}
	keep := make([]uint64, 0, len(seqs))
	for _, seq := range seqs {
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
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
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
