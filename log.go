// Package log implements an append-only, batch-commit segmented log.
//
// A writer stages a batch of opaque records with Append and publishes it
// atomically with Commit. Readers only ever see committed batches, in
// strictly increasing sequence order starting at 1.
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
	// ErrSyncFailed is returned by Commit when Options.Sync is set and
	// fsyncing the segment file fails. The batch stays staged and keeps
	// its reserved sequence; committing it again retries.
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
	// A single batch larger than the capacity occupies its own segment.
	// Must be positive.
	SegmentBytes int
	// Sync makes Commit fsync the segment file before returning, so a
	// returned commit is durable.
	Sync bool
}

// Batch is a group of records identified by a single sequence number.
// A Batch returned by Append is staged; after Commit it is readable.
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
	committed  map[uint64][][]byte
	segs       []Segment // segments holding at least one committed batch
	closed     bool
}

// Open opens the log in dir, creating the directory if needed. Existing
// segments are validated and their committed batches become readable.
// A half-written tail of the last segment — the remnant of a crash
// mid-write — is discarded without error and its reserved sequence
// stays a permanent hole; any other inconsistency is ErrCorruptSegment.
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
		dir:       dir,
		opts:      opts,
		nextSeq:   1,
		staged:    make(map[uint64][][]byte),
		committed: make(map[uint64][][]byte),
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
				l.committed[pb.seq] = pb.records
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
			// Drop the torn entry so later appends stay parseable, and
			// keep its reserved sequence as a permanent hole: the
			// crashed batch stays staged forever and its sequence is
			// never reused.
			if err := os.Truncate(l.segmentPath(n), int64(tail.off)); err != nil {
				return nil, err
			}
			if tail.hasSeq && tail.seq > 0 {
				if _, ok := l.committed[tail.seq]; !ok {
					l.staged[tail.seq] = nil
				}
				if tail.seq >= l.nextSeq {
					l.nextSeq = tail.seq + 1
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
// for it. The batch becomes readable only after Commit.
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
	if err := l.writeEntry(entry); err != nil {
		return 0, err
	}
	if l.opts.Sync {
		if err := syncFile(l.file); err != nil {
			// The batch stays staged and keeps its reserved sequence;
			// a later Commit retries. Drop the un-synced entry so the
			// retry cannot duplicate it on disk.
			l.file.Truncate(int64(l.fileSize - len(entry)))
			l.fileSize -= len(entry)
			return 0, fmt.Errorf("%w: %v", ErrSyncFailed, err)
		}
	}
	delete(l.staged, b.seq)
	l.committed[b.seq] = recs
	if l.curBatches == 0 {
		l.segs = append(l.segs, Segment{FirstSeq: b.seq, LastSeq: b.seq})
	} else {
		seg := &l.segs[len(l.segs)-1]
		if b.seq < seg.FirstSeq {
			seg.FirstSeq = b.seq
		}
		if b.seq > seg.LastSeq {
			seg.LastSeq = b.seq
		}
	}
	l.curBatches++
	return b.seq, nil
}

// Read returns the records of the committed batch with the given
// sequence. A staged but uncommitted sequence yields ErrNotCommitted;
// sequence 0, never-reserved sequences, and sequences beyond the
// reserved range yield ErrUnknownBatch.
func (l *Log) Read(seq uint64) ([][]byte, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil, errClosed
	}
	if recs, ok := l.committed[seq]; ok {
		return copyRecords(recs), nil
	}
	if _, ok := l.staged[seq]; ok {
		return nil, ErrNotCommitted
	}
	return nil, ErrUnknownBatch
}

// Scan replays committed batches with sequence >= from in increasing
// sequence order, stopping at the first error returned by fn.
func (l *Log) Scan(from uint64, fn func(Batch) error) error {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return errClosed
	}
	seqs := make([]uint64, 0, len(l.committed))
	for seq := range l.committed {
		if seq >= from {
			seqs = append(seqs, seq)
		}
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	batches := make([]Batch, len(seqs))
	for i, seq := range seqs {
		batches[i] = Batch{seq: seq, records: l.committed[seq], owner: l}
	}
	l.mu.Unlock()
	for _, b := range batches {
		if err := fn(b); err != nil {
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

// writeEntry appends one encoded batch to the current segment, rolling
// to a new segment when the current one is non-empty and would exceed
// the configured capacity.
func (l *Log) writeEntry(entry []byte) error {
	if l.file != nil && l.fileSize > 0 && l.fileSize+len(entry) > l.opts.SegmentBytes {
		if err := l.file.Close(); err != nil {
			return err
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
			return err
		}
	}
	n, err := l.file.Write(entry)
	if err != nil {
		return err
	}
	l.fileSize += n
	return nil
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
