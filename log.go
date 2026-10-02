// Package log implements an append-only, batch-commit segmented log.
//
// A writer stages a batch of opaque records with Append and publishes it
// atomically with Commit, or publishes several staged batches atomically
// with CommitGroup. AppendIdempotent stages a batch under a caller key:
// repeating a key with byte-identical records returns the first batch
// and its sequence across retries and restarts, while different records
// conflict. Readers only ever see committed batches, in strictly
// increasing sequence order starting at 1. Reads locate each batch
// through a persistent segment-level index in a sidecar file
// ("index.idx"): Open adopts it incrementally, and when it is missing,
// truncated, version-mismatched, checksum-bad or contradictory to the
// segments, Open rebuilds it from the segment files, so losing the
// index never loses data — idempotency keys live in the segments, not
// in the sidecar, and are recovered on every open path.
//
// DeleteThrough optionally reclaims consumed history with a prefix
// truncation that removes whole segment files only. The truncation
// point is journaled in its own sidecar ("truncate.idx"), separate
// from the rebuildable index, so historical sequences keep reading as
// ErrTruncated — never ErrUnknownBatch — across restarts and index
// rebuilds.
//
// Named consumers persist their replay checkpoints in a third sidecar
// ("consumers.idx"): AckConsumer confirms a processed prefix,
// ConsumerSeq reports it and DropConsumer retires the registration.
// Unlike index.idx the checkpoint file is authoritative — a damaged or
// unsupported image makes Open return ErrCorruptCheckpoint — and a
// registered consumer keeps prefix history from being reclaimed until
// it has confirmed it.
//
// Abort abandons a staged batch without committing it: the reserved
// sequence becomes a permanent hole (durable across restarts and index
// rebuilds, exactly like a crash-recovery hole), its idempotency key is
// released for reuse, and every other staged or committed batch is
// unaffected.
package log

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"unicode/utf8"
)

var (
	// ErrNotCommitted is returned when reading a sequence that has been
	// staged with Append but not yet committed.
	ErrNotCommitted = errors.New("log: batch not committed")
	// ErrUnknownBatch is returned for sequence 0, sequences that were
	// never reserved, sequences beyond the reserved range, and batches
	// that have already been committed or aborted.
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
	// ErrInvalidBatchID is returned by AppendIdempotent for an empty
	// key, a key containing a NUL byte, a key that is not valid UTF-8 or
	// a key longer than 256 bytes.
	ErrInvalidBatchID = errors.New("log: invalid batch id")
	// ErrBatchIDConflict is returned by AppendIdempotent when a key was
	// used before but the new records differ from the first call's in
	// length, order or bytes.
	ErrBatchIDConflict = errors.New("log: batch id conflict")
	// ErrTruncated is returned by Read and Scan for a sequence whose
	// segment was reclaimed by a prefix truncation (DeleteThrough). It
	// is distinct from ErrUnknownBatch: the sequence was once committed,
	// and that fact survives restarts and index rebuilds.
	ErrTruncated = errors.New("log: sequence truncated")
	// ErrInvalidRetention is returned by DeleteThrough when its argument
	// is not a usable truncation point: a sequence that was never
	// committed, a hole, a still-staged batch, or a point past which
	// uncommitted staged or hole-marked reservations would be reclaimed
	// together with the history. Nothing is deleted when it is returned.
	ErrInvalidRetention = errors.New("log: invalid retention point")
	// ErrRetentionFailed is returned by DeleteThrough when persisting
	// the truncation point or removing the old segment files fails. The
	// log is left in one of its two consistent states — either the full
	// old state or the full new state — both of which stay readable; a
	// later reopen completes or discards the pending truncation.
	ErrRetentionFailed = errors.New("log: retention failed")
	// ErrInvalidConsumer is returned for a consumer name that fails the
	// idempotency-key naming rule (empty, NUL byte, invalid UTF-8 or
	// longer than 256 bytes). Consumer names and idempotency keys are
	// separate namespaces, but they share the rule.
	ErrInvalidConsumer = errors.New("log: invalid consumer name")
	// ErrUnknownConsumer is returned by AckConsumer, AckConsumerState,
	// ConsumerSeq, ConsumerCheckpoint and DropConsumer for a syntactically
	// legal name that was never registered. Registration happens only
	// through AckConsumer(name, 0) or AckConsumerState(name, 0, state).
	ErrUnknownConsumer = errors.New("log: unknown consumer")
	// ErrInvalidAck is returned by AckConsumer and AckConsumerState when
	// seq cannot confirm a prefix: it moves a registered consumer
	// backwards, names a batch that is not committed (unknown, a
	// permanent hole or still staged), names history already reclaimed by
	// DeleteThrough, or a still-staged batch with a smaller sequence
	// would be reclaimed together with the confirmed prefix. Permanent
	// holes above the confirmed prefix may be skipped, and a group may be
	// confirmed as a unit.
	ErrInvalidAck = errors.New("log: invalid ack")
	// ErrInvalidCheckpointState is returned by AckConsumerState when the
	// opaque state blob exceeds 1048576 bytes. Nothing is written and the
	// checkpoint does not change.
	ErrInvalidCheckpointState = errors.New("log: invalid checkpoint state")
	// ErrCheckpointConflict is returned by AckConsumerState when the
	// stored sequence is repeated with a different state blob. Sequence
	// and state are published together, so reconfirming a sequence may
	// only repeat the exact state; changing the state requires progress.
	// Nothing is written.
	ErrCheckpointConflict = errors.New("log: checkpoint conflict")
	// ErrCheckpointFailed is returned when a checkpoint change cannot be
	// made durable: forcing the committed data to disk, writing or
	// syncing consumers.idx fails. No consumer state changes and the call
	// can be retried.
	ErrCheckpointFailed = errors.New("log: checkpoint failed")
	// ErrCorruptCheckpoint is returned by Open when consumers.idx is
	// present but cannot be trusted: truncated, checksum-bad, framed
	// wrongly, from an unsupported version, structurally invalid
	// (including an oversized state blob) or semantically impossible (a
	// confirmed sequence that is not committed, already reclaimed, ahead
	// of a staged batch, or duplicated). Progress is never reset
	// silently; the caller must intervene.
	ErrCorruptCheckpoint = errors.New("log: corrupt checkpoint")
	// ErrRetentionBlocked is returned by DeleteThrough when the reclaimable
	// prefix contains a batch above at least one consumer's confirmed
	// sequence. Nothing is deleted — no segment, index or idempotency key
	// changes — until every consumer has confirmed the prefix.
	ErrRetentionBlocked = errors.New("log: retention blocked by consumer")
	// ErrAbortFailed is returned by Abort when the hole marker or its
	// index record cannot be written or made durable. The batch and its
	// idempotency key stay staged under the original sequence, the
	// partial marker and index record are rolled back, and retrying the
	// abort — or committing the batch instead — cannot produce a
	// duplicate record.
	ErrAbortFailed = errors.New("log: abort failed")
)

var errClosed = errors.New("log: closed")

// syncFile fsyncs a file. It is a variable so tests can simulate
// sync failures.
var syncFile = func(f *os.File) error { return f.Sync() }

// removeFile deletes a file. It is a variable so tests can simulate
// segment removal failures during prefix truncation.
var removeFile = func(name string) error { return os.Remove(name) }

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
// is readable. A batch staged with AppendIdempotent additionally
// carries the caller-chosen idempotency key, returned by ID; anonymous
// batches return "" from ID.
type Batch struct {
	seq     uint64
	id      string
	records [][]byte
	owner   *Log
}

// Seq returns the sequence number reserved for the batch.
func (b Batch) Seq() uint64 { return b.seq }

// ID returns the batch's idempotency key as given to
// AppendIdempotent, or "" for an anonymous batch staged with Append.
func (b Batch) ID() string { return b.id }

// Records returns a copy of the batch's records.
func (b Batch) Records() [][]byte { return copyRecords(b.records) }

// Segment describes one segment file by the committed sequences it holds.
type Segment struct {
	FirstSeq uint64
	LastSeq  uint64
}

// entryRef locates one committed batch body inside a segment file. The
// persistent index sidecar stores the same locations; its loss or
// invalidation is always safe because Open rebuilds it from the
// segments. gen is the publication generation of the commit that
// published the batch; every member of a group commit shares one gen,
// which lets a streaming replay snapshot see the whole group or none of
// it.
type entryRef struct {
	seg    int // segment file number
	off    int // offset of the batch body within the segment
	length int // length of the batch body
	gen    uint64
	id     string // idempotency key, "" for an anonymous batch
	keyed  bool   // body uses key framing (BCLI, or any BCLK member)
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
	idxFile    *os.File // index sidecar, opened for append
	idxSize    int
	segIndex   int // index of the current segment file, 0 = none yet
	curBatches int // committed batches in the current segment
	nextSeq    uint64
	staged     map[uint64][][]byte
	stagedID   map[uint64]string // idempotency key of staged keyed batches
	ids        map[string]uint64 // key -> seq of staged or committed keyed batch
	index      map[uint64]entryRef
	segs       []segInfo // segment files still on disk, in file order
	gen        uint64    // publication generation of the last commit
	// through is the persisted prefix-truncation point (DeleteThrough):
	// every committed sequence <= through used to live in a segment that
	// has been reclaimed or is pending reclamation. 0 when the log has
	// never been truncated. Those sequences read as ErrTruncated, never
	// as ErrUnknownBatch, across reopens and index rebuilds.
	through uint64
	// consumers maps every registered consumer name to its confirmed
	// prefix sequence (0 for a name just registered) and the opaque
	// replay state published together with it. Persisted in consumers.idx,
	// which is authoritative state unlike index.idx.
	consumers map[string]consumerState
	// segDirty records segment files carrying committed bytes that no
	// successful fsync has covered yet (Options.Sync == false commits). A
	// checkpoint confirmation must barrier-sync exactly these files
	// before it becomes durable, even when Sync is false. Rolling to a
	// new segment does not clear the old file's bit: its bytes stay
	// un-durable until a sync or a truncation that removes it.
	segDirty map[int]bool
	closed   bool
}

// segInfo describes one segment file that still exists on disk. The
// public Segments view reports only its FirstSeq/LastSeq; the file
// number and the highest hole marker it carries stay internal. The
// committed ranges are strictly increasing across the list, but a
// segment that also holds hole markers may carry holes above LastSeq
// (e.g. a torn tail), which prefix truncation must not reclaim.
type segInfo struct {
	Segment
	file       int
	hasCommits bool   // the file holds at least one committed batch
	maxHole    uint64 // highest reserved sequence in this segment's hole markers
	hasHoles   bool
}

// Open opens the log in dir, creating the directory if needed. The
// committed set is taken from the persistent index sidecar when it can
// be adopted incrementally; if the sidecar is missing, truncated, from
// another version, checksum-bad or contradictory to the segment files,
// Open transparently rebuilds it by scanning the segments once. Either
// path yields identical reads, replays, segment listings and errors.
//
// A half-written tail of the last segment — the remnant of a crash
// mid-write — is replaced by a durable hole marker without error and
// its reserved sequences stay permanent holes across reopens; any other
// inconsistency is ErrCorruptSegment.
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
		stagedID:  make(map[uint64]string),
		ids:       make(map[string]uint64),
		index:     make(map[uint64]entryRef),
		consumers: make(map[string]consumerState),
		segDirty:  make(map[int]bool),
	}

	// A persisted truncation point makes every segment up to its
	// segment boundary reclaimable. A crash between persisting the point
	// and finishing the removal is completed here, before any index is
	// adopted, so a reopen can only ever see the full old or the full new
	// state — never half-deleted files.
	marker, markerOK, err := l.readTruncateMarker()
	if err != nil {
		return nil, err
	}
	crashCompleted := false
	if markerOK {
		var removed int
		indices, removed, err = l.completeTruncation(indices, marker)
		if err != nil {
			return nil, err
		}
		l.through = marker.through
		// A non-zero removal means this reopen is finishing a crash
		// mid-truncation: the sidecar may still describe the deleted
		// files. Force the rebuild path so it is rewritten from the
		// retained segments instead of being adopted and appended to.
		crashCompleted = removed > 0
	}

	// Segment sizes are all the segment metadata adoption needs; the
	// contents are touched only by the cheap CRC pass, never decoded
	// entry by entry.
	sizes := make(map[int]int, len(indices))
	for _, n := range indices {
		st, err := os.Stat(l.segmentPath(n))
		if err != nil {
			return nil, err
		}
		sizes[n] = int(st.Size())
	}

	var snapshot parsedIndex
	var ok bool
	if !crashCompleted {
		snapshot, ok = l.adoptIndex(indices, sizes)
	}
	if crashCompleted || !ok {
		snapshot, err = l.rebuildFromSegments(indices)
		if err != nil {
			return nil, err
		}
	}
	sortIndexSnapshot(&snapshot)
	if err := l.installSnapshot(snapshot, indices); err != nil {
		return nil, err
	}
	// A previous process may have run with Options.Sync false and left
	// committed bytes in the page cache. The first checkpoint must
	// barrier them too, so assume every retained segment needs a sync
	// until one actually happens; an already-durable file merely costs a
	// redundant fsync.
	for i := range l.segs {
		if l.segs[i].hasCommits {
			l.segDirty[l.segs[i].file] = true
		}
	}

	if len(indices) > 0 {
		l.segIndex = indices[len(indices)-1]
		if err := l.openCurrent(); err != nil {
			return nil, err
		}
	} else if markerOK {
		// Every segment has been reclaimed: remember the highest segment
		// number ever used so the next commit opens segBound+1 instead of
		// reusing a number the truncation marker still considers gone.
		l.segIndex = int(marker.segBound)
	}
	if err := l.openIndex(); err != nil {
		return nil, err
	}
	// Consumer checkpoints are authoritative metadata loaded last, once
	// the committed set, holes and truncation point are final. A
	// checkpoint that disagrees with that state (recycled, uncommitted or
	// unknown target, or a staged batch before it) is fatal corruption —
	// progress is never silently reset.
	centries, cOK, cerr := l.readConsumers()
	if cerr != nil {
		return nil, cerr
	}
	if cOK {
		for _, e := range centries {
			if !validConsumerName(e.name) {
				return nil, ErrCorruptCheckpoint
			}
			if _, dup := l.consumers[e.name]; dup {
				return nil, ErrCorruptCheckpoint
			}
			if e.seq > 0 && e.seq > l.through {
				// Above the truncation point the checkpoint must name a
				// batch that is committed now, with no staged batch below
				// it. A value at or below through is ordinary
				// post-truncation state: the consumer confirmed a prefix
				// DeleteThrough was allowed to reclaim (its checkpoint
				// can sit below the anchor when the anchor lives in a
				// retained segment), and the bytes are gone, so there is
				// nothing stronger to re-verify.
				if _, committed := l.index[e.seq]; !committed {
					return nil, ErrCorruptCheckpoint
				}
				for s, recs := range l.staged {
					if recs != nil && s < e.seq {
						return nil, ErrCorruptCheckpoint
					}
				}
			}
			l.consumers[e.name] = consumerState{seq: e.seq, state: e.state}
		}
	}
	return l, nil
}

// completeTruncation finishes an interrupted DeleteThrough: every
// segment file whose number is at or before the marker's boundary that
// is still on disk is removed, and the returned list only contains
// surviving segments. removed counts files that were actually deleted,
// letting the caller tell a crash-completing reopen (stale sidecar
// possible) apart from an ordinary one. A missing file means the crash
// removal had already reached it; a removal that fails aborts the open,
// leaving the directory readable but untouched in mixed state for a
// retry.
func (l *Log) completeTruncation(indices []int, m truncateMarker) (kept []int, removed int, err error) {
	kept = make([]int, 0, len(indices))
	for _, n := range indices {
		if uint64(n) <= m.segBound {
			rerr := removeFile(l.segmentPath(n))
			if rerr != nil && !os.IsNotExist(rerr) {
				return nil, 0, fmt.Errorf("%w: %v", ErrRetentionFailed, rerr)
			}
			if rerr == nil {
				removed++
			}
			continue
		}
		kept = append(kept, n)
	}
	return kept, removed, nil
}

// installSnapshot populates the in-memory index, segment listing, staged
// holes, key map and next-sequence counter from an adopted or rebuilt
// snapshot. Both open paths funnel through here so their observable
// state is identical. Entries must be ordered by (segment, disk offset);
// each entry (a single commit or one group) gets one publication
// generation. A key claimed by two complete entries, or a complete
// entry whose member framing disagrees with the index, is corruption.
//
// Entries at or below the persisted truncation point belong to segment
// files that are gone (or pending removal): they never reach this view,
// because Open filters those files out before adoption or rebuild. The
// truncation point itself still feeds nextSeq so historical sequences
// stay ErrTruncated rather than being handed out again.
func (l *Log) installSnapshot(p parsedIndex, indices []int) error {
	for i := range p.entries {
		e := &p.entries[i]
		l.gen++
		for _, m := range e.members {
			l.index[m.seq] = entryRef{
				seg: e.seg, off: m.off, length: m.length, gen: l.gen,
				id: m.id, keyed: e.keyed,
			}
			if m.id != "" {
				if _, dup := l.ids[m.id]; dup {
					return ErrCorruptSegment
				}
				l.ids[m.id] = m.seq
			}
			if m.seq >= l.nextSeq {
				l.nextSeq = m.seq + 1
			}
		}
	}
	if l.through >= l.nextSeq {
		l.nextSeq = l.through + 1
	}
	// Segment listing: one entry per physical segment that holds at
	// least one committed batch. Each entry also tracks the highest hole
	// marker in its file, which prefix truncation consults so it never
	// reclaims a segment carrying a live permanent hole.
	segSeen := make(map[int]int)
	for i := range p.entries {
		e := &p.entries[i]
		var first, last uint64
		for j, m := range e.members {
			if j == 0 {
				first, last = m.seq, m.seq
			}
			if m.seq < first {
				first = m.seq
			}
			if m.seq > last {
				last = m.seq
			}
		}
		if idx, known := segSeen[e.seg]; known {
			l.segs[idx].hasCommits = true
			if l.segs[idx].FirstSeq == 0 || first < l.segs[idx].FirstSeq {
				l.segs[idx].FirstSeq = first
			}
			if last > l.segs[idx].LastSeq {
				l.segs[idx].LastSeq = last
			}
		} else {
			segSeen[e.seg] = len(l.segs)
			l.segs = append(l.segs, segInfo{
				Segment:    Segment{FirstSeq: first, LastSeq: last},
				file:       e.seg,
				hasCommits: true,
			})
		}
	}
	for i := range p.holes {
		h := &p.holes[i]
		if idx, known := segSeen[h.seg]; known {
			for _, seq := range h.seqs {
				if seq > l.segs[idx].maxHole {
					l.segs[idx].maxHole = seq
					l.segs[idx].hasHoles = true
				}
			}
		} else {
			// A segment carrying only a hole marker and no committed
			// batch is not part of the public Segments view, but its
			// reservation must still pin the sequence counter and the
			// truncation boundary; represent it as a range-less entry.
			var maxHole uint64
			for _, seq := range h.seqs {
				if seq > maxHole {
					maxHole = seq
				}
			}
			segSeen[h.seg] = len(l.segs)
			l.segs = append(l.segs, segInfo{file: h.seg, maxHole: maxHole, hasHoles: maxHole > 0})
		}
	}
	// Keep the list ordered by file number even if a hole-only segment
	// was discovered out of band.
	sort.SliceStable(l.segs, func(i, j int) bool { return l.segs[i].file < l.segs[j].file })
	// Holes reserve sequences permanently: they read as staged (never
	// committed) and are never reused, across any number of reopens.
	// Holes inside reclaimed segments are gone with their file and are
	// covered by the truncation point, so they are not staged again.
	for i := range p.holes {
		for _, seq := range p.holes[i].seqs {
			if seq == 0 || seq <= l.through {
				continue
			}
			if _, committed := l.index[seq]; !committed {
				l.staged[seq] = nil
			}
			if seq >= l.nextSeq {
				l.nextSeq = seq + 1
			}
		}
	}
	if len(indices) > 0 {
		last := indices[len(indices)-1]
		l.curBatches = 0
		for i := range p.entries {
			if p.entries[i].seg == last {
				l.curBatches += len(p.entries[i].members)
			}
		}
	}
	return nil
}

// sortIndexSnapshot orders a snapshot's records by (segment, offset),
// the canonical on-disk and publication order.
func sortIndexSnapshot(p *parsedIndex) {
	sort.SliceStable(p.entries, func(i, j int) bool {
		if p.entries[i].seg != p.entries[j].seg {
			return p.entries[i].seg < p.entries[j].seg
		}
		return p.entries[i].off < p.entries[j].off
	})
	sort.SliceStable(p.holes, func(i, j int) bool {
		if p.holes[i].seg != p.holes[j].seg {
			return p.holes[i].seg < p.holes[j].seg
		}
		return p.holes[i].off < p.holes[j].off
	})
}

// adoptIndex is the incremental open path. It verifies the sidecar
// against the segment files without decoding record bodies: its records
// must tile a prefix of the segment files from offset zero, every
// on-disk CRC must match, and member locations must agree with the entry
// headers. Any framing defect, a truncated final record, a version
// mismatch or a contradiction makes it return ok=false and Open rebuilds
// from the segments instead.
//
// The segments may legitimately extend past a verified index — the
// crash window between the segment sync and the index record sync. That
// suffix (extra bytes of the last covered segment, plus any whole later
// segment) is parsed incrementally: complete entries get indexed, a
// torn tail of the last segment becomes a hole marker, so adoption
// still costs one small decode rather than a full scan.
func (l *Log) adoptIndex(indices []int, sizes map[int]int) (parsedIndex, bool) {
	if len(indices) == 0 {
		// No segments: nothing to index; openIndex creates the sidecar
		// lazily with its header when the first commit lands.
		return parsedIndex{}, true
	}
	data, err := l.readIndexBytes()
	if err != nil {
		return parsedIndex{}, false
	}
	p, tailLen, err := readIndexFile(data)
	if err != nil || tailLen > 0 {
		// Missing/truncated/foreign sidecar: the rebuild path owns
		// these cases.
		return parsedIndex{}, false
	}
	if len(p.entries) == 0 && len(p.holes) == 0 {
		// A recordless sidecar is valid only when every segment is
		// empty; otherwise the sidecar is simply behind and the
		// rebuild path catches up.
		for _, n := range indices {
			if sizes[n] != 0 {
				return parsedIndex{}, false
			}
		}
		return p, true
	}
	covered, covEnd, ok := l.verifySidecar(p, indices, sizes)
	if !ok {
		return parsedIndex{}, false
	}

	// Everything at or after covered is unverified suffix: the unindexed
	// tail bytes of segment covered, then every whole later segment.
	pos := 0
	for pos < len(indices) && indices[pos] < covered {
		pos++
	}
	var extra []diskRecord
	for si := pos; si < len(indices); si++ {
		n := indices[si]
		raw, err := os.ReadFile(l.segmentPath(n))
		if err != nil {
			return parsedIndex{}, false
		}
		base := 0
		if n == covered {
			if covEnd > len(raw) {
				return parsedIndex{}, false
			}
			base = covEnd
		}
		batches, markers, tail, perr := parseSegment(raw[base:])
		if perr != nil {
			// Tampering in the suffix surfaces through the rebuild path
			// with the identical ErrCorruptSegment.
			return parsedIndex{}, false
		}
		if tail != nil && si != len(indices)-1 {
			return parsedIndex{}, false
		}
		for k := 0; k < len(batches); {
			diskOff := batches[k].diskOff + base
			j := k + 1
			for j < len(batches) && batches[j].diskOff+base == diskOff {
				j++
			}
			e := indexEntry{
				seg:     n,
				off:     diskOff,
				length:  batches[k].diskLen,
				diskCRC: batches[k].diskCRC,
				keyed:   batches[k].keyed,
				members: make([]indexMember, 0, j-k),
			}
			for _, pb := range batches[k:j] {
				e.members = append(e.members, indexMember{
					seq: pb.seq, id: pb.id, off: pb.off + base, length: pb.length,
				})
			}
			p.entries = append(p.entries, e)
			extra = append(extra, diskRecord{seg: n, off: diskOff, enc: encodeEntryRecord(e)})
			k = j
		}
		for _, h := range markers {
			ih := indexHole{
				seg: n, off: h.off + base, length: h.length, diskCRC: h.crc,
				seqs: append(make([]uint64, 0, len(h.seqs)), h.seqs...),
			}
			p.holes = append(p.holes, ih)
			extra = append(extra, diskRecord{seg: n, off: ih.off, enc: encodeHoleRecord(ih)})
		}
		if tail != nil {
			keep := make([]uint64, 0, len(tail.seqs))
			for _, seq := range tail.seqs {
				if seq > 0 {
					keep = append(keep, seq)
				}
			}
			if err := l.rewriteTail(n, base+tail.off, keep); err != nil {
				return parsedIndex{}, false
			}
			if len(keep) > 0 {
				marker := encodeHoles(keep)
				crc := binary.LittleEndian.Uint32(marker[len(marker)-4:])
				ih := indexHole{
					seg: n, off: base + tail.off, length: len(marker), diskCRC: crc,
					seqs: append(make([]uint64, 0, len(keep)), keep...),
				}
				p.holes = append(p.holes, ih)
				extra = append(extra, diskRecord{seg: n, off: ih.off, enc: encodeHoleRecord(ih)})
			}
		}
	}

	// Persist the incrementally recovered records so later opens adopt
	// them too. A crash during this append leaves a torn final index
	// record, and the next open simply rebuilds.
	if len(extra) > 0 {
		if err := l.appendAdoptedSuffix(extra); err != nil {
			return parsedIndex{}, false
		}
	}
	return p, true
}

// appendAdoptedSuffix appends incrementally recovered records to the
// sidecar and fsyncs it. Failure means the caller rebuilds instead.
func (l *Log) appendAdoptedSuffix(recs []diskRecord) error {
	sort.SliceStable(recs, func(i, j int) bool {
		if recs[i].seg != recs[j].seg {
			return recs[i].seg < recs[j].seg
		}
		return recs[i].off < recs[j].off
	})
	f, err := os.OpenFile(l.indexPath(), os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	for _, r := range recs {
		if _, err := f.Write(r.enc); err != nil {
			return err
		}
	}
	return f.Sync()
}

// sidecarRec is one physical record the index claims.
type sidecarRec struct {
	seg    int
	off    int
	length int
	crc    uint32
	entry  *indexEntry
	hole   *indexHole
}

// verifySidecar checks an already-framed parse against the physical
// segments without decoding any record body. It returns the last segment
// the index covers and the offset immediately after its records there.
//
// The covered segments must be a prefix of the segment files in the
// directory; within each one records must be strictly ordered, start at
// offset zero and tile without gaps or overlap. Every covered segment
// except the last must end exactly at the file size (nothing
// unindexed); the last one may stop short, leaving a suffix for
// incremental parsing. Every stored disk CRC is recomputed, and entry
// headers (magic and group membership layout) are checked.
func (l *Log) verifySidecar(p parsedIndex, indices []int, sizes map[int]int) (coveredSeg int, coveredEnd int, ok bool) {
	if len(indices) == 0 {
		return 0, 0, false
	}
	recs := make([]sidecarRec, 0, len(p.entries)+len(p.holes))
	for i := range p.entries {
		e := &p.entries[i]
		recs = append(recs, sidecarRec{seg: e.seg, off: e.off, length: e.length, crc: e.diskCRC, entry: e})
	}
	for i := range p.holes {
		h := &p.holes[i]
		recs = append(recs, sidecarRec{seg: h.seg, off: h.off, length: h.length, crc: h.diskCRC, hole: h})
	}
	sort.SliceStable(recs, func(i, j int) bool {
		if recs[i].seg != recs[j].seg {
			return recs[i].seg < recs[j].seg
		}
		return recs[i].off < recs[j].off
	})

	known := make(map[int]bool, len(indices))
	for _, n := range indices {
		known[n] = true
	}
	covered := false
	seenSeq := make(map[uint64]bool)
	seenKey := make(map[string]bool)
	var f *os.File
	openSeg := -1
	closeFile := func() {
		if f != nil {
			f.Close()
			f = nil
			openSeg = -1
		}
	}
	defer closeFile()

	prevSeg, prevEnd := -1, 0
	for i := range recs {
		r := &recs[i]
		if !known[r.seg] || r.off < 0 || r.length < 16 {
			return 0, 0, false
		}
		if r.seg != prevSeg {
			// A new covered segment: the previous one must be tiled to
			// its EOF, and the covered segments must be a prefix with no
			// unindexed segment file in between.
			if prevSeg != -1 && prevEnd != sizes[prevSeg] {
				return 0, 0, false
			}
			if covered {
				next := 0
				found := false
				for _, n := range indices {
					if n > prevSeg {
						next, found = n, true
						break
					}
				}
				if !found || r.seg != next || r.off != 0 {
					return 0, 0, false
				}
			} else {
				if r.seg != indices[0] || r.off != 0 {
					return 0, 0, false
				}
			}
			covered = true
			prevSeg = r.seg
			prevEnd = 0
			closeFile()
			var err error
			f, err = os.Open(l.segmentPath(r.seg))
			if err != nil {
				return 0, 0, false
			}
			openSeg = r.seg
		}
		if openSeg != r.seg || r.off != prevEnd || r.off+r.length > sizes[r.seg] {
			return 0, 0, false
		}
		if !l.verifyDiskRecord(f, r, seenSeq, seenKey) {
			return 0, 0, false
		}
		prevEnd = r.off + r.length
	}
	lastSeg := recs[len(recs)-1].seg
	lastEnd := prevEnd

	// Every segment file before the last covered one must carry at
	// least one index record (no unindexed segment in the middle).
	refSegs := map[int]bool{}
	for i := range recs {
		refSegs[recs[i].seg] = true
	}
	for _, n := range indices {
		if n < lastSeg && !refSegs[n] {
			return 0, 0, false
		}
	}
	return lastSeg, lastEnd, true
}

// verifyDiskRecord checks one index claim against the segment without
// holding the entry in memory: a small header read, a streaming CRC over
// a fixed-size scratch buffer, and small reads of group-member sequences
// and key frames. It never decodes record bodies.
func (l *Log) verifyDiskRecord(f *os.File, r *sidecarRec, seenSeq map[uint64]bool, seenKey map[string]bool) bool {
	var hdr [16]byte
	if _, err := f.ReadAt(hdr[:12], int64(r.off)); err != nil {
		return false
	}
	// Stream the entry's CRC-covered prefix through one reused buffer,
	// then compare against the stored trailing checksum.
	h := crc32.NewIEEE()
	buf := make([]byte, 64*1024)
	remaining := r.length - 4
	at := int64(r.off)
	for remaining > 0 {
		n := len(buf)
		if n > remaining {
			n = remaining
		}
		if _, err := f.ReadAt(buf[:n], at); err != nil {
			return false
		}
		h.Write(buf[:n])
		remaining -= n
		at += int64(n)
	}
	var crcBytes [4]byte
	if _, err := f.ReadAt(crcBytes[:], at); err != nil {
		return false
	}
	stored := binary.LittleEndian.Uint32(crcBytes[:])
	if h.Sum32() != stored || stored != r.crc {
		return false
	}

	switch {
	case r.entry != nil:
		e := r.entry
		switch {
		case bytes.Equal(hdr[:4], segmentMagic):
			if e.keyed || len(e.members) != 1 || e.members[0].id != "" {
				return false
			}
			return len(e.members) == 1 &&
				e.members[0].seq == binary.LittleEndian.Uint64(hdr[4:12]) &&
				e.members[0].off == r.off+4 &&
				e.members[0].length == r.length-8 &&
				markSeen(e.members, seenSeq, nil)
		case bytes.Equal(hdr[:4], keyedMagic):
			if !e.keyed || len(e.members) != 1 {
				return false
			}
			m := e.members[0]
			if m.id == "" ||
				m.seq != binary.LittleEndian.Uint64(hdr[4:12]) ||
				m.off != r.off+4 || m.length != r.length-8 {
				return false
			}
			if _, ok := verifyKeyOnDisk(f, int64(m.off+8), m.id); !ok {
				return false
			}
			return markSeen(e.members, seenSeq, seenKey)
		case bytes.Equal(hdr[:4], groupMagic):
			if e.keyed {
				return false
			}
			if uint32(len(e.members)) != binary.LittleEndian.Uint32(hdr[4:8]) {
				return false
			}
			if !checkMemberLayout(r, e) {
				return false
			}
			for _, m := range e.members {
				if m.id != "" {
					return false
				}
				var seqBytes [8]byte
				if _, err := f.ReadAt(seqBytes[:], int64(m.off)); err != nil {
					return false
				}
				if m.seq != binary.LittleEndian.Uint64(seqBytes[:]) {
					return false
				}
			}
			return markSeen(e.members, seenSeq, nil)
		case bytes.Equal(hdr[:4], keyedGroup):
			if !e.keyed {
				return false
			}
			if uint32(len(e.members)) != binary.LittleEndian.Uint32(hdr[4:8]) {
				return false
			}
			if !checkMemberLayout(r, e) {
				return false
			}
			for _, m := range e.members {
				if _, ok := verifyKeyOnDisk(f, int64(m.off+8), m.id); !ok {
					return false
				}
				var seqBytes [8]byte
				if _, err := f.ReadAt(seqBytes[:], int64(m.off)); err != nil {
					return false
				}
				if m.seq != binary.LittleEndian.Uint64(seqBytes[:]) {
					return false
				}
			}
			return markSeen(e.members, seenSeq, seenKey)
		default:
			return false
		}
	case r.hole != nil:
		h2 := r.hole
		if !bytes.Equal(hdr[:4], holeMagic) ||
			uint32(len(h2.seqs)) != binary.LittleEndian.Uint32(hdr[4:8]) ||
			r.length != 4+4+8*len(h2.seqs)+4 {
			return false
		}
		// The reserved sequences themselves must agree with the marker,
		// not just their count.
		seqBytes := make([]byte, 8*len(h2.seqs))
		if len(seqBytes) > 0 {
			if _, err := f.ReadAt(seqBytes, int64(r.off+8)); err != nil {
				return false
			}
			for i, seq := range h2.seqs {
				if seq == 0 || seenSeq[seq] ||
					seq != binary.LittleEndian.Uint64(seqBytes[8*i:8*i+8]) {
					return false
				}
			}
		}
		return true
	default:
		return false
	}
}

// checkMemberLayout verifies that a group entry's member bodies tile the
// entry's CRC-covered payload with no gaps or overhang, starting right
// after the 8-byte group header.
func checkMemberLayout(r *sidecarRec, e *indexEntry) bool {
	bodyStart := r.off + 8
	for i, m := range e.members {
		rel := m.off - r.off
		if m.length <= 0 || rel < 8 || rel+m.length > r.length-4 {
			return false
		}
		if i == 0 {
			if m.off != bodyStart {
				return false
			}
		} else {
			prev := e.members[i-1]
			if m.off != prev.off+prev.length {
				return false
			}
		}
	}
	return e.members[len(e.members)-1].off+e.members[len(e.members)-1].length == r.off+r.length-4
}

// verifyKeyOnDisk reads the 2-byte key length and key bytes at off and
// checks they name exactly want ("" requires a zero length). It returns
// the on-disk key length so callers can locate the seq that follows.
func verifyKeyOnDisk(f *os.File, off int64, want string) (int, bool) {
	var lenBytes [2]byte
	if _, err := f.ReadAt(lenBytes[:], off); err != nil {
		return 0, false
	}
	klen := int(binary.LittleEndian.Uint16(lenBytes[:]))
	if want == "" {
		return 0, klen == 0
	}
	if klen != len(want) || klen > maxBatchIDLen {
		return 0, false
	}
	raw := make([]byte, klen)
	if _, err := f.ReadAt(raw, off+2); err != nil {
		return 0, false
	}
	if string(raw) != want {
		return 0, false
	}
	return klen, true
}

// markSeen records every member sequence, rejecting 0 and duplicates;
// when seenKey is non-nil it does the same for member idempotency keys.
func markSeen(members []indexMember, seen map[uint64]bool, seenKey map[string]bool) bool {
	for _, m := range members {
		if m.seq == 0 || seen[m.seq] {
			return false
		}
		seen[m.seq] = true
		if seenKey != nil && m.id != "" {
			if seenKey[m.id] {
				return false
			}
			seenKey[m.id] = true
		}
	}
	return true
}

// rebuildFromSegments is the fallback open path: it decodes every
// segment exactly as a historical open did, recovers a torn tail of the
// last segment into a durable hole marker, and finally persists a fresh
// index snapshot. Tampering anywhere but the last segment's tail stays
// ErrCorruptSegment.
func (l *Log) rebuildFromSegments(indices []int) (parsedIndex, error) {
	p, err := l.scanSegments(indices)
	if err != nil {
		return parsedIndex{}, err
	}
	if err := l.writeIndexSnapshot(p); err != nil {
		return parsedIndex{}, err
	}
	return p, nil
}

// scanSegments decodes every listed segment into a canonical, sorted
// parsedIndex, recovering a torn tail of the last listed segment into
// a durable hole marker exactly like rebuildFromSegments. It never
// touches the sidecar, so DeleteThrough can rebuild its in-memory view
// of the retained files and write the snapshot separately.
func (l *Log) scanSegments(indices []int) (parsedIndex, error) {
	var p parsedIndex
	for i, n := range indices {
		data, err := os.ReadFile(l.segmentPath(n))
		if err != nil {
			return parsedIndex{}, err
		}
		batches, markers, tail, err := parseSegment(data)
		if err != nil {
			return parsedIndex{}, err
		}
		if tail != nil && i != len(indices)-1 {
			// A half-written tail is a crash remnant only in the last
			// segment; anywhere else it is tampering.
			return parsedIndex{}, ErrCorruptSegment
		}
		for k := 0; k < len(batches); {
			// Members of one group entry share diskOff; a single commit
			// is an entry with one member.
			diskOff := batches[k].diskOff
			j := k + 1
			for j < len(batches) && batches[j].diskOff == diskOff {
				j++
			}
			e := indexEntry{
				seg:     n,
				off:     batches[k].diskOff,
				length:  batches[k].diskLen,
				diskCRC: batches[k].diskCRC,
				keyed:   batches[k].keyed,
				members: make([]indexMember, 0, j-k),
			}
			for _, pb := range batches[k:j] {
				e.members = append(e.members, indexMember{seq: pb.seq, id: pb.id, off: pb.off, length: pb.length})
			}
			p.entries = append(p.entries, e)
			k = j
		}
		for _, h := range markers {
			p.holes = append(p.holes, indexHole{
				seg: n, off: h.off, length: h.length, diskCRC: h.crc,
				seqs: append(make([]uint64, 0, len(h.seqs)), h.seqs...),
			})
		}
		if tail != nil {
			// Replace the torn entry with a durable hole marker so its
			// reserved sequences stay permanent holes, then index the
			// replacement marker as part of the fresh snapshot.
			keep := make([]uint64, 0, len(tail.seqs))
			for _, seq := range tail.seqs {
				if seq > 0 {
					keep = append(keep, seq)
				}
			}
			if err := l.rewriteTail(n, tail.off, keep); err != nil {
				return parsedIndex{}, err
			}
			if len(keep) > 0 {
				marker := encodeHoles(keep)
				crc := binary.LittleEndian.Uint32(marker[len(marker)-4:])
				p.holes = append(p.holes, indexHole{
					seg: n, off: tail.off, length: len(marker), diskCRC: crc,
					seqs: append(make([]uint64, 0, len(keep)), keep...),
				})
			}
		}
	}
	sortIndexSnapshot(&p)
	return p, nil
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

// AppendIdempotent stages a batch under a caller-chosen key that is
// unique within the log directory, and reserves the next sequence
// number for it, exactly as Append does for anonymous batches. Repeating
// a call with a key already in use is safe across network retries and
// process restarts:
//
//   - If records match the first call in length, order and every byte,
//     the first batch is returned with its original Seq — whether it is
//     still staged or already committed. No new sequence is reserved and
//     nothing is written again.
//   - If the records differ, ErrBatchIDConflict is returned.
//
// An empty key, a key containing a NUL byte, a key that is not valid
// UTF-8, or a key longer than 256 bytes returns ErrInvalidBatchID.
func (l *Log) AppendIdempotent(key string, records [][]byte) (Batch, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return Batch{}, errClosed
	}
	if !validBatchID(key) {
		return Batch{}, ErrInvalidBatchID
	}
	recs := copyRecords(records)
	if seq, ok := l.ids[key]; ok {
		// The first call is either still staged or already committed;
		// compare against the exact bytes it owns so a retry cannot
		// reserve another sequence or write a second entry.
		var prior [][]byte
		if staged, isStaged := l.staged[seq]; isStaged {
			prior = staged
		} else {
			ref, known := l.index[seq]
			if !known {
				return Batch{}, ErrCorruptSegment
			}
			got, id, err := l.readRef(seq, ref)
			if err != nil {
				return Batch{}, err
			}
			if id != key {
				return Batch{}, ErrCorruptSegment
			}
			prior = got
		}
		if !recordsEqual(prior, recs) {
			return Batch{}, ErrBatchIDConflict
		}
		return Batch{seq: seq, id: key, records: copyRecords(prior), owner: l}, nil
	}
	seq := l.nextSeq
	l.nextSeq++
	l.staged[seq] = recs
	l.stagedID[seq] = key
	l.ids[key] = seq
	return Batch{seq: seq, id: key, records: recs, owner: l}, nil
}

// validBatchID reports whether key is an acceptable idempotency key:
// non-empty, valid UTF-8, NUL-free and at most 256 bytes long.
func validBatchID(key string) bool {
	if key == "" || len(key) > maxBatchIDLen {
		return false
	}
	if strings.IndexByte(key, 0) >= 0 {
		return false
	}
	return utf8.ValidString(key)
}

// recordsEqual reports whether two record slices agree in length, order
// and every byte.
func recordsEqual(a, b [][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !bytes.Equal(a[i], b[i]) {
			return false
		}
	}
	return true
}

// Commit makes a staged batch durable and readable, and returns its
// reserved sequence number. Committing a batch that is not staged —
// including one already committed — returns ErrUnknownBatch. With
// Options.Sync a returned commit is durable: the segment entry and the
// index sidecar record are both synced, one fsync each. If either
// fsync fails Commit returns ErrSyncFailed, rolls both files back and
// the batch stays staged, keeping its reserved sequence for a retry
// that cannot produce a duplicate entry.
func (l *Log) Commit(b Batch) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return 0, errClosed
	}
	recs, ok := l.staged[b.seq]
	if !ok || recs == nil || b.owner != l {
		// Not staged here (never reserved, already committed, or a
		// permanent hole — crash-recovered or aborted), or owned by
		// another Log or open.
		return 0, ErrUnknownBatch
	}
	id := l.stagedID[b.seq]
	var entry []byte
	if id != "" {
		entry = encodeKeyedBatch(b.seq, id, recs)
	} else {
		entry = encodeBatch(b.seq, recs)
	}
	// Both single-batch encodings put the indexed body (seq onward)
	// right after the 4-byte magic.
	bodyOff := len(segmentMagic)
	idxBefore := l.idxSize
	off, err := l.writeEntry(entry)
	if err != nil {
		return 0, err
	}
	if l.opts.Sync {
		if err := syncFile(l.file); err != nil {
			// The batch stays staged and keeps its reserved sequence;
			// a later Commit retries. Drop the un-synced entry so the
			// retry cannot duplicate it on disk.
			l.rollbackEntry(off, idxBefore)
			return 0, fmt.Errorf("%w: %v", ErrSyncFailed, err)
		}
	}
	rec := encodeEntryRecord(indexEntry{
		seg:     l.segIndex,
		off:     off,
		length:  len(entry),
		diskCRC: binary.LittleEndian.Uint32(entry[len(entry)-4:]),
		keyed:   id != "",
		members: []indexMember{{
			seq: b.seq, id: id, off: off + bodyOff,
			length: len(entry) - bodyOff - 4,
		}},
	})
	if err := l.appendIndexRecord(rec); err != nil {
		// Index write/fsync failed: the commit is not published. Roll
		// the index record and the segment entry both back so a retry
		// republishes exactly once.
		l.rollbackEntry(off, idxBefore)
		return 0, err
	}
	delete(l.staged, b.seq)
	delete(l.stagedID, b.seq)
	l.gen++
	l.index[b.seq] = entryRef{
		seg: l.segIndex, off: off + bodyOff, length: len(entry) - bodyOff - 4,
		gen: l.gen, id: id, keyed: id != "",
	}
	l.commitSpan(b.seq, b.seq, 1)
	if l.opts.Sync {
		delete(l.segDirty, l.segIndex)
	} else {
		l.segDirty[l.segIndex] = true
	}
	return b.seq, nil
}

// CommitGroup makes several staged batches durable and readable as one
// atomic unit: a single segment write and, with Options.Sync, a single
// segment fsync covering the whole group, followed by one index record
// synced once. Each batch keeps the sequence it reserved at Append, and
// the batches become visible in that reserved order — the merge never
// reorders them. If any batch is not staged — including one already
// committed — nothing is written and the error is ErrUnknownBatch. If
// either fsync fails, the segment entry and the index record are rolled
// back and every batch stays staged with its reserved sequence; the
// error is ErrSyncFailed and retrying the group commits it without
// duplicating entries. A crash can never leave half of the group
// visible: the group is either wholly durable or wholly staged, and
// sequences reserved by a torn group stay permanent holes.
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
	keyed := false
	for i, b := range batches {
		recs, ok := l.staged[b.seq]
		if !ok || recs == nil || b.owner != l {
			// Not staged here — including a permanent hole left by a
			// crash recovery or an abort — or foreign-owned. Every
			// other batch of the group stays staged.
			return nil, ErrUnknownBatch
		}
		if _, dup := seen[b.seq]; dup {
			return nil, ErrUnknownBatch
		}
		seen[b.seq] = struct{}{}
		id := l.stagedID[b.seq]
		if id != "" {
			keyed = true
		}
		members[i] = groupMember{seq: b.seq, id: id, records: recs}
		seqs[i] = b.seq
	}
	entry, offs, lens := encodeGroup(members)
	idxBefore := l.idxSize
	base, err := l.writeEntry(entry)
	if err != nil {
		return nil, err
	}
	if l.opts.Sync {
		if err := syncFile(l.file); err != nil {
			// Every batch stays staged and keeps its reserved sequence;
			// a later CommitGroup retries. Drop the un-synced group
			// entry so the retry cannot duplicate it.
			l.rollbackEntry(base, idxBefore)
			return nil, fmt.Errorf("%w: %v", ErrSyncFailed, err)
		}
	}
	imembers := make([]indexMember, len(members))
	for i, m := range members {
		imembers[i] = indexMember{seq: m.seq, id: m.id, off: base + offs[i], length: lens[i]}
	}
	rec := encodeEntryRecord(indexEntry{
		seg:     l.segIndex,
		off:     base,
		length:  len(entry),
		diskCRC: binary.LittleEndian.Uint32(entry[len(entry)-4:]),
		keyed:   keyed,
		members: imembers,
	})
	if err := l.appendIndexRecord(rec); err != nil {
		// The index never advertises a batch whose commit failed: roll
		// both files back and leave the whole group staged for retry.
		l.rollbackEntry(base, idxBefore)
		return nil, err
	}
	first, last := seqs[0], seqs[0]
	l.gen++
	for i, m := range members {
		delete(l.staged, m.seq)
		delete(l.stagedID, m.seq)
		l.index[m.seq] = entryRef{
			seg: l.segIndex, off: base + offs[i], length: lens[i],
			gen: l.gen, id: m.id, keyed: keyed,
		}
		if m.seq < first {
			first = m.seq
		}
		if m.seq > last {
			last = m.seq
		}
	}
	l.commitSpan(first, last, len(members))
	if l.opts.Sync {
		delete(l.segDirty, l.segIndex)
	} else {
		l.segDirty[l.segIndex] = true
	}
	return seqs, nil
}

// Abort abandons a staged batch — anonymous or keyed, empty or not —
// without committing it, turning its reserved sequence into a permanent
// hole: the sequence reads as ErrNotCommitted, Scan skips it, and no
// later Append ever reuses it, exactly like the hole a crash recovery
// leaves behind. The batch's idempotency key, if any, is released:
// appending under the same key again — with identical or different
// records — reserves a fresh sequence. Every other staged or committed
// batch and every other key is unaffected.
//
// The batch must be one this Log currently holds staged: a zero Batch,
// a batch belonging to another Log or to a previous open, or a batch
// already committed or aborted returns ErrUnknownBatch and changes
// nothing. Commit and CommitGroup reject an aborted batch the same way.
//
// The hole is durable before Abort returns, regardless of Options.Sync:
// a hole marker is appended to the current segment and fsynced, then an
// index sidecar record is appended and fsynced, so the hole and the
// sequence allocation survive restarts and index.idx rebuilds. A write,
// close or sync failure returns ErrAbortFailed; the batch and its key
// stay staged under the original sequence, the partial marker and index
// record are rolled back, and retrying the abort — or committing the
// batch instead — cannot produce a duplicate record. A crash mid-abort
// is recovered by the ordinary torn-tail rules: the recognizable
// aborted sequences stay permanent holes and are never reused, while
// every durable commit is preserved.
func (l *Log) Abort(b Batch) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return errClosed
	}
	recs, ok := l.staged[b.seq]
	if !ok || recs == nil || b.owner != l {
		// Never staged here, already committed, already aborted (a
		// staged hole), or owned by another Log or open: nothing to
		// abandon.
		return ErrUnknownBatch
	}
	id := l.stagedID[b.seq]
	marker := encodeHoles([]uint64{b.seq})
	idxBefore := l.idxSize
	off, err := l.writeEntry(marker)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrAbortFailed, err)
	}
	// The abort is durable regardless of Options.Sync: force the marker
	// to disk before publishing it in the index.
	if err := syncFile(l.file); err != nil {
		l.rollbackEntry(off, idxBefore)
		return fmt.Errorf("%w: %v", ErrAbortFailed, err)
	}
	rec := encodeHoleRecord(indexHole{
		seg:     l.segIndex,
		off:     off,
		length:  len(marker),
		diskCRC: binary.LittleEndian.Uint32(marker[len(marker)-4:]),
		seqs:    []uint64{b.seq},
	})
	if err := l.appendAbortRecord(rec); err != nil {
		// The index never advertises a hole whose abort failed: roll
		// both files back and leave the batch staged for a retry.
		l.rollbackEntry(off, idxBefore)
		return fmt.Errorf("%w: %v", ErrAbortFailed, err)
	}
	// Best effort, as everywhere else: a segment file this marker may
	// have just created gets its directory entry flushed.
	_ = syncDir(l.dir)
	// The sequence is now a permanent hole: staged as nil, exactly like
	// a recovered crash hole, so Read, Scan, AckConsumer and
	// DeleteThrough all treat it the same way.
	l.staged[b.seq] = nil
	delete(l.stagedID, b.seq)
	if id != "" {
		delete(l.ids, id)
	}
	l.recordHole(b.seq)
	return nil
}

// recordHole notes a freshly aborted sequence in the segment listing so
// prefix truncation counts it toward the segment's highest reservation,
// exactly like a recovered crash hole. The marker always lands in the
// current segment, the highest-numbered file, so appending a new
// hole-only entry keeps the list ordered.
func (l *Log) recordHole(seq uint64) {
	for i := range l.segs {
		if l.segs[i].file == l.segIndex {
			if seq > l.segs[i].maxHole {
				l.segs[i].maxHole = seq
			}
			l.segs[i].hasHoles = true
			return
		}
	}
	l.segs = append(l.segs, segInfo{file: l.segIndex, maxHole: seq, hasHoles: true})
}

// Read returns the records of the committed batch with the given
// sequence, located directly through the segment-level index. A staged
// but uncommitted sequence yields ErrNotCommitted; sequence 0,
// never-reserved sequences, and sequences beyond the reserved range
// yield ErrUnknownBatch. A sequence whose segment was reclaimed by
// DeleteThrough yields ErrTruncated — it was committed once and stays
// distinguishable after restarts and index rebuilds.
//
// The lock is held only for the index lookup; the segment file is read
// and the body decoded without it, so a slow read never blocks writers.
func (l *Log) Read(seq uint64) ([][]byte, error) {
	l.mu.RLock()
	if l.closed {
		l.mu.RUnlock()
		return nil, errClosed
	}
	through := l.through
	ref, ok := l.index[seq]
	if !ok {
		_, staged := l.staged[seq]
		l.mu.RUnlock()
		if seq > 0 && seq <= through {
			return nil, ErrTruncated
		}
		if staged {
			return nil, ErrNotCommitted
		}
		return nil, ErrUnknownBatch
	}
	l.mu.RUnlock()
	records, _, err := l.readRef(seq, ref)
	if err != nil && os.IsNotExist(err) {
		// A truncation running while this read held an old ref can
		// remove the file underneath it; re-check under the lock rather
		// than trusting a stale truncation point.
		l.mu.RLock()
		truncated := l.through >= seq
		l.mu.RUnlock()
		if truncated {
			return nil, ErrTruncated
		}
	}
	return records, err
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
	through uint64
	// firstSeq is the lowest committed sequence still on disk. When a
	// truncation stops one segment short (a batch or group straddles
	// the boundary), it can be at or below through; when every segment
	// has been reclaimed it is 0.
	firstSeq uint64
}

// Scan replays committed batches with sequence >= from in increasing
// sequence order, stopping at the first error returned by fn.
//
// The replay is a consistent snapshot: batches committed after the
// replay starts are invisible to it, and a group commit is seen either
// whole or not at all. When from falls inside a prefix reclaimed by
// DeleteThrough, the replay silently starts at the first retained batch;
// because truncation removes whole segments and never splits a batch
// or group, that batch can sit at or below the requested point when a
// group straddled the boundary and its segment was kept. With no
// retained batches it returns nil without a callback. The replay
// streams batches straight from the segment files one sequence at a
// time and keeps only the current batch in memory; it never loads the
// whole replay first. Each sequence lookup takes the lock briefly,
// while the file read, decode and the fn callback all run outside it,
// so a long replay never blocks writers.
func (l *Log) Scan(from uint64, fn func(Batch) error) error {
	l.mu.RLock()
	if l.closed {
		l.mu.RUnlock()
		return errClosed
	}
	snap := scanSnapshot{gen: l.gen, highSeq: l.nextSeq - 1, through: l.through}
	for i := range l.segs {
		if l.segs[i].hasCommits {
			snap.firstSeq = l.segs[i].FirstSeq
			break
		}
	}
	l.mu.RUnlock()

	// No retained committed batches at all: a from inside the deleted
	// prefix ends cleanly with no callback.
	if snap.firstSeq == 0 {
		return nil
	}
	// A from inside the deleted prefix starts no earlier than the first
	// batch still on disk; deleted batches are skipped, never errors, to
	// a range scan. A from that names a retained batch (it can sit at or
	// below through when a group straddled the boundary) is honored
	// exactly, hence max(from, firstSeq).
	seq := from
	if snap.through > 0 && seq < snap.firstSeq {
		seq = snap.firstSeq
	}
	if seq > snap.highSeq {
		return nil
	}
	for {
		// One brief read-lock per sequence: only the index metadata is
		// touched while holding it.
		l.mu.RLock()
		ref, ok := l.index[seq]
		l.mu.RUnlock()
		// Skip holes (reserved but uncommitted sequences) and batches
		// published after the snapshot was taken.
		if ok && ref.gen <= snap.gen {
			records, id, err := l.readRef(seq, ref)
			if err != nil {
				if os.IsNotExist(err) {
					// A concurrent truncation removed this file after the
					// snapshot was taken; the sequence is now history.
					l.mu.RLock()
					truncated := l.through >= seq
					l.mu.RUnlock()
					if truncated {
						if seq == snap.highSeq {
							break
						}
						seq++
						continue
					}
				}
				return err
			}
			if err := fn(Batch{seq: seq, id: id, records: records, owner: l}); err != nil {
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

// Segments lists the retained segments that hold committed batches, in
// file order, each with its first and last committed sequence. Segment
// files reclaimed by DeleteThrough are not listed; a segment that
// carries only a hole marker and no committed batch is not listed
// either.
func (l *Log) Segments() []Segment {
	l.mu.RLock()
	defer l.mu.RUnlock()
	out := make([]Segment, 0, len(l.segs))
	for i := range l.segs {
		if l.segs[i].hasCommits {
			out = append(out, l.segs[i].Segment)
		}
	}
	return out
}

// Close closes the current segment file and the index sidecar. The log
// must not be used afterwards.
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
		if ierr := l.idxFile.Close(); err == nil {
			err = ierr
		}
	}
	return err
}

// readRef loads the records of a committed batch straight from its
// segment file. It acquires no locks itself; the ordinary read path
// calls it without l.mu held so a slow read never blocks writers.
// AppendIdempotent also calls it while holding the write lock: that
// writer is the single writer and the brief locked file read is the
// exact byte-for-byte conflict check. The on-disk bytes are immutable
// once committed (only the torn tail of the very last segment is ever
// rewritten, and that tail never has an index entry), so a ref captured
// under the read lock stays valid afterwards. The returned records are
// freshly decoded, so callers can mutate them without affecting the log
// or later reads. Keyed bodies also return the stored key.
func (l *Log) readRef(seq uint64, ref entryRef) ([][]byte, string, error) {
	f, err := os.Open(l.segmentPath(ref.seg))
	if err != nil {
		return nil, "", err
	}
	defer f.Close()
	body := make([]byte, ref.length)
	if _, err := f.ReadAt(body, int64(ref.off)); err != nil {
		return nil, "", err
	}
	if ref.keyed {
		got, id, records, err := decodeKeyedBody(body)
		if err != nil {
			return nil, "", err
		}
		if got != seq || id != ref.id {
			return nil, "", ErrCorruptSegment
		}
		return records, id, nil
	}
	got, records, err := decodeBody(body)
	if err != nil {
		return nil, "", err
	}
	if got != seq {
		return nil, "", ErrCorruptSegment
	}
	return records, "", nil
}

// commitSpan records n newly committed batches covering sequences
// [first, last] in the segment listing.
func (l *Log) commitSpan(first, last uint64, n int) {
	// After a reopen the current segment file may already be listed as a
	// hole-only segment (a recovered torn tail); the first new commit
	// upgrades that entry rather than adding a second one for the file.
	if l.curBatches == 0 && len(l.segs) > 0 && l.segs[len(l.segs)-1].file == l.segIndex {
		seg := &l.segs[len(l.segs)-1]
		seg.hasCommits = true
		if !seg.hasHoles {
			seg.FirstSeq, seg.LastSeq = first, last
		} else {
			if first < seg.FirstSeq || seg.FirstSeq == 0 {
				seg.FirstSeq = first
			}
			if last > seg.LastSeq {
				seg.LastSeq = last
			}
		}
		l.curBatches += n
		return
	}
	if l.curBatches == 0 {
		l.segs = append(l.segs, segInfo{
			Segment:    Segment{FirstSeq: first, LastSeq: last},
			file:       l.segIndex,
			hasCommits: true,
		})
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

// rollbackEntry undoes a commit that failed after its segment entry was
// written: the segment file is truncated back to segOff and the just
// appended index sidecar record back to idxSize. The batches involved
// remain staged and a retry writes each entry exactly once.
func (l *Log) rollbackEntry(segOff, idxSize int) {
	if l.file != nil {
		l.file.Truncate(int64(segOff))
		l.fileSize = segOff
	}
	l.truncateIndex(idxSize)
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
// written at. A nil l.file also means the previous segment was reclaimed
// by DeleteThrough: in every case segIndex is the last segment number
// that has been used (0 before the first write), and the next file is
// segIndex+1, opened lazily.
func (l *Log) writeEntry(entry []byte) (int, error) {
	if l.file != nil && l.fileSize > 0 && l.fileSize+len(entry) > l.opts.SegmentBytes {
		if err := l.file.Close(); err != nil {
			return 0, err
		}
		l.file = nil
		l.curBatches = 0
	}
	if l.file == nil {
		l.segIndex++
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

// syncDirtySegments forces every segment with committed bytes not yet
// known durable to disk before a checkpoint claims those bytes. The
// current segment is synced through its live append handle; segments
// rolled past are closed and reopened read-only just for the fsync. A
// Sync:false commit may also have created the segment file without the
// directory entry ever being flushed, so the directory is synced too
// (best effort, as everywhere else). Synced files drop out of
// segDirty, so a failed call followed by a retry never duplicates
// meaningful work. A segment that a concurrent DeleteThrough removed is
// simply forgotten — its data is gone by design and needs no sync.
func (l *Log) syncDirtySegments() error {
	if l.file != nil && l.segDirty[l.segIndex] {
		if err := syncFile(l.file); err != nil {
			return fmt.Errorf("%w: %v", ErrCheckpointFailed, err)
		}
		delete(l.segDirty, l.segIndex)
	}
	dirty := make([]int, 0, len(l.segDirty))
	for n := range l.segDirty {
		dirty = append(dirty, n)
	}
	sort.Ints(dirty)
	for _, n := range dirty {
		f, err := os.Open(l.segmentPath(n))
		if err != nil {
			if os.IsNotExist(err) {
				delete(l.segDirty, n)
				continue
			}
			return fmt.Errorf("%w: %v", ErrCheckpointFailed, err)
		}
		serr := syncFile(f)
		cerr := f.Close()
		if serr != nil {
			return fmt.Errorf("%w: %v", ErrCheckpointFailed, serr)
		}
		if cerr != nil {
			return fmt.Errorf("%w: %v", ErrCheckpointFailed, cerr)
		}
		delete(l.segDirty, n)
	}
	_ = syncDir(l.dir)
	return nil
}

func (l *Log) segmentPath(index int) string {
	return filepath.Join(l.dir, fmt.Sprintf("%06d.seg", index))
}

// indexPath is the persistent index sidecar, one file per log directory.
func (l *Log) indexPath() string {
	return filepath.Join(l.dir, "index.idx")
}

// syncDir fsyncs the log directory so a freshly renamed sidecar (or
// created segment file) is durable itself. Best effort on filesystems
// that cannot fsync directories.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	err = d.Sync()
	if cerr := d.Close(); err == nil {
		err = cerr
	}
	return err
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
