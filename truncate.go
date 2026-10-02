package log

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// The prefix-truncation point lives in its own sidecar
// ("truncate.idx"), deliberately separate from the rebuildable
// index.idx: index.idx is pure cache and may be deleted at any time,
// but the truncation point is the only record that historical
// sequences once existed, so deleting index.idx must not turn
// ErrTruncated into ErrUnknownBatch.
//
// Layout, little-endian:
//
//	magic    6 bytes  "BCLTRN"
//	version  2 bytes  uint16
//	through  8 bytes  highest sequence reclaimed so far
//	segBound 8 bytes  highest segment file number reclaimed so far
//	crc32    4 bytes  IEEE checksum of every preceding byte
//
// The file is replaced atomically (fsynced temp + rename) before any
// segment file is removed, so a crash can only leave the previous
// marker or the new one, never a torn record. Open completes the file
// removal a persisted marker describes, hence a reopen after a crash
// mid-truncation sees either the complete old state or the complete
// new state.
var (
	truncateMagic   = []byte{'B', 'C', 'L', 'T', 'R', 'N'}
	truncateVersion = uint16(1)
)

type truncateMarker struct {
	through  uint64
	segBound uint64
}

const truncateFileLen = 6 + 2 + 8 + 8 + 4

func (l *Log) truncatePath() string {
	return filepath.Join(l.dir, "truncate.idx")
}

// encodeTruncateMarker serialises a truncation point with its trailing
// checksum.
func encodeTruncateMarker(m truncateMarker) []byte {
	buf := make([]byte, 0, truncateFileLen)
	buf = append(buf, truncateMagic...)
	var tmp [8]byte
	binary.LittleEndian.PutUint16(tmp[:2], truncateVersion)
	buf = append(buf, tmp[:2]...)
	binary.LittleEndian.PutUint64(tmp[:], m.through)
	buf = append(buf, tmp[:]...)
	binary.LittleEndian.PutUint64(tmp[:], m.segBound)
	buf = append(buf, tmp[:]...)
	binary.LittleEndian.PutUint32(tmp[:4], crc32.ChecksumIEEE(buf))
	return append(buf, tmp[:4]...)
}

// readTruncateMarker loads the truncation point. No marker (the common
// case for a log that has never been truncated) reports ok=false. A
// present but malformed marker is tampering, like any other framing
// inconsistency: ErrCorruptSegment.
func (l *Log) readTruncateMarker() (truncateMarker, bool, error) {
	path := l.truncatePath()
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return truncateMarker{}, false, nil
	}
	if err != nil {
		return truncateMarker{}, false, err
	}
	if len(data) != truncateFileLen ||
		string(data[:6]) != string(truncateMagic) ||
		binary.LittleEndian.Uint16(data[6:8]) != truncateVersion {
		return truncateMarker{}, false, ErrCorruptSegment
	}
	stored := binary.LittleEndian.Uint32(data[len(data)-4:])
	if crc32.ChecksumIEEE(data[:len(data)-4]) != stored {
		return truncateMarker{}, false, ErrCorruptSegment
	}
	// A stale temp file from a crash before the rename never replaces a
	// good marker; best effort cleanup.
	_ = os.Remove(path + ".tmp")
	return truncateMarker{
		through:  binary.LittleEndian.Uint64(data[8:16]),
		segBound: binary.LittleEndian.Uint64(data[16:24]),
	}, true, nil
}

// writeTruncateMarker durably replaces truncate.idx: the bytes land in
// a synced temp file first, then the rename makes the new point
// atomic. A lost rename leaves the previous marker in place, so the
// operation reads as the old state; syncing the directory is best
// effort and never required for correctness because Open converges.
func (l *Log) writeTruncateMarker(m truncateMarker) error {
	path := l.truncatePath()
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(encodeTruncateMarker(m)); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := syncFile(f); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return err
	}
	_ = syncDir(l.dir)
	return nil
}

// DeleteThrough reclaims the consumed prefix of the log. It removes
// whole segment files only: every complete segment whose highest
// reserved sequence (its last committed batch or its last hole
// marker, whichever is greater) is at or below seq is deleted, and
// the number of removed files is returned. Entries are never
// rewritten, and a batch or group is never split across the boundary:
// when seq falls in the middle of a segment, that segment is
// retained and the truncation stops one segment short.
//
// DeleteThrough(0) changes nothing and returns 0. seq must be a
// committed batch; otherwise — seq is unknown, a permanent hole,
// still staged, or a staged batch reserves a sequence at or below
// seq — DeleteThrough returns ErrInvalidRetention before touching
// anything. A call that finds no fully-reclaimable segment also
// changes nothing and returns 0.
//
// The truncation point is persisted (truncate.idx, atomically
// replaced) before any segment file is removed, so a crash leaves
// either the complete old state or the complete new state: a reopen
// finishes any pending file removal, and historical sequences keep
// reading as ErrTruncated even when index.idx is rebuilt from
// scratch. Idempotency keys that lived in deleted segments are
// released and may be reused; keys in retained segments keep their
// same-key dedup and different-key ErrBatchIDConflict.
//
// Registered consumers protect the prefix they have not replayed: if
// any committed batch in the otherwise-deletable prefix is above any
// consumer's acknowledged point, DeleteThrough returns
// ErrRetentionBlocked before the marker is written, so files, the
// index and idempotency keys all stay put. Once every consumer has
// acknowledged the prefix (or has been dropped), reclamation proceeds
// exactly as above; with no consumers registered the behavior is
// unchanged.
func (l *Log) DeleteThrough(seq uint64) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return 0, errClosed
	}
	if seq == 0 {
		return 0, nil
	}
	if _, ok := l.index[seq]; !ok {
		// Unknown, a permanent hole, or merely staged: only a sequence
		// with a committed batch may anchor a truncation.
		return 0, ErrInvalidRetention
	}
	// An uncommitted staged batch at or below seq would be reclaimed
	// together with the history it is logically still ahead of; its
	// reservation has to outlive the truncation.
	for s, recs := range l.staged {
		if recs != nil && s <= seq {
			return 0, ErrInvalidRetention
		}
	}

	physical, err := listSegmentNumbers(l.dir)
	if err != nil {
		return 0, err
	}
	boundOf := make(map[int]uint64, len(l.segs))
	for i := range l.segs {
		s := &l.segs[i]
		bound := uint64(0)
		if s.hasCommits {
			bound = s.LastSeq
		}
		if s.hasHoles && s.maxHole > bound {
			bound = s.maxHole
		}
		boundOf[s.file] = bound
	}
	// Segment files absent from l.segs are empty (a torn first entry
	// truncated to zero); their bound is 0. Take the contiguous prefix
	// whose every bound is at or below seq — the first segment that
	// overhangs seq ends the prefix, along with everything after it.
	var deleted []int
	for _, n := range physical {
		if boundOf[n] > seq {
			break
		}
		deleted = append(deleted, n)
	}
	if len(deleted) == 0 {
		return 0, nil
	}
	segBound := deleted[len(deleted)-1]
	retained := physical[len(deleted):]

	// Registered consumers pin the history they have not acknowledged
	// yet. When the reclaimable prefix carries any committed batch above
	// the slowest consumer's point, the whole call is refused before
	// anything (marker, files, index or idempotency keys) changes. Hole
	// markers do not count: only committed batches do, and the segments'
	// LastSeq is exactly the highest committed sequence of each file.
	if len(l.consumers) > 0 {
		var blocked uint64
		for i := range l.segs {
			s := &l.segs[i]
			if s.file > segBound {
				break
			}
			if s.hasCommits && s.LastSeq > blocked {
				blocked = s.LastSeq
			}
		}
		for _, ack := range l.consumers {
			if blocked > ack {
				return 0, ErrRetentionBlocked
			}
		}
	}

	// 1) Durably commit the truncation point before removing anything.
	// A failure here leaves the full old state byte-for-byte.
	if err := l.writeTruncateMarker(truncateMarker{through: seq, segBound: uint64(segBound)}); err != nil {
		return 0, fmt.Errorf("%w: %v", ErrRetentionFailed, err)
	}

	// 2) Close the open current segment when it is being removed.
	currentDeleted := false
	for _, n := range deleted {
		if n == l.segIndex {
			currentDeleted = true
			break
		}
	}
	if currentDeleted && l.file != nil {
		l.file.Close()
		l.file = nil
		l.fileSize = 0
		// The next write opens segBound+1; segment numbers are never
		// reused, even when every file has been reclaimed. Reset the
		// write-side counters now so no later path can commit into the
		// stale segment view.
		l.segIndex = segBound
		l.curBatches = 0
	}

	// 3) Remove the old segment files. The marker already owns the
	// outcome: a failure converges on reopen, so keep going to reach
	// the complete new state in this process too.
	var firstErr error
	for _, n := range deleted {
		if err := removeFile(l.segmentPath(n)); err != nil && !os.IsNotExist(err) && firstErr == nil {
			firstErr = err
		}
	}
	_ = syncDir(l.dir)

	// 4) Rebuild the in-memory view from exactly the retained files,
	// preserving live staged batches (all of which reserve sequences
	// above seq, checked above) and their keys. Scanning also recovers
	// a torn tail of the last retained segment as usual.
	live := make(map[uint64][][]byte)
	liveIDs := make(map[uint64]string)
	for s, recs := range l.staged {
		if recs != nil {
			live[s] = recs
			if id := l.stagedID[s]; id != "" {
				liveIDs[s] = id
			}
		}
	}
	// Retained batches keep their original publication generations, so
	// a Scan snapshot taken before the truncation keeps seeing exactly
	// the retained batches it could already see (their files survive),
	// while every post-truncation commit lands above oldGen and stays
	// invisible to it — the same snapshot guarantee as always.
	oldGen := l.gen
	oldGenBySeq := make(map[uint64]uint64, len(l.index))
	for s, ref := range l.index {
		oldGenBySeq[s] = ref.gen
	}
	p, scanErr := l.scanSegments(retained)
	if scanErr != nil {
		// The retained files are unreadable: do not adopt a partial
		// view. The old state is already partly deleted on disk, but the
		// durable marker makes the outcome converge on the next open;
		// surface the failure and leave index state untouched.
		return len(deleted), fmt.Errorf("%w: %v", ErrRetentionFailed, scanErr)
	}
	l.index = make(map[uint64]entryRef)
	l.ids = make(map[string]uint64)
	l.staged = make(map[uint64][][]byte)
	l.stagedID = make(map[uint64]string)
	l.segs = nil
	l.gen = 0
	l.nextSeq = 1
	l.through = seq
	if err := l.installSnapshot(p, retained); err != nil && firstErr == nil {
		firstErr = err
	}
	for s := range l.index {
		if g, ok := oldGenBySeq[s]; ok {
			ref := l.index[s]
			ref.gen = g
			l.index[s] = ref
		}
	}
	// Generations are an in-process monotonic counter; resume after the
	// highest generation ever handed out so new commits never collide
	// with a pre-truncation snapshot.
	l.gen = oldGen
	for s, recs := range live {
		l.staged[s] = recs
		if id := liveIDs[s]; id != "" {
			l.stagedID[s] = id
			// A staged keyed batch survives the truncation; its key
			// must keep resolving to its reserved sequence for retries.
			l.ids[id] = s
		}
		if s >= l.nextSeq {
			l.nextSeq = s + 1
		}
	}

	// 5) Replace the sidecar with a snapshot of the retained segments
	// and reopen the live append handle. A failure here does not lose
	// data: the retained files are the source of truth and a stale or
	// missing sidecar is rebuilt on the next open. The process keeps
	// serving from the in-memory view just installed.
	if l.idxFile != nil {
		l.idxFile.Close()
		l.idxFile = nil
	}
	if err := l.writeIndexSnapshot(p); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := l.openIndex(); err != nil && firstErr == nil {
		firstErr = err
	}
	if firstErr != nil {
		return len(deleted), fmt.Errorf("%w: %v", ErrRetentionFailed, firstErr)
	}
	return len(deleted), nil
}

// listSegmentNumbers returns the numeric names of the *.seg files in
// dir, sorted ascending.
func listSegmentNumbers(dir string) ([]int, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var nums []int
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".seg") {
			continue
		}
		n, err := strconv.Atoi(strings.TrimSuffix(e.Name(), ".seg"))
		if err != nil {
			continue
		}
		nums = append(nums, n)
	}
	sort.Ints(nums)
	return nums, nil
}
