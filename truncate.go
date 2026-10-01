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
)

// Prefix truncation (DeleteThrough) reclaims the oldest whole segments
// once the caller no longer needs their history.
//
// Truncation is deliberately conservative: it never rewrites entries and
// never splits a batch or a group across the retention boundary. A call
// deletes only complete segments forming a prefix of the on-disk segment
// order whose last committed batch sequence is <= seq. A segment
// straddling the boundary (seq lands in its middle) is kept whole, so one
// fewer segment may be reclaimed than the caller hinted at.
//
// Crash safety follows an authoritative-marker discipline. A tiny file
// "truncate.idx" records the truncation point, the reserved-sequence
// frontier and every segment number ever reclaimed. Unlike index.idx it
// is not derived: it is the durable record of which history the caller
// chose to discard, and Open reads it before anything else. Reclamation
// is committed in this order:
//
//  1. fsync a marker temp file and atomically rename it over any older
//     marker, then fsync the directory;
//  2. remove the obsolete segment files and fsync the directory;
//  3. rewrite index.idx as a snapshot of the surviving segments.
//
// A crash at any point is reconciled on the next Open: when the new
// marker is present, segments it records as deleted stay (or become)
// deleted and a missing segment is never mistaken for tampering; when it
// is absent, nothing was committed and the old state is intact. A reopen
// therefore observes either the complete old state or the complete new
// state, never a partial deletion.
//
// Layout, little-endian:
//
//	header       8 bytes  "BCLTRUNC"
//	version      2 bytes  uint16
//	through      8 bytes  last truncation point a caller requested
//	reclaimed    8 bytes  sequence up to which segments were actually
//	                      removed (LastSeq of the last deleted segment;
//	                      <= through because a segment straddling the
//	                      requested point is retained whole)
//	nextSeq      8 bytes  reserved-sequence frontier at truncation
//	deletedSegs  4 bytes  number d of deleted segment file numbers
//	d × 8 bytes           deleted segment file numbers, ascending
//	crc32        4 bytes  IEEE checksum of everything before it
var (
	truncateMagic   = []byte{'B', 'C', 'L', 'T', 'R', 'U', 'N', 'C'}
	truncateVersion = uint16(1)
)

var (
	// ErrTruncated is returned by Read for a sequence whose segment was
	// reclaimed by an earlier DeleteThrough. It stays distinct from
	// ErrNotCommitted and ErrUnknownBatch across restarts and even after
	// index.idx has been rebuilt: the truncation marker, not the sidecar,
	// remembers the history.
	ErrTruncated = errors.New("log: sequence truncated")
	// ErrInvalidRetention is returned by DeleteThrough without changing
	// the directory when seq is not a committed sequence, lies beyond the
	// committed high-water mark, or cannot be honoured at a whole-segment
	// boundary without forgetting a reserved-but-uncommitted sequence.
	ErrInvalidRetention = errors.New("log: invalid retention point")
	// ErrRetentionFailed is returned by DeleteThrough when the truncation
	// point cannot be persisted, synced or fully applied. Before the
	// marker is durable the old state is wholly intact; afterwards every
	// surviving sequence stays readable and the removal is finished on
	// the next open. No call leaves a partly deleted log.
	ErrRetentionFailed = errors.New("log: retention failed")
)

// truncation is the durable prefix-truncation state. The zero value
// (reclaimed == 0) means nothing has ever been truncated.
type truncation struct {
	through     uint64 // last truncation point a caller requested
	reclaimed   uint64 // highest sequence that lived in a removed segment
	nextSeq     uint64 // reserved-sequence frontier at the last truncation
	deletedSegs []int  // every segment number ever removed, ascending
}

func (l *Log) truncMarkerPath() string {
	return filepath.Join(l.dir, "truncate.idx")
}

// encodeTruncation serialises the marker.
func encodeTruncation(t truncation) []byte {
	buf := make([]byte, 0, 8+2+8+8+8+4+8*len(t.deletedSegs)+4)
	buf = append(buf, truncateMagic...)
	var tmp [8]byte
	binary.LittleEndian.PutUint16(tmp[:2], truncateVersion)
	buf = append(buf, tmp[:2]...)
	binary.LittleEndian.PutUint64(tmp[:], t.through)
	buf = append(buf, tmp[:]...)
	binary.LittleEndian.PutUint64(tmp[:], t.reclaimed)
	buf = append(buf, tmp[:]...)
	binary.LittleEndian.PutUint64(tmp[:], t.nextSeq)
	buf = append(buf, tmp[:]...)
	binary.LittleEndian.PutUint32(tmp[:4], uint32(len(t.deletedSegs)))
	buf = append(buf, tmp[:4]...)
	for _, n := range t.deletedSegs {
		binary.LittleEndian.PutUint64(tmp[:], uint64(n))
		buf = append(buf, tmp[:]...)
	}
	binary.LittleEndian.PutUint32(tmp[:4], crc32.ChecksumIEEE(buf))
	return append(buf, tmp[:4]...)
}

// parseTruncation decodes the marker strictly. Any defect is ok=false.
func parseTruncation(data []byte) (truncation, bool) {
	const minLen = 8 + 2 + 8 + 8 + 8 + 4 + 4
	if len(data) < minLen {
		return truncation{}, false
	}
	if !bytes.Equal(data[:8], truncateMagic) {
		return truncation{}, false
	}
	if binary.LittleEndian.Uint16(data[8:10]) != truncateVersion {
		return truncation{}, false
	}
	stored := binary.LittleEndian.Uint32(data[len(data)-4:])
	if crc32.ChecksumIEEE(data[:len(data)-4]) != stored {
		return truncation{}, false
	}
	pos := 10
	through := binary.LittleEndian.Uint64(data[pos : pos+8])
	pos += 8
	reclaimed := binary.LittleEndian.Uint64(data[pos : pos+8])
	pos += 8
	nextSeq := binary.LittleEndian.Uint64(data[pos : pos+8])
	pos += 8
	d := int(binary.LittleEndian.Uint32(data[pos : pos+4]))
	pos += 4
	if d < 0 || pos+8*d != len(data)-4 {
		return truncation{}, false
	}
	segs := make([]int, d)
	prev := 0
	for i := 0; i < d; i++ {
		n := int(binary.LittleEndian.Uint64(data[pos : pos+8]))
		pos += 8
		if n <= 0 || (i > 0 && n <= prev) {
			return truncation{}, false
		}
		segs[i] = n
		prev = n
	}
	if through == 0 || reclaimed == 0 || nextSeq == 0 || reclaimed > through {
		return truncation{}, false
	}
	return truncation{
		through: through, reclaimed: reclaimed, nextSeq: nextSeq, deletedSegs: segs,
	}, true
}

// readTruncation loads the marker. A missing file is the ordinary
// untruncated state; an unreadable or malformed marker is reported rather
// than guessed at, because it is the sole record of discarded history.
func (l *Log) readTruncation() (t truncation, ok bool, err error) {
	data, err := os.ReadFile(l.truncMarkerPath())
	if err != nil {
		if os.IsNotExist(err) {
			return truncation{}, false, nil
		}
		return truncation{}, false, err
	}
	t, ok = parseTruncation(data)
	if !ok {
		return truncation{}, false, ErrCorruptSegment
	}
	return t, true, nil
}

// persistTruncation commits the marker durably: synced temp file, atomic
// rename, directory sync. Any failure leaves the previous marker (or its
// absence) intact.
func (l *Log) persistTruncation(t truncation) error {
	buf := encodeTruncation(t)
	tmp := l.truncMarkerPath() + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(buf); err != nil {
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
	if err := os.Rename(tmp, l.truncMarkerPath()); err != nil {
		os.Remove(tmp)
		return err
	}
	return syncDir(l.dir)
}

// committedSegNumbers returns the physical segment numbers that hold at
// least one committed batch, in ascending order. The listing l.segs is
// in one-to-one order with this slice.
func (l *Log) committedSegNumbers() []int {
	set := make(map[int]struct{})
	for _, r := range l.index {
		set[r.seg] = struct{}{}
	}
	out := make([]int, 0, len(set))
	for n := range set {
		out = append(out, n)
	}
	sort.Ints(out)
	return out
}

// DeleteThrough reclaims the oldest complete segments: every whole
// segment file up to and including the segment of seq whose last
// committed batch sequence is <= seq is deleted, and the number of
// segment files removed is returned. Nothing is rewritten — a batch, a
// group and a hole marker each stay wholly inside one segment, so a
// segment straddling seq is retained whole and one fewer segment may be
// removed.
//
// DeleteThrough(0) changes nothing and returns 0. Otherwise seq must name
// a committed sequence at or below the committed high-water mark: a
// never-reserved, merely staged, or out-of-range sequence returns
// ErrInvalidRetention and deletes nothing. A prefix segment whose hole
// marker reserves a sequence above seq is refused for the same reason,
// since that permanent hole must not be silently forgotten.
//
// The truncation point is persisted before any segment file is removed,
// so Read keeps distinguishing reclaimed sequences (ErrTruncated) from
// unknown ones after a reopen and after index.idx is rebuilt. Keys used
// by removed batches are released and may be claimed by later
// AppendIdempotent calls; retained keys keep their dedup and conflict
// behaviour. A persistence or sync failure returns ErrRetentionFailed
// with the old state fully readable; a crash during deletion is
// reconciled on the next Open into either the complete old or the
// complete new state.
func (l *Log) DeleteThrough(seq uint64) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return 0, errClosed
	}
	if seq == 0 {
		return 0, nil
	}
	// A point a prior truncation already fully reclaimed is a successful
	// no-op, so retries and replays stay idempotent.
	if seq <= l.trunc.reclaimed {
		return 0, nil
	}
	if _, committed := l.index[seq]; !committed {
		return 0, ErrInvalidRetention
	}
	if len(l.segs) == 0 {
		return 0, ErrInvalidRetention
	}
	high := l.segs[0].LastSeq
	for _, s := range l.segs[1:] {
		if s.LastSeq > high {
			high = s.LastSeq
		}
	}
	if seq > high {
		return 0, ErrInvalidRetention
	}

	// Locate the boundary: count whole committed segments whose LastSeq
	// <= seq. The physical files removed are then every surviving on-disk
	// segment up to that file number, which also sweeps any leading file
	// that holds only a recovered hole marker.
	committed := l.committedSegNumbers()
	drop := 0
	for i := range l.segs {
		if l.segs[i].LastSeq > seq {
			break
		}
		drop = i + 1
	}
	if drop == 0 {
		// seq is committed but no whole segment ends at or before it
		// (its own segment straddles the boundary, or earlier segments
		// carry later sequences from out-of-order commits): nothing more
		// is reclaimable at this boundary. Nothing changes.
		return 0, nil
	}
	boundary := committed[drop-1]
	dropSegs := make([]int, 0, drop)
	dropped := make(map[int]bool, drop)
	for _, n := range l.segFiles {
		if n <= boundary {
			dropSegs = append(dropSegs, n)
			dropped[n] = true
		}
	}

	// A batch still staged by a live writer must not be cut off: its
	// sequence is below the retained prefix, so committing it later would
	// resurrect a sequence the truncation advertised as gone. The caller
	// commits or outlives such batches first.
	cutSeq := uint64(0)
	for i := 0; i < drop; i++ {
		if l.segs[i].LastSeq > cutSeq {
			cutSeq = l.segs[i].LastSeq
		}
	}
	for seq := range l.stagedLive {
		if seq <= cutSeq {
			return 0, ErrInvalidRetention
		}
	}

	// Refuse if a discarded segment reserves a hole above the highest
	// sequence that actually leaves the log: that permanent reservation
	// must not be forgotten by the truncation.
	for i := range l.holes {
		h := &l.holes[i]
		if !dropped[h.seg] {
			continue
		}
		for _, s := range h.seqs {
			if s > cutSeq {
				return 0, ErrInvalidRetention
			}
		}
	}

	// 1. Commit the durable intent before touching any segment file.
	// Reclaimed numbers accumulate across truncations; new segments
	// always number above every earlier one. reclaimed is the highest
	// committed sequence that actually lived in a removed segment, which
	// can be below seq when seq's own segment straddles the boundary and
	// is retained.
	newTrunc := truncation{
		through:     seq,
		reclaimed:   max64(l.trunc.reclaimed, cutSeq),
		nextSeq:     l.nextSeq,
		deletedSegs: make([]int, 0, len(l.trunc.deletedSegs)+len(dropSegs)),
	}
	newTrunc.deletedSegs = append(newTrunc.deletedSegs, l.trunc.deletedSegs...)
	newTrunc.deletedSegs = append(newTrunc.deletedSegs, dropSegs...)
	if err := l.persistTruncation(newTrunc); err != nil {
		return 0, fmt.Errorf("%w: %v", ErrRetentionFailed, err)
	}

	// 2. Remove the obsolete segments. The marker already authorises the
	// new state, so a crash or an error here is finished on the next open;
	// every surviving sequence remains readable meanwhile.
	if err := l.removeSegments(dropSegs); err != nil {
		return 0, err
	}

	// 3. Rebuild in-memory state and the index sidecar from survivors.
	if err := l.applyTruncationLocked(newTrunc, dropped); err != nil {
		return 0, fmt.Errorf("%w: %v", ErrRetentionFailed, err)
	}
	return len(dropSegs), nil
}

// removeSegments unlinks the given segment files and syncs the directory
// so the removals are durable. Already-missing files are the expected
// state after a crash midway through deletion.
func (l *Log) removeSegments(nums []int) error {
	for _, n := range nums {
		if err := os.Remove(l.segmentPath(n)); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("%w: %v", ErrRetentionFailed, err)
		}
	}
	if err := syncDir(l.dir); err != nil {
		return fmt.Errorf("%w: %v", ErrRetentionFailed, err)
	}
	return nil
}

// applyTruncationLocked moves the live log to the new state after the
// marker is durable and the segment files are unlinked. It re-derives the
// index, key map, segment listing and hole set from the surviving segment
// files exactly as an Open would (which also rewrites index.idx as a
// survivors-only snapshot), preserving staged batches whose reserved
// sequences lie above the truncation point and the reserved-sequence
// frontier. The open segment handle is replaced only when its own file
// was reclaimed, which can only be the all-segments-deleted case.
func (l *Log) applyTruncationLocked(t truncation, dropped map[int]bool) error {
	survivors := make([]int, 0, len(l.segFiles))
	for _, n := range l.segFiles {
		if !dropped[n] {
			survivors = append(survivors, n)
		}
	}
	currentRemoved := l.file != nil && dropped[l.segIndex]

	// The snapshot rename replaces index.idx on disk; close the append
	// handle first so later records cannot land on the detached inode.
	if l.idxFile != nil {
		l.idxFile.Close()
		l.idxFile = nil
	}

	savedStaged := l.staged
	savedStagedID := l.stagedID
	savedLive := l.stagedLive
	p, err := l.rebuildFromSegments(survivors)
	if err != nil {
		// The new state is durable on disk; reopen the sidecar so the
		// live log stays usable, and let a future Open finish cleanly.
		_ = l.openIndex()
		return err
	}
	l.index = make(map[uint64]entryRef)
	l.ids = make(map[string]uint64)
	l.staged = make(map[uint64][][]byte)
	l.stagedID = make(map[uint64]string)
	l.stagedLive = make(map[uint64]bool)
	l.segs = nil
	l.holes = nil
	l.gen = 0
	sortIndexSnapshot(&p)
	if err := l.installSnapshot(p, survivors); err != nil {
		_ = l.openIndex()
		return err
	}

	// Restore live staged batches whose reservation survives the cut.
	// Permanent holes installed from survivor markers take precedence,
	// and reservations inside the discarded prefix are gone for good.
	for seq, recs := range savedStaged {
		if !savedLive[seq] {
			continue
		}
		if seq <= t.reclaimed {
			continue
		}
		if _, hole := l.staged[seq]; hole {
			continue
		}
		l.staged[seq] = recs
		l.stagedLive[seq] = true
	}
	for seq, key := range savedStagedID {
		if !savedLive[seq] || seq <= t.reclaimed {
			continue
		}
		if _, hole := l.staged[seq]; hole {
			continue
		}
		l.stagedID[seq] = key
		if _, known := l.ids[key]; !known {
			l.ids[key] = seq
		}
	}
	// The frontier recorded by the marker covers reservations (staged
	// batches, permanent holes) that lived in the discarded files.
	if l.nextSeq < t.nextSeq {
		l.nextSeq = t.nextSeq
	}

	l.segFiles = survivors
	l.trunc = t
	if currentRemoved {
		if l.file != nil {
			l.file.Close()
			l.file = nil
		}
		l.fileSize = 0
		l.curBatches = 0
		// A fresh segment numbers strictly above every reclaimed one, so
		// it can never be mistaken for discarded history on reopen.
		l.segIndex = intsMax(t.deletedSegs) + 1
	} else if l.file != nil {
		// Rebuilding may have rewritten a torn tail of the retained
		// current segment; refresh the cached size from disk.
		if st, err := l.file.Stat(); err == nil {
			l.fileSize = int(st.Size())
		}
	}
	return l.openIndex()
}

// reconcileTruncation completes or confirms a committed deletion during
// Open: segment files the marker records as deleted are unlinked if a
// crash left them behind. It returns the physical segment numbers that
// remain, in ascending order.
func (l *Log) reconcileTruncation(t truncation, present []int) ([]int, error) {
	// A temp marker left by a crash before the rename is dead weight.
	_ = os.Remove(l.truncMarkerPath() + ".tmp")
	deleted := make(map[int]bool, len(t.deletedSegs))
	for _, n := range t.deletedSegs {
		deleted[n] = true
	}
	kept := make([]int, 0, len(present))
	changed := false
	for _, n := range present {
		if deleted[n] {
			if err := os.Remove(l.segmentPath(n)); err != nil && !os.IsNotExist(err) {
				return nil, fmt.Errorf("%w: %v", ErrRetentionFailed, err)
			}
			changed = true
			continue
		}
		kept = append(kept, n)
	}
	if changed {
		_ = syncDir(l.dir)
	}
	return kept, nil
}

// intsMax returns the largest of xs, or 0 when empty.
func intsMax(xs []int) int {
	m := 0
	for _, x := range xs {
		if x > m {
			m = x
		}
	}
	return m
}

// max64 returns the larger of a and b.
func max64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}
