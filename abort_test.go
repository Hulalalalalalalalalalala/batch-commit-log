package log_test

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	log "github.com/Hulalalalalalalalalalala/batch-commit-log"
)

func TestAbortAnonymousBatch(t *testing.T) {
	l, err := log.Open(t.TempDir(), log.Options{SegmentBytes: 1 << 20})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer l.Close()

	b, err := l.Append([][]byte{[]byte("x")})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := l.Abort(b); err != nil {
		t.Fatalf("Abort: %v", err)
	}
	// The sequence is a permanent hole: staged-shaped reads, no commit.
	if _, err := l.Read(1); !errors.Is(err, log.ErrNotCommitted) {
		t.Fatalf("Read(1) = %v, want ErrNotCommitted", err)
	}
	if _, err := l.Commit(b); !errors.Is(err, log.ErrUnknownBatch) {
		t.Fatalf("Commit after Abort = %v, want ErrUnknownBatch", err)
	}
	if err := l.Abort(b); !errors.Is(err, log.ErrUnknownBatch) {
		t.Fatalf("second Abort = %v, want ErrUnknownBatch", err)
	}
	// The next append reserves a fresh sequence; the hole is not reused.
	b2, err := l.Append([][]byte{[]byte("y")})
	if err != nil || b2.Seq() != 2 {
		t.Fatalf("Append after Abort = seq %d, %v; want seq 2", b2.Seq(), err)
	}
	if _, err := l.Commit(b2); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	// Scan skips the hole and sees only the committed batch.
	var seqs []uint64
	if err := l.Scan(1, func(b log.Batch) error { seqs = append(seqs, b.Seq()); return nil }); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if !reflect.DeepEqual(seqs, []uint64{2}) {
		t.Fatalf("Scan seqs = %v, want [2]", seqs)
	}
	// The only segment holds one committed batch; a hole does not list.
	segs := l.Segments()
	if len(segs) != 1 || segs[0].FirstSeq != 2 || segs[0].LastSeq != 2 {
		t.Fatalf("Segments = %+v, want [{2 2}]", segs)
	}
}

func TestAbortEmptyRecordsBatch(t *testing.T) {
	l, err := log.Open(t.TempDir(), log.Options{SegmentBytes: 1 << 20})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer l.Close()

	b, err := l.Append(nil)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := l.Abort(b); err != nil {
		t.Fatalf("Abort: %v", err)
	}
	if _, err := l.Read(b.Seq()); !errors.Is(err, log.ErrNotCommitted) {
		t.Fatalf("Read = %v, want ErrNotCommitted", err)
	}
	// A segment carrying only the abort hole is not listed.
	if segs := l.Segments(); len(segs) != 0 {
		t.Fatalf("Segments = %+v, want none for a hole-only segment", segs)
	}
}

func TestAbortKeyedReleasesKey(t *testing.T) {
	l, err := log.Open(t.TempDir(), log.Options{SegmentBytes: 1 << 20})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer l.Close()

	other, err := l.AppendIdempotent("other", [][]byte{[]byte("o")})
	if err != nil {
		t.Fatalf("AppendIdempotent: %v", err)
	}
	b, err := l.AppendIdempotent("k", [][]byte{[]byte("x")})
	if err != nil {
		t.Fatalf("AppendIdempotent: %v", err)
	}
	b2, err := l.AppendIdempotent("k2", [][]byte{[]byte("x")})
	if err != nil {
		t.Fatalf("AppendIdempotent: %v", err)
	}
	if err := l.Abort(b); err != nil {
		t.Fatalf("Abort: %v", err)
	}
	if err := l.Abort(b2); err != nil {
		t.Fatalf("Abort: %v", err)
	}
	// The released keys dedup nothing: identical records get a new
	// sequence, and different records do not conflict either.
	again, err := l.AppendIdempotent("k", [][]byte{[]byte("x")})
	if err != nil || again.Seq() != 4 {
		t.Fatalf("same key same records = seq %d, %v; want seq 4", again.Seq(), err)
	}
	changed, err := l.AppendIdempotent("k2", [][]byte{[]byte("y")})
	if err != nil || changed.Seq() != 5 {
		t.Fatalf("same key different records = seq %d, %v; want seq 5", changed.Seq(), err)
	}
	// The re-staged keys dedup normally from here on.
	if still, err := l.AppendIdempotent("k", [][]byte{[]byte("x")}); err != nil || still.Seq() != 4 {
		t.Fatalf("re-staged key dedup = seq %d, %v; want seq 4", still.Seq(), err)
	}
	// Another batch's key is untouched: same content dedups, different
	// content conflicts.
	still, err := l.AppendIdempotent("other", [][]byte{[]byte("o")})
	if err != nil || still.Seq() != other.Seq() {
		t.Fatalf("other key dedup = seq %d, %v; want seq %d", still.Seq(), err, other.Seq())
	}
	if _, err := l.AppendIdempotent("other", [][]byte{[]byte("z")}); !errors.Is(err, log.ErrBatchIDConflict) {
		t.Fatalf("other key conflict = %v, want ErrBatchIDConflict", err)
	}
}

func TestAbortUnknownBatches(t *testing.T) {
	dir := t.TempDir()
	l, err := log.Open(dir, log.Options{SegmentBytes: 1 << 20})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	staged, err := l.Append([][]byte{[]byte("s")})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	committed, err := l.Append([][]byte{[]byte("c")})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if _, err := l.Commit(committed); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// A zero-value batch.
	if err := l.Abort(log.Batch{}); !errors.Is(err, log.ErrUnknownBatch) {
		t.Fatalf("Abort(zero) = %v, want ErrUnknownBatch", err)
	}
	// A batch owned by another log.
	l2, err := log.Open(t.TempDir(), log.Options{SegmentBytes: 1 << 20})
	if err != nil {
		t.Fatalf("Open other: %v", err)
	}
	defer l2.Close()
	foreign, err := l2.Append([][]byte{[]byte("f")})
	if err != nil {
		t.Fatalf("Append foreign: %v", err)
	}
	if err := l.Abort(foreign); !errors.Is(err, log.ErrUnknownBatch) {
		t.Fatalf("Abort(foreign) = %v, want ErrUnknownBatch", err)
	}
	// An already committed batch.
	if err := l.Abort(committed); !errors.Is(err, log.ErrUnknownBatch) {
		t.Fatalf("Abort(committed) = %v, want ErrUnknownBatch", err)
	}
	l.Close()

	// A batch handed out by a previous open of the same directory.
	l3, err := log.Open(dir, log.Options{SegmentBytes: 1 << 20})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer l3.Close()
	if err := l3.Abort(staged); !errors.Is(err, log.ErrUnknownBatch) {
		t.Fatalf("Abort(previous open) = %v, want ErrUnknownBatch", err)
	}
	// None of the rejections changed the log: the staged batch from the
	// previous open is gone (never persisted), the committed one reads.
	got, err := l3.Read(committed.Seq())
	if err != nil || !reflect.DeepEqual(got, [][]byte{[]byte("c")}) {
		t.Fatalf("Read(%d) = %q, %v", committed.Seq(), got, err)
	}
}

func TestAbortDurableAcrossReopenAndIndexRebuild(t *testing.T) {
	for _, sync := range []bool{false, true} {
		dir := t.TempDir()
		l, err := log.Open(dir, log.Options{SegmentBytes: 1 << 20, Sync: sync})
		if err != nil {
			t.Fatalf("Open(sync=%v): %v", sync, err)
		}
		kb, err := l.AppendIdempotent("k", [][]byte{[]byte("x")})
		if err != nil {
			t.Fatalf("AppendIdempotent: %v", err)
		}
		if err := l.Abort(kb); err != nil {
			t.Fatalf("Abort: %v", err)
		}
		next, err := l.Append([][]byte{[]byte("n")})
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
		if _, err := l.Commit(next); err != nil {
			t.Fatalf("Commit: %v", err)
		}
		l.Close()

		// Reopen: the hole and the sequence allocation survive, and the
		// released key starts a fresh batch.
		check := func(l *log.Log, rebuilt bool) {
			t.Helper()
			if _, err := l.Read(1); !errors.Is(err, log.ErrNotCommitted) {
				t.Fatalf("sync=%v rebuilt=%v Read(1) = %v, want ErrNotCommitted", sync, rebuilt, err)
			}
			var seqs []uint64
			if err := l.Scan(1, func(b log.Batch) error { seqs = append(seqs, b.Seq()); return nil }); err != nil {
				t.Fatalf("Scan: %v", err)
			}
			if !reflect.DeepEqual(seqs, []uint64{2}) {
				t.Fatalf("sync=%v rebuilt=%v Scan = %v, want [2]", sync, rebuilt, seqs)
			}
			fresh, err := l.Append([][]byte{[]byte("m")})
			if err != nil || fresh.Seq() != 3 {
				t.Fatalf("sync=%v rebuilt=%v Append = seq %d, %v; want seq 3", sync, rebuilt, fresh.Seq(), err)
			}
			rel, err := l.AppendIdempotent("k", [][]byte{[]byte("x")})
			if err != nil || rel.Seq() != 4 {
				t.Fatalf("sync=%v rebuilt=%v released key = seq %d, %v; want seq 4", sync, rebuilt, rel.Seq(), err)
			}
		}
		l2, err := log.Open(dir, log.Options{SegmentBytes: 1 << 20, Sync: sync})
		if err != nil {
			t.Fatalf("reopen(sync=%v): %v", sync, err)
		}
		check(l2, false)
		l2.Close()

		// Losing the index sidecar changes nothing: the rebuild recovers
		// the same hole from the segment marker.
		if err := os.Remove(filepath.Join(dir, "index.idx")); err != nil {
			t.Fatalf("remove index: %v", err)
		}
		l3, err := log.Open(dir, log.Options{SegmentBytes: 1 << 20, Sync: sync})
		if err != nil {
			t.Fatalf("rebuild open(sync=%v): %v", sync, err)
		}
		check(l3, true)
		l3.Close()
	}
}

func TestAbortCommitGroupRejectsAbortedMember(t *testing.T) {
	l, err := log.Open(t.TempDir(), log.Options{SegmentBytes: 1 << 20})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer l.Close()

	a, _ := l.Append([][]byte{[]byte("a")})
	b, _ := l.Append([][]byte{[]byte("b")})
	c, _ := l.Append([][]byte{[]byte("c")})
	if err := l.Abort(b); err != nil {
		t.Fatalf("Abort: %v", err)
	}
	// The group fails as a unit; the other members stay staged.
	if _, err := l.CommitGroup([]log.Batch{a, b, c}); !errors.Is(err, log.ErrUnknownBatch) {
		t.Fatalf("CommitGroup = %v, want ErrUnknownBatch", err)
	}
	for _, seq := range []uint64{1, 3} {
		if _, err := l.Read(seq); !errors.Is(err, log.ErrNotCommitted) {
			t.Fatalf("Read(%d) = %v, want ErrNotCommitted (still staged)", seq, err)
		}
	}
	// The surviving members still commit, individually or as a group.
	if _, err := l.CommitGroup([]log.Batch{a, c}); err != nil {
		t.Fatalf("CommitGroup without aborted member: %v", err)
	}
	var seqs []uint64
	l.Scan(1, func(b log.Batch) error { seqs = append(seqs, b.Seq()); return nil })
	if !reflect.DeepEqual(seqs, []uint64{1, 3}) {
		t.Fatalf("Scan = %v, want [1 3]", seqs)
	}
}

func TestAbortConsumerAcksCrossHole(t *testing.T) {
	l, err := log.Open(t.TempDir(), log.Options{SegmentBytes: 1 << 20})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer l.Close()

	b1, _ := l.Append([][]byte{[]byte("1")})
	b2, _ := l.Append([][]byte{[]byte("2")})
	b3, _ := l.Append([][]byte{[]byte("3")})
	if err := l.Abort(b2); err != nil {
		t.Fatalf("Abort: %v", err)
	}
	if _, err := l.Commit(b1); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if _, err := l.Commit(b3); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	// A smaller still-staged batch keeps blocking a later confirmation.
	staged, _ := l.Append([][]byte{[]byte("4")})
	b5, _ := l.Append([][]byte{[]byte("5")})
	if _, err := l.Commit(b5); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if err := l.AckConsumer("c", 0); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := l.AckConsumer("c", 5); !errors.Is(err, log.ErrInvalidAck) {
		t.Fatalf("AckConsumer past staged = %v, want ErrInvalidAck", err)
	}
	// Once the smaller batch is aborted, the consumer crosses the hole.
	if err := l.Abort(staged); err != nil {
		t.Fatalf("Abort staged: %v", err)
	}
	if err := l.AckConsumer("c", 5); err != nil {
		t.Fatalf("AckConsumer across holes: %v", err)
	}
	if seq, err := l.ConsumerSeq("c"); err != nil || seq != 5 {
		t.Fatalf("ConsumerSeq = %d, %v; want 5", seq, err)
	}
	// The hole itself is neither an ack target nor a retention point.
	if err := l.AckConsumer("d", 0); err != nil {
		t.Fatalf("register d: %v", err)
	}
	if err := l.AckConsumer("d", 2); !errors.Is(err, log.ErrInvalidAck) {
		t.Fatalf("AckConsumer(hole) = %v, want ErrInvalidAck", err)
	}
	if err := l.AckConsumer("d", 4); !errors.Is(err, log.ErrInvalidAck) {
		t.Fatalf("AckConsumer(aborted) = %v, want ErrInvalidAck", err)
	}
	if _, err := l.DeleteThrough(2); !errors.Is(err, log.ErrInvalidRetention) {
		t.Fatalf("DeleteThrough(hole) = %v, want ErrInvalidRetention", err)
	}
	if _, err := l.DeleteThrough(4); !errors.Is(err, log.ErrInvalidRetention) {
		t.Fatalf("DeleteThrough(aborted) = %v, want ErrInvalidRetention", err)
	}
}

func TestAbortDoesNotTouchConsumers(t *testing.T) {
	l, err := log.Open(t.TempDir(), log.Options{SegmentBytes: 1 << 20})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer l.Close()

	if err := l.AckConsumer("c", 0); err != nil {
		t.Fatalf("register: %v", err)
	}
	b, _ := l.Append([][]byte{[]byte("x")})
	if err := l.Abort(b); err != nil {
		t.Fatalf("Abort: %v", err)
	}
	if seq, err := l.ConsumerSeq("c"); err != nil || seq != 0 {
		t.Fatalf("ConsumerSeq = %d, %v; want 0", seq, err)
	}
	if _, err := l.ConsumerSeq("never"); !errors.Is(err, log.ErrUnknownConsumer) {
		t.Fatalf("ConsumerSeq(never) = %v, want ErrUnknownConsumer", err)
	}
}

func TestAbortRetentionReclaimsHoleSegment(t *testing.T) {
	dir := t.TempDir()
	// Small segments: seq 1 and the abort hole land in segment 1, the
	// later commits roll into segment 2.
	l, err := log.Open(dir, log.Options{SegmentBytes: 64})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	b1, _ := l.Append([][]byte{[]byte("a")})
	if _, err := l.Commit(b1); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	b2, _ := l.Append([][]byte{[]byte("b")})
	if err := l.Abort(b2); err != nil {
		t.Fatalf("Abort: %v", err)
	}
	b3, _ := l.Append([][]byte{[]byte("c")})
	b4, _ := l.Append([][]byte{[]byte("d")})
	if _, err := l.Commit(b3); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if _, err := l.Commit(b4); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if len(l.Segments()) != 2 {
		t.Fatalf("Segments = %+v, want 2 segments", l.Segments())
	}
	// The hole pins its segment: a truncation point below it reclaims
	// nothing, and the hole itself cannot anchor one.
	if n, err := l.DeleteThrough(1); err != nil || n != 0 {
		t.Fatalf("DeleteThrough(1) = %d, %v; want 0, nil (hole pins segment)", n, err)
	}
	// Reclaim through 3: segment 1 (commit 1 + hole 2) goes, segment 2
	// stays. The reclaimed hole reads as ErrTruncated afterwards.
	if n, err := l.DeleteThrough(3); err != nil || n != 1 {
		t.Fatalf("DeleteThrough(3) = %d, %v; want 1, nil", n, err)
	}
	if _, err := l.Read(2); !errors.Is(err, log.ErrTruncated) {
		t.Fatalf("Read(2) = %v, want ErrTruncated", err)
	}
	l.Close()

	l2, err := log.Open(dir, log.Options{SegmentBytes: 64})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer l2.Close()
	if _, err := l2.Read(2); !errors.Is(err, log.ErrTruncated) {
		t.Fatalf("Read(2) after reopen = %v, want ErrTruncated", err)
	}
	// The reclaimed sequence is never handed out again.
	b5, err := l2.Append([][]byte{[]byte("e")})
	if err != nil || b5.Seq() != 5 {
		t.Fatalf("Append after reopen = seq %d, %v; want seq 5", b5.Seq(), err)
	}
}

func TestAbortClosedLog(t *testing.T) {
	l, err := log.Open(t.TempDir(), log.Options{SegmentBytes: 1 << 20})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	b, _ := l.Append([][]byte{[]byte("x")})
	l.Close()
	abortErr := l.Abort(b)
	if abortErr == nil || abortErr.Error() != "log: closed" {
		t.Fatalf("Abort after Close = %v, want log: closed", abortErr)
	}
	if _, err := l.Append([][]byte{[]byte("y")}); err == nil || err.Error() != abortErr.Error() {
		t.Fatalf("Append after Close = %v, want the same closed error", err)
	}
}

// TestAbortTornMarkerRecovery writes a crash-torn abort marker by hand:
// the recognizable sequence inside it becomes a permanent hole under the
// ordinary tail-recovery rules and is never reused.
func TestAbortTornMarkerRecovery(t *testing.T) {
	dir := t.TempDir()
	l, err := log.Open(dir, log.Options{SegmentBytes: 1 << 20})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	b1, _ := l.Append([][]byte{[]byte("a")})
	if _, err := l.Commit(b1); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	l.Close()

	// A torn BCLH marker at the tail: magic + count + one whole sequence
	// (2), then nothing — the remnant of a crash mid-abort.
	marker := []byte{'B', 'C', 'L', 'H', 1, 0, 0, 0}
	var seqBytes [8]byte
	binary.LittleEndian.PutUint64(seqBytes[:], 2)
	marker = append(marker, seqBytes[:]...)
	f, err := os.OpenFile(filepath.Join(dir, "000001.seg"), os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatalf("open segment: %v", err)
	}
	if _, err := f.Write(marker); err != nil {
		t.Fatalf("write torn marker: %v", err)
	}
	f.Close()

	l2, err := log.Open(dir, log.Options{SegmentBytes: 1 << 20})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer l2.Close()
	if _, err := l2.Read(2); !errors.Is(err, log.ErrNotCommitted) {
		t.Fatalf("Read(2) = %v, want ErrNotCommitted", err)
	}
	got, err := l2.Read(1)
	if err != nil || !reflect.DeepEqual(got, [][]byte{[]byte("a")}) {
		t.Fatalf("Read(1) = %q, %v; durable commit lost", got, err)
	}
	b3, err := l2.Append([][]byte{[]byte("b")})
	if err != nil || b3.Seq() != 3 {
		t.Fatalf("Append = seq %d, %v; want seq 3 (torn abort seq not reused)", b3.Seq(), err)
	}
}

// TestAbortCorruptMarkerIsCorruptSegment appends a complete but
// checksum-bad hole marker: recovery must not treat it as a torn tail.
func TestAbortCorruptMarkerIsCorruptSegment(t *testing.T) {
	dir := t.TempDir()
	l, err := log.Open(dir, log.Options{SegmentBytes: 1 << 20})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	b1, _ := l.Append([][]byte{[]byte("a")})
	if _, err := l.Commit(b1); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	l.Close()

	// A complete BCLH marker for seq 2 with a corrupted sequence byte.
	marker := []byte{'B', 'C', 'L', 'H', 1, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	f, err := os.OpenFile(filepath.Join(dir, "000001.seg"), os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatalf("open segment: %v", err)
	}
	if _, err := f.Write(marker); err != nil {
		t.Fatalf("write marker: %v", err)
	}
	f.Close()

	if _, err := log.Open(dir, log.Options{SegmentBytes: 1 << 20}); !errors.Is(err, log.ErrCorruptSegment) {
		t.Fatalf("Open = %v, want ErrCorruptSegment", err)
	}
}

// TestAbortMarkerRollsToHoleOnlySegment: an abort marker that does not
// fit the current segment rolls into a fresh one, which stays unlisted
// until a commit lands in it, and its hole still bounds retention.
func TestAbortMarkerRollsToHoleOnlySegment(t *testing.T) {
	dir := t.TempDir()
	l, err := log.Open(dir, log.Options{SegmentBytes: 64})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	b1, _ := l.Append([][]byte{[]byte("a")})
	b2, _ := l.Append([][]byte{[]byte("b")})
	if _, err := l.Commit(b1); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if _, err := l.Commit(b2); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	// The marker does not fit segment 1 and rolls into segment 2.
	b3, _ := l.Append([][]byte{[]byte("c")})
	if err := l.Abort(b3); err != nil {
		t.Fatalf("Abort: %v", err)
	}
	segs := l.Segments()
	if len(segs) != 1 || segs[0].FirstSeq != 1 || segs[0].LastSeq != 2 {
		t.Fatalf("Segments = %+v, want [{1 2}] (hole-only segment unlisted)", segs)
	}
	// A commit landing in the hole-only segment lists it.
	b4, _ := l.Append([][]byte{[]byte("d")})
	if _, err := l.Commit(b4); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	segs = l.Segments()
	if len(segs) != 2 || segs[1].FirstSeq != 4 || segs[1].LastSeq != 4 {
		t.Fatalf("Segments = %+v, want [{1 2} {4 4}]", segs)
	}
	// Retention sees the hole reservation in segment 2: reclaiming
	// through 2 removes segment 1 only, and the hole survives.
	if n, err := l.DeleteThrough(2); err != nil || n != 1 {
		t.Fatalf("DeleteThrough(2) = %d, %v; want 1, nil", n, err)
	}
	if _, err := l.Read(3); !errors.Is(err, log.ErrNotCommitted) {
		t.Fatalf("Read(3) = %v, want ErrNotCommitted", err)
	}
	l.Close()

	l2, err := log.Open(dir, log.Options{SegmentBytes: 64})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer l2.Close()
	if _, err := l2.Read(3); !errors.Is(err, log.ErrNotCommitted) {
		t.Fatalf("Read(3) after reopen = %v, want ErrNotCommitted", err)
	}
	segs = l2.Segments()
	if len(segs) != 1 || segs[0].FirstSeq != 4 || segs[0].LastSeq != 4 {
		t.Fatalf("Segments after reopen = %+v, want [{4 4}]", segs)
	}
	b5, err := l2.Append([][]byte{[]byte("e")})
	if err != nil || b5.Seq() != 5 {
		t.Fatalf("Append = seq %d, %v; want seq 5", b5.Seq(), err)
	}
}
