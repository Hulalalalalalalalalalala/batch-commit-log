package log

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// TestAbortSyncFailureKeepsBatchStaged fails the segment fsync of an
// abort: the call reports ErrAbortFailed, the marker is rolled back, and
// the batch keeps its sequence and key for a retry.
func TestAbortSyncFailureKeepsBatchStaged(t *testing.T) {
	l, err := Open(t.TempDir(), Options{SegmentBytes: 1 << 20, Sync: false})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer l.Close()

	failing := true
	orig := syncFile
	defer func() { syncFile = orig }()
	syncFile = func(f *os.File) error {
		if failing {
			return errors.New("fsync boom")
		}
		return orig(f)
	}

	b, err := l.AppendIdempotent("k", [][]byte{[]byte("x")})
	if err != nil {
		t.Fatalf("AppendIdempotent: %v", err)
	}
	if err := l.Abort(b); !errors.Is(err, ErrAbortFailed) {
		t.Fatalf("Abort err = %v, want ErrAbortFailed", err)
	}
	// Still staged with its key and sequence; the marker is gone.
	if _, err := l.Read(1); !errors.Is(err, ErrNotCommitted) {
		t.Fatalf("Read after failed abort: %v", err)
	}
	again, err := l.AppendIdempotent("k", [][]byte{[]byte("x")})
	if err != nil || again.Seq() != 1 {
		t.Fatalf("key after failed abort = seq %d, %v; want seq 1", again.Seq(), err)
	}
	if _, err := l.AppendIdempotent("k", [][]byte{[]byte("y")}); !errors.Is(err, ErrBatchIDConflict) {
		t.Fatalf("conflict while still staged = %v, want ErrBatchIDConflict", err)
	}
	if l.fileSize != 0 {
		t.Fatalf("segment size after failed abort = %d, want 0", l.fileSize)
	}
	if recs, ok := l.staged[1]; !ok || recs == nil {
		t.Fatalf("staged[1] = %v, %v; want a live staged batch", recs, ok)
	}

	// The retry aborts for good.
	failing = false
	if err := l.Abort(b); err != nil {
		t.Fatalf("retry Abort: %v", err)
	}
	if _, err := l.Commit(b); !errors.Is(err, ErrUnknownBatch) {
		t.Fatalf("Commit after abort = %v, want ErrUnknownBatch", err)
	}
	fresh, err := l.AppendIdempotent("k", [][]byte{[]byte("x")})
	if err != nil || fresh.Seq() != 2 {
		t.Fatalf("released key = seq %d, %v; want seq 2", fresh.Seq(), err)
	}
}

// TestAbortIndexSyncFailureRollsBack fails the sidecar fsync (the second
// sync of an abort): both files roll back and committing the batch
// instead publishes it exactly once.
func TestAbortIndexSyncFailureRollsBack(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, Options{SegmentBytes: 1 << 20, Sync: false})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer l.Close()

	calls, restore := installFailingSync(t, func(n int) bool { return n == 2 })
	b, err := l.Append([][]byte{[]byte("x")})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := l.Abort(b); !errors.Is(err, ErrAbortFailed) {
		t.Fatalf("Abort err = %v, want ErrAbortFailed", err)
	}
	if *calls < 2 {
		t.Fatalf("expected the sidecar fsync to fail, sync calls=%d", *calls)
	}
	restore()
	if l.fileSize != 0 {
		t.Fatalf("segment size after failed abort = %d, want 0", l.fileSize)
	}

	// Committing instead of retrying the abort works and cannot
	// duplicate: the failed abort left nothing on disk.
	if seq, err := l.Commit(b); err != nil || seq != 1 {
		t.Fatalf("Commit after failed abort = %d, %v", seq, err)
	}
	l.Close()

	l2, err := Open(dir, Options{SegmentBytes: 1 << 20})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer l2.Close()
	var got [][]byte
	count := 0
	l2.Scan(1, func(b Batch) error {
		count++
		got = b.Records()
		return nil
	})
	if count != 1 || !reflect.DeepEqual(got, [][]byte{[]byte("x")}) {
		t.Fatalf("Scan = %d batches %q, want exactly one committed batch", count, got)
	}
}

// TestAbortAlwaysSyncs pins the durability contract: even with
// Options.Sync false an abort fsyncs the segment and the sidecar, and
// the segment's dirty bit drops out of the checkpoint barrier set.
func TestAbortAlwaysSyncs(t *testing.T) {
	l, err := Open(t.TempDir(), Options{SegmentBytes: 1 << 20, Sync: false})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer l.Close()

	// A Sync:false commit leaves the segment dirty; the abort's fsync
	// must cover it too.
	c, _ := l.Append([][]byte{[]byte("c")})
	if _, err := l.Commit(c); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if !l.segDirty[l.segIndex] {
		t.Fatalf("segment not dirty after Sync:false commit")
	}

	calls, restore := installFailingSync(t, func(int) bool { return false })
	defer restore()
	b, _ := l.Append([][]byte{[]byte("x")})
	if err := l.Abort(b); err != nil {
		t.Fatalf("Abort: %v", err)
	}
	if *calls != 2 {
		t.Fatalf("sync calls during Abort = %d, want 2 (segment + sidecar)", *calls)
	}
	if l.segDirty[l.segIndex] {
		t.Fatalf("segment still dirty after abort fsync")
	}
	// In memory the aborted sequence has the same shape as a
	// crash-recovered hole: present in staged with nil records, and the
	// segment carries the hole bound for retention.
	if recs, ok := l.staged[b.Seq()]; !ok || recs != nil {
		t.Fatalf("staged[%d] = %v, %v; want nil hole marker", b.Seq(), recs, ok)
	}
	last := l.segs[len(l.segs)-1]
	if !last.hasHoles || last.maxHole != b.Seq() {
		t.Fatalf("segment hole bound = %+v, want maxHole %d", last, b.Seq())
	}
}

// TestAbortHoleOnlySegmentUpgradedByCommit covers a log whose first
// on-disk record is an abort marker: the segment stays unlisted until a
// commit lands in it, and the reopen view agrees.
func TestAbortHoleOnlySegmentUpgradedByCommit(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, Options{SegmentBytes: 1 << 20})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	b, _ := l.Append([][]byte{[]byte("x")})
	if err := l.Abort(b); err != nil {
		t.Fatalf("Abort: %v", err)
	}
	if segs := l.Segments(); len(segs) != 0 {
		t.Fatalf("Segments = %+v, want none for a hole-only segment", segs)
	}
	c, _ := l.Append([][]byte{[]byte("y")})
	if _, err := l.Commit(c); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	segs := l.Segments()
	if len(segs) != 1 || segs[0].FirstSeq != 2 || segs[0].LastSeq != 2 {
		t.Fatalf("Segments = %+v, want [{2 2}]", segs)
	}
	l.Close()

	l2, err := Open(dir, Options{SegmentBytes: 1 << 20})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer l2.Close()
	segs = l2.Segments()
	if len(segs) != 1 || segs[0].FirstSeq != 2 || segs[0].LastSeq != 2 {
		t.Fatalf("Segments after reopen = %+v, want [{2 2}]", segs)
	}
	if _, err := l2.Read(1); !errors.Is(err, ErrNotCommitted) {
		t.Fatalf("Read(1) after reopen = %v, want ErrNotCommitted", err)
	}
}

// TestAbortHoleRecoveredWhenSidecarLags simulates a crash after the
// abort marker was synced to the segment but before its sidecar record
// landed: the next open adopts the stale sidecar and recovers the hole
// from the segment suffix.
func TestAbortHoleRecoveredWhenSidecarLags(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, Options{SegmentBytes: 1 << 20, Sync: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	c, _ := l.Append([][]byte{[]byte("c")})
	if _, err := l.Commit(c); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	idxBefore := l.idxSize
	b, _ := l.Append([][]byte{[]byte("x")})
	if err := l.Abort(b); err != nil {
		t.Fatalf("Abort: %v", err)
	}
	l.Close()

	// The crash window: the sidecar never got the abort's record.
	if err := os.Truncate(filepath.Join(dir, "index.idx"), int64(idxBefore)); err != nil {
		t.Fatalf("truncate sidecar: %v", err)
	}
	l2, err := Open(dir, Options{SegmentBytes: 1 << 20})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer l2.Close()
	if _, err := l2.Read(2); !errors.Is(err, ErrNotCommitted) {
		t.Fatalf("Read(2) = %v, want ErrNotCommitted", err)
	}
	if recs, ok := l2.staged[2]; !ok || recs != nil {
		t.Fatalf("staged[2] = %v, %v; want nil hole marker", recs, ok)
	}
	next, err := l2.Append([][]byte{[]byte("y")})
	if err != nil || next.Seq() != 3 {
		t.Fatalf("Append = seq %d, %v; want seq 3", next.Seq(), err)
	}
}
