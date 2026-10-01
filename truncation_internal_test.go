package log

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func openTruncLog(t *testing.T, dir string, segBytes int) *Log {
	t.Helper()
	l, err := Open(dir, Options{SegmentBytes: segBytes, Sync: false})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { l.Close() })
	return l
}

func commitOne(t *testing.T, l *Log, payload string) uint64 {
	t.Helper()
	b, err := l.Append([][]byte{[]byte(payload)})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	seq, err := l.Commit(b)
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	return seq
}

func TestDeleteThroughMarkerSyncFailureLeavesOldState(t *testing.T) {
	dir := t.TempDir()
	l := openTruncLog(t, dir, 32)
	commitOne(t, l, "a")
	commitOne(t, l, "b")

	orig := syncFile
	defer func() { syncFile = orig }()
	syncFile = func(*os.File) error { return errors.New("marker fsync boom") }

	n, err := l.DeleteThrough(1)
	syncFile = orig
	if !errors.Is(err, ErrRetentionFailed) {
		t.Fatalf("DeleteThrough err = %v, want ErrRetentionFailed", err)
	}
	if n != 0 {
		t.Fatalf("deleted count = %d, want 0 (nothing removed before marker persisted)", n)
	}
	// Full old state, readable.
	if got, err := l.Read(1); err != nil || string(got[0]) != "a" {
		t.Fatalf("Read(1) = %q, %v; want old state intact", got, err)
	}
	if segs := l.Segments(); len(segs) != 2 {
		t.Fatalf("segments = %+v, want 2", segs)
	}
	if files, _ := filepath.Glob(filepath.Join(dir, "*.seg")); len(files) != 2 {
		t.Fatalf("segment files = %v, want both kept", files)
	}
	if _, err := os.Stat(filepath.Join(dir, "truncate.idx")); !os.IsNotExist(err) {
		t.Fatalf("truncate.idx should not exist, stat err = %v", err)
	}
	// Recovery: the same call succeeds once sync heals.
	if n, err := l.DeleteThrough(1); err != nil || n != 1 {
		t.Fatalf("retry DeleteThrough = %d, %v; want 1, nil", n, err)
	}
}

func TestDeleteThroughRemoveFailureConvergesOnReopen(t *testing.T) {
	dir := t.TempDir()
	l := openTruncLog(t, dir, 32)
	for _, s := range []string{"a", "b", "c"} {
		commitOne(t, l, s)
	}

	origRemove := removeFile
	defer func() { removeFile = origRemove }()
	calls := 0
	removeFile = func(name string) error {
		calls++
		if filepath.Base(name) == "000001.seg" {
			return fmt.Errorf("cannot remove")
		}
		return origRemove(name)
	}
	n, err := l.DeleteThrough(1)
	removeFile = origRemove
	if !errors.Is(err, ErrRetentionFailed) {
		t.Fatalf("DeleteThrough err = %v, want ErrRetentionFailed", err)
	}
	if n != 1 {
		t.Fatalf("count = %d, want 1 segment targeted", n)
	}
	// The marker is durable and the process already serves the new view;
	// the surviving retained data stays readable.
	if got, err := l.Read(2); err != nil || string(got[0]) != "b" {
		t.Fatalf("retained Read(2) = %q, %v", got, err)
	}
	if _, err := l.Read(1); !errors.Is(err, ErrTruncated) {
		t.Fatalf("Read(1) = %v, want ErrTruncated", err)
	}
	l.Close()

	// Reopen completes the pending removal: only the full new state.
	l2, err := Open(dir, Options{SegmentBytes: 32})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer l2.Close()
	if _, err := os.Stat(filepath.Join(dir, "000001.seg")); !os.IsNotExist(err) {
		t.Fatalf("pending segment was not removed on reopen")
	}
	if _, err := l2.Read(1); !errors.Is(err, ErrTruncated) {
		t.Fatalf("Read(1) after reopen = %v, want ErrTruncated", err)
	}
	if got, err := l2.Read(3); err != nil || string(got[0]) != "c" {
		t.Fatalf("Read(3) = %q, %v", got, err)
	}
	if segs := l2.Segments(); len(segs) != 2 {
		t.Fatalf("segments = %+v, want 2", segs)
	}
}

func TestCrashAfterMarkerBeforeRemovalCompleted(t *testing.T) {
	dir := t.TempDir()
	l := openTruncLog(t, dir, 32)
	for _, s := range []string{"a", "b", "c", "d"} {
		commitOne(t, l, s)
	}
	// Truncate through 2: marker segBound=2 covers the first two files.
	if n, err := l.DeleteThrough(2); err != nil || n != 2 {
		t.Fatalf("DeleteThrough = %d, %v", n, err)
	}
	l.Close()

	// Simulate a crash that happened after the marker was durably
	// renamed but before the segment removals reached disk by restoring
	// placeholders within the boundary. They are removed on reopen,
	// never indexed.
	if err := os.WriteFile(filepath.Join(dir, "000001.seg"), []byte("junk"), 0o644); err != nil {
		t.Fatal(err)
	}
	l2, err := Open(dir, Options{SegmentBytes: 32})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer l2.Close()
	for _, n := range []string{"000001.seg", "000002.seg"} {
		if _, err := os.Stat(filepath.Join(dir, n)); !os.IsNotExist(err) {
			t.Fatalf("%s survived reopen: %v", n, err)
		}
	}
	if segs := l2.Segments(); len(segs) != 2 || segs[0].FirstSeq != 3 {
		t.Fatalf("segments = %+v, want two retained starting at 3", segs)
	}
	for seq := uint64(1); seq <= 2; seq++ {
		if _, err := l2.Read(seq); !errors.Is(err, ErrTruncated) {
			t.Fatalf("Read(%d) = %v, want ErrTruncated", seq, err)
		}
	}
}

func TestCorruptTruncateMarkerRejected(t *testing.T) {
	dir := t.TempDir()
	l := openTruncLog(t, dir, 32)
	commitOne(t, l, "a")
	commitOne(t, l, "b")
	if n, err := l.DeleteThrough(1); err != nil || n != 1 {
		t.Fatalf("DeleteThrough = %d, %v", n, err)
	}
	l.Close()

	path := filepath.Join(dir, "truncate.idx")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)-1] ^= 0xff // break the checksum
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir, Options{SegmentBytes: 32}); !errors.Is(err, ErrCorruptSegment) {
		t.Fatalf("Open with corrupt marker err = %v, want ErrCorruptSegment", err)
	}
}

func TestReopenAfterCrashRewritesStaleSidecar(t *testing.T) {
	// Simulate the precise crash window: truncate.idx is durable, the
	// deleted segment files are still present, and index.idx still
	// describes them. Reopen must finish the removal and rewrite the
	// sidecar so no stale record survives into adoption, then a second
	// reopen adopts the clean state with identical results.
	dir := t.TempDir()
	opts := Options{SegmentBytes: 32, Sync: true}
	l := openTruncLog(t, dir, 32)
	for _, s := range []string{"a", "b", "c"} {
		commitOne(t, l, s)
	}
	l.Close()

	// Write a marker claiming segs 1 and 2 through seq 2, without
	// deleting the files (the crash window).
	marker := encodeTruncateMarker(truncateMarker{through: 2, segBound: 2})
	if err := os.WriteFile(filepath.Join(dir, "truncate.idx"), marker, 0o644); err != nil {
		t.Fatal(err)
	}

	l2, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	for _, n := range []string{"000001.seg", "000002.seg"} {
		if _, err := os.Stat(filepath.Join(dir, n)); !os.IsNotExist(err) {
			t.Fatalf("%s not removed on crash-completing reopen: %v", n, err)
		}
	}
	if segs := l2.Segments(); len(segs) != 1 || segs[0].FirstSeq != 3 {
		t.Fatalf("segments = %+v, want one starting at 3", segs)
	}
	for seq := uint64(1); seq <= 2; seq++ {
		if _, err := l2.Read(seq); !errors.Is(err, ErrTruncated) {
			t.Fatalf("Read(%d) = %v, want ErrTruncated", seq, err)
		}
	}
	if got, err := l2.Read(3); err != nil || string(got[0]) != "c" {
		t.Fatalf("Read(3) = %q, %v", got, err)
	}
	l2.Close()

	// A second reopen adopts the freshly rewritten sidecar: same view.
	l3, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("second reopen: %v", err)
	}
	defer l3.Close()
	if segs := l3.Segments(); len(segs) != 1 || segs[0].FirstSeq != 3 {
		t.Fatalf("segments after second reopen = %+v, want one starting at 3", segs)
	}
	if got, err := l3.Read(3); err != nil || string(got[0]) != "c" {
		t.Fatalf("Read(3) after second reopen = %q, %v", got, err)
	}
}

func TestTruncationKeepsScanSnapshotStable(t *testing.T) {
	// A scan that starts before a truncation must not suddenly see a
	// batch that was only staged when the snapshot was taken, even
	// though DeleteThrough rebuilds the in-memory index and generations.
	dir := t.TempDir()
	l := openTruncLog(t, dir, 32)
	commitOne(t, l, "a")                           // seq 1, seg 1
	commitOne(t, l, "a2")                          // seq 2, seg 2 (retained)
	staged, err := l.Append([][]byte{[]byte("b")}) // seq 3 reserved, staged
	if err != nil {
		t.Fatalf("Append: %v", err)
	}

	var seen []uint64
	err = l.Scan(1, func(b Batch) error {
		seen = append(seen, b.Seq())
		if b.Seq() == 1 {
			// seq 3 is staged above the truncation point, so this is a
			// valid retention call that reclaims seg 1 and rebuilds the
			// index/generations while the replay is paused.
			if n, err := l.DeleteThrough(1); err != nil || n != 1 {
				t.Fatalf("DeleteThrough inside scan = %d, %v", n, err)
			}
			if _, err := l.Commit(staged); err != nil {
				t.Fatalf("Commit staged after truncation: %v", err)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	// The snapshot was taken while seq 3 was staged: the replay still
	// delivers retained seq 2 (its file survived), but never seq 3,
	// despite the intervening commit and generation rebuild.
	if len(seen) != 2 || seen[0] != 1 || seen[1] != 2 {
		t.Fatalf("scan seen = %v, want [1 2]", seen)
	}
	// A fresh scan sees only seq 3; seq 1 is history and seq 2's file
	// was the retained one, still readable.
	var fresh []uint64
	if err := l.Scan(1, func(b Batch) error { fresh = append(fresh, b.Seq()); return nil }); err != nil {
		t.Fatalf("fresh Scan: %v", err)
	}
	if len(fresh) != 2 || fresh[0] != 2 || fresh[1] != 3 {
		t.Fatalf("fresh scan = %v, want [2 3]", fresh)
	}
}

func TestCommitAfterReopenUpgradesHoleOnlySegment(t *testing.T) {
	// A torn first entry on a brand-new log recovers to a segment that
	// holds a hole marker but no committed batch (curBatches == 0).
	// Reopening and committing into the same file must upgrade that one
	// listing entry, never duplicate it, and the hole stays permanent.
	dir := t.TempDir()
	opts := Options{SegmentBytes: 1 << 20}

	// Fabricate segment 1 containing only a half-written entry for
	// seq 1 (no successful commit ever happened).
	var torn []byte
	torn = append(torn, 'B', 'C', 'L', '1')
	var tmp [8]byte
	binary.LittleEndian.PutUint64(tmp[:], 1)
	torn = append(torn, tmp[:]...)
	binary.LittleEndian.PutUint32(tmp[:4], 1)
	torn = append(torn, tmp[:4]...)
	binary.LittleEndian.PutUint32(tmp[:4], 100)
	torn = append(torn, tmp[:4]...)
	torn = append(torn, []byte("only-a-prefix")...)
	if err := os.WriteFile(filepath.Join(dir, "000001.seg"), torn, 0o644); err != nil {
		t.Fatal(err)
	}

	l, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer l.Close()
	// No public segment yet: the file holds only a hole marker.
	if segs := l.Segments(); len(segs) != 0 {
		t.Fatalf("segments before commit = %+v, want none", segs)
	}
	if _, err := l.Read(1); !errors.Is(err, ErrNotCommitted) {
		t.Fatalf("hole 1: %v", err)
	}
	if l.curBatches != 0 {
		t.Fatalf("curBatches = %d, want 0 (hole-only current segment)", l.curBatches)
	}
	// Commit into the same file: the listing must gain one upgraded
	// entry rather than a duplicate.
	seq := commitOne(t, l, "after-hole")
	if seq != 2 {
		t.Fatalf("seq = %d, want 2", seq)
	}
	if segs := l.Segments(); len(segs) != 1 || segs[0].FirstSeq != 2 || segs[0].LastSeq != 2 {
		t.Fatalf("segments after commit = %+v, want one [2,2]", segs)
	}
	// Internal invariant: exactly one listing entry for the file — the
	// commit upgraded the hole-only entry instead of duplicating it.
	if len(l.segs) != 1 || l.segs[0].file != 1 || !l.segs[0].hasCommits || !l.segs[0].hasHoles {
		t.Fatalf("internal segs = %+v, want one upgraded file-1 entry", l.segs)
	}
	if _, err := l.Read(1); !errors.Is(err, ErrNotCommitted) {
		t.Fatalf("hole 1 after commit: %v", err)
	}
	// And it survives a reopen with the same single listing entry.
	l.Close()
	l2, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("second reopen: %v", err)
	}
	defer l2.Close()
	if segs := l2.Segments(); len(segs) != 1 || segs[0].FirstSeq != 2 {
		t.Fatalf("segments after reopen = %+v, want one starting at 2", segs)
	}
}

func TestTruncateAllThenReopenMarkerRoundTrip(t *testing.T) {
	dir := t.TempDir()
	l := openTruncLog(t, dir, 1<<20)
	commitOne(t, l, "only")
	if n, err := l.DeleteThrough(1); err != nil || n != 1 {
		t.Fatalf("DeleteThrough = %d, %v", n, err)
	}
	l.Close()

	l2, err := Open(dir, Options{SegmentBytes: 1 << 20})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer l2.Close()
	if l2.through != 1 {
		t.Fatalf("through after reopen = %d, want 1", l2.through)
	}
	if segs := l2.Segments(); len(segs) != 0 {
		t.Fatalf("segments = %+v, want none", segs)
	}
	b, err := l2.Append([][]byte{[]byte("next")})
	if err != nil || b.Seq() != 2 {
		t.Fatalf("append after full truncation = %d, %v; want seq 2", b.Seq(), err)
	}
	if _, err := l2.Commit(b); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if l2.segIndex != 2 {
		t.Fatalf("segIndex = %d, want 2 (new file past bound)", l2.segIndex)
	}
}
