package log_test

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	log "github.com/Hulalalalalalalalalalala/batch-commit-log"
)

func TestDeleteThroughZeroIsNoOp(t *testing.T) {
	dir := t.TempDir()
	l := open(t, dir, log.Options{SegmentBytes: 32, Sync: true})
	commit(t, l, []byte("a"))
	commit(t, l, []byte("b"))

	before, _ := filepath.Glob(filepath.Join(dir, "*"))
	n, err := l.DeleteThrough(0)
	if err != nil || n != 0 {
		t.Fatalf("DeleteThrough(0) = %d, %v; want 0, nil", n, err)
	}
	after, _ := filepath.Glob(filepath.Join(dir, "*"))
	sort.Strings(before)
	sort.Strings(after)
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("directory changed: before=%v after=%v", before, after)
	}
	if segs := l.Segments(); len(segs) != 2 {
		t.Fatalf("segments = %+v, want 2", segs)
	}
}

func TestDeleteThroughRemovesWholeSegmentPrefix(t *testing.T) {
	dir := t.TempDir()
	l := open(t, dir, log.Options{SegmentBytes: 32, Sync: true})
	commit(t, l, []byte("one"))
	commit(t, l, []byte("two"))
	commit(t, l, []byte("three"))

	n, err := l.DeleteThrough(1)
	if err != nil {
		t.Fatalf("DeleteThrough: %v", err)
	}
	if n != 1 {
		t.Fatalf("deleted = %d, want 1", n)
	}
	files, _ := filepath.Glob(filepath.Join(dir, "*.seg"))
	sort.Strings(files)
	if len(files) != 2 {
		t.Fatalf("segment files = %v, want 2", files)
	}
	if segs := l.Segments(); len(segs) != 2 || segs[0].FirstSeq != 2 {
		t.Fatalf("segments = %+v, want 2 starting at 2", segs)
	}
	if _, err := l.Read(1); !errors.Is(err, log.ErrTruncated) {
		t.Fatalf("Read(1) err = %v, want ErrTruncated", err)
	}
	if got, err := l.Read(2); err != nil || string(got[0]) != "two" {
		t.Fatalf("Read(2) = %q, %v", got, err)
	}
}

func TestDeleteThroughMidSegmentDeletesOneFewer(t *testing.T) {
	dir := t.TempDir()
	// One large segment holds three batches.
	l := open(t, dir, log.Options{SegmentBytes: 1 << 20, Sync: true})
	commit(t, l, []byte("a"))
	commit(t, l, []byte("b"))
	commit(t, l, []byte("c"))

	// seq 2 sits in the middle of the only segment: nothing is a fully
	// reclaimable prefix, so nothing is deleted.
	n, err := l.DeleteThrough(2)
	if err != nil {
		t.Fatalf("DeleteThrough(2): %v", err)
	}
	if n != 0 {
		t.Fatalf("deleted = %d, want 0 (boundary keeps the segment)", n)
	}
	if got, err := l.Read(1); err != nil || string(got[0]) != "a" {
		t.Fatalf("Read(1) = %q, %v; want still readable", got, err)
	}

	// Deleting to the segment's last committed sequence reclaims it all.
	n, err = l.DeleteThrough(3)
	if err != nil || n != 1 {
		t.Fatalf("DeleteThrough(3) = %d, %v; want 1, nil", n, err)
	}
	if segs := l.Segments(); len(segs) != 0 {
		t.Fatalf("segments = %+v, want none", segs)
	}
}

func TestDeleteThroughNeverSplitsGroup(t *testing.T) {
	// Small capacity puts one batch in seg 1 and a three-member group
	// (seqs 2,3,4) in seg 2. Truncating through seq 3 cannot split the
	// group: seg 2's highest committed sequence is 4, so only seg 1 goes.
	dir := t.TempDir()
	l := open(t, dir, log.Options{SegmentBytes: 64, Sync: true})
	commit(t, l, []byte("one")) // seg 1: seq 1
	var bs []log.Batch
	for _, s := range []string{"g1", "g2", "g3"} {
		b, _ := l.Append([][]byte{[]byte(s)})
		bs = append(bs, b)
	}
	if _, err := l.CommitGroup(bs); err != nil {
		t.Fatalf("CommitGroup: %v", err)
	} // seg 2: seqs 2,3,4

	// Truncating through seq 3 cannot split the group: seg 2's highest
	// committed sequence is 4, so only seg 1 goes.
	n, err := l.DeleteThrough(3)
	if err != nil {
		t.Fatalf("DeleteThrough(3): %v", err)
	}
	if n != 1 {
		t.Fatalf("deleted = %d, want 1 (group kept whole)", n)
	}
	for seq, want := range map[uint64]string{2: "g1", 3: "g2", 4: "g3"} {
		got, err := l.Read(seq)
		if err != nil || string(got[0]) != want {
			t.Fatalf("group member %d = %q, %v; want %q", seq, got, err, want)
		}
	}
}

func TestDeleteThroughInvalidRetention(t *testing.T) {
	dir := t.TempDir()
	l := open(t, dir, log.Options{SegmentBytes: 32, Sync: true})
	commit(t, l, []byte("a"))
	commit(t, l, []byte("b"))

	cases := []struct {
		name string
		seq  uint64
	}{
		{"never reserved", 7},
		{"past high water", 1 << 20},
		{"staged batch", func() uint64 {
			b, _ := l.Append([][]byte{[]byte("pending")})
			return b.Seq()
		}()},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			files, _ := filepath.Glob(filepath.Join(dir, "*.seg"))
			if _, err := l.DeleteThrough(c.seq); !errors.Is(err, log.ErrInvalidRetention) {
				t.Fatalf("DeleteThrough(%d) err = %v, want ErrInvalidRetention", c.seq, err)
			}
			after, _ := filepath.Glob(filepath.Join(dir, "*.seg"))
			sort.Strings(files)
			sort.Strings(after)
			if !reflect.DeepEqual(after, files) {
				t.Fatalf("files changed on rejection: %v -> %v", files, after)
			}
		})
	}

	// An out-of-order commit leaves a staged batch below the target:
	// its reservation blocks truncation.
	dir = t.TempDir()
	l = open(t, dir, log.Options{SegmentBytes: 1 << 20, Sync: true})
	b1, _ := l.Append([][]byte{[]byte("first")})
	b2, _ := l.Append([][]byte{[]byte("second")})
	if _, err := l.Commit(b2); err != nil { // commit seq 2 first
		t.Fatalf("Commit: %v", err)
	}
	if _, err := l.DeleteThrough(2); !errors.Is(err, log.ErrInvalidRetention) {
		t.Fatalf("DeleteThrough with staged seq 1 below: %v, want ErrInvalidRetention", err)
	}
	if _, err := l.Commit(b1); err != nil {
		t.Fatalf("Commit b1: %v", err)
	}
	if n, err := l.DeleteThrough(2); err != nil || n != 1 {
		t.Fatalf("DeleteThrough(2) after both committed = %d, %v; want 1", n, err)
	}
}

func TestDeleteThroughHoleSegmentNotReclaimed(t *testing.T) {
	dir := t.TempDir()
	opts := log.Options{SegmentBytes: 1 << 20}
	l := open(t, dir, opts)
	commit(t, l, []byte("one"))
	commit(t, l, []byte("two"))
	l.Close()

	// A torn tail reserving seq 3 is recovered into a permanent hole in
	// the last segment; that segment's highest reservation is 3, so
	// truncating through 2 must retain it.
	appendTornEntry(t, dir, 3)
	l = open(t, dir, opts)
	if _, err := l.Read(3); !errors.Is(err, log.ErrNotCommitted) {
		t.Fatalf("hole 3: %v", err)
	}
	n, err := l.DeleteThrough(2)
	if err != nil {
		t.Fatalf("DeleteThrough(2): %v", err)
	}
	if n != 0 {
		t.Fatalf("deleted = %d, want 0 (hole 3 pins the segment)", n)
	}
	// Truncating through the hole itself is invalid: the hole is not a
	// committed sequence.
	if _, err := l.DeleteThrough(3); !errors.Is(err, log.ErrInvalidRetention) {
		t.Fatalf("DeleteThrough(hole) err = %v, want ErrInvalidRetention", err)
	}
}

func TestTruncatedDistinctFromUnknownAcrossReopens(t *testing.T) {
	dir := t.TempDir()
	opts := log.Options{SegmentBytes: 32, Sync: true}
	l := open(t, dir, opts)
	for _, s := range []string{"a", "b", "c"} {
		commit(t, l, []byte(s))
	}
	if n, err := l.DeleteThrough(1); err != nil || n != 1 {
		t.Fatalf("DeleteThrough = %d, %v", n, err)
	}
	l.Close()

	check := func(t *testing.T, l *log.Log) {
		t.Helper()
		if _, err := l.Read(1); !errors.Is(err, log.ErrTruncated) {
			t.Fatalf("Read(1) err = %v, want ErrTruncated", err)
		}
		if _, err := l.Read(0); !errors.Is(err, log.ErrUnknownBatch) {
			t.Fatalf("Read(0) err = %v, want ErrUnknownBatch", err)
		}
		if _, err := l.Read(99); !errors.Is(err, log.ErrUnknownBatch) {
			t.Fatalf("Read(99) err = %v, want ErrUnknownBatch", err)
		}
		if got, err := l.Read(2); err != nil || string(got[0]) != "b" {
			t.Fatalf("Read(2) = %q, %v", got, err)
		}
	}

	l = open(t, dir, opts)
	check(t, l)
	l.Close()

	// Delete the rebuildable sidecar: historical sequences must still
	// read as ErrTruncated, never ErrUnknownBatch.
	if err := os.Remove(filepath.Join(dir, "index.idx")); err != nil {
		t.Fatal(err)
	}
	l = open(t, dir, opts)
	check(t, l)
}

func TestScanFromDeletedPrefixStartsAtRetained(t *testing.T) {
	dir := t.TempDir()
	l := open(t, dir, log.Options{SegmentBytes: 32, Sync: true})
	for _, s := range []string{"a", "b", "c", "d"} {
		commit(t, l, []byte(s))
	}
	if _, err := l.DeleteThrough(2); err != nil {
		t.Fatalf("DeleteThrough: %v", err)
	}

	for _, from := range []uint64{0, 1, 2} {
		var seqs []uint64
		if err := l.Scan(from, func(b log.Batch) error {
			seqs = append(seqs, b.Seq())
			return nil
		}); err != nil {
			t.Fatalf("Scan(%d): %v", from, err)
		}
		if !reflect.DeepEqual(seqs, []uint64{3, 4}) {
			t.Fatalf("Scan(%d) = %v, want [3 4]", from, seqs)
		}
	}

	// Scanning a wholly-retained range still works.
	var seqs []uint64
	if err := l.Scan(4, func(b log.Batch) error { seqs = append(seqs, b.Seq()); return nil }); err != nil {
		t.Fatalf("Scan(4): %v", err)
	}
	if !reflect.DeepEqual(seqs, []uint64{4}) {
		t.Fatalf("Scan(4) = %v, want [4]", seqs)
	}
}

func TestScanStraddlingBoundaryGroupSeesWholeGroup(t *testing.T) {
	// seg 1 holds seq 1; seg 2 holds one group [2,3,4]. Truncating
	// through seq 3 keeps seg 2 whole (one segment fewer deleted). A
	// Scan whose from is inside the deleted prefix must still replay
	// the surviving group members 2,3,4 in full — truncation never
	// splits the group for readers either.
	dir := t.TempDir()
	l := open(t, dir, log.Options{SegmentBytes: 64, Sync: true})
	commit(t, l, []byte("one"))
	var bs []log.Batch
	for _, s := range []string{"g1", "g2", "g3"} {
		b, _ := l.Append([][]byte{[]byte(s)})
		bs = append(bs, b)
	}
	if _, err := l.CommitGroup(bs); err != nil {
		t.Fatalf("CommitGroup: %v", err)
	}
	if n, err := l.DeleteThrough(3); err != nil || n != 1 {
		t.Fatalf("DeleteThrough(3) = %d, %v; want 1", n, err)
	}
	// from in the deleted prefix (1) jumps onto the first retained
	// batch; from naming a retained member (2 or 3, both at or below
	// the truncation point because the group straddled it) is honored
	// exactly as a lower bound.
	cases := []struct {
		from uint64
		want []uint64
	}{
		{1, []uint64{2, 3, 4}},
		{2, []uint64{2, 3, 4}},
		{3, []uint64{3, 4}},
		{4, []uint64{4}},
	}
	for _, c := range cases {
		var seqs []uint64
		if err := l.Scan(c.from, func(b log.Batch) error {
			seqs = append(seqs, b.Seq())
			return nil
		}); err != nil {
			t.Fatalf("Scan(%d): %v", c.from, err)
		}
		if !reflect.DeepEqual(seqs, c.want) {
			t.Fatalf("Scan(%d) = %v, want %v", c.from, seqs, c.want)
		}
	}

	// The same survives a reopen (and an index rebuild).
	l.Close()
	if err := os.Remove(filepath.Join(dir, "index.idx")); err != nil {
		t.Fatal(err)
	}
	l = open(t, dir, log.Options{SegmentBytes: 64, Sync: true})
	defer l.Close()
	var seqs []uint64
	if err := l.Scan(1, func(b log.Batch) error { seqs = append(seqs, b.Seq()); return nil }); err != nil {
		t.Fatalf("Scan after rebuild: %v", err)
	}
	if !reflect.DeepEqual(seqs, []uint64{2, 3, 4}) {
		t.Fatalf("scan after rebuild = %v, want [2 3 4]", seqs)
	}
}

func TestScanAfterFullTruncationEndsCleanly(t *testing.T) {
	dir := t.TempDir()
	l := open(t, dir, log.Options{SegmentBytes: 1 << 20, Sync: true})
	commit(t, l, []byte("a"))
	commit(t, l, []byte("b"))
	if n, err := l.DeleteThrough(2); err != nil || n != 1 {
		t.Fatalf("DeleteThrough = %d, %v", n, err)
	}
	calls := 0
	if err := l.Scan(1, func(log.Batch) error { calls++; return nil }); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if calls != 0 {
		t.Fatalf("scan callbacks = %d, want 0", calls)
	}
}

func TestTruncateThenAppendContinuesSequence(t *testing.T) {
	dir := t.TempDir()
	opts := log.Options{SegmentBytes: 32, Sync: true}
	l := open(t, dir, opts)
	for _, s := range []string{"a", "b", "c"} {
		commit(t, l, []byte(s))
	}
	if _, err := l.DeleteThrough(3); err != nil {
		t.Fatalf("DeleteThrough: %v", err)
	}
	// New batches reserve sequences after the truncation point and land
	// in never-used segment numbers.
	seq := commit(t, l, []byte("d"))
	if seq != 4 {
		t.Fatalf("seq after truncation = %d, want 4", seq)
	}
	l.Close()

	l = open(t, dir, opts)
	defer l.Close()
	if seq := commit(t, l, []byte("e")); seq != 5 {
		t.Fatalf("seq after reopen = %d, want 5", seq)
	}
	got, err := l.Read(5)
	if err != nil || string(got[0]) != "e" {
		t.Fatalf("Read(5) = %q, %v", got, err)
	}
	files, _ := filepath.Glob(filepath.Join(dir, "*.seg"))
	if len(files) != 2 {
		t.Fatalf("segment files = %v, want 2 new files (numbers not reused)", files)
	}
	for _, f := range files {
		base := filepath.Base(f)
		if base == "000001.seg" || base == "000002.seg" || base == "000003.seg" {
			t.Fatalf("reused deleted segment number: %s", base)
		}
	}
}

func TestTruncationReleasesKeysButRetainedKeysKeepSemantics(t *testing.T) {
	dir := t.TempDir()
	l := open(t, dir, log.Options{SegmentBytes: 32, Sync: true})
	old, err := l.AppendIdempotent("gone", [][]byte{[]byte("old")})
	if err != nil {
		t.Fatalf("AppendIdempotent: %v", err)
	}
	if _, err := l.Commit(old); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	keep, err := l.AppendIdempotent("kept", [][]byte{[]byte("same")})
	if err != nil {
		t.Fatalf("AppendIdempotent: %v", err)
	}
	if _, err := l.Commit(keep); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	// One batch per segment: deleting through 1 reclaims "gone".
	if n, err := l.DeleteThrough(1); err != nil || n != 1 {
		t.Fatalf("DeleteThrough = %d, %v", n, err)
	}

	// The deleted key is released: reuse with brand-new content reserves
	// a fresh sequence and does not conflict.
	reused, err := l.AppendIdempotent("gone", [][]byte{[]byte("brand-new")})
	if err != nil {
		t.Fatalf("reuse deleted key: %v", err)
	}
	if reused.Seq() != 3 {
		t.Fatalf("reused key seq = %d, want 3", reused.Seq())
	}
	if _, err := l.Commit(reused); err != nil {
		t.Fatalf("Commit reused: %v", err)
	}

	// The retained key keeps both semantics: identical content dedups to
	// the original batch, different content conflicts.
	same, err := l.AppendIdempotent("kept", [][]byte{[]byte("same")})
	if err != nil || same.Seq() != 2 {
		t.Fatalf("retained-key dedup = %d, %v; want seq 2", same.Seq(), err)
	}
	if _, err := l.AppendIdempotent("kept", [][]byte{[]byte("different")}); !errors.Is(err, log.ErrBatchIDConflict) {
		t.Fatalf("retained-key conflict err = %v, want ErrBatchIDConflict", err)
	}
}

func TestTruncatedKeyReleaseSurvivesReopenAndRebuild(t *testing.T) {
	dir := t.TempDir()
	opts := log.Options{SegmentBytes: 32, Sync: true}
	l := open(t, dir, opts)
	b, _ := l.AppendIdempotent("k", [][]byte{[]byte("v1")})
	l.Commit(b)
	commit(t, l, []byte("tail")) // seg 2 retained
	l.DeleteThrough(1)
	l.Close()

	for _, removeSidecar := range []bool{false, true} {
		name := "adopt"
		if removeSidecar {
			name = "rebuild"
			if err := os.Remove(filepath.Join(dir, "index.idx")); err != nil {
				t.Fatal(err)
			}
		}
		t.Run(name, func(t *testing.T) {
			l2 := open(t, dir, opts)
			defer l2.Close()
			nb, err := l2.AppendIdempotent("k", [][]byte{[]byte("v2-different")})
			if err != nil {
				t.Fatalf("reuse key after reopen: %v", err)
			}
			if nb.Seq() != 3 {
				t.Fatalf("reused key seq = %d, want 3", nb.Seq())
			}
		})
	}
}

func TestDeleteThroughStagedKeyedBatchBelowRejected(t *testing.T) {
	l := open(t, t.TempDir(), log.Options{SegmentBytes: 1 << 20, Sync: true})
	commit(t, l, []byte("a"))
	// A staged keyed batch with seq 2 while seq 1 is committed.
	if _, err := l.AppendIdempotent("staged-key", [][]byte{[]byte("x")}); err != nil {
		t.Fatalf("AppendIdempotent: %v", err)
	}
	if _, err := l.DeleteThrough(10); !errors.Is(err, log.ErrInvalidRetention) {
		t.Fatalf("DeleteThrough err = %v, want ErrInvalidRetention", err)
	}
	// Its key stays bound to its reservation after the rejected call.
	again, err := l.AppendIdempotent("staged-key", [][]byte{[]byte("x")})
	if err != nil || again.Seq() != 2 {
		t.Fatalf("staged key after rejected truncation = %d, %v; want seq 2", again.Seq(), err)
	}
}

func TestMultipleDeleteThroughAdvancesPoint(t *testing.T) {
	dir := t.TempDir()
	l := open(t, dir, log.Options{SegmentBytes: 32, Sync: true})
	for _, s := range []string{"a", "b", "c", "d"} {
		commit(t, l, []byte(s))
	}
	if n, _ := l.DeleteThrough(1); n != 1 {
		t.Fatalf("first delete = %d, want 1", n)
	}
	if _, err := l.Read(1); !errors.Is(err, log.ErrTruncated) {
		t.Fatalf("Read(1): %v", err)
	}
	if n, _ := l.DeleteThrough(2); n != 1 {
		t.Fatalf("second delete = %d, want 1", n)
	}
	if _, err := l.Read(2); !errors.Is(err, log.ErrTruncated) {
		t.Fatalf("Read(2): %v", err)
	}
	if got, err := l.Read(3); err != nil || string(got[0]) != "c" {
		t.Fatalf("Read(3) = %q, %v", got, err)
	}
	// Deleting an already-truncated sequence is invalid.
	if _, err := l.DeleteThrough(1); !errors.Is(err, log.ErrInvalidRetention) {
		t.Fatalf("re-delete historical seq err = %v, want ErrInvalidRetention", err)
	}
	l.Close()

	l = open(t, dir, log.Options{SegmentBytes: 32, Sync: true})
	defer l.Close()
	for seq := uint64(1); seq <= 2; seq++ {
		if _, err := l.Read(seq); !errors.Is(err, log.ErrTruncated) {
			t.Fatalf("Read(%d) after reopen = %v, want ErrTruncated", seq, err)
		}
	}
}

func TestDeleteThroughWithoutSyncOption(t *testing.T) {
	dir := t.TempDir()
	opts := log.Options{SegmentBytes: 32}
	l := open(t, dir, opts)
	commit(t, l, []byte("a"))
	commit(t, l, []byte("b"))
	if n, err := l.DeleteThrough(1); err != nil || n != 1 {
		t.Fatalf("DeleteThrough = %d, %v", n, err)
	}
	l.Close()
	l = open(t, dir, opts)
	defer l.Close()
	if _, err := l.Read(1); !errors.Is(err, log.ErrTruncated) {
		t.Fatalf("Read(1) after reopen = %v, want ErrTruncated", err)
	}
}

func TestDeleteThroughKeepsStagedBatchAbovePoint(t *testing.T) {
	// One batch per segment; a keyed batch staged above the truncation
	// point survives the call with its reservation and key intact.
	l := open(t, t.TempDir(), log.Options{SegmentBytes: 32, Sync: true})
	commit(t, l, []byte("a"))                                          // seq 1, seg 1
	commit(t, l, []byte("b"))                                          // seq 2, seg 2
	staged, err := l.AppendIdempotent("future", [][]byte{[]byte("f")}) // seq 3
	if err != nil {
		t.Fatalf("AppendIdempotent: %v", err)
	}
	if n, err := l.DeleteThrough(1); err != nil || n != 1 {
		t.Fatalf("DeleteThrough = %d, %v; want 1", n, err)
	}
	// The staged batch is still staged and keeps seq 3 and its key.
	if _, err := l.Read(3); !errors.Is(err, log.ErrNotCommitted) {
		t.Fatalf("Read(3) = %v, want ErrNotCommitted", err)
	}
	again, err := l.AppendIdempotent("future", [][]byte{[]byte("f")})
	if err != nil || again.Seq() != 3 {
		t.Fatalf("staged key after truncation = %d, %v; want seq 3", again.Seq(), err)
	}
	if seq, err := l.Commit(staged); err != nil || seq != 3 {
		t.Fatalf("Commit staged = %d, %v; want 3", seq, err)
	}
	if got, err := l.Read(3); err != nil || string(got[0]) != "f" {
		t.Fatalf("Read(3) = %q, %v", got, err)
	}
}

func TestDeleteThroughPartialThenCommitSameProcess(t *testing.T) {
	// After a partial truncation the retained current segment keeps
	// accepting commits in the same process. A 40-byte first record
	// fills seg 1 by itself (64-byte capacity); seqs 2 and 3 share seg 2.
	dir := t.TempDir()
	l := open(t, dir, log.Options{SegmentBytes: 64, Sync: true})
	commit(t, l, make([]byte, 40)) // seg 1: seq 1
	commit(t, l, []byte("b"))      // seg 2: seq 2
	if n, err := l.DeleteThrough(1); err != nil || n != 1 {
		t.Fatalf("DeleteThrough = %d, %v", n, err)
	}
	// Committing into the retained segment keeps sequence order and
	// extends that same segment rather than rolling.
	seq := commit(t, l, []byte("c"))
	if seq != 3 {
		t.Fatalf("seq = %d, want 3", seq)
	}
	if segs := l.Segments(); len(segs) != 1 || segs[0].FirstSeq != 2 || segs[0].LastSeq != 3 {
		t.Fatalf("segments = %+v, want one [2,3]", segs)
	}
	if got, err := l.Read(2); err != nil || string(got[0]) != "b" {
		t.Fatalf("Read(2) = %q, %v", got, err)
	}
}
