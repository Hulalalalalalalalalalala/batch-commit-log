package log_test

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	log "github.com/Hulalalalalalalalalalala/batch-commit-log"
)

// segDirs lists the physical .seg files' basenames in a directory.
func segFiles(t *testing.T, dir string) []string {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(dir, "*.seg"))
	if err != nil {
		t.Fatal(err)
	}
	out := make([]string, len(files))
	for i, f := range files {
		out[i] = filepath.Base(f)
	}
	sort.Strings(out)
	return out
}

func TestDeleteThroughZeroIsNoop(t *testing.T) {
	dir := t.TempDir()
	l := open(t, dir, log.Options{SegmentBytes: 32})
	commit(t, l, []byte("a"))
	before := segFiles(t, dir)

	n, err := l.DeleteThrough(0)
	if err != nil || n != 0 {
		t.Fatalf("DeleteThrough(0) = %d, %v; want 0, nil", n, err)
	}
	if got := segFiles(t, dir); !reflect.DeepEqual(got, before) {
		t.Fatalf("files changed after DeleteThrough(0): %v vs %v", got, before)
	}
	if _, err := l.Read(1); err != nil {
		t.Fatalf("Read(1) after noop: %v", err)
	}
}

func TestDeleteThroughBasic(t *testing.T) {
	dir := t.TempDir()
	opts := log.Options{SegmentBytes: 32, Sync: true}
	l := open(t, dir, opts)
	for i := 1; i <= 5; i++ {
		commit(t, l, []byte(fmt.Sprintf("rec-%d", i)))
	}
	// Five small batches, one per segment.
	if segs := l.Segments(); len(segs) != 5 {
		t.Fatalf("segments = %d, want 5", len(segs))
	}

	// Cut through seq 3: segments of 1,2,3 are whole and removed.
	n, err := l.DeleteThrough(3)
	if err != nil {
		t.Fatalf("DeleteThrough(3): %v", err)
	}
	if n != 3 {
		t.Fatalf("deleted %d segments, want 3", n)
	}
	if segs := l.Segments(); len(segs) != 2 {
		t.Fatalf("retained segments = %d, want 2: %+v", len(segs), segs)
	}
	if files := segFiles(t, dir); len(files) != 2 {
		t.Fatalf("segment files = %v, want 2", files)
	}

	// Deleted sequences read as ErrTruncated, distinct from unknown.
	for _, seq := range []uint64{1, 2, 3} {
		if _, err := l.Read(seq); !errors.Is(err, log.ErrTruncated) {
			t.Fatalf("Read(%d) err = %v, want ErrTruncated", seq, err)
		}
	}
	// Retained sequences still read.
	for seq, want := range map[uint64]string{4: "rec-4", 5: "rec-5"} {
		got, err := l.Read(seq)
		if err != nil || string(got[0]) != want {
			t.Fatalf("Read(%d) = %q, %v; want %q", seq, got, err, want)
		}
	}
	// Unknown stays ErrUnknownBatch, never ErrTruncated.
	if _, err := l.Read(6); !errors.Is(err, log.ErrUnknownBatch) {
		t.Fatalf("Read(6) err = %v, want ErrUnknownBatch", err)
	}
	if _, err := l.Read(0); !errors.Is(err, log.ErrUnknownBatch) {
		t.Fatalf("Read(0) err = %v, want ErrUnknownBatch", err)
	}

	// Scan from a deleted prefix starts at the first retained batch.
	var seen []uint64
	if err := l.Scan(1, func(b log.Batch) error {
		seen = append(seen, b.Seq())
		return nil
	}); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if !reflect.DeepEqual(seen, []uint64{4, 5}) {
		t.Fatalf("scan = %v, want [4 5]", seen)
	}

	// New appends keep their sequence frontier.
	if seq := commit(t, l, []byte("rec-6")); seq != 6 {
		t.Fatalf("seq after truncate = %d, want 6", seq)
	}
	l.Close()

	// Reopen: truncation persists, and index rebuild reaches the same
	// distinction. seq 6 committed after the cut rolled a third surviving
	// segment.
	l = open(t, dir, opts)
	if segs := l.Segments(); len(segs) != 3 {
		t.Fatalf("after reopen segments = %d, want 3", len(segs))
	}
	if _, err := l.Read(2); !errors.Is(err, log.ErrTruncated) {
		t.Fatalf("Read(2) after reopen err = %v, want ErrTruncated", err)
	}
	if _, err := l.Read(9); !errors.Is(err, log.ErrUnknownBatch) {
		t.Fatalf("Read(9) err = %v, want ErrUnknownBatch", err)
	}
	seen = nil
	if err := l.Scan(1, func(b log.Batch) error {
		seen = append(seen, b.Seq())
		return nil
	}); err != nil {
		t.Fatalf("Scan after reopen: %v", err)
	}
	if !reflect.DeepEqual(seen, []uint64{4, 5, 6}) {
		t.Fatalf("scan after reopen = %v, want [4 5 6]", seen)
	}
}

func TestDeleteThroughRebuiltIndexDistinguishesTruncated(t *testing.T) {
	dir := t.TempDir()
	opts := log.Options{SegmentBytes: 32}
	l := open(t, dir, opts)
	for i := 1; i <= 4; i++ {
		commit(t, l, []byte(fmt.Sprintf("r%d", i)))
	}
	if _, err := l.DeleteThrough(2); err != nil {
		t.Fatalf("DeleteThrough: %v", err)
	}
	l.Close()

	// Remove the derived sidecar entirely; the marker must still tell
	// truncated from unknown after the forced rebuild.
	if err := os.Remove(filepath.Join(dir, "index.idx")); err != nil {
		t.Fatal(err)
	}
	l = open(t, dir, opts)
	if _, err := l.Read(1); !errors.Is(err, log.ErrTruncated) {
		t.Fatalf("Read(1) err = %v, want ErrTruncated", err)
	}
	if _, err := l.Read(2); !errors.Is(err, log.ErrTruncated) {
		t.Fatalf("Read(2) err = %v, want ErrTruncated", err)
	}
	if _, err := l.Read(7); !errors.Is(err, log.ErrUnknownBatch) {
		t.Fatalf("Read(7) err = %v, want ErrUnknownBatch", err)
	}
	if got, err := l.Read(3); err != nil || string(got[0]) != "r3" {
		t.Fatalf("Read(3) = %q, %v", got, err)
	}
}

func TestDeleteThroughSegmentMiddleKeepsSegment(t *testing.T) {
	dir := t.TempDir()
	// Small capacity: the three-member group is larger than one segment
	// and lands in its own file between two single-batch segments.
	l := open(t, dir, log.Options{SegmentBytes: 32})

	// Segments: [1], [2,3,4 group], [5]. Cut through seq 3: the group's
	// segment straddles 3 (LastSeq 4), so only segment [1] is removed.
	commit(t, l, []byte("one"))
	var group []log.Batch
	for _, s := range []string{"two", "three", "four"} {
		b, err := l.Append([][]byte{[]byte(s)})
		if err != nil {
			t.Fatal(err)
		}
		group = append(group, b)
	}
	if _, err := l.CommitGroup(group); err != nil {
		t.Fatalf("CommitGroup: %v", err)
	}
	commit(t, l, []byte("five"))

	n, err := l.DeleteThrough(3)
	if err != nil {
		t.Fatalf("DeleteThrough(3): %v", err)
	}
	if n != 1 {
		t.Fatalf("deleted %d, want 1 (boundary segment retained whole)", n)
	}
	// Seq 2 and 3 are still readable: their segment was not removed.
	if _, err := l.Read(2); err != nil {
		t.Fatalf("Read(2) should survive the boundary: %v", err)
	}
	if _, err := l.Read(1); !errors.Is(err, log.ErrTruncated) {
		t.Fatalf("Read(1) err = %v, want ErrTruncated", err)
	}
	// A second cut through 4 now reclaims the whole group segment.
	n, err = l.DeleteThrough(4)
	if err != nil || n != 1 {
		t.Fatalf("second DeleteThrough = %d, %v; want 1, nil", n, err)
	}
	for _, seq := range []uint64{2, 3, 4} {
		if _, err := l.Read(seq); !errors.Is(err, log.ErrTruncated) {
			t.Fatalf("Read(%d) err = %v, want ErrTruncated", seq, err)
		}
	}
	if _, err := l.Read(5); err != nil {
		t.Fatalf("Read(5): %v", err)
	}
}

func TestDeleteThroughInvalidRetentionDeletesNothing(t *testing.T) {
	dir := t.TempDir()
	l := open(t, dir, log.Options{SegmentBytes: 32})
	commit(t, l, []byte("a"))
	commit(t, l, []byte("b"))
	staged, err := l.Append([][]byte{[]byte("pending")})
	if err != nil {
		t.Fatal(err)
	}
	before := segFiles(t, dir)

	for _, seq := range []uint64{0, 3, 1 << 20, staged.Seq()} {
		_, err := l.DeleteThrough(seq)
		if seq == 0 {
			continue // 0 is a documented no-op
		}
		if !errors.Is(err, log.ErrInvalidRetention) {
			t.Fatalf("DeleteThrough(%d) err = %v, want ErrInvalidRetention", seq, err)
		}
	}
	if got := segFiles(t, dir); !reflect.DeepEqual(got, before) {
		t.Fatalf("files changed after invalid calls: %v vs %v", got, before)
	}
	if _, err := l.Read(1); err != nil {
		t.Fatalf("Read(1) after refused truncation: %v", err)
	}
	// The staged batch is untouched and still committable.
	if seq, err := l.Commit(staged); err != nil || seq != 3 {
		t.Fatalf("Commit staged = %d, %v", seq, err)
	}
}

func TestDeleteThroughAllSegments(t *testing.T) {
	dir := t.TempDir()
	opts := log.Options{SegmentBytes: 32}
	l := open(t, dir, opts)
	for i := 1; i <= 3; i++ {
		commit(t, l, []byte(fmt.Sprintf("r%d", i)))
	}
	n, err := l.DeleteThrough(3)
	if err != nil || n != 3 {
		t.Fatalf("DeleteThrough(3) = %d, %v", n, err)
	}
	if segs := l.Segments(); len(segs) != 0 {
		t.Fatalf("Segments = %+v, want none", segs)
	}
	if files := segFiles(t, dir); len(files) != 0 {
		t.Fatalf("files = %v, want none", files)
	}
	// Scan with no retained batches ends normally with no callbacks.
	called := 0
	if err := l.Scan(1, func(log.Batch) error { called++; return nil }); err != nil {
		t.Fatalf("Scan empty: %v", err)
	}
	if called != 0 {
		t.Fatalf("scan called %d times, want 0", called)
	}
	// History is truncated; the frontier continues.
	if seq := commit(t, l, []byte("fresh")); seq != 4 {
		t.Fatalf("new seq = %d, want 4", seq)
	}
	if _, err := l.Read(4); err != nil {
		t.Fatalf("Read(4): %v", err)
	}
	if _, err := l.Read(1); !errors.Is(err, log.ErrTruncated) {
		t.Fatalf("Read(1) err = %v, want ErrTruncated", err)
	}
	l.Close()

	l = open(t, dir, opts)
	if segs := l.Segments(); len(segs) != 1 || segs[0].FirstSeq != 4 {
		t.Fatalf("after reopen Segments = %+v, want one starting at 4", segs)
	}
}

func TestDeleteThroughReleasesKeys(t *testing.T) {
	dir := t.TempDir()
	opts := log.Options{SegmentBytes: 32}
	l := open(t, dir, opts)

	old, err := l.AppendIdempotent("order-1", [][]byte{[]byte("old")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := l.Commit(old); err != nil {
		t.Fatal(err)
	}
	keep, err := l.AppendIdempotent("order-2", [][]byte{[]byte("keep")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := l.Commit(keep); err != nil {
		t.Fatal(err)
	}
	if _, err := l.DeleteThrough(1); err != nil {
		t.Fatalf("DeleteThrough: %v", err)
	}

	// The deleted key is released and may be reused with new content.
	reuse, err := l.AppendIdempotent("order-1", [][]byte{[]byte("brand-new")})
	if err != nil {
		t.Fatalf("reuse deleted key: %v", err)
	}
	if reuse.Seq() != 3 {
		t.Fatalf("reused batch seq = %d, want 3", reuse.Seq())
	}
	if _, err := l.Commit(reuse); err != nil {
		t.Fatalf("Commit reuse: %v", err)
	}
	got, err := l.Read(3)
	if err != nil || string(got[0]) != "brand-new" {
		t.Fatalf("Read(3) = %q, %v", got, err)
	}

	// The retained key keeps same-key/same-content dedup and conflicts.
	same, err := l.AppendIdempotent("order-2", [][]byte{[]byte("keep")})
	if err != nil || same.Seq() != 2 {
		t.Fatalf("idempotent retry = seq %d, %v; want 2", same.Seq(), err)
	}
	if _, err := l.AppendIdempotent("order-2", [][]byte{[]byte("different")}); !errors.Is(err, log.ErrBatchIDConflict) {
		t.Fatalf("conflict err = %v, want ErrBatchIDConflict", err)
	}
}

func TestDeleteThroughReopenAfterCrashMidDeletion(t *testing.T) {
	dir := t.TempDir()
	backup := t.TempDir()
	opts := log.Options{SegmentBytes: 32}
	l := open(t, dir, opts)
	for i := 1; i <= 4; i++ {
		commit(t, l, []byte(fmt.Sprintf("r%d", i)))
	}
	// Back up the segment files so we can simulate a crash after the
	// marker was renamed but before the files were unlinked.
	for _, name := range segFiles(t, dir) {
		data, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(backup, name), data, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := l.DeleteThrough(2); err != nil {
		t.Fatalf("DeleteThrough: %v", err)
	}
	l.Close()

	// Restore the two "deleted" files as if unlink never happened.
	for _, name := range []string{"000001.seg", "000002.seg"} {
		data, err := os.ReadFile(filepath.Join(backup, name))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, name), data, 0o644); err != nil {
			t.Fatal(err)
		}
	}

	// Reopen must converge to the complete new state: prefix gone.
	l = open(t, dir, opts)
	if files := segFiles(t, dir); len(files) != 2 {
		t.Fatalf("files after reconcile = %v, want 2", files)
	}
	if _, err := l.Read(1); !errors.Is(err, log.ErrTruncated) {
		t.Fatalf("Read(1) err = %v, want ErrTruncated", err)
	}
	if got, err := l.Read(3); err != nil || string(got[0]) != "r3" {
		t.Fatalf("Read(3) = %q, %v", got, err)
	}
}

func TestDeleteThroughMultipleRoundsAndReopen(t *testing.T) {
	dir := t.TempDir()
	opts := log.Options{SegmentBytes: 32}
	l := open(t, dir, opts)

	next := uint64(1)
	write := func(n int) {
		for i := 0; i < n; i++ {
			commit(t, l, []byte(fmt.Sprintf("seq-%d", next)))
			next++
		}
	}
	write(3)
	if n, err := l.DeleteThrough(2); err != nil || n != 2 {
		t.Fatalf("round 1 = %d, %v", n, err)
	}
	write(3) // seqs 4,5,6 in fresh segments numbered above 3
	write(2) // 7,8
	if n, err := l.DeleteThrough(6); err != nil {
		t.Fatalf("round 2: %v", n)
	} else if n != 4 {
		// Segments holding seqs 3,4,5,6 all end at or below 6.
		t.Fatalf("round 2 deleted %d, want 4", n)
	}
	l.Close()

	l = open(t, dir, opts)
	for seq := uint64(1); seq <= 6; seq++ {
		if _, err := l.Read(seq); !errors.Is(err, log.ErrTruncated) {
			t.Fatalf("Read(%d) err = %v, want ErrTruncated", seq, err)
		}
	}
	for seq, want := range map[uint64]string{7: "seq-7", 8: "seq-8"} {
		got, err := l.Read(seq)
		if err != nil || string(got[0]) != want {
			t.Fatalf("Read(%d) = %q, %v", seq, got, err)
		}
	}
	// New writes after two rounds keep numbering past all deleted files.
	write(1) // seq 9
	if seq := uint64(9); true {
		got, err := l.Read(seq)
		if err != nil || string(got[0]) != "seq-9" {
			t.Fatalf("Read(9) = %q, %v", got, err)
		}
	}
}

func TestDeleteThroughIdempotentRepeated(t *testing.T) {
	dir := t.TempDir()
	l := open(t, dir, log.Options{SegmentBytes: 32})
	for i := 1; i <= 4; i++ {
		commit(t, l, []byte(fmt.Sprintf("r%d", i)))
	}
	if n, err := l.DeleteThrough(2); err != nil || n != 2 {
		t.Fatalf("first DeleteThrough = %d, %v", n, err)
	}
	// Same point again is a successful no-op.
	if n, err := l.DeleteThrough(2); err != nil || n != 0 {
		t.Fatalf("repeat DeleteThrough = %d, %v; want 0 nil", n, err)
	}
	// A point inside the retained boundary segment that frees nothing
	// new also returns 0 with no error and no change.
	if segs := l.Segments(); len(segs) != 2 {
		t.Fatalf("segments = %d, want 2", len(segs))
	}
}

func TestDeleteThroughKeyedGroupAcrossSegments(t *testing.T) {
	dir := t.TempDir()
	l := open(t, dir, log.Options{SegmentBytes: 32})
	commit(t, l, []byte("plain")) // seq 1, segment 1

	var bs []log.Batch
	for _, s := range []string{"k1", "k2"} {
		b, err := l.AppendIdempotent("key-"+s, [][]byte{[]byte(s)})
		if err != nil {
			t.Fatal(err)
		}
		bs = append(bs, b)
	}
	if _, err := l.CommitGroup(bs); err != nil {
		t.Fatal(err)
	}
	// Delete only the first segment; the keyed group survives intact and
	// both keys still dedup.
	if n, err := l.DeleteThrough(1); err != nil || n != 1 {
		t.Fatalf("DeleteThrough = %d, %v", n, err)
	}
	for seq, want := range map[uint64]string{2: "k1", 3: "k2"} {
		got, err := l.Read(seq)
		if err != nil || string(got[0]) != want {
			t.Fatalf("Read(%d) = %q, %v", seq, got, err)
		}
	}
	b, err := l.AppendIdempotent("key-k1", [][]byte{[]byte("k1")})
	if err != nil || b.Seq() != 2 {
		t.Fatalf("retained key dedup = seq %d, %v", b.Seq(), err)
	}
}
