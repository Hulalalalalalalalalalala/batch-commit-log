package log_test

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"sync"
	"testing"

	log "github.com/Hulalalalalalalalalalala/batch-commit-log"
)

func open(t *testing.T, dir string, opts log.Options) *log.Log {
	t.Helper()
	l, err := log.Open(dir, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { l.Close() })
	return l
}

func commit(t *testing.T, l *log.Log, records ...[]byte) uint64 {
	t.Helper()
	b, err := l.Append(records)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	seq, err := l.Commit(b)
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	return seq
}

func TestAppendCommitRead(t *testing.T) {
	l := open(t, t.TempDir(), log.Options{SegmentBytes: 1 << 20})

	want := [][]byte{[]byte("alpha"), []byte("beta"), {}}
	seq := commit(t, l, want...)
	if seq != 1 {
		t.Fatalf("first seq = %d, want 1", seq)
	}
	got, err := l.Read(seq)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Read = %q, want %q", got, want)
	}
	if seq2 := commit(t, l, []byte("next")); seq2 != 2 {
		t.Fatalf("second seq = %d, want 2", seq2)
	}
}

func TestSequencesReservedAtAppend(t *testing.T) {
	l := open(t, t.TempDir(), log.Options{SegmentBytes: 1 << 20})

	b1, _ := l.Append([][]byte{[]byte("a")})
	b2, _ := l.Append([][]byte{[]byte("b")})
	if b1.Seq() != 1 || b2.Seq() != 2 {
		t.Fatalf("reserved seqs = %d, %d; want 1, 2", b1.Seq(), b2.Seq())
	}
	// Commit out of reservation order; each keeps its reserved seq.
	if seq, _ := l.Commit(b2); seq != 2 {
		t.Fatalf("commit b2 seq = %d, want 2", seq)
	}
	if seq, _ := l.Commit(b1); seq != 1 {
		t.Fatalf("commit b1 seq = %d, want 1", seq)
	}
}

func TestReadStagedReturnsNotCommitted(t *testing.T) {
	l := open(t, t.TempDir(), log.Options{SegmentBytes: 1 << 20})

	b, err := l.Append([][]byte{[]byte("secret")})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	got, err := l.Read(b.Seq())
	if !errors.Is(err, log.ErrNotCommitted) {
		t.Fatalf("Read staged err = %v, want ErrNotCommitted", err)
	}
	if got != nil {
		t.Fatalf("Read staged leaked records: %q", got)
	}
}

func TestReadUnknownBatch(t *testing.T) {
	l := open(t, t.TempDir(), log.Options{SegmentBytes: 1 << 20})
	commit(t, l, []byte("x"))

	for _, seq := range []uint64{0, 7, 1 << 40} {
		if _, err := l.Read(seq); !errors.Is(err, log.ErrUnknownBatch) {
			t.Fatalf("Read(%d) err = %v, want ErrUnknownBatch", seq, err)
		}
	}
}

func TestDoubleCommit(t *testing.T) {
	l := open(t, t.TempDir(), log.Options{SegmentBytes: 1 << 20})

	b, _ := l.Append([][]byte{[]byte("once")})
	if _, err := l.Commit(b); err != nil {
		t.Fatalf("first Commit: %v", err)
	}
	if _, err := l.Commit(b); !errors.Is(err, log.ErrUnknownBatch) {
		t.Fatalf("second Commit err = %v, want ErrUnknownBatch", err)
	}
	got, err := l.Read(b.Seq())
	if err != nil || !reflect.DeepEqual(got, [][]byte{[]byte("once")}) {
		t.Fatalf("committed content changed: %q, %v", got, err)
	}
	// A zero-value batch was never staged.
	if _, err := l.Commit(log.Batch{}); !errors.Is(err, log.ErrUnknownBatch) {
		t.Fatalf("Commit(zero) err = %v, want ErrUnknownBatch", err)
	}
}

func TestReopenReplaysCommittedOnly(t *testing.T) {
	dir := t.TempDir()
	opts := log.Options{SegmentBytes: 1 << 20, Sync: true}

	l := open(t, dir, opts)
	commit(t, l, []byte("one"))
	commit(t, l, []byte("two"), []byte("three"))
	staged, _ := l.Append([][]byte{[]byte("never")})
	_ = staged // staged but never committed: must not survive reopen
	l.Close()

	l = open(t, dir, opts)
	got, err := l.Read(2)
	if err != nil || !reflect.DeepEqual(got, [][]byte{[]byte("two"), []byte("three")}) {
		t.Fatalf("Read(2) = %q, %v", got, err)
	}
	// The uncommitted reservation is gone after reopen; seq 3 is unknown.
	if _, err := l.Read(3); !errors.Is(err, log.ErrUnknownBatch) {
		t.Fatalf("Read(3) err = %v, want ErrUnknownBatch", err)
	}
	// New appends continue the sequence after the committed maximum.
	if seq := commit(t, l, []byte("four")); seq != 3 {
		t.Fatalf("seq after reopen = %d, want 3", seq)
	}
}

func TestScanInOrderAndAtomic(t *testing.T) {
	l := open(t, t.TempDir(), log.Options{SegmentBytes: 1 << 20})

	// Stage three batches, commit the middle one last.
	b1, _ := l.Append([][]byte{[]byte("r1")})
	b2, _ := l.Append([][]byte{[]byte("r2a"), []byte("r2b")})
	b3, _ := l.Append([][]byte{[]byte("r3")})
	for _, b := range []log.Batch{b1, b3, b2} {
		if _, err := l.Commit(b); err != nil {
			t.Fatalf("Commit: %v", err)
		}
	}
	if _, err := l.Append([][]byte{[]byte("staged")}); err != nil {
		t.Fatalf("Append: %v", err)
	}

	var seqs []uint64
	var recs [][]string
	err := l.Scan(1, func(b log.Batch) error {
		seqs = append(seqs, b.Seq())
		var rs []string
		for _, r := range b.Records() {
			rs = append(rs, string(r))
		}
		recs = append(recs, rs)
		return nil
	})
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if !reflect.DeepEqual(seqs, []uint64{1, 2, 3}) {
		t.Fatalf("scan seqs = %v, want [1 2 3]", seqs)
	}
	want := [][]string{{"r1"}, {"r2a", "r2b"}, {"r3"}}
	if !reflect.DeepEqual(recs, want) {
		t.Fatalf("scan records = %v, want %v", recs, want)
	}

	// From filters, and a callback error stops the replay.
	count := 0
	boom := errors.New("boom")
	err = l.Scan(2, func(b log.Batch) error {
		count++
		return boom
	})
	if !errors.Is(err, boom) || count != 1 {
		t.Fatalf("Scan error propagation: err=%v count=%d", err, count)
	}
}

func TestSegmentRollAndListing(t *testing.T) {
	dir := t.TempDir()
	// Capacity for exactly one small batch per segment.
	l := open(t, dir, log.Options{SegmentBytes: 32})

	var seqs []uint64
	for i := 0; i < 3; i++ {
		seqs = append(seqs, commit(t, l, []byte(fmt.Sprintf("rec-%d", i))))
	}
	segs := l.Segments()
	if len(segs) != 3 {
		t.Fatalf("len(Segments) = %d, want 3: %+v", len(segs), segs)
	}
	for i, s := range segs {
		want := uint64(i + 1)
		if s.FirstSeq != want || s.LastSeq != want {
			t.Fatalf("segment %d = %+v, want first=last=%d", i, s, want)
		}
	}
	files, _ := filepath.Glob(filepath.Join(dir, "*.seg"))
	if len(files) != 3 {
		t.Fatalf("segment files = %d, want 3", len(files))
	}

	// Segments survive a reopen.
	l.Close()
	l = open(t, dir, log.Options{SegmentBytes: 32})
	if segs := l.Segments(); len(segs) != 3 {
		t.Fatalf("after reopen len(Segments) = %d, want 3", len(segs))
	}
	if got, err := l.Read(2); err != nil || string(got[0]) != "rec-1" {
		t.Fatalf("Read(2) = %q, %v", got, err)
	}
}

func TestBatchLargerThanSegmentGetsOwnSegment(t *testing.T) {
	l := open(t, t.TempDir(), log.Options{SegmentBytes: 16})

	commit(t, l, []byte("small"))
	big := make([]byte, 100)
	commit(t, l, big)
	commit(t, l, []byte("tail"))

	segs := l.Segments()
	if len(segs) != 3 {
		t.Fatalf("len(Segments) = %d, want 3: %+v", len(segs), segs)
	}
	got, err := l.Read(2)
	if err != nil || len(got) != 1 || len(got[0]) != 100 {
		t.Fatalf("Read(2) = %d records, %v", len(got), err)
	}
}

func TestEmptyLogHasNoSegments(t *testing.T) {
	l := open(t, t.TempDir(), log.Options{SegmentBytes: 1 << 20})
	if segs := l.Segments(); len(segs) != 0 {
		t.Fatalf("Segments = %+v, want none", segs)
	}
}

func TestCorruptSegmentDetected(t *testing.T) {
	newCorruptLog := func(t *testing.T, mutate func([]byte) []byte) string {
		dir := t.TempDir()
		l := open(t, dir, log.Options{SegmentBytes: 1 << 20})
		commit(t, l, []byte("payload"))
		l.Close()

		files, err := filepath.Glob(filepath.Join(dir, "*.seg"))
		if err != nil || len(files) != 1 {
			t.Fatalf("segment files = %v, %v", files, err)
		}
		data, err := os.ReadFile(files[0])
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(files[0], mutate(data), 0o644); err != nil {
			t.Fatal(err)
		}
		return dir
	}

	t.Run("tampered content", func(t *testing.T) {
		dir := newCorruptLog(t, func(d []byte) []byte {
			// Flip a byte inside the record payload; the frame stays
			// complete and the checksum no longer matches.
			d[entryHeaderLenForTest+4] ^= 0xff
			return d
		})
		if _, err := log.Open(dir, log.Options{SegmentBytes: 1 << 20}); !errors.Is(err, log.ErrCorruptSegment) {
			t.Fatalf("Open err = %v, want ErrCorruptSegment", err)
		}
	})
	t.Run("truncated earlier segment", func(t *testing.T) {
		dir := t.TempDir()
		l := open(t, dir, log.Options{SegmentBytes: 32})
		commit(t, l, []byte("rec-0"))
		commit(t, l, []byte("rec-1"))
		l.Close()
		files, err := filepath.Glob(filepath.Join(dir, "*.seg"))
		if err != nil || len(files) != 2 {
			t.Fatalf("segment files = %v, %v", files, err)
		}
		sort.Strings(files)
		data, err := os.ReadFile(files[0])
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(files[0], data[:len(data)-3], 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := log.Open(dir, log.Options{SegmentBytes: 32}); !errors.Is(err, log.ErrCorruptSegment) {
			t.Fatalf("Open err = %v, want ErrCorruptSegment", err)
		}
	})
	t.Run("garbage", func(t *testing.T) {
		dir := newCorruptLog(t, func(d []byte) []byte { return []byte("not a segment") })
		if _, err := log.Open(dir, log.Options{SegmentBytes: 1 << 20}); !errors.Is(err, log.ErrCorruptSegment) {
			t.Fatalf("Open err = %v, want ErrCorruptSegment", err)
		}
	})
}

// entryHeaderLenForTest mirrors entryHeaderLen in segment.go (4 magic +
// 8 seq + 4 count), followed by a 4-byte record length.
const entryHeaderLenForTest = 4 + 8 + 4

func TestCommitWriteFailureStaysUncommitted(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root bypasses directory write permissions")
	}
	dir := t.TempDir()
	l := open(t, dir, log.Options{SegmentBytes: 32})
	commit(t, l, []byte("rec-0")) // fills segment 1

	// Force the next commit's segment roll to fail: the new segment file
	// cannot be created in a read-only directory.
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })

	b, err := l.Append([][]byte{[]byte("rec-1")})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if _, err := l.Commit(b); !errors.Is(err, log.ErrSyncFailed) {
		t.Fatalf("Commit err = %v, want ErrSyncFailed", err)
	}
	if _, err := l.Read(b.Seq()); !errors.Is(err, log.ErrNotCommitted) {
		t.Fatalf("failed commit Read err = %v, want ErrNotCommitted", err)
	}
	// Re-committing the same staged batch after the fault clears keeps
	// its reserved sequence and succeeds.
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	seq, err := l.Commit(b)
	if err != nil {
		t.Fatalf("retry Commit: %v", err)
	}
	if seq != b.Seq() {
		t.Fatalf("retried seq = %d, want reserved %d", seq, b.Seq())
	}
	if got, err := l.Read(seq); err != nil || string(got[0]) != "rec-1" {
		t.Fatalf("Read after retry = %q, %v", got, err)
	}
	// No duplicate/half batch from the failed attempt survived.
	var seqs []uint64
	if err := l.Scan(1, func(b log.Batch) error {
		seqs = append(seqs, b.Seq())
		return nil
	}); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if !reflect.DeepEqual(seqs, []uint64{1, 2}) {
		t.Fatalf("scan seqs = %v, want [1 2]", seqs)
	}
}

func TestInvalidOptions(t *testing.T) {
	for _, n := range []int{0, -1} {
		if _, err := log.Open(t.TempDir(), log.Options{SegmentBytes: n}); !errors.Is(err, log.ErrInvalidOptions) {
			t.Fatalf("Open(SegmentBytes=%d) err = %v, want ErrInvalidOptions", n, err)
		}
	}
}

func TestOpenCreatesDirectory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "nested", "log")
	l := open(t, dir, log.Options{SegmentBytes: 1 << 20})
	commit(t, l, []byte("x"))
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("directory not created: %v", err)
	}
}

func TestRecordsAreOpaqueAndCopied(t *testing.T) {
	l := open(t, t.TempDir(), log.Options{SegmentBytes: 1 << 20})

	rec := []byte{0x00, 0xff, 0x0a, 0x00}
	b, _ := l.Append([][]byte{rec})
	rec[0] = 0xee // mutating the caller's slice must not affect the staged batch
	if _, err := l.Commit(b); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	got, err := l.Read(b.Seq())
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !reflect.DeepEqual(got[0], []byte{0x00, 0xff, 0x0a, 0x00}) {
		t.Fatalf("Read = %v, want original bytes", got[0])
	}
	got[0][1] = 0x11 // mutating the read result must not affect the log
	again, _ := l.Read(b.Seq())
	if again[0][1] != 0xff {
		t.Fatalf("log content mutated through returned slice")
	}
}

// crashTail appends a torn prefix of a fresh encoded batch to the newest
// segment file, simulating a power loss mid-write.
func crashTail(t *testing.T, dir string) {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(dir, "*.seg"))
	if err != nil || len(files) == 0 {
		t.Fatalf("segment files = %v, %v", files, err)
	}
	sort.Strings(files)
	path := files[len(files)-1]
	// Build an entry by encoding through the public API is not possible,
	// so hand-craft a magic-prefixed partial header.
	tail := []byte{'B', 'C', 'L', '1', 0x09, 0x00}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(tail); err != nil {
		t.Fatal(err)
	}
	f.Close()
}

func TestCrashTailIsDroppedOnReopen(t *testing.T) {
	dir := t.TempDir()
	opts := log.Options{SegmentBytes: 1 << 20, Sync: true}

	l := open(t, dir, opts)
	s1 := commit(t, l, []byte("one"))
	s2 := commit(t, l, []byte("two"))
	l.Close()

	crashTail(t, dir)

	// Open must not error and must not replay the torn batch.
	l = open(t, dir, opts)
	if got, err := l.Read(s1); err != nil || string(got[0]) != "one" {
		t.Fatalf("Read(%d) = %q, %v", s1, got, err)
	}
	if got, err := l.Read(s2); err != nil || string(got[0]) != "two" {
		t.Fatalf("Read(%d) = %q, %v", s2, got, err)
	}
	// The torn reservation never existed: next batch takes seq 3.
	segs := l.Segments()
	if len(segs) != 1 || segs[0].FirstSeq != 1 || segs[0].LastSeq != 2 {
		t.Fatalf("Segments = %+v, want one segment 1..2", segs)
	}
	if seq := commit(t, l, []byte("three")); seq != 3 {
		t.Fatalf("seq after crash-tail reopen = %d, want 3", seq)
	}
	var scanned []uint64
	if err := l.Scan(1, func(b log.Batch) error {
		scanned = append(scanned, b.Seq())
		return nil
	}); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if !reflect.DeepEqual(scanned, []uint64{1, 2, 3}) {
		t.Fatalf("scan seqs = %v, want [1 2 3]", scanned)
	}
	l.Close()

	// The tail must have been physically removed; a second reopen is clean
	// and sees all three committed batches.
	l = open(t, dir, opts)
	if segs := l.Segments(); len(segs) != 1 || segs[0].LastSeq != 3 {
		t.Fatalf("second reopen Segments = %+v", segs)
	}
}

func TestCrashTailLastSegmentBoundary(t *testing.T) {
	dir := t.TempDir()
	opts := log.Options{SegmentBytes: 32}

	l := open(t, dir, opts)
	commit(t, l, []byte("rec-0"))
	commit(t, l, []byte("rec-1"))
	l.Close()

	crashTail(t, dir)
	l = open(t, dir, opts)
	segs := l.Segments()
	if len(segs) != 2 {
		t.Fatalf("Segments = %+v, want 2", segs)
	}
	if segs[1].FirstSeq != 2 || segs[1].LastSeq != 2 {
		t.Fatalf("last segment = %+v, want 2..2", segs[1])
	}
	// The next batch must start a new segment (the torn file is full-ish
	// at capacity); it must not be appended after the removed tail.
	if seq := commit(t, l, []byte("rec-2")); seq != 3 {
		t.Fatalf("seq = %d, want 3", seq)
	}
	if got, err := l.Read(3); err != nil || string(got[0]) != "rec-2" {
		t.Fatalf("Read(3) = %q, %v", got, err)
	}
}

func TestEmptyBatchCommitAndRead(t *testing.T) {
	l := open(t, t.TempDir(), log.Options{SegmentBytes: 1 << 20})

	b, err := l.Append(nil)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	seq, err := l.Commit(b)
	if err != nil {
		t.Fatalf("Commit empty batch: %v", err)
	}
	got, err := l.Read(seq)
	if err != nil {
		t.Fatalf("Read empty batch: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("empty batch records = %q, want none", got)
	}
	var n int
	if err := l.Scan(1, func(b log.Batch) error { n++; return nil }); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if n != 1 {
		t.Fatalf("scan saw %d batches, want 1 (empty batch replays)", n)
	}
}

func TestHoleFromUncommittedReservation(t *testing.T) {
	l := open(t, t.TempDir(), log.Options{SegmentBytes: 1 << 20})

	b1, _ := l.Append([][]byte{[]byte("a")})
	hole, _ := l.Append([][]byte{[]byte("never committed")})
	b3, _ := l.Append([][]byte{[]byte("c")})
	if _, err := l.Commit(b1); err != nil {
		t.Fatalf("Commit b1: %v", err)
	}
	if _, err := l.Commit(b3); err != nil {
		t.Fatalf("Commit b3: %v", err)
	}

	if _, err := l.Read(hole.Seq()); !errors.Is(err, log.ErrNotCommitted) {
		t.Fatalf("Read(hole) err = %v, want ErrNotCommitted", err)
	}
	var seqs []uint64
	if err := l.Scan(1, func(b log.Batch) error {
		seqs = append(seqs, b.Seq())
		return nil
	}); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if !reflect.DeepEqual(seqs, []uint64{1, 3}) {
		t.Fatalf("scan seqs = %v, want [1 3] (hole skipped)", seqs)
	}
}

func TestReadZeroAndNeverReserved(t *testing.T) {
	l := open(t, t.TempDir(), log.Options{SegmentBytes: 1 << 20})
	for _, seq := range []uint64{0, 1, 99} {
		if _, err := l.Read(seq); !errors.Is(err, log.ErrUnknownBatch) {
			t.Fatalf("Read(%d) err = %v, want ErrUnknownBatch", seq, err)
		}
	}
}

func TestCommittedSeqNeverReusedAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	opts := log.Options{SegmentBytes: 1 << 20, Sync: true}

	l := open(t, dir, opts)
	b1, _ := l.Append([][]byte{[]byte("a")})
	b2, _ := l.Append([][]byte{[]byte("b")})
	l.Commit(b1)
	l.Commit(b2)
	l.Close()

	l = open(t, dir, opts)
	if seq := commit(t, l, []byte("c")); seq != 3 {
		t.Fatalf("next seq after reopen = %d, want 3", seq)
	}
}

func TestConcurrentReadersSeeCommittedOnly(t *testing.T) {
	l := open(t, t.TempDir(), log.Options{SegmentBytes: 1 << 20})

	const batches = 200
	var wg sync.WaitGroup
	wg.Add(2)

	// Reader hammering Read/Scan/Segments while the single writer commits.
	go func() {
		defer wg.Done()
		for i := 0; i < batches; i++ {
			if _, err := l.Read(uint64(i + 1)); err != nil &&
				!errors.Is(err, log.ErrUnknownBatch) &&
				!errors.Is(err, log.ErrNotCommitted) {
				t.Errorf("concurrent Read err = %v", err)
				return
			}
			if err := l.Scan(1, func(b log.Batch) error { return nil }); err != nil {
				t.Errorf("concurrent Scan err = %v", err)
				return
			}
			_ = l.Segments()
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < batches; i++ {
			b, err := l.Append([][]byte{[]byte(fmt.Sprintf("r%d", i))})
			if err != nil {
				t.Errorf("Append: %v", err)
				return
			}
			// A staged batch must never surface to concurrent readers.
			if _, err := l.Read(b.Seq()); !errors.Is(err, log.ErrNotCommitted) {
				t.Errorf("staged seq %d Read err = %v, want ErrNotCommitted", b.Seq(), err)
				return
			}
			if _, err := l.Commit(b); err != nil {
				t.Errorf("Commit: %v", err)
				return
			}
		}
	}()
	wg.Wait()

	var seqs []uint64
	if err := l.Scan(1, func(b log.Batch) error {
		seqs = append(seqs, b.Seq())
		return nil
	}); err != nil {
		t.Fatalf("final Scan: %v", err)
	}
	if len(seqs) != batches {
		t.Fatalf("final scan count = %d, want %d", len(seqs), batches)
	}
}

func TestScanRangePinnedAtStart(t *testing.T) {
	l := open(t, t.TempDir(), log.Options{SegmentBytes: 1 << 20})
	commit(t, l, []byte("one"))

	seen := make(chan uint64, 4)
	done := make(chan struct{})
	var commitErr error
	err := l.Scan(1, func(b log.Batch) error {
		seen <- b.Seq()
		if b.Seq() == 1 {
			// While this replay is in flight, commit more batches.
			go func() {
				defer close(done)
				for _, r := range []string{"two", "three"} {
					nb, err := l.Append([][]byte{[]byte(r)})
					if err != nil {
						commitErr = err
						return
					}
					if _, err := l.Commit(nb); err != nil {
						commitErr = err
						return
					}
				}
			}()
			<-done
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if commitErr != nil {
		t.Fatalf("concurrent commit: %v", commitErr)
	}
	close(seen)
	var got []uint64
	for s := range seen {
		got = append(got, s)
	}
	if !reflect.DeepEqual(got, []uint64{1}) {
		t.Fatalf("pinned scan saw %v, want only [1]", got)
	}
}
