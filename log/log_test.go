package log_test

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	batchlog "github.com/Hulalalalalalalalalalala/batch-commit-log/log"
)

func open(t *testing.T, dir string, opts batchlog.Options) *batchlog.Log {
	t.Helper()
	l, err := batchlog.Open(dir, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return l
}

func commit(t *testing.T, l *batchlog.Log, records ...[]byte) uint64 {
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

func TestCommitAndRead(t *testing.T) {
	dir := t.TempDir()
	l := open(t, dir, batchlog.Options{SegmentBytes: 1 << 20})
	defer l.Close()

	seq1 := commit(t, l, []byte("a"), []byte("b"))
	if seq1 != 1 {
		t.Fatalf("first seq = %d, want 1", seq1)
	}
	seq2 := commit(t, l, []byte("c"))
	if seq2 != 2 {
		t.Fatalf("second seq = %d, want 2", seq2)
	}

	recs, err := l.Read(seq1)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(recs) != 2 || string(recs[0]) != "a" || string(recs[1]) != "b" {
		t.Fatalf("Read returned %q", recs)
	}
}

func TestSequencesAreStrictlyIncreasingFromOne(t *testing.T) {
	dir := t.TempDir()
	l := open(t, dir, batchlog.Options{SegmentBytes: 1 << 20})
	defer l.Close()

	for want := uint64(1); want <= 5; want++ {
		if got := commit(t, l, []byte{byte(want)}); got != want {
			t.Fatalf("seq = %d, want %d", got, want)
		}
	}
}

func TestReadStagedBatchReturnsNotCommitted(t *testing.T) {
	dir := t.TempDir()
	l := open(t, dir, batchlog.Options{SegmentBytes: 1 << 20})
	defer l.Close()

	if _, err := l.Append([][]byte{[]byte("x")}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if _, err := l.Read(1); !errors.Is(err, batchlog.ErrNotCommitted) {
		t.Fatalf("Read(staged) = %v, want ErrNotCommitted", err)
	}
}

func TestReadUnknownSequences(t *testing.T) {
	dir := t.TempDir()
	l := open(t, dir, batchlog.Options{SegmentBytes: 1 << 20})
	defer l.Close()

	commit(t, l, []byte("a"))
	for _, seq := range []uint64{0, 7, 1 << 40} {
		if _, err := l.Read(seq); !errors.Is(err, batchlog.ErrUnknownBatch) {
			t.Fatalf("Read(%d) = %v, want ErrUnknownBatch", seq, err)
		}
	}
}

func TestDoubleCommitReturnsUnknownBatch(t *testing.T) {
	dir := t.TempDir()
	l := open(t, dir, batchlog.Options{SegmentBytes: 1 << 20})
	defer l.Close()

	b, _ := l.Append([][]byte{[]byte("x")})
	seq, err := l.Commit(b)
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if _, err := l.Commit(b); !errors.Is(err, batchlog.ErrUnknownBatch) {
		t.Fatalf("second Commit = %v, want ErrUnknownBatch", err)
	}
	// The committed content is unchanged and no new sequence was issued.
	recs, err := l.Read(seq)
	if err != nil || len(recs) != 1 || string(recs[0]) != "x" {
		t.Fatalf("Read after double commit = %q, %v", recs, err)
	}
	if _, err := l.Read(seq + 1); !errors.Is(err, batchlog.ErrUnknownBatch) {
		t.Fatalf("Read(%d) = %v, want ErrUnknownBatch", seq+1, err)
	}
}

func TestBatchIsAtomicallyVisible(t *testing.T) {
	dir := t.TempDir()
	l := open(t, dir, batchlog.Options{SegmentBytes: 1 << 20})
	defer l.Close()

	b, _ := l.Append([][]byte{[]byte("r1"), []byte("r2")})
	var seen int
	if err := l.Scan(1, func(batchlog.Batch) error { seen++; return nil }); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if seen != 0 {
		t.Fatalf("scan saw %d batches before commit", seen)
	}
	if _, err := l.Commit(b); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	var got [][]byte
	if err := l.Scan(1, func(b batchlog.Batch) error { got = b.Records(); return nil }); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if len(got) != 2 || string(got[0]) != "r1" || string(got[1]) != "r2" {
		t.Fatalf("scan got %q after commit", got)
	}
}

func TestScanReplaysInSequenceOrder(t *testing.T) {
	dir := t.TempDir()
	l := open(t, dir, batchlog.Options{SegmentBytes: 1 << 20})
	defer l.Close()

	for i := 0; i < 5; i++ {
		commit(t, l, []byte(fmt.Sprintf("rec-%d", i)))
	}
	var order []uint64
	err := l.Scan(3, func(b batchlog.Batch) error {
		order = append(order, b.Seq())
		return nil
	})
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if fmt.Sprint(order) != "[3 4 5]" {
		t.Fatalf("scan order = %v, want [3 4 5]", order)
	}
}

func TestScanStopsOnCallbackError(t *testing.T) {
	dir := t.TempDir()
	l := open(t, dir, batchlog.Options{SegmentBytes: 1 << 20})
	defer l.Close()

	commit(t, l, []byte("a"))
	commit(t, l, []byte("b"))
	boom := errors.New("boom")
	calls := 0
	err := l.Scan(1, func(batchlog.Batch) error { calls++; return boom })
	if !errors.Is(err, boom) || calls != 1 {
		t.Fatalf("Scan = %v after %d calls, want boom after 1", err, calls)
	}
}

func TestSegmentRollover(t *testing.T) {
	dir := t.TempDir()
	// One record of 10 bytes encodes to 34 bytes; capacity 40 fits one
	// entry per segment.
	l := open(t, dir, batchlog.Options{SegmentBytes: 40})
	for i := 0; i < 3; i++ {
		commit(t, l, []byte("0123456789"))
	}
	segs := l.Segments()
	if len(segs) != 3 {
		t.Fatalf("Segments() = %v, want 3 segments", segs)
	}
	for i, s := range segs {
		want := uint64(i + 1)
		if s.FirstSeq != want || s.LastSeq != want {
			t.Fatalf("segment %d = {%d,%d}, want {%d,%d}", i, s.FirstSeq, s.LastSeq, want, want)
		}
	}
	l.Close()

	// Segments survive a reopen.
	l = open(t, dir, batchlog.Options{SegmentBytes: 40})
	defer l.Close()
	if segs := l.Segments(); len(segs) != 3 {
		t.Fatalf("Segments() after reopen = %v", segs)
	}
	recs, err := l.Read(2)
	if err != nil || string(recs[0]) != "0123456789" {
		t.Fatalf("Read(2) after reopen = %q, %v", recs, err)
	}
}

func TestPersistenceAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	l := open(t, dir, batchlog.Options{SegmentBytes: 1 << 20, Sync: true})
	commit(t, l, []byte("one"))
	commit(t, l, []byte("two"))
	l.Close()

	l = open(t, dir, batchlog.Options{SegmentBytes: 1 << 20})
	defer l.Close()
	recs, err := l.Read(2)
	if err != nil || string(recs[0]) != "two" {
		t.Fatalf("Read(2) = %q, %v", recs, err)
	}
	// Sequence numbering continues where the previous writer left off.
	if seq := commit(t, l, []byte("three")); seq != 3 {
		t.Fatalf("seq after reopen = %d, want 3", seq)
	}
}

func TestRecordsAreOpaqueBytes(t *testing.T) {
	dir := t.TempDir()
	l := open(t, dir, batchlog.Options{SegmentBytes: 1 << 20})
	defer l.Close()

	blob := []byte{0x00, 0xff, 0x0a, 0x00, 0x7f, 0x80}
	seq := commit(t, l, blob, []byte{})
	recs, err := l.Read(seq)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !bytes.Equal(recs[0], blob) || len(recs[1]) != 0 {
		t.Fatalf("Read = %v, want %v and empty record", recs, blob)
	}
}

func TestTruncatedSegmentIsCorrupt(t *testing.T) {
	dir := t.TempDir()
	l := open(t, dir, batchlog.Options{SegmentBytes: 1 << 20})
	commit(t, l, []byte("payload"))
	l.Close()

	path := filepath.Join(dir, "segment-000001.log")
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(path, info.Size()/2); err != nil {
		t.Fatal(err)
	}
	if _, err := batchlog.Open(dir, batchlog.Options{SegmentBytes: 1 << 20}); !errors.Is(err, batchlog.ErrCorruptSegment) {
		t.Fatalf("Open after truncate = %v, want ErrCorruptSegment", err)
	}
}

func TestTamperedSegmentIsCorrupt(t *testing.T) {
	dir := t.TempDir()
	l := open(t, dir, batchlog.Options{SegmentBytes: 1 << 20})
	commit(t, l, []byte("payload"))
	l.Close()

	path := filepath.Join(dir, "segment-000001.log")
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte{0xde}, 12); err != nil { // inside the payload
		t.Fatal(err)
	}
	f.Close()
	if _, err := batchlog.Open(dir, batchlog.Options{SegmentBytes: 1 << 20}); !errors.Is(err, batchlog.ErrCorruptSegment) {
		t.Fatalf("Open after tamper = %v, want ErrCorruptSegment", err)
	}
}

func TestInvalidOptions(t *testing.T) {
	for _, size := range []int{0, -1} {
		if _, err := batchlog.Open(t.TempDir(), batchlog.Options{SegmentBytes: size}); !errors.Is(err, batchlog.ErrInvalidOptions) {
			t.Fatalf("Open(SegmentBytes=%d) = %v, want ErrInvalidOptions", size, err)
		}
	}
}

func TestOpenCreatesMissingDirectory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "nested", "log")
	l, err := batchlog.Open(dir, batchlog.Options{SegmentBytes: 1 << 20})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer l.Close()
	if segs := l.Segments(); len(segs) != 0 {
		t.Fatalf("Segments() = %v, want none", segs)
	}
}

func TestEmptyBatchCommits(t *testing.T) {
	dir := t.TempDir()
	l := open(t, dir, batchlog.Options{SegmentBytes: 1 << 20})
	defer l.Close()

	seq := commit(t, l)
	recs, err := l.Read(seq)
	if err != nil || len(recs) != 0 {
		t.Fatalf("Read(empty batch) = %v, %v", recs, err)
	}
}
