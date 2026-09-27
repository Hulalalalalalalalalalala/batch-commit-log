package log_test

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
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
	// newCorruptLog writes two one-batch segments and mutates one of
	// them. Mutating an earlier segment can never look like a crash's
	// torn tail, which only the last segment may carry.
	newCorruptLog := func(t *testing.T, last bool, mutate func([]byte) []byte) string {
		dir := t.TempDir()
		l := open(t, dir, log.Options{SegmentBytes: 32})
		commit(t, l, []byte("rec-0"))
		commit(t, l, []byte("rec-1"))
		l.Close()

		name := "000001.seg"
		if last {
			name = "000002.seg"
		}
		path := filepath.Join(dir, name)
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, mutate(data), 0o644); err != nil {
			t.Fatal(err)
		}
		return dir
	}

	t.Run("tampered", func(t *testing.T) {
		dir := newCorruptLog(t, false, func(d []byte) []byte {
			d[len(d)-1] ^= 0xff // corrupt the trailing checksum
			return d
		})
		if _, err := log.Open(dir, log.Options{SegmentBytes: 32}); !errors.Is(err, log.ErrCorruptSegment) {
			t.Fatalf("Open err = %v, want ErrCorruptSegment", err)
		}
	})
	t.Run("tampered-last-segment", func(t *testing.T) {
		dir := newCorruptLog(t, true, func(d []byte) []byte {
			d[len(d)-1] ^= 0xff // full-length entry, bad checksum: not a torn write
			return d
		})
		if _, err := log.Open(dir, log.Options{SegmentBytes: 32}); !errors.Is(err, log.ErrCorruptSegment) {
			t.Fatalf("Open err = %v, want ErrCorruptSegment", err)
		}
	})
	t.Run("truncated", func(t *testing.T) {
		dir := newCorruptLog(t, false, func(d []byte) []byte { return d[:len(d)-3] })
		if _, err := log.Open(dir, log.Options{SegmentBytes: 32}); !errors.Is(err, log.ErrCorruptSegment) {
			t.Fatalf("Open err = %v, want ErrCorruptSegment", err)
		}
	})
	t.Run("garbage", func(t *testing.T) {
		dir := newCorruptLog(t, false, func(d []byte) []byte { return []byte("not a segment") })
		if _, err := log.Open(dir, log.Options{SegmentBytes: 32}); !errors.Is(err, log.ErrCorruptSegment) {
			t.Fatalf("Open err = %v, want ErrCorruptSegment", err)
		}
	})
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

// appendTornEntry simulates a crash mid-commit: it writes the beginning
// of a batch entry for seq — header complete, record cut short, no
// checksum — to the end of the last segment file.
func appendTornEntry(t *testing.T, dir string, seq uint64, record string) {
	t.Helper()
	var buf []byte
	buf = append(buf, 'B', 'C', 'L', '1')
	var tmp [8]byte
	binary.LittleEndian.PutUint64(tmp[:], seq)
	buf = append(buf, tmp[:]...)
	binary.LittleEndian.PutUint32(tmp[:4], 1)
	buf = append(buf, tmp[:4]...)
	binary.LittleEndian.PutUint32(tmp[:4], uint32(len(record)))
	buf = append(buf, tmp[:4]...)
	buf = append(buf, record[:len(record)/2]...) // torn mid-record

	files, err := filepath.Glob(filepath.Join(dir, "*.seg"))
	if err != nil || len(files) == 0 {
		t.Fatalf("segment files = %v, %v", files, err)
	}
	f, err := os.OpenFile(files[len(files)-1], os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(buf); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestTornTailDiscarded(t *testing.T) {
	dir := t.TempDir()
	opts := log.Options{SegmentBytes: 1 << 20}

	l := open(t, dir, opts)
	commit(t, l, []byte("one"))
	commit(t, l, []byte("two"))
	l.Close()

	appendTornEntry(t, dir, 3, "three")

	// The torn tail neither errors nor becomes visible.
	l = open(t, dir, opts)
	if got, err := l.Read(2); err != nil || string(got[0]) != "two" {
		t.Fatalf("Read(2) = %q, %v", got, err)
	}
	if _, err := l.Read(3); !errors.Is(err, log.ErrNotCommitted) {
		t.Fatalf("Read(3) err = %v, want ErrNotCommitted", err)
	}
	var seqs []uint64
	if err := l.Scan(1, func(b log.Batch) error { seqs = append(seqs, b.Seq()); return nil }); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if !reflect.DeepEqual(seqs, []uint64{1, 2}) {
		t.Fatalf("scan seqs = %v, want [1 2]", seqs)
	}

	// A reopen with no commits in between still finds the torn tail, so
	// the reserved sequence is neither rolled back nor reused.
	l.Close()
	l = open(t, dir, opts)
	if _, err := l.Read(3); !errors.Is(err, log.ErrNotCommitted) {
		t.Fatalf("after quiet reopen Read(3) err = %v, want ErrNotCommitted", err)
	}
	b, err := l.Append([][]byte{[]byte("four")})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if b.Seq() != 4 {
		t.Fatalf("next seq after torn tail = %d, want 4", b.Seq())
	}
	if _, err := l.Commit(b); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	l.Close()

	// The next write cut the torn bytes away; seq 3 stays a permanent
	// hole that replay skips.
	l = open(t, dir, opts)
	if _, err := l.Read(3); errors.Is(err, log.ErrNotCommitted) {
		t.Fatalf("seq 3 must not be committed by recovery")
	}
	seqs = nil
	if err := l.Scan(1, func(b log.Batch) error { seqs = append(seqs, b.Seq()); return nil }); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if !reflect.DeepEqual(seqs, []uint64{1, 2, 4}) {
		t.Fatalf("scan seqs = %v, want [1 2 4]", seqs)
	}
	if got, err := l.Read(4); err != nil || string(got[0]) != "four" {
		t.Fatalf("Read(4) = %q, %v", got, err)
	}
}

func TestTornTailInEarlierSegmentIsCorrupt(t *testing.T) {
	dir := t.TempDir()
	l := open(t, dir, log.Options{SegmentBytes: 32})
	commit(t, l, []byte("rec-0"))
	commit(t, l, []byte("rec-1"))
	l.Close()

	// A half-written entry anywhere but the last segment is tampering.
	f, err := os.OpenFile(filepath.Join(dir, "000001.seg"), os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{'B', 'C'}); err != nil {
		t.Fatal(err)
	}
	f.Close()

	if _, err := log.Open(dir, log.Options{SegmentBytes: 32}); !errors.Is(err, log.ErrCorruptSegment) {
		t.Fatalf("Open err = %v, want ErrCorruptSegment", err)
	}
}

func TestEmptyBatch(t *testing.T) {
	dir := t.TempDir()
	opts := log.Options{SegmentBytes: 1 << 20}
	l := open(t, dir, opts)

	seq := commit(t, l) // no records at all
	got, err := l.Read(seq)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if got == nil || len(got) != 0 {
		t.Fatalf("Read = %v, want empty record set", got)
	}
	var seen []uint64
	if err := l.Scan(1, func(b log.Batch) error {
		seen = append(seen, b.Seq())
		if len(b.Records()) != 0 {
			t.Fatalf("empty batch replayed with records")
		}
		return nil
	}); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if !reflect.DeepEqual(seen, []uint64{1}) {
		t.Fatalf("scan seqs = %v, want [1]", seen)
	}

	l.Close()
	l = open(t, dir, opts)
	if got, err := l.Read(seq); err != nil || len(got) != 0 {
		t.Fatalf("after reopen Read = %v, %v; want empty record set", got, err)
	}
}

func TestScanIsSnapshot(t *testing.T) {
	l := open(t, t.TempDir(), log.Options{SegmentBytes: 1 << 20})
	commit(t, l, []byte("a"))
	commit(t, l, []byte("b"))

	var seen []uint64
	err := l.Scan(1, func(b log.Batch) error {
		seen = append(seen, b.Seq())
		if b.Seq() == 1 {
			commit(t, l, []byte("late")) // committed mid-scan
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if !reflect.DeepEqual(seen, []uint64{1, 2}) {
		t.Fatalf("scan saw %v, want [1 2]; mid-scan commit leaked in", seen)
	}

	seen = nil
	if err := l.Scan(1, func(b log.Batch) error { seen = append(seen, b.Seq()); return nil }); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if !reflect.DeepEqual(seen, []uint64{1, 2, 3}) {
		t.Fatalf("second scan = %v, want [1 2 3]", seen)
	}
}

func TestConcurrentReadAndScan(t *testing.T) {
	l := open(t, t.TempDir(), log.Options{SegmentBytes: 1 << 20})
	const batches = 50

	var wg sync.WaitGroup
	// The single writer.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 1; i <= batches; i++ {
			b, err := l.Append([][]byte{[]byte(fmt.Sprintf("rec-%d", i))})
			if err != nil {
				t.Errorf("Append: %v", err)
				return
			}
			if _, err := l.Commit(b); err != nil {
				t.Errorf("Commit: %v", err)
				return
			}
		}
	}()
	// Concurrent readers: whatever Read returns must be a consistent,
	// immutable view of one committed batch.
	for g := 0; g < 3; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 300; i++ {
				seq := uint64(i%batches + 1)
				got, err := l.Read(seq)
				switch {
				case errors.Is(err, log.ErrNotCommitted), errors.Is(err, log.ErrUnknownBatch):
					continue
				case err != nil:
					t.Errorf("Read(%d): %v", seq, err)
					return
				}
				want := fmt.Sprintf("rec-%d", seq)
				if len(got) != 1 || string(got[0]) != want {
					t.Errorf("Read(%d) = %q, want %q", seq, got, want)
					return
				}
				got[0][0] = 'X' // must not corrupt the log
			}
		}()
	}
	// Concurrent replayers: every scan sees committed batches in order.
	for g := 0; g < 2; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				var last uint64
				err := l.Scan(1, func(b log.Batch) error {
					if b.Seq() <= last {
						t.Errorf("scan out of order: %d after %d", b.Seq(), last)
					}
					last = b.Seq()
					return nil
				})
				if err != nil {
					t.Errorf("Scan: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()

	// After the writer finished, every batch is readable and intact.
	for seq := uint64(1); seq <= batches; seq++ {
		got, err := l.Read(seq)
		if err != nil {
			t.Fatalf("Read(%d): %v", seq, err)
		}
		if want := fmt.Sprintf("rec-%d", seq); string(got[0]) != want {
			t.Fatalf("Read(%d) = %q, want %q", seq, got[0], want)
		}
	}
}
