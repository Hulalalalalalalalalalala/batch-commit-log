package log_test

import (
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

	t.Run("tampered", func(t *testing.T) {
		dir := newCorruptLog(t, func(d []byte) []byte {
			d[20] ^= 0xff // inside the record payload: checksum mismatch
			return d
		})
		if _, err := log.Open(dir, log.Options{SegmentBytes: 1 << 20}); !errors.Is(err, log.ErrCorruptSegment) {
			t.Fatalf("Open err = %v, want ErrCorruptSegment", err)
		}
	})
	t.Run("tampered checksum", func(t *testing.T) {
		dir := newCorruptLog(t, func(d []byte) []byte {
			d[len(d)-1] ^= 0xff // inside the trailing checksum itself
			return d
		})
		if _, err := log.Open(dir, log.Options{SegmentBytes: 1 << 20}); !errors.Is(err, log.ErrCorruptSegment) {
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

func TestTruncatedEarlierSegmentIsCorrupt(t *testing.T) {
	// One small batch per segment; damaging anything but the latest
	// segment's tail is corruption, not crash recovery.
	for _, victim := range []string{"000001.seg", "000002.seg"} {
		t.Run(victim, func(t *testing.T) {
			dir := t.TempDir()
			l := open(t, dir, log.Options{SegmentBytes: 32})
			for i := 0; i < 3; i++ {
				commit(t, l, []byte(fmt.Sprintf("rec-%d", i)))
			}
			l.Close()

			path := filepath.Join(dir, victim)
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, data[:len(data)-3], 0o644); err != nil {
				t.Fatal(err)
			}
			if _, err := log.Open(dir, log.Options{SegmentBytes: 32}); !errors.Is(err, log.ErrCorruptSegment) {
				t.Fatalf("Open err = %v, want ErrCorruptSegment", err)
			}
		})
	}
}

func TestTornTailRecoveredOnOpen(t *testing.T) {
	// A crash mid-write leaves a prefix of the last entry: inside the
	// checksum, inside a record, inside the header, or inside the magic.
	// Entry for "three-torn" is 4+8+4+4+10+4 = 34 bytes.
	for _, cut := range []int{3, 10, 22, 32} {
		t.Run(fmt.Sprintf("cut-%d", cut), func(t *testing.T) {
			dir := t.TempDir()
			opts := log.Options{SegmentBytes: 1 << 20, Sync: true}
			l := open(t, dir, opts)
			commit(t, l, []byte("one"))
			commit(t, l, []byte("two"))
			commit(t, l, []byte("three-torn"))
			l.Close()

			path := filepath.Join(dir, "000001.seg")
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, data[:len(data)-cut], 0o644); err != nil {
				t.Fatal(err)
			}

			l = open(t, dir, opts)
			// Batches committed before the crash are intact, same seqs.
			for seq, want := range map[uint64]string{1: "one", 2: "two"} {
				got, err := l.Read(seq)
				if err != nil || string(got[0]) != want {
					t.Fatalf("Read(%d) = %q, %v; want %q", seq, got, err, want)
				}
			}
			// The torn batch was never committed: unknown, not replayed.
			if _, err := l.Read(3); !errors.Is(err, log.ErrUnknownBatch) {
				t.Fatalf("Read(3) err = %v, want ErrUnknownBatch", err)
			}
			var scanned []uint64
			if err := l.Scan(1, func(b log.Batch) error {
				scanned = append(scanned, b.Seq())
				return nil
			}); err != nil {
				t.Fatalf("Scan: %v", err)
			}
			if !reflect.DeepEqual(scanned, []uint64{1, 2}) {
				t.Fatalf("scanned = %v, want [1 2]", scanned)
			}
			// Segment listing ends at the last complete batch.
			if segs := l.Segments(); !reflect.DeepEqual(segs, []log.Segment{{FirstSeq: 1, LastSeq: 2}}) {
				t.Fatalf("Segments = %+v, want [{1 2}]", segs)
			}
			// The next batch continues from the last committed seq and
			// may reuse the torn batch's number.
			if seq := commit(t, l, []byte("three")); seq != 3 {
				t.Fatalf("seq after recovery = %d, want 3", seq)
			}
			l.Close()

			// The torn bytes were dropped, so the log reopens cleanly.
			l = open(t, dir, opts)
			got, err := l.Read(3)
			if err != nil || string(got[0]) != "three" {
				t.Fatalf("Read(3) after rewrite = %q, %v", got, err)
			}
		})
	}
}

func TestCommitSyncFailureKeepsBatchStaged(t *testing.T) {
	dir := t.TempDir()
	// Capacity forces a roll on the second commit; a directory squatting
	// on the next segment's path makes the roll fail.
	l := open(t, dir, log.Options{SegmentBytes: 24, Sync: true})
	commit(t, l, []byte("first"))

	block := filepath.Join(dir, "000002.seg")
	if err := os.Mkdir(block, 0o755); err != nil {
		t.Fatal(err)
	}
	b, err := l.Append([][]byte{[]byte("second")})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if _, err := l.Commit(b); !errors.Is(err, log.ErrSyncFailed) {
		t.Fatalf("Commit err = %v, want ErrSyncFailed", err)
	}
	// The batch stayed uncommitted: still staged, not readable, not scanned.
	if _, err := l.Read(b.Seq()); !errors.Is(err, log.ErrNotCommitted) {
		t.Fatalf("Read after failed commit err = %v, want ErrNotCommitted", err)
	}
	count := 0
	if err := l.Scan(1, func(b log.Batch) error { count++; return nil }); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if count != 1 {
		t.Fatalf("Scan visited %d batches, want 1", count)
	}

	// Once the obstruction is gone the same batch commits with its seq.
	if err := os.Remove(block); err != nil {
		t.Fatal(err)
	}
	seq, err := l.Commit(b)
	if err != nil {
		t.Fatalf("retry Commit: %v", err)
	}
	if seq != 2 {
		t.Fatalf("retry seq = %d, want 2", seq)
	}
	got, err := l.Read(2)
	if err != nil || string(got[0]) != "second" {
		t.Fatalf("Read(2) = %q, %v", got, err)
	}
	if segs := l.Segments(); !reflect.DeepEqual(segs, []log.Segment{{FirstSeq: 1, LastSeq: 1}, {FirstSeq: 2, LastSeq: 2}}) {
		t.Fatalf("Segments = %+v, want [{1 1} {2 2}]", segs)
	}
}

func TestEmptyBatchCommits(t *testing.T) {
	dir := t.TempDir()
	opts := log.Options{SegmentBytes: 1 << 20}
	l := open(t, dir, opts)

	b, err := l.Append(nil)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	seq, err := l.Commit(b)
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if seq != 1 {
		t.Fatalf("empty batch seq = %d, want 1", seq)
	}
	got, err := l.Read(seq)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("Read = %q, want zero records", got)
	}
	var scanned []uint64
	if err := l.Scan(1, func(b log.Batch) error {
		scanned = append(scanned, b.Seq())
		if n := len(b.Records()); n != 0 {
			t.Fatalf("scanned batch has %d records, want 0", n)
		}
		return nil
	}); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if !reflect.DeepEqual(scanned, []uint64{1}) {
		t.Fatalf("scanned = %v, want [1]", scanned)
	}
	l.Close()

	l = open(t, dir, opts)
	if got, err := l.Read(1); err != nil || len(got) != 0 {
		t.Fatalf("Read(1) after reopen = %q, %v; want zero records", got, err)
	}
	if seq := commit(t, l, []byte("next")); seq != 2 {
		t.Fatalf("seq after empty batch = %d, want 2", seq)
	}
}

func TestScanSnapshotExcludesLaterCommits(t *testing.T) {
	l := open(t, t.TempDir(), log.Options{SegmentBytes: 1 << 20})
	commit(t, l, []byte("a"))
	commit(t, l, []byte("b"))

	var seen []uint64
	err := l.Scan(1, func(b log.Batch) error {
		seen = append(seen, b.Seq())
		if len(seen) == 1 {
			// Committed after the scan started: must not join this replay.
			commit(t, l, []byte("late"))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if !reflect.DeepEqual(seen, []uint64{1, 2}) {
		t.Fatalf("seen = %v, want [1 2]", seen)
	}
	// A fresh scan does see it.
	seen = seen[:0]
	if err := l.Scan(1, func(b log.Batch) error { seen = append(seen, b.Seq()); return nil }); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if !reflect.DeepEqual(seen, []uint64{1, 2, 3}) {
		t.Fatalf("seen = %v, want [1 2 3]", seen)
	}
}

func TestConcurrentReadersSeeOnlyCommitted(t *testing.T) {
	l := open(t, t.TempDir(), log.Options{SegmentBytes: 1 << 20, Sync: true})

	done := make(chan struct{})
	errs := make(chan error, 4)
	var wg sync.WaitGroup
	for r := 0; r < 3; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
				}
				prev := uint64(0)
				err := l.Scan(1, func(b log.Batch) error {
					if b.Seq() <= prev {
						return fmt.Errorf("scan out of order: %d after %d", b.Seq(), prev)
					}
					prev = b.Seq()
					// Every scanned batch is fully readable.
					if _, err := l.Read(b.Seq()); err != nil {
						return err
					}
					return nil
				})
				if err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	for i := 0; i < 50; i++ {
		commit(t, l, []byte(fmt.Sprintf("rec-%d", i)))
	}
	close(done)
	wg.Wait()
	select {
	case err := <-errs:
		t.Fatalf("concurrent scan: %v", err)
	default:
	}
}

func TestCommitAfterReopenExtendsSegmentListing(t *testing.T) {
	dir := t.TempDir()
	opts := log.Options{SegmentBytes: 1 << 20}
	l := open(t, dir, opts)
	commit(t, l, []byte("one"))
	l.Close()

	l = open(t, dir, opts)
	commit(t, l, []byte("two"))
	if segs := l.Segments(); !reflect.DeepEqual(segs, []log.Segment{{FirstSeq: 1, LastSeq: 2}}) {
		t.Fatalf("Segments = %+v, want [{1 2}]", segs)
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
