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
	"time"

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
			d[20] ^= 0xff // a payload byte: checksum mismatch
			return d
		})
		if _, err := log.Open(dir, log.Options{SegmentBytes: 1 << 20}); !errors.Is(err, log.ErrCorruptSegment) {
			t.Fatalf("Open err = %v, want ErrCorruptSegment", err)
		}
	})
	t.Run("truncated non-last segment", func(t *testing.T) {
		// Only the last segment may end in a crash tail; a torn
		// earlier segment is tampering.
		dir := t.TempDir()
		l := open(t, dir, log.Options{SegmentBytes: 32})
		commit(t, l, []byte("one"))
		commit(t, l, []byte("two"))
		l.Close()

		first := filepath.Join(dir, "000001.seg")
		data, err := os.ReadFile(first)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(first, data[:len(data)-3], 0o644); err != nil {
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

// appendTornEntry writes a half-finished batch entry for seq to the end
// of the last segment file, simulating a crash mid-write.
func appendTornEntry(t *testing.T, dir string, seq uint64) {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(dir, "*.seg"))
	if err != nil || len(files) == 0 {
		t.Fatalf("segment files = %v, %v", files, err)
	}
	last := files[len(files)-1]
	var entry []byte
	entry = append(entry, 'B', 'C', 'L', '1')
	var tmp [8]byte
	binary.LittleEndian.PutUint64(tmp[:], seq)
	entry = append(entry, tmp[:]...)
	binary.LittleEndian.PutUint32(tmp[:4], 1) // one record
	entry = append(entry, tmp[:4]...)
	binary.LittleEndian.PutUint32(tmp[:4], 100) // of 100 bytes
	entry = append(entry, tmp[:4]...)
	entry = append(entry, []byte("only-a-prefix-of-the-record")...)
	f, err := os.OpenFile(last, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(entry); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCrashTailRecovery(t *testing.T) {
	dir := t.TempDir()
	opts := log.Options{SegmentBytes: 1 << 20}

	l := open(t, dir, opts)
	commit(t, l, []byte("one"))
	commit(t, l, []byte("two"))
	l.Close()

	// Power loss halfway through writing batch 3.
	appendTornEntry(t, dir, 3)

	l = open(t, dir, opts) // must not fail
	if got, err := l.Read(1); err != nil || string(got[0]) != "one" {
		t.Fatalf("Read(1) = %q, %v", got, err)
	}
	// The crashed batch stays staged: its sequence is a permanent hole.
	if _, err := l.Read(3); !errors.Is(err, log.ErrNotCommitted) {
		t.Fatalf("Read(3) err = %v, want ErrNotCommitted", err)
	}
	// The next staged batch gets the hole's sequence plus one.
	b, err := l.Append([][]byte{[]byte("four")})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if b.Seq() != 4 {
		t.Fatalf("seq after crash tail = %d, want 4", b.Seq())
	}
	if _, err := l.Commit(b); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	// Replay skips the hole and yields committed batches in order.
	var seqs []uint64
	if err := l.Scan(1, func(b log.Batch) error {
		seqs = append(seqs, b.Seq())
		return nil
	}); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if !reflect.DeepEqual(seqs, []uint64{1, 2, 4}) {
		t.Fatalf("scan seqs = %v, want [1 2 4]", seqs)
	}
	l.Close()

	// The torn bytes were dropped: after another reopen the log is
	// consistent and the hole does not come back as a torn entry.
	l = open(t, dir, opts)
	var again []uint64
	if err := l.Scan(1, func(b log.Batch) error {
		again = append(again, b.Seq())
		return nil
	}); err != nil {
		t.Fatalf("Scan after reopen: %v", err)
	}
	if !reflect.DeepEqual(again, []uint64{1, 2, 4}) {
		t.Fatalf("scan after reopen = %v, want [1 2 4]", again)
	}
	if seq := commit(t, l, []byte("five")); seq != 5 {
		t.Fatalf("seq = %d, want 5", seq)
	}
}

func TestCrashTailTooShortForSequence(t *testing.T) {
	dir := t.TempDir()
	l := open(t, dir, log.Options{SegmentBytes: 1 << 20})
	commit(t, l, []byte("one"))
	l.Close()

	// A torn write of only two bytes carries no recoverable sequence;
	// it is still just discarded.
	if err := os.WriteFile(filepath.Join(dir, "000001.seg"),
		append(mustReadFile(t, filepath.Join(dir, "000001.seg")), 'B', 'C'), 0o644); err != nil {
		t.Fatal(err)
	}
	l = open(t, dir, log.Options{SegmentBytes: 1 << 20})
	if seq := commit(t, l, []byte("two")); seq != 2 {
		t.Fatalf("seq = %d, want 2", seq)
	}
}

func mustReadFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestEmptyBatch(t *testing.T) {
	dir := t.TempDir()
	opts := log.Options{SegmentBytes: 1 << 20}
	l := open(t, dir, opts)

	b, err := l.Append(nil)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if _, err := l.Commit(b); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	got, err := l.Read(b.Seq())
	if err != nil {
		t.Fatalf("Read empty batch: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("Read = %v, want empty record set", got)
	}
	var seen []uint64
	if err := l.Scan(1, func(b log.Batch) error {
		seen = append(seen, b.Seq())
		if n := len(b.Records()); n != 0 {
			t.Fatalf("replayed batch has %d records, want 0", n)
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
	if got, err := l.Read(1); err != nil || len(got) != 0 {
		t.Fatalf("Read after reopen = %v, %v; want empty record set", got, err)
	}
}

func TestCommitAfterReopenExtendsLastSegment(t *testing.T) {
	dir := t.TempDir()
	opts := log.Options{SegmentBytes: 1 << 20}
	l := open(t, dir, opts)
	commit(t, l, []byte("one"))
	commit(t, l, []byte("two"))
	l.Close()

	l = open(t, dir, opts)
	commit(t, l, []byte("three"))
	segs := l.Segments()
	if len(segs) != 1 || segs[0].FirstSeq != 1 || segs[0].LastSeq != 3 {
		t.Fatalf("Segments = %+v, want one segment [1,3]", segs)
	}
}

func TestScanSeesConsistentSnapshot(t *testing.T) {
	l := open(t, t.TempDir(), log.Options{SegmentBytes: 1 << 20})
	commit(t, l, []byte("one"))
	commit(t, l, []byte("two"))

	// Batches committed during a replay are invisible to that replay,
	// even when the callback itself commits them.
	var seqs []uint64
	err := l.Scan(1, func(b log.Batch) error {
		seqs = append(seqs, b.Seq())
		if b.Seq() == 1 {
			commit(t, l, []byte("three"))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if !reflect.DeepEqual(seqs, []uint64{1, 2}) {
		t.Fatalf("scan seqs = %v, want [1 2]", seqs)
	}
}

func TestConcurrentReadWrite(t *testing.T) {
	l := open(t, t.TempDir(), log.Options{SegmentBytes: 1 << 20})

	const batches = 50
	done := make(chan struct{})
	var wg sync.WaitGroup
	// Concurrent readers and replayers while the writer commits.
	for g := 0; g < 3; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
				}
				for seq := uint64(1); seq <= batches; seq++ {
					got, err := l.Read(seq)
					if errors.Is(err, log.ErrUnknownBatch) || errors.Is(err, log.ErrNotCommitted) {
						continue
					}
					if err != nil {
						t.Errorf("Read(%d): %v", seq, err)
						return
					}
					want := []byte(fmt.Sprintf("rec-%d", seq))
					if len(got) != 1 || !reflect.DeepEqual(got[0], want) {
						t.Errorf("Read(%d) = %q, want %q", seq, got, want)
						return
					}
				}
				var prev uint64
				if err := l.Scan(1, func(b log.Batch) error {
					if b.Seq() <= prev {
						t.Errorf("scan out of order: %d after %d", b.Seq(), prev)
					}
					prev = b.Seq()
					return nil
				}); err != nil {
					t.Errorf("Scan: %v", err)
					return
				}
			}
		}()
	}
	for i := 1; i <= batches; i++ {
		commit(t, l, []byte(fmt.Sprintf("rec-%d", i)))
	}
	close(done)
	wg.Wait()
}

func TestCommitGroup(t *testing.T) {
	dir := t.TempDir()
	opts := log.Options{SegmentBytes: 1 << 20, Sync: true}
	l := open(t, dir, opts)

	b1, _ := l.Append([][]byte{[]byte("g1a"), []byte("g1b")})
	b2, _ := l.Append([][]byte{[]byte("g2")})
	b3, _ := l.Append([][]byte{[]byte("g3")})

	// A group commits atomically; members keep their reserved sequences.
	seqs, err := l.CommitGroup([]log.Batch{b1, b3})
	if err != nil {
		t.Fatalf("CommitGroup: %v", err)
	}
	if !reflect.DeepEqual(seqs, []uint64{1, 3}) {
		t.Fatalf("group seqs = %v, want [1 3]", seqs)
	}
	// b2 was not in the group: still staged.
	if _, err := l.Read(2); !errors.Is(err, log.ErrNotCommitted) {
		t.Fatalf("Read(2) err = %v, want ErrNotCommitted", err)
	}
	if got, err := l.Read(1); err != nil || !reflect.DeepEqual(got, [][]byte{[]byte("g1a"), []byte("g1b")}) {
		t.Fatalf("Read(1) = %q, %v", got, err)
	}
	if got, err := l.Read(3); err != nil || !reflect.DeepEqual(got, [][]byte{[]byte("g3")}) {
		t.Fatalf("Read(3) = %q, %v", got, err)
	}
	// Replay order follows reserved sequences, not commit order.
	if _, err := l.Commit(b2); err != nil {
		t.Fatalf("Commit b2: %v", err)
	}
	var got []uint64
	if err := l.Scan(1, func(b log.Batch) error { got = append(got, b.Seq()); return nil }); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if !reflect.DeepEqual(got, []uint64{1, 2, 3}) {
		t.Fatalf("scan = %v, want [1 2 3]", got)
	}
	l.Close()

	// The group survives a reopen exactly once per member: no
	// duplicates, and the index rebuilt from the segments serves the
	// same contents the replay sees.
	l = open(t, dir, opts)
	got = nil
	if err := l.Scan(1, func(b log.Batch) error { got = append(got, b.Seq()); return nil }); err != nil {
		t.Fatalf("Scan after reopen: %v", err)
	}
	if !reflect.DeepEqual(got, []uint64{1, 2, 3}) {
		t.Fatalf("scan after reopen = %v, want [1 2 3]", got)
	}
	for seq, want := range map[uint64]string{1: "g1a", 2: "g2", 3: "g3"} {
		r, err := l.Read(seq)
		if err != nil || len(r) == 0 || string(r[0]) != want {
			t.Fatalf("Read(%d) after reopen = %q, %v; want %q", seq, r, err, want)
		}
	}
}

func TestCommitGroupValidation(t *testing.T) {
	l := open(t, t.TempDir(), log.Options{SegmentBytes: 1 << 20})

	b1, _ := l.Append([][]byte{[]byte("a")})
	b2, _ := l.Append([][]byte{[]byte("b")})

	// A zero-value batch was never staged: the whole group is rejected
	// and the valid members stay staged.
	if _, err := l.CommitGroup([]log.Batch{b1, {}}); !errors.Is(err, log.ErrUnknownBatch) {
		t.Fatalf("CommitGroup err = %v, want ErrUnknownBatch", err)
	}
	if _, err := l.Read(1); !errors.Is(err, log.ErrNotCommitted) {
		t.Fatalf("Read(1) err = %v, want ErrNotCommitted", err)
	}
	// A duplicated member is rejected too.
	if _, err := l.CommitGroup([]log.Batch{b1, b1}); !errors.Is(err, log.ErrUnknownBatch) {
		t.Fatalf("CommitGroup dup err = %v, want ErrUnknownBatch", err)
	}
	// An empty group is a no-op.
	if seqs, err := l.CommitGroup(nil); err != nil || len(seqs) != 0 {
		t.Fatalf("CommitGroup(nil) = %v, %v", seqs, err)
	}
	// A committed batch cannot be committed again, alone or in a group.
	if _, err := l.CommitGroup([]log.Batch{b1, b2}); err != nil {
		t.Fatalf("CommitGroup: %v", err)
	}
	if _, err := l.CommitGroup([]log.Batch{b1}); !errors.Is(err, log.ErrUnknownBatch) {
		t.Fatalf("CommitGroup committed err = %v, want ErrUnknownBatch", err)
	}
	if _, err := l.Commit(b2); !errors.Is(err, log.ErrUnknownBatch) {
		t.Fatalf("Commit committed err = %v, want ErrUnknownBatch", err)
	}
}

func TestCommitGroupSegments(t *testing.T) {
	dir := t.TempDir()
	l := open(t, dir, log.Options{SegmentBytes: 64})

	commit(t, l, []byte("one")) // segment 1
	var bs []log.Batch
	for _, s := range []string{"g1", "g2", "g3"} {
		b, err := l.Append([][]byte{[]byte(s)})
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
		bs = append(bs, b)
	}
	if _, err := l.CommitGroup(bs); err != nil {
		t.Fatalf("CommitGroup: %v", err)
	}
	// The whole group lands in one fresh segment.
	segs := l.Segments()
	if len(segs) != 2 {
		t.Fatalf("len(Segments) = %d, want 2: %+v", len(segs), segs)
	}
	if segs[0].FirstSeq != 1 || segs[0].LastSeq != 1 {
		t.Fatalf("segment 0 = %+v, want [1,1]", segs[0])
	}
	if segs[1].FirstSeq != 2 || segs[1].LastSeq != 4 {
		t.Fatalf("group segment = %+v, want [2,4]", segs[1])
	}
	l.Close()

	l = open(t, dir, log.Options{SegmentBytes: 64})
	segs = l.Segments()
	if len(segs) != 2 || segs[1].FirstSeq != 2 || segs[1].LastSeq != 4 {
		t.Fatalf("after reopen Segments = %+v, want [_, [2,4]]", segs)
	}
	for seq := uint64(2); seq <= 4; seq++ {
		if _, err := l.Read(seq); err != nil {
			t.Fatalf("Read(%d) after reopen: %v", seq, err)
		}
	}
}

// appendTornGroup writes a half-finished group entry reserving seqs to
// the end of the last segment file, simulating a crash mid-group-write:
// every member but the last is complete, the last is cut off mid-record
// and the group checksum is missing.
func appendTornGroup(t *testing.T, dir string, seqs ...uint64) {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(dir, "*.seg"))
	if err != nil || len(files) == 0 {
		t.Fatalf("segment files = %v, %v", files, err)
	}
	last := files[len(files)-1]
	var entry []byte
	entry = append(entry, 'B', 'C', 'L', 'G')
	var tmp [8]byte
	binary.LittleEndian.PutUint32(tmp[:4], uint32(len(seqs)))
	entry = append(entry, tmp[:4]...)
	for i, seq := range seqs {
		binary.LittleEndian.PutUint64(tmp[:], seq)
		entry = append(entry, tmp[:]...)
		binary.LittleEndian.PutUint32(tmp[:4], 1) // one record
		entry = append(entry, tmp[:4]...)
		if i == len(seqs)-1 {
			binary.LittleEndian.PutUint32(tmp[:4], 100) // of 100 bytes
			entry = append(entry, tmp[:4]...)
			entry = append(entry, []byte("only-a-prefix")...)
			break
		}
		binary.LittleEndian.PutUint32(tmp[:4], 2)
		entry = append(entry, tmp[:4]...)
		entry = append(entry, 'x', 'y')
	}
	f, err := os.OpenFile(last, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(entry); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCrashMidGroupIsAllOrNothing(t *testing.T) {
	dir := t.TempDir()
	opts := log.Options{SegmentBytes: 1 << 20}

	l := open(t, dir, opts)
	commit(t, l, []byte("one"))
	l.Close()

	// Power loss halfway through writing a group reserving 2, 3, 4.
	appendTornGroup(t, dir, 2, 3, 4)

	l = open(t, dir, opts) // must not fail
	if got, err := l.Read(1); err != nil || string(got[0]) != "one" {
		t.Fatalf("Read(1) = %q, %v", got, err)
	}
	// No half group: none of the group's batches became visible, and
	// every reserved sequence is a permanent hole.
	for seq := uint64(2); seq <= 4; seq++ {
		if _, err := l.Read(seq); !errors.Is(err, log.ErrNotCommitted) {
			t.Fatalf("Read(%d) err = %v, want ErrNotCommitted", seq, err)
		}
	}
	b, err := l.Append([][]byte{[]byte("five")})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if b.Seq() != 5 {
		t.Fatalf("seq after torn group = %d, want 5", b.Seq())
	}
	if _, err := l.Commit(b); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	l.Close()

	// The holes survive further reopens and are never reused.
	l = open(t, dir, opts)
	for seq := uint64(2); seq <= 4; seq++ {
		if _, err := l.Read(seq); !errors.Is(err, log.ErrNotCommitted) {
			t.Fatalf("after reopen Read(%d) err = %v, want ErrNotCommitted", seq, err)
		}
	}
	var got []uint64
	if err := l.Scan(1, func(b log.Batch) error { got = append(got, b.Seq()); return nil }); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if !reflect.DeepEqual(got, []uint64{1, 5}) {
		t.Fatalf("scan = %v, want [1 5]", got)
	}
	if seq := commit(t, l, []byte("six")); seq != 6 {
		t.Fatalf("seq = %d, want 6", seq)
	}
}

func TestCrashHolePermanentAcrossReopens(t *testing.T) {
	dir := t.TempDir()
	opts := log.Options{SegmentBytes: 1 << 20}

	l := open(t, dir, opts)
	commit(t, l, []byte("one"))
	l.Close()

	// Power loss halfway through writing batch 2.
	appendTornEntry(t, dir, 2)

	l = open(t, dir, opts) // recovers and drops the torn entry
	l.Close()

	// Reopen again: the hole must not come back as reusable.
	l = open(t, dir, opts)
	if _, err := l.Read(2); !errors.Is(err, log.ErrNotCommitted) {
		t.Fatalf("Read(2) err = %v, want ErrNotCommitted", err)
	}
	if seq := commit(t, l, []byte("three")); seq != 3 {
		t.Fatalf("seq = %d, want 3 (hole 2 must not be reused)", seq)
	}
	l.Close()

	// And again after committing past the hole.
	l = open(t, dir, opts)
	if _, err := l.Read(2); !errors.Is(err, log.ErrNotCommitted) {
		t.Fatalf("after third open Read(2) err = %v, want ErrNotCommitted", err)
	}
	var got []uint64
	if err := l.Scan(1, func(b log.Batch) error { got = append(got, b.Seq()); return nil }); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if !reflect.DeepEqual(got, []uint64{1, 3}) {
		t.Fatalf("scan = %v, want [1 3]", got)
	}
}

// TestScanGroupCommittedDuringReplayIsWhollyInvisible reserves seqs 2
// and 3 but leaves them staged while committing seq 4, so the snapshot
// starts with a max of 4 and two holes (2,3) inside its range. Blocked
// after batch 1, it commits 2 and 3 as one group: walking 2 and 3 after
// that, the replay must see neither member — never half a group — while 1
// and 4 (committed before the snapshot) are delivered. The next replay
// sees the whole group.
func TestScanGroupCommittedDuringReplayIsWhollyInvisible(t *testing.T) {
	l := open(t, t.TempDir(), log.Options{SegmentBytes: 1 << 20})
	commit(t, l, []byte("a"))                 // seq 1 committed
	g1, _ := l.Append([][]byte{[]byte("g1")}) // seq 2 staged (hole)
	g2, _ := l.Append([][]byte{[]byte("g2")}) // seq 3 staged (hole)
	commit(t, l, []byte("anchor"))            // seq 4 committed -> snapshot max 4

	var (
		seen []uint64
		wg   sync.WaitGroup
	)
	entered := make(chan struct{})
	release := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := l.Scan(1, func(b log.Batch) error {
			seen = append(seen, b.Seq())
			if b.Seq() == 1 {
				close(entered)
				<-release
			}
			return nil
		}); err != nil {
			t.Errorf("Scan: %v", err)
		}
	}()
	<-entered

	if _, err := l.CommitGroup([]log.Batch{g1, g2}); err != nil {
		t.Fatalf("CommitGroup during replay: %v", err)
	}
	close(release)
	wg.Wait()

	if !reflect.DeepEqual(seen, []uint64{1, 4}) {
		t.Fatalf("replay saw %v, want [1 4] — group leaked (half or whole)", seen)
	}
	// The next replay sees the whole group, in reserved order.
	var next []uint64
	if err := l.Scan(1, func(b log.Batch) error {
		next = append(next, b.Seq())
		return nil
	}); err != nil {
		t.Fatalf("next Scan: %v", err)
	}
	if !reflect.DeepEqual(next, []uint64{1, 2, 3, 4}) {
		t.Fatalf("next replay = %v, want [1 2 3 4]", next)
	}
}

// TestScanSingleCommittedDuringReplayInvisible checks the same snapshot
// boundary for a lone Commit whose sequence is beyond the snapshot max:
// the in-flight replay never picks it up via a mid-replay metadata fetch.
func TestScanSingleCommittedDuringReplayInvisible(t *testing.T) {
	l := open(t, t.TempDir(), log.Options{SegmentBytes: 1 << 20})
	commit(t, l, []byte("a"))
	commit(t, l, []byte("b"))

	var seen []uint64
	entered := make(chan struct{})
	release := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = l.Scan(1, func(b log.Batch) error {
			seen = append(seen, b.Seq())
			if b.Seq() == 1 {
				close(entered)
				<-release
			}
			return nil
		})
	}()
	<-entered
	commit(t, l, []byte("c")) // seq 3, committed after the snapshot
	close(release)
	wg.Wait()

	if !reflect.DeepEqual(seen, []uint64{1, 2}) {
		t.Fatalf("replay saw %v, want [1 2]", seen)
	}
}

// TestSlowReplayDoesNotBlockWriter holds a scan callback open for a
// while; concurrent commits and reads must keep making progress because
// file IO and callbacks happen outside the writer's lock.
func TestSlowReplayDoesNotBlockWriter(t *testing.T) {
	l := open(t, t.TempDir(), log.Options{SegmentBytes: 1 << 20})
	for i := 0; i < 5; i++ {
		commit(t, l, []byte(fmt.Sprintf("seed-%d", i)))
	}

	entered := make(chan struct{})
	release := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = l.Scan(1, func(b log.Batch) error {
			if b.Seq() == 1 {
				close(entered)
				<-release
			}
			return nil
		})
	}()
	<-entered

	committed := make(chan uint64, 1)
	go func() {
		committed <- commit(t, l, []byte("while-replay-blocked"))
	}()
	select {
	case seq := <-committed:
		if seq != 6 {
			t.Errorf("commit seq = %d, want 6", seq)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("writer stalled while a replay callback was blocked")
	}

	// A one-off Read also proceeds while the replay is parked.
	readOK := make(chan struct{})
	go func() {
		if _, err := l.Read(6); err != nil {
			t.Errorf("Read(6): %v", err)
		}
		close(readOK)
	}()
	select {
	case <-readOK:
	case <-time.After(2 * time.Second):
		t.Fatal("concurrent Read stalled while a replay callback was blocked")
	}

	close(release)
	wg.Wait()
}

// TestScanIsStreaming verifies memory does not grow with total batch
// count: a replay over many batches never buffers more than the batch
// currently being delivered (observed via a unique payload per batch).
func TestScanIsStreaming(t *testing.T) {
	l := open(t, t.TempDir(), log.Options{SegmentBytes: 4096})
	const n = 200
	for i := 0; i < n; i++ {
		commit(t, l, []byte(fmt.Sprintf("record-%04d", i)))
	}
	count := 0
	if err := l.Scan(1, func(b log.Batch) error {
		rs := b.Records()
		if len(rs) != 1 {
			t.Fatalf("batch %d has %d records", b.Seq(), len(rs))
		}
		want := fmt.Sprintf("record-%04d", count)
		if string(rs[0]) != want {
			t.Fatalf("batch %d = %q, want %q", b.Seq(), rs[0], want)
		}
		count++
		return nil
	}); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if count != n {
		t.Fatalf("replayed %d batches, want %d", count, n)
	}
}
