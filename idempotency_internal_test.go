package log

import (
	"errors"
	"os"
	"reflect"
	"testing"
)

// failingSync turns fsync failures on for the first n segment/sidecar
// syncs while it is installed.
func installFailingSync(t *testing.T, failAt func(calls int) bool) (calls *int, restore func()) {
	t.Helper()
	orig := syncFile
	n := 0
	syncFile = func(f *os.File) error {
		n++
		if failAt(n) {
			return errors.New("fsync boom")
		}
		return orig(f)
	}
	return &n, func() { syncFile = orig }
}

func TestKeyedSyncFailureKeepsBatchStagedAndDedups(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, Options{SegmentBytes: 1 << 20, Sync: true})
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
	if _, err := l.Commit(b); !errors.Is(err, ErrSyncFailed) {
		t.Fatalf("Commit err = %v, want ErrSyncFailed", err)
	}
	// Still staged: the key keeps resolving to the same reserved batch.
	if _, err := l.Read(1); !errors.Is(err, ErrNotCommitted) {
		t.Fatalf("Read after failed sync: %v", err)
	}
	again, err := l.AppendIdempotent("k", [][]byte{[]byte("x")})
	if err != nil {
		t.Fatalf("dedup after failed sync: %v", err)
	}
	if again.Seq() != 1 {
		t.Fatalf("dedup seq = %d, want 1", again.Seq())
	}
	if l.fileSize != 0 {
		t.Fatalf("segment size after failed sync = %d, want 0", l.fileSize)
	}
	// Conflicting content still conflicts while staged.
	if _, err := l.AppendIdempotent("k", [][]byte{[]byte("y")}); !errors.Is(err, ErrBatchIDConflict) {
		t.Fatalf("conflict after failed sync: %v", err)
	}
	// Retry once fsync recovers: exactly one entry.
	failing = false
	if seq, err := l.Commit(again); err != nil || seq != 1 {
		t.Fatalf("retry Commit = %d, %v", seq, err)
	}
	got, err := l.Read(1)
	if err != nil || !reflect.DeepEqual(got, [][]byte{[]byte("x")}) {
		t.Fatalf("Read(1) = %q, %v", got, err)
	}
	l.Close()

	l2, err := Open(dir, Options{SegmentBytes: 1 << 20, Sync: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer l2.Close()
	count := 0
	l2.Scan(1, func(Batch) error { count++; return nil })
	if count != 1 {
		t.Fatalf("replayed %d batches, want 1 (no duplicate from keyed retry)", count)
	}
	if b, err := l2.AppendIdempotent("k", [][]byte{[]byte("x")}); err != nil || b.Seq() != 1 {
		t.Fatalf("dedup after reopen = %d, %v", b.Seq(), err)
	}
}

func TestMixedGroupSyncFailureKeepsAllStaged(t *testing.T) {
	l, err := Open(t.TempDir(), Options{SegmentBytes: 1 << 20, Sync: true})
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

	a, _ := l.Append([][]byte{[]byte("a")})
	k, _ := l.AppendIdempotent("gk", [][]byte{[]byte("k")})
	if _, err := l.CommitGroup([]Batch{a, k}); !errors.Is(err, ErrSyncFailed) {
		t.Fatalf("CommitGroup err = %v, want ErrSyncFailed", err)
	}
	for seq := uint64(1); seq <= 2; seq++ {
		if _, err := l.Read(seq); !errors.Is(err, ErrNotCommitted) {
			t.Fatalf("Read(%d) = %v, want ErrNotCommitted", seq, err)
		}
	}
	if b, err := l.AppendIdempotent("gk", [][]byte{[]byte("k")}); err != nil || b.Seq() != 2 {
		t.Fatalf("keyed member dedup while staged = %d, %v", b.Seq(), err)
	}
	if l.fileSize != 0 {
		t.Fatalf("segment size after failed group sync = %d, want 0", l.fileSize)
	}

	failing = false
	seqs, err := l.CommitGroup([]Batch{a, k})
	if err != nil || !reflect.DeepEqual(seqs, []uint64{1, 2}) {
		t.Fatalf("retry CommitGroup = %v, %v", seqs, err)
	}
	var ids []string
	l.Scan(1, func(b Batch) error { ids = append(ids, b.ID()); return nil })
	if !reflect.DeepEqual(ids, []string{"", "gk"}) {
		t.Fatalf("scan ids = %v, want [\"\" gk]", ids)
	}
}

func TestKeyedIndexSyncFailureRollsBack(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, Options{SegmentBytes: 1 << 20, Sync: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer l.Close()

	calls, restore := installFailingSync(t, func(n int) bool { return n == 2 })
	defer restore()

	b, err := l.AppendIdempotent("ik", [][]byte{[]byte("r")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := l.Commit(b); !errors.Is(err, ErrSyncFailed) {
		t.Fatalf("Commit err = %v, want ErrSyncFailed", err)
	}
	restore()
	if seq, err := l.Commit(b); err != nil || seq != 1 {
		t.Fatalf("retry = %d, %v", seq, err)
	}
	if *calls < 2 {
		t.Fatalf("expected index fsync to have failed once, calls=%d", *calls)
	}
	l.Close()

	l2, err := Open(dir, Options{SegmentBytes: 1 << 20, Sync: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer l2.Close()
	if b, err := l2.AppendIdempotent("ik", [][]byte{[]byte("r")}); err != nil || b.Seq() != 1 {
		t.Fatalf("dedup after reopen = %d, %v", b.Seq(), err)
	}
}
