package log

import (
	"errors"
	"os"
	"testing"
)

func TestIndexSyncFailureRollsCommitBack(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, Options{SegmentBytes: 1 << 20, Sync: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer l.Close()

	boom := errors.New("index fsync boom")
	orig := syncFile
	defer func() { syncFile = orig }()
	calls := 0
	syncFile = func(f *os.File) error {
		calls++
		if calls == 2 {
			// 1 = segment fsync (ok), 2 = index fsync (fails).
			return boom
		}
		return orig(f)
	}

	b, err := l.Append([][]byte{[]byte("x")})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if _, err := l.Commit(b); !errors.Is(err, ErrSyncFailed) {
		t.Fatalf("Commit err = %v, want ErrSyncFailed", err)
	}
	// Still staged, both files rolled back to empty.
	if _, err := l.Read(b.Seq()); !errors.Is(err, ErrNotCommitted) {
		t.Fatalf("Read err = %v, want ErrNotCommitted", err)
	}
	if l.fileSize != 0 {
		t.Fatalf("segment size after index fsync failure = %d, want 0", l.fileSize)
	}
	if l.idxSize != len(encodeIndexHeader()) {
		t.Fatalf("index size after rollback = %d, want %d", l.idxSize, len(encodeIndexHeader()))
	}

	// Retry after the index recovers: one entry, no duplicates.
	syncFile = orig
	seq, err := l.Commit(b)
	if err != nil {
		t.Fatalf("retry Commit: %v", err)
	}
	if seq != 1 {
		t.Fatalf("retry seq = %d, want 1", seq)
	}
	l.Close()

	l2, err := Open(dir, Options{SegmentBytes: 1 << 20, Sync: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer l2.Close()
	count := 0
	if err := l2.Scan(1, func(Batch) error { count++; return nil }); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if count != 1 {
		t.Fatalf("replayed %d batches, want 1 (no duplicate from retry)", count)
	}
}

func TestGroupIndexSyncFailureRollsWholeGroupBack(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, Options{SegmentBytes: 1 << 20, Sync: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer l.Close()

	boom := errors.New("index fsync boom")
	orig := syncFile
	defer func() { syncFile = orig }()
	calls := 0
	syncFile = func(f *os.File) error {
		calls++
		if calls == 2 {
			return boom
		}
		return orig(f)
	}

	b1, _ := l.Append([][]byte{[]byte("a")})
	b2, _ := l.Append([][]byte{[]byte("b")})
	if _, err := l.CommitGroup([]Batch{b1, b2}); !errors.Is(err, ErrSyncFailed) {
		t.Fatalf("CommitGroup err = %v, want ErrSyncFailed", err)
	}
	for seq := uint64(1); seq <= 2; seq++ {
		if _, err := l.Read(seq); !errors.Is(err, ErrNotCommitted) {
			t.Fatalf("Read(%d) err = %v, want ErrNotCommitted", seq, err)
		}
	}
	if l.fileSize != 0 || l.idxSize != len(encodeIndexHeader()) {
		t.Fatalf("sizes after rollback: seg=%d idx=%d", l.fileSize, l.idxSize)
	}

	syncFile = orig
	seqs, err := l.CommitGroup([]Batch{b1, b2})
	if err != nil {
		t.Fatalf("retry CommitGroup: %v", err)
	}
	if len(seqs) != 2 || seqs[0] != 1 || seqs[1] != 2 {
		t.Fatalf("retry seqs = %v, want [1 2]", seqs)
	}
	l.Close()

	l2, err := Open(dir, Options{SegmentBytes: 1 << 20, Sync: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer l2.Close()
	count := 0
	if err := l2.Scan(1, func(Batch) error { count++; return nil }); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if count != 2 {
		t.Fatalf("replayed %d batches, want 2", count)
	}
}

func TestTornIndexRecordFallsBackToRebuild(t *testing.T) {
	dir := t.TempDir()
	opts := Options{SegmentBytes: 1 << 20, Sync: true}
	l, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	b, err := l.Append([][]byte{[]byte("survives")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := l.Commit(b); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	l.Close()

	// Crash between the index record write and its sync: the record is
	// half there. The segment entry is fully durable, so the reopen must
	// still publish the batch exactly once via the rebuild path.
	path := l.indexPath()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(path, int64(len(data)-2)); err != nil {
		t.Fatal(err)
	}

	l2, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("reopen with torn index record: %v", err)
	}
	defer l2.Close()
	got, err := l2.Read(1)
	if err != nil || len(got) != 1 || string(got[0]) != "survives" {
		t.Fatalf("Read(1) = %q, %v", got, err)
	}
	count := 0
	if err := l2.Scan(1, func(Batch) error { count++; return nil }); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if count != 1 {
		t.Fatalf("replayed %d batches, want 1", count)
	}
}

func TestManyReopenCyclesAdoptThenRebuild(t *testing.T) {
	dir := t.TempDir()
	opts := Options{SegmentBytes: 48, Sync: false}
	const cycles = 6
	for c := 0; c < cycles; c++ {
		l, err := Open(dir, opts)
		if err != nil {
			t.Fatalf("Open cycle %d: %v", c, err)
		}
		for k := 0; k < 3; k++ {
			b, err := l.Append([][]byte{[]byte("payload")})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := l.Commit(b); err != nil {
				t.Fatal(err)
			}
		}
		if err := l.Close(); err != nil {
			t.Fatal(err)
		}
	}
	total := cycles * 3
	check := func(t *testing.T, l *Log) {
		t.Helper()
		count := 0
		if err := l.Scan(1, func(Batch) error { count++; return nil }); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		if count != total {
			t.Fatalf("replayed %d batches, want %d", count, total)
		}
		b, err := l.Append([][]byte{[]byte("after")})
		if err != nil {
			t.Fatal(err)
		}
		if b.Seq() != uint64(total+1) {
			t.Fatalf("next seq = %d, want %d", b.Seq(), total+1)
		}
		if _, err := l.Commit(b); err != nil {
			t.Fatal(err)
		}
	}

	l, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	check(t, l)
	l.Close()

	// Delete the sidecar mid-log-life: rebuild must reach the same view.
	if err := os.Remove(l.indexPath()); err != nil {
		t.Fatal(err)
	}
	l, err = Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	got := 0
	if err := l.Scan(1, func(Batch) error { got++; return nil }); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if got != total+1 {
		t.Fatalf("after rebuild replayed %d, want %d", got, total+1)
	}
}
