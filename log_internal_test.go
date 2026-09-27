package log

import (
	"errors"
	"os"
	"testing"
)

func TestSyncFailureKeepsBatchStaged(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, Options{SegmentBytes: 1 << 20, Sync: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer l.Close()

	boom := errors.New("fsync boom")
	failing := true
	orig := syncFile
	defer func() { syncFile = orig }()
	syncFile = func(f *os.File) error {
		if failing {
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
	// The batch stays staged and keeps its reserved sequence.
	if _, err := l.Read(b.Seq()); !errors.Is(err, ErrNotCommitted) {
		t.Fatalf("Read err = %v, want ErrNotCommitted", err)
	}
	// The un-synced entry was dropped, so the retry cannot duplicate it.
	if l.fileSize != 0 {
		t.Fatalf("fileSize after failed sync = %d, want 0", l.fileSize)
	}
	// Retrying the same batch commits it.
	failing = false
	seq, err := l.Commit(b)
	if err != nil {
		t.Fatalf("retry Commit: %v", err)
	}
	if seq != 1 {
		t.Fatalf("retry seq = %d, want 1", seq)
	}
	got, err := l.Read(1)
	if err != nil || len(got) != 1 || string(got[0]) != "x" {
		t.Fatalf("Read(1) = %q, %v", got, err)
	}
	l.Close()

	// After a reopen exactly one batch is on disk: no duplicate entry.
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
		t.Fatalf("replayed %d batches, want 1", count)
	}
}

func TestGroupSyncFailureKeepsAllStaged(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, Options{SegmentBytes: 1 << 20, Sync: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer l.Close()

	boom := errors.New("fsync boom")
	failing := true
	orig := syncFile
	defer func() { syncFile = orig }()
	syncFile = func(f *os.File) error {
		if failing {
			return boom
		}
		return orig(f)
	}

	b1, err := l.Append([][]byte{[]byte("a")})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	b2, err := l.Append([][]byte{[]byte("b")})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if _, err := l.CommitGroup([]Batch{b1, b2}); !errors.Is(err, ErrSyncFailed) {
		t.Fatalf("CommitGroup err = %v, want ErrSyncFailed", err)
	}
	// All or nothing: every batch of the group stays staged and keeps
	// its reserved sequence.
	for _, seq := range []uint64{1, 2} {
		if _, err := l.Read(seq); !errors.Is(err, ErrNotCommitted) {
			t.Fatalf("Read(%d) err = %v, want ErrNotCommitted", seq, err)
		}
	}
	// The un-synced group entry was rolled back cleanly, so the retry
	// cannot duplicate it.
	if l.fileSize != 0 {
		t.Fatalf("fileSize after failed sync = %d, want 0", l.fileSize)
	}
	failing = false
	seqs, err := l.CommitGroup([]Batch{b1, b2})
	if err != nil {
		t.Fatalf("retry CommitGroup: %v", err)
	}
	if len(seqs) != 2 || seqs[0] != 1 || seqs[1] != 2 {
		t.Fatalf("retry seqs = %v, want [1 2]", seqs)
	}
	for seq, want := range map[uint64]string{1: "a", 2: "b"} {
		got, err := l.Read(seq)
		if err != nil || len(got) != 1 || string(got[0]) != want {
			t.Fatalf("Read(%d) = %q, %v; want %q", seq, got, err, want)
		}
	}
	l.Close()

	// After a reopen exactly two batches are on disk: no duplicates.
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
