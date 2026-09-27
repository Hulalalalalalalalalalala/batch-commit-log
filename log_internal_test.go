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

	b1, err := l.Append([][]byte{[]byte("x")})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	b2, err := l.Append([][]byte{[]byte("y")})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if _, err := l.CommitGroup([]Batch{b1, b2}); !errors.Is(err, ErrSyncFailed) {
		t.Fatalf("CommitGroup err = %v, want ErrSyncFailed", err)
	}
	// The whole group stays staged; nothing becomes half visible.
	for _, b := range []Batch{b1, b2} {
		if _, err := l.Read(b.Seq()); !errors.Is(err, ErrNotCommitted) {
			t.Fatalf("Read(%d) err = %v, want ErrNotCommitted", b.Seq(), err)
		}
	}
	// The un-synced frame was rolled back cleanly.
	if l.fileSize != 0 {
		t.Fatalf("fileSize after failed sync = %d, want 0", l.fileSize)
	}
	// Retrying the same group commits it with the same sequences.
	failing = false
	seqs, err := l.CommitGroup([]Batch{b1, b2})
	if err != nil {
		t.Fatalf("retry CommitGroup: %v", err)
	}
	if len(seqs) != 2 || seqs[0] != 1 || seqs[1] != 2 {
		t.Fatalf("retry seqs = %v, want [1 2]", seqs)
	}
	l.Close()

	// After a reopen each sequence appears exactly once: the retry left
	// no duplicate entries behind.
	l2, err := Open(dir, Options{SegmentBytes: 1 << 20, Sync: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer l2.Close()
	var seen []uint64
	if err := l2.Scan(1, func(b Batch) error {
		seen = append(seen, b.Seq())
		return nil
	}); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if len(seen) != 2 || seen[0] != 1 || seen[1] != 2 {
		t.Fatalf("replayed seqs = %v, want [1 2]", seen)
	}
}
