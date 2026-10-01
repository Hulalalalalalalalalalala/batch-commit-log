package log

import (
	"errors"
	"os"
	"reflect"
	"testing"
)

func TestKeyedSyncFailureRetry(t *testing.T) {
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

	b, err := l.AppendIdempotent("sync-key", [][]byte{[]byte("x")})
	if err != nil {
		t.Fatalf("AppendIdempotent: %v", err)
	}
	if _, err := l.Commit(b); !errors.Is(err, ErrSyncFailed) {
		t.Fatalf("Commit err = %v, want ErrSyncFailed", err)
	}
	// Still staged under the same key and sequence; retrying the
	// idempotent append returns that staged batch and reserves nothing.
	again, err := l.AppendIdempotent("sync-key", [][]byte{[]byte("x")})
	if err != nil {
		t.Fatalf("retry append while un-synced: %v", err)
	}
	if again.Seq() != b.Seq() || again.ID() != "sync-key" {
		t.Fatalf("retry batch = %d/%q, want %d/sync-key", again.Seq(), again.ID(), b.Seq())
	}
	if _, err := l.Read(b.Seq()); !errors.Is(err, ErrNotCommitted) {
		t.Fatalf("Read err = %v, want ErrNotCommitted", err)
	}
	if l.fileSize != 0 {
		t.Fatalf("fileSize after failed sync = %d, want 0", l.fileSize)
	}
	failing = false
	if seq, err := l.Commit(again); err != nil || seq != 1 {
		t.Fatalf("retry commit = %d, %v; want 1", seq, err)
	}
	l.Close()

	// One durable batch after reopen, and the key still dedups.
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
	d, err := l2.AppendIdempotent("sync-key", [][]byte{[]byte("x")})
	if err != nil || d.Seq() != 1 {
		t.Fatalf("dedup after reopen = seq %d, %v; want 1", d.Seq(), err)
	}
}

func TestMixedGroupSyncFailureRetry(t *testing.T) {
	l, err := Open(t.TempDir(), Options{SegmentBytes: 1 << 20, Sync: true})
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

	anon, _ := l.Append([][]byte{[]byte("a")})
	keyed, _ := l.AppendIdempotent("grp", [][]byte{[]byte("g")})
	if _, err := l.CommitGroup([]Batch{anon, keyed}); !errors.Is(err, ErrSyncFailed) {
		t.Fatalf("CommitGroup err = %v, want ErrSyncFailed", err)
	}
	for seq := uint64(1); seq <= 2; seq++ {
		if _, err := l.Read(seq); !errors.Is(err, ErrNotCommitted) {
			t.Fatalf("Read(%d) err = %v, want ErrNotCommitted", seq, err)
		}
	}
	if l.fileSize != 0 {
		t.Fatalf("fileSize after failed sync = %d, want 0", l.fileSize)
	}
	// Both stay staged; the keyed member keeps its key and sequence.
	again, err := l.AppendIdempotent("grp", [][]byte{[]byte("g")})
	if err != nil || again.Seq() != keyed.Seq() {
		t.Fatalf("retry keyed = seq %d, %v; want %d", again.Seq(), err, keyed.Seq())
	}
	failing = false
	seqs, err := l.CommitGroup([]Batch{anon, keyed})
	if err != nil || !reflect.DeepEqual(seqs, []uint64{1, 2}) {
		t.Fatalf("retry group = %v, %v; want [1 2]", seqs, err)
	}
}

func TestKeyedIndexSyncFailureRollsBack(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, Options{SegmentBytes: 1 << 20, Sync: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer l.Close()

	calls := 0
	orig := syncFile
	defer func() { syncFile = orig }()
	syncFile = func(f *os.File) error {
		calls++
		if calls == 2 { // segment fsync ok, index fsync fails
			return errors.New("index fsync boom")
		}
		return orig(f)
	}

	b, err := l.AppendIdempotent("idx-key", [][]byte{[]byte("x")})
	if err != nil {
		t.Fatalf("AppendIdempotent: %v", err)
	}
	if _, err := l.Commit(b); !errors.Is(err, ErrSyncFailed) {
		t.Fatalf("Commit err = %v, want ErrSyncFailed", err)
	}
	if l.fileSize != 0 {
		t.Fatalf("segment size after rollback = %d, want 0", l.fileSize)
	}
	if _, ok := l.committedKeys["idx-key"]; ok {
		t.Fatalf("key registered as committed after failed commit")
	}
	// Retry of the append returns the still-staged batch.
	again, err := l.AppendIdempotent("idx-key", [][]byte{[]byte("x")})
	if err != nil || again.Seq() != 1 {
		t.Fatalf("retry append = seq %d, %v; want staged seq 1", again.Seq(), err)
	}
	syncFile = orig
	if seq, err := l.Commit(again); err != nil || seq != 1 {
		t.Fatalf("retry commit = %d, %v", seq, err)
	}
}
