package log

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// failSync stubs syncFile to fail while the returned func restores the
// original. The predicate selects which files fail.
func failSync(t *testing.T, fail func(f *os.File) (bool, error)) {
	t.Helper()
	orig := syncFile
	syncFile = func(f *os.File) error {
		if yes, err := fail(f); yes {
			return err
		}
		return orig(f)
	}
	t.Cleanup(func() { syncFile = orig })
}

func TestAbortSyncFailureKeepsBatchStaged(t *testing.T) {
	dir := t.TempDir()
	// Sync false on purpose: Abort must still reach for the fsync, so
	// its durability never depends on Options.Sync.
	l, err := Open(dir, Options{SegmentBytes: 1 << 20, Sync: false})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer l.Close()

	boom := errors.New("fsync boom")
	failing := true
	failSync(t, func(*os.File) (bool, error) {
		if failing {
			return true, boom
		}
		return false, nil
	})

	b, err := l.AppendIdempotent("k", [][]byte{[]byte("x")})
	if err != nil {
		t.Fatalf("AppendIdempotent: %v", err)
	}
	if err := l.Abort(b); !errors.Is(err, ErrAbortFailed) {
		t.Fatalf("Abort err = %v, want ErrAbortFailed", err)
	}
	// The batch and its key stay staged under the original sequence;
	// the partial marker was rolled back.
	if l.fileSize != 0 {
		t.Fatalf("fileSize after failed abort = %d, want 0", l.fileSize)
	}
	again, err := l.AppendIdempotent("k", [][]byte{[]byte("x")})
	if err != nil {
		t.Fatalf("AppendIdempotent retry: %v", err)
	}
	if again.Seq() != b.Seq() {
		t.Fatalf("key retry seq = %d, want %d (key still owned)", again.Seq(), b.Seq())
	}

	// Retrying the abort after recovery succeeds and writes no
	// duplicate marker.
	failing = false
	if err := l.Abort(b); err != nil {
		t.Fatalf("retry Abort: %v", err)
	}
	if _, err := l.Read(b.Seq()); !errors.Is(err, ErrNotCommitted) {
		t.Fatalf("Read err = %v, want ErrNotCommitted", err)
	}
	if _, err := l.Commit(b); !errors.Is(err, ErrUnknownBatch) {
		t.Fatalf("Commit aborted err = %v, want ErrUnknownBatch", err)
	}
	l.Close()

	// Exactly one hole marker landed on disk.
	l2, err := Open(dir, Options{SegmentBytes: 1 << 20})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer l2.Close()
	if _, err := l2.Read(1); !errors.Is(err, ErrNotCommitted) {
		t.Fatalf("Read after reopen err = %v, want ErrNotCommitted", err)
	}
	b2, err := l2.Append([][]byte{[]byte("y")})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if b2.Seq() != 2 {
		t.Fatalf("seq = %d, want 2", b2.Seq())
	}
}

func TestAbortIndexFailureKeepsBatchStaged(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, Options{SegmentBytes: 1 << 20, Sync: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer l.Close()

	boom := errors.New("index fsync boom")
	failing := true
	failSync(t, func(f *os.File) (bool, error) {
		// Only the sidecar sync fails; the segment marker sync succeeds.
		if failing && l.idxFile != nil && f == l.idxFile {
			return true, boom
		}
		return false, nil
	})

	b, err := l.Append([][]byte{[]byte("x")})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := l.Abort(b); !errors.Is(err, ErrAbortFailed) {
		t.Fatalf("Abort err = %v, want ErrAbortFailed", err)
	}
	// Both files rolled back: the batch can still be committed instead,
	// and neither path can duplicate a record.
	if l.fileSize != 0 {
		t.Fatalf("fileSize after failed abort = %d, want 0", l.fileSize)
	}
	failing = false
	seq, err := l.Commit(b)
	if err != nil {
		t.Fatalf("Commit after failed abort: %v", err)
	}
	if seq != 1 {
		t.Fatalf("seq = %d, want 1", seq)
	}
	l.Close()

	l2, err := Open(dir, Options{SegmentBytes: 1 << 20})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer l2.Close()
	count := 0
	if err := l2.Scan(1, func(Batch) error { count++; return nil }); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if count != 1 {
		t.Fatalf("replayed %d batches, want 1 (no duplicate)", count)
	}
}

// appendTornHoleMarker appends a half-written BCLH marker reserving seq
// to the last segment, simulating a crash in the middle of Abort.
func appendTornHoleMarker(t *testing.T, dir string, seq uint64) {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(dir, "*.seg"))
	if err != nil || len(files) == 0 {
		t.Fatalf("segment files = %v, %v", files, err)
	}
	last := files[len(files)-1]
	var entry []byte
	entry = append(entry, 'B', 'C', 'L', 'H')
	var tmp [8]byte
	binary.LittleEndian.PutUint32(tmp[:4], 1) // one reserved sequence
	entry = append(entry, tmp[:4]...)
	binary.LittleEndian.PutUint64(tmp[:], seq)
	entry = append(entry, tmp[:]...)
	// No trailing checksum: the write was torn.
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

func TestCrashMidAbortKeepsHole(t *testing.T) {
	dir := t.TempDir()
	opts := Options{SegmentBytes: 1 << 20}
	l, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	b1, _ := l.Append([][]byte{[]byte("one")})
	if _, err := l.Commit(b1); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	l.Close()

	// Power loss halfway through writing the abort marker for seq 2.
	appendTornHoleMarker(t, dir, 2)

	l, err = Open(dir, opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer l.Close()
	// The recognizable aborted sequence is a permanent hole, never
	// reused; the durable commit before it is intact.
	if got, err := l.Read(1); err != nil || string(got[0]) != "one" {
		t.Fatalf("Read(1) = %q, %v", got, err)
	}
	if _, err := l.Read(2); !errors.Is(err, ErrNotCommitted) {
		t.Fatalf("Read(2) err = %v, want ErrNotCommitted", err)
	}
	b, err := l.Append([][]byte{[]byte("three")})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if b.Seq() != 3 {
		t.Fatalf("seq = %d, want 3", b.Seq())
	}
}

func TestCorruptAbortMarkerDetected(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, Options{SegmentBytes: 1 << 20})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	b1, _ := l.Append([][]byte{[]byte("one")})
	if _, err := l.Commit(b1); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	l.Close()

	// A complete hole marker with a bad checksum is tampering, not a
	// torn write: Open must report corruption.
	marker := encodeHoles([]uint64{2})
	marker[len(marker)-1] ^= 0xFF
	f, err := os.OpenFile(filepath.Join(dir, "000001.seg"), os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(marker); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir, Options{SegmentBytes: 1 << 20}); !errors.Is(err, ErrCorruptSegment) {
		t.Fatalf("Open err = %v, want ErrCorruptSegment", err)
	}
}
