package log_test

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"

	log "github.com/Hulalalalalalalalalalala/batch-commit-log"
)

// appendTornRaw writes a half-finished BCL1 batch entry for seq to the
// given segment file.
func appendTornRaw(t *testing.T, path string, seq uint64) {
	t.Helper()
	var entry []byte
	entry = append(entry, 'B', 'C', 'L', '1')
	var tmp [8]byte
	binary.LittleEndian.PutUint64(tmp[:], seq)
	entry = append(entry, tmp[:]...)
	binary.LittleEndian.PutUint32(tmp[:4], 1)
	entry = append(entry, tmp[:4]...)
	binary.LittleEndian.PutUint32(tmp[:4], 100)
	entry = append(entry, tmp[:4]...)
	entry = append(entry, []byte("only-a-prefix-of-the-record")...)
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
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

func TestDeleteThroughRefusesPrefixHoleAboveCut(t *testing.T) {
	dir := t.TempDir()
	opts := log.Options{SegmentBytes: 32}

	// Segment 1 ends with a recovered hole marker reserving seq 2; later
	// segments hold committed 3 and 4.
	l := open(t, dir, opts)
	commit(t, l, []byte("one")) // seq 1 in 000001.seg
	l.Close()
	appendTornRaw(t, filepath.Join(dir, "000001.seg"), 2)

	l = open(t, dir, opts)        // rewrites the torn tail into a hole [2]
	commit(t, l, []byte("three")) // seq 3 -> 000002.seg
	commit(t, l, []byte("four"))  // seq 4 -> 000003.seg

	// Cutting only segment 1 would forget the permanent hole 2, which is
	// above the cut sequence 1: refused, nothing deleted.
	n, err := l.DeleteThrough(1)
	if !errors.Is(err, log.ErrInvalidRetention) {
		t.Fatalf("DeleteThrough(1) = %d, %v; want ErrInvalidRetention", n, err)
	}
	if _, err := l.Read(2); !errors.Is(err, log.ErrNotCommitted) {
		t.Fatalf("hole 2 changed: %v, want ErrNotCommitted", err)
	}
	if files := segFiles(t, dir); len(files) != 3 {
		t.Fatalf("files = %v, want all 3 kept", files)
	}

	// Cutting through the high-water mark discards the hole segment too:
	// the hole reservation is below the cut and leaves with the history.
	n, err = l.DeleteThrough(4)
	if err != nil || n != 3 {
		t.Fatalf("DeleteThrough(4) = %d, %v; want 3 nil", n, err)
	}
	l.Close()

	l = open(t, dir, opts)
	if _, err := l.Read(2); !errors.Is(err, log.ErrTruncated) {
		t.Fatalf("Read(2) after full cut err = %v, want ErrTruncated", err)
	}
	// The frontier still advances past the forgotten hole.
	if seq := commit(t, l, []byte("five")); seq != 5 {
		t.Fatalf("seq = %d, want 5", seq)
	}
}

func TestDeleteThroughRefusesLiveStagedBatchInPrefix(t *testing.T) {
	dir := t.TempDir()
	l := open(t, dir, log.Options{SegmentBytes: 32})

	// Reserve 1 and 2, commit only 2 first; seq 1 stays staged below it.
	b1, err := l.Append([][]byte{[]byte("late-one")})
	if err != nil {
		t.Fatal(err)
	}
	b2, err := l.Append([][]byte{[]byte("two")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := l.Commit(b2); err != nil {
		t.Fatal(err)
	}
	b3, err := l.Append([][]byte{[]byte("three")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := l.Commit(b3); err != nil {
		t.Fatal(err)
	}

	// Segment 1 holds committed seq 2; truncating it while staged seq 1
	// exists would let a later commit resurrect sequence 1.
	if _, err := l.DeleteThrough(2); !errors.Is(err, log.ErrInvalidRetention) {
		t.Fatalf("DeleteThrough(2) err = %v, want ErrInvalidRetention", err)
	}
	// Committing the stale batch first, then cutting, is allowed.
	if seq, err := l.Commit(b1); err != nil || seq != 1 {
		t.Fatalf("Commit b1 = %d, %v", seq, err)
	}
	if _, err := l.DeleteThrough(2); err != nil {
		t.Fatalf("DeleteThrough after commit: %v", err)
	}
	if _, err := l.Read(2); !errors.Is(err, log.ErrTruncated) {
		t.Fatalf("Read(2) err = %v, want ErrTruncated", err)
	}
}
