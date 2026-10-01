package log

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func openTemp(t *testing.T, opts Options) (*Log, string) {
	t.Helper()
	dir := t.TempDir()
	l, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { l.Close() })
	return l, dir
}

func TestCommitSequences(t *testing.T) {
	l, _ := openTemp(t, Options{Sync: true})

	b1, err := l.Append([][]byte{[]byte("a"), []byte("b")})
	if err != nil {
		t.Fatal(err)
	}
	if b1.Sequence != 0 {
		t.Fatalf("staged Sequence = %d, want 0", b1.Sequence)
	}
	if b1.ID == 0 {
		t.Fatal("staged batch ID = 0")
	}

	seq, err := l.Commit(b1)
	if err != nil || seq != 1 {
		t.Fatalf("first Commit = %d, %v; want 1", seq, err)
	}
	b2, _ := l.Append([][]byte{[]byte("c")})
	seq, err = l.Commit(b2)
	if err != nil || seq != 2 {
		t.Fatalf("second Commit = %d, %v; want 2", seq, err)
	}

	// Idempotent recommit returns the original sequence without writing.
	again, err := l.Commit(b1)
	if err != nil || again != 1 {
		t.Fatalf("recommit = %d, %v; want 1", again, err)
	}
	if got := len(l.index); got != 2 {
		t.Fatalf("index size after recommit = %d, want 2", got)
	}
}

func TestReadAndScan(t *testing.T) {
	l, _ := openTemp(t, Options{Sync: true})
	for i := 0; i < 5; i++ {
		b, err := l.Append([][]byte{[]byte(fmt.Sprintf("rec-%d", i))})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := l.Commit(b); err != nil {
			t.Fatal(err)
		}
	}

	got, err := l.Read(3)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || string(got[0]) != "rec-2" {
		t.Fatalf("Read(3) = %v", got)
	}

	if _, err := l.Read(6); !errors.Is(err, ErrNotCommitted) {
		t.Fatalf("Read(6) err = %v, want ErrNotCommitted", err)
	}
	if _, err := l.Read(0); !errors.Is(err, ErrNotCommitted) {
		t.Fatalf("Read(0) err = %v, want ErrNotCommitted", err)
	}

	var seqs []uint64
	if err := l.Scan(3, func(b Batch) error {
		seqs = append(seqs, b.Sequence)
		if b.ID != 0 {
			t.Errorf("scanned batch ID = %d, want 0", b.ID)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(seqs) != "[3 4 5]" {
		t.Fatalf("Scan(3) seqs = %v", seqs)
	}

	seqs = nil
	if err := l.Scan(0, func(b Batch) error {
		seqs = append(seqs, b.Sequence)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(seqs) != "[1 2 3 4 5]" {
		t.Fatalf("Scan(0) seqs = %v", seqs)
	}

	if err := l.Scan(1, func(Batch) error { return errSentinel }); !errors.Is(err, errSentinel) {
		t.Fatalf("Scan callback error = %v, want propagated", err)
	}
}

var errSentinel = errors.New("sentinel")

func TestAtomicVisibility(t *testing.T) {
	l, _ := openTemp(t, Options{Sync: false})
	b, _ := l.Append([][]byte{[]byte("x")})

	// Staged but not committed: not readable.
	if _, err := l.Read(1); !errors.Is(err, ErrNotCommitted) {
		t.Fatalf("Read before commit = %v, want ErrNotCommitted", err)
	}
	if err := l.Scan(0, func(Batch) error {
		t.Fatal("Scan saw uncommitted batch")
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Commit(b); err != nil {
		t.Fatal(err)
	}
	got, err := l.Read(1)
	if err != nil || string(got[0]) != "x" {
		t.Fatalf("Read after commit = %v, %v", got, err)
	}
}

func TestUnknownBatch(t *testing.T) {
	l, _ := openTemp(t, Options{})
	if _, err := l.Commit(Batch{ID: 999}); !errors.Is(err, ErrUnknownBatch) {
		t.Fatalf("Commit unknown = %v, want ErrUnknownBatch", err)
	}
	if _, err := l.Commit(Batch{}); !errors.Is(err, ErrUnknownBatch) {
		t.Fatalf("Commit zero batch = %v, want ErrUnknownBatch", err)
	}
}

func TestEmptyRecords(t *testing.T) {
	l, _ := openTemp(t, Options{Sync: true})
	b, err := l.Append(nil)
	if err != nil {
		t.Fatal(err)
	}
	seq, err := l.Commit(b)
	if err != nil {
		t.Fatal(err)
	}
	got, err := l.Read(seq)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("empty batch records = %v", got)
	}
}

func TestSegmentRolling(t *testing.T) {
	l, dir := openTemp(t, Options{SegmentBytes: 100, Sync: true})
	for i := 0; i < 6; i++ {
		b, _ := l.Append([][]byte{bytes.Repeat([]byte("x"), 60)})
		if _, err := l.Commit(b); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	segs, err := listSegmentFiles(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(segs) < 2 {
		t.Fatalf("segment files = %d, want >= 2", len(segs))
	}

	l2, err := Open(dir, Options{SegmentBytes: 100, Sync: true})
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Close()
	for seq := uint64(1); seq <= 6; seq++ {
		got, err := l2.Read(seq)
		if err != nil {
			t.Fatalf("after reopen Read(%d): %v", seq, err)
		}
		if len(got) != 1 || len(got[0]) != 60 {
			t.Fatalf("after reopen Read(%d) = %v", seq, got)
		}
	}
	info := l2.Segments()
	if len(info) != len(segs) {
		t.Fatalf("Segments = %d entries, want %d files", len(info), len(segs))
	}
	if info[0].FirstSeq != 1 || info[len(info)-1].LastSeq != 6 {
		t.Fatalf("segment ranges = %+v", info)
	}
	for i := 0; i+1 < len(info); i++ {
		if info[i].LastSeq+1 != info[i+1].FirstSeq {
			t.Fatalf("non-contiguous segment ranges: %+v", info)
		}
	}
	fi, err := os.Stat(info[0].File)
	if err != nil {
		t.Fatal(err)
	}
	if info[0].Bytes != fi.Size() {
		t.Fatalf("Bytes = %d, file size = %d", info[0].Bytes, fi.Size())
	}
}

func TestRecoveryTruncatesTail(t *testing.T) {
	l, dir := openTemp(t, Options{Sync: true})
	for i := 0; i < 3; i++ {
		b, _ := l.Append([][]byte{[]byte(fmt.Sprintf("committed-%d", i))})
		if _, err := l.Commit(b); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	// Simulate a crash mid-write: append garbage / a partial frame to
	// the last segment file.
	segs, err := listSegmentFiles(dir)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, segs[len(segs)-1].Name())
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{0x00, 0x00, 0x00, 0xff}); err != nil {
		t.Fatal(err)
	}
	f.Close()

	l2, err := Open(dir, Options{Sync: true})
	if err != nil {
		t.Fatalf("Open after partial write: %v", err)
	}
	defer l2.Close()
	if l2.lastSeq != 3 {
		t.Fatalf("lastSeq = %d, want 3", l2.lastSeq)
	}

	// Truncation must have happened on disk.
	st2, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if st2.Size() != st.Size() {
		t.Fatalf("tail not truncated: %d -> %d", st.Size(), st2.Size())
	}

	var seqs []uint64
	if err := l2.Scan(0, func(b Batch) error {
		seqs = append(seqs, b.Sequence)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(seqs) != "[1 2 3]" {
		t.Fatalf("recovered seqs = %v", seqs)
	}

	// New commits continue with sequence 4.
	b, _ := l2.Append([][]byte{[]byte("after")})
	seq, err := l2.Commit(b)
	if err != nil || seq != 4 {
		t.Fatalf("commit after recovery = %d, %v", seq, err)
	}
}

func TestCorruptMiddle(t *testing.T) {
	l, dir := openTemp(t, Options{Sync: true})
	for i := 0; i < 4; i++ {
		b, _ := l.Append([][]byte{bytes.Repeat([]byte{byte('A' + i)}, 40)})
		if _, err := l.Commit(b); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	segs, err := listSegmentFiles(dir)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, segs[0].Name())
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	// Flip a byte inside the first frame's payload (after the 4-byte
	// length header): checksum mismatch, and it is NOT a tail frame.
	pos := headerSize + 10
	data[pos] ^= 0xff
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	st, _ := os.Stat(path)

	if _, err := Open(dir, Options{}); !errors.Is(err, ErrCorruptSegment) {
		t.Fatalf("Open with corrupt middle = %v, want ErrCorruptSegment", err)
	}

	// Directory must be left unchanged.
	st2, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if st2.Size() != st.Size() {
		t.Fatalf("corrupt file was modified: %d -> %d", st.Size(), st2.Size())
	}
	got, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("corrupt file contents changed, err = %v", err)
	}
}

func TestSequenceGapCorrupt(t *testing.T) {
	l, dir := openTemp(t, Options{Sync: true})
	b, _ := l.Append([][]byte{[]byte("only-one")})
	if _, err := l.Commit(b); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	// Fabricate a segment whose frame claims seq 5 (gap after seq 1).
	frame, err := encodeFrame(5, [][]byte{[]byte("skip")})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, fmt.Sprintf(segNameFmt, 2))
	if err := os.WriteFile(path, frame, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir, Options{}); !errors.Is(err, ErrCorruptSegment) {
		t.Fatalf("Open with seq gap = %v, want ErrCorruptSegment", err)
	}
}

func TestOpenCreatesEmpty(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "nested", "log")
	l, err := Open(dir, Options{Sync: true})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if got := l.Segments(); len(got) != 0 {
		t.Fatalf("Segments on empty = %v", got)
	}
	if err := l.Scan(0, func(Batch) error {
		t.Fatal("Scan on empty log")
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestReopenEmptyDir(t *testing.T) {
	dir := t.TempDir()
	l1, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := l1.Close(); err != nil {
		t.Fatal(err)
	}
	l2, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Close()
	b, _ := l2.Append([][]byte{[]byte("z")})
	seq, err := l2.Commit(b)
	if err != nil || seq != 1 {
		t.Fatalf("first seq after reopening empty dir = %d, %v", seq, err)
	}
}

func listSegmentFiles(dir string) ([]os.DirEntry, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var out []os.DirEntry
	for _, e := range entries {
		if !e.IsDir() && filepath.Ext(e.Name()) == segSuffix {
			out = append(out, e)
		}
	}
	return out, nil
}
