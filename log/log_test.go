package log

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

func newTestLog(t *testing.T, opts Options) (*Log, string) {
	t.Helper()
	dir := t.TempDir()
	l, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { l.Close() })
	return l, dir
}

func recordsEqual(a, b [][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !bytes.Equal(a[i], b[i]) {
			return false
		}
	}
	return true
}

func TestCommitSequenceAndRead(t *testing.T) {
	l, _ := newTestLog(t, Options{Sync: true})

	b1, err := l.Append([][]byte{[]byte("alpha"), []byte("beta")})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if b1.Sequence != 0 {
		t.Fatalf("staged Sequence = %d, want 0", b1.Sequence)
	}
	if got, err := l.Read(1); !errors.Is(err, ErrNotCommitted) {
		t.Fatalf("Read before commit = %v, %v", got, err)
	}

	s1, err := l.Commit(b1)
	if err != nil || s1 != 1 {
		t.Fatalf("Commit = %d, %v, want 1", s1, err)
	}
	b2, _ := l.Append(nil)
	s2, err := l.Commit(b2)
	if err != nil || s2 != 2 {
		t.Fatalf("Commit = %d, %v, want 2", s2, err)
	}
	b3, _ := l.Append([][]byte{{0x00, 0xff, 0x10}, {}, []byte("γ")})
	s3, err := l.Commit(b3)
	if err != nil || s3 != 3 {
		t.Fatalf("Commit = %d, %v, want 3", s3, err)
	}

	got, err := l.Read(s1)
	if err != nil || !recordsEqual(got, b1.Records) {
		t.Fatalf("Read(1) = %q, %v", got, err)
	}
	got, err = l.Read(s2)
	if err != nil || len(got) != 0 {
		t.Fatalf("Read(2) = %q, %v", got, err)
	}
	got, err = l.Read(s3)
	if err != nil || !recordsEqual(got, b3.Records) {
		t.Fatalf("Read(3) = %q, %v", got, err)
	}
	if _, err := l.Read(4); !errors.Is(err, ErrNotCommitted) {
		t.Fatalf("Read(4) err = %v, want ErrNotCommitted", err)
	}
	if _, err := l.Read(0); !errors.Is(err, ErrNotCommitted) {
		t.Fatalf("Read(0) err = %v, want ErrNotCommitted", err)
	}
}

func TestCommitIdempotent(t *testing.T) {
	l, _ := newTestLog(t, Options{})
	b, _ := l.Append([][]byte{[]byte("once")})
	seq, err := l.Commit(b)
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	sizeAfter := l.Segments()[0].Bytes

	again, err := l.Commit(b)
	if err != nil || again != seq {
		t.Fatalf("re-Commit = %d, %v, want %d", again, err, seq)
	}
	if size := l.Segments()[0].Bytes; size != sizeAfter {
		t.Fatalf("segment grew from %d to %d on re-commit", sizeAfter, size)
	}
}

func TestUnknownBatch(t *testing.T) {
	l1, dir := newTestLog(t, Options{})
	b, _ := l1.Append([][]byte{[]byte("x")})
	if _, err := l1.Commit(b); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	l1.Close()

	l2, err := Open(dir, Options{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { l2.Close() })
	if _, err := l2.Commit(b); !errors.Is(err, ErrUnknownBatch) {
		t.Fatalf("Commit across instances err = %v, want ErrUnknownBatch", err)
	}
	if _, err := l2.Commit(Batch{}); !errors.Is(err, ErrUnknownBatch) {
		t.Fatalf("Commit zero Batch err = %v, want ErrUnknownBatch", err)
	}
}

func TestReopenRecoversCommittedPrefix(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, Options{Sync: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	want := make(map[uint64][][]byte)
	for i := 1; i <= 5; i++ {
		b, _ := l.Append([][]byte{[]byte(fmt.Sprintf("rec-%d-a", i)), []byte(fmt.Sprintf("rec-%d-b", i))})
		seq, err := l.Commit(b)
		if err != nil {
			t.Fatalf("Commit: %v", err)
		}
		want[seq] = b.Records
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	l2, err := Open(dir, Options{Sync: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer l2.Close()
	if l2.lastSeq != 5 {
		t.Fatalf("lastSeq = %d, want 5", l2.lastSeq)
	}
	var seen int
	err = l2.Scan(1, func(b Batch) error {
		seen++
		if !recordsEqual(b.Records, want[b.Sequence]) {
			t.Fatalf("batch %d = %q, want %q", b.Sequence, b.Records, want[b.Sequence])
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if seen != 5 {
		t.Fatalf("scanned %d batches, want 5", seen)
	}
}

func TestTornTailTruncation(t *testing.T) {
	cases := []struct {
		name string
		torn func(t *testing.T, path string, goodSize int64, records [][]byte)
	}{
		{
			name: "garbage bytes",
			torn: func(t *testing.T, path string, _ int64, _ [][]byte) {
				appendRaw(t, path, []byte("JUNKJUNKJUNKJUNKJUNK"))
			},
		},
		{
			name: "batch without commit marker",
			torn: func(t *testing.T, path string, goodSize int64, records [][]byte) {
				buf, err := encodeFrames(999, 42, records)
				if err != nil {
					t.Fatal(err)
				}
				n := batchFrameBytes(records)
				appendRaw(t, path, buf[:n])
			},
		},
		{
			name: "batch plus partial commit frame",
			torn: func(t *testing.T, path string, goodSize int64, records [][]byte) {
				buf, err := encodeFrames(999, 42, records)
				if err != nil {
					t.Fatal(err)
				}
				n := batchFrameBytes(records)
				appendRaw(t, path, buf[:n+3]) // 3 bytes into the commit header
			},
		},
		{
			name: "partial header",
			torn: func(t *testing.T, path string, goodSize int64, _ [][]byte) {
				appendRaw(t, path, []byte(magic+"B"))
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			l, err := Open(dir, Options{Sync: true})
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			var committed [][]byte
			for i := 1; i <= 3; i++ {
				b, _ := l.Append([][]byte{[]byte(fmt.Sprintf("batch-%d", i))})
				if _, err := l.Commit(b); err != nil {
					t.Fatalf("Commit: %v", err)
				}
				committed = b.Records
			}
			seg := l.Segments()[0].File
			goodSize := l.Segments()[0].Bytes
			if err := l.Close(); err != nil {
				t.Fatal(err)
			}

			tc.torn(t, seg, goodSize, committed)

			l2, err := Open(dir, Options{Sync: true})
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			defer l2.Close()
			if l2.lastSeq != 3 {
				t.Fatalf("lastSeq = %d, want 3", l2.lastSeq)
			}
			if got := l2.Segments()[0].Bytes; got != goodSize {
				t.Fatalf("segment size = %d, want truncated to %d", got, goodSize)
			}
			var n int
			if err := l2.Scan(1, func(Batch) error { n++; return nil }); err != nil {
				t.Fatalf("Scan: %v", err)
			}
			if n != 3 {
				t.Fatalf("scanned %d, want 3", n)
			}

			// New commits continue the sequence after recovery.
			b, _ := l2.Append([][]byte{[]byte("after")})
			seq, err := l2.Commit(b)
			if err != nil || seq != 4 {
				t.Fatalf("Commit after recovery = %d, %v, want 4", seq, err)
			}
		})
	}
}

func appendRaw(t *testing.T, path string, p []byte) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.Write(p); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
}

func TestMidSegmentCorruption(t *testing.T) {
	dir := t.TempDir()
	// Small limit so batches land in multiple segments; corruption in an
	// earlier segment is not a torn tail.
	l, err := Open(dir, Options{SegmentBytes: 64, Sync: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := 1; i <= 4; i++ {
		b, _ := l.Append([][]byte{bytes.Repeat([]byte{byte('a' + i)}, 40)})
		if _, err := l.Commit(b); err != nil {
			t.Fatalf("Commit: %v", err)
		}
	}
	segs := l.Segments()
	if len(segs) < 2 {
		t.Fatalf("expected >= 2 segments, got %d", len(segs))
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	first := segs[0].File
	orig, err := os.ReadFile(first)
	if err != nil {
		t.Fatal(err)
	}
	// Flip a byte in the middle of complete data (inside the record body).
	mid := len(orig) / 2
	damaged := make([]byte, len(orig))
	copy(damaged, orig)
	damaged[mid] ^= 0xff
	if err := os.WriteFile(first, damaged, 0o644); err != nil {
		t.Fatal(err)
	}

	_, err = Open(dir, Options{Sync: true})
	if !errors.Is(err, ErrCorruptSegment) {
		t.Fatalf("Open err = %v, want ErrCorruptSegment", err)
	}

	// Directory must be left unchanged.
	after, err := os.ReadFile(first)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(after, damaged) {
		t.Fatal("corrupt segment was modified during failed Open")
	}
}

func TestSequenceGapCorruption(t *testing.T) {
	dir := t.TempDir()
	// Fabricate two valid-CRC transactions with a gap (seq 1 then seq 3).
	var buf []byte
	recs := [][]byte{[]byte("r1")}
	buf = appendFrame(buf, frameBatch, batchBody(t, 1, recs))
	buf = appendFrame(buf, frameCommit, commitBody(t, 1, 1))
	buf = appendFrame(buf, frameBatch, batchBody(t, 2, recs))
	buf = appendFrame(buf, frameCommit, commitBody(t, 3, 2))
	if err := os.WriteFile(filepath.Join(dir, segmentName(1)), buf, 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := Open(dir, Options{})
	if !errors.Is(err, ErrCorruptSegment) {
		t.Fatalf("Open err = %v, want ErrCorruptSegment", err)
	}
}

func TestSegmentNameGapCorruption(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, segmentName(2)), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := Open(dir, Options{})
	if !errors.Is(err, ErrCorruptSegment) {
		t.Fatalf("Open err = %v, want ErrCorruptSegment", err)
	}
}

func batchBody(t *testing.T, id uint64, records [][]byte) []byte {
	t.Helper()
	body := make([]byte, 12)
	binary.BigEndian.PutUint64(body[0:8], id)
	binary.BigEndian.PutUint32(body[8:12], uint32(len(records)))
	for _, r := range records {
		var lb [4]byte
		binary.BigEndian.PutUint32(lb[:], uint32(len(r)))
		body = append(body, lb[:]...)
		body = append(body, r...)
	}
	return body
}

func commitBody(t *testing.T, seq, id uint64) []byte {
	t.Helper()
	b := make([]byte, 16)
	binary.BigEndian.PutUint64(b[0:8], seq)
	binary.BigEndian.PutUint64(b[8:16], id)
	return b
}

func TestScan(t *testing.T) {
	l, _ := newTestLog(t, Options{})
	for i := 1; i <= 5; i++ {
		b, _ := l.Append([][]byte{[]byte(fmt.Sprintf("n-%d", i))})
		if _, err := l.Commit(b); err != nil {
			t.Fatal(err)
		}
	}

	var got []uint64
	if err := l.Scan(3, func(b Batch) error {
		got = append(got, b.Sequence)
		if b.ID == 0 {
			t.Errorf("replayed batch %d has zero ID", b.Sequence)
		}
		return nil
	}); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if fmt.Sprint(got) != "[3 4 5]" {
		t.Fatalf("Scan(3) seqs = %v", got)
	}

	// from beyond the end is an empty, successful scan.
	if err := l.Scan(6, func(Batch) error {
		t.Fatal("callback should not run")
		return nil
	}); err != nil {
		t.Fatalf("Scan(6): %v", err)
	}

	// Callback error aborts and propagates.
	stop := errors.New("stop")
	err := l.Scan(1, func(b Batch) error {
		if b.Sequence == 2 {
			return stop
		}
		return nil
	})
	if !errors.Is(err, stop) {
		t.Fatalf("Scan callback err = %v, want stop", err)
	}
}

func TestSegments(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, Options{SegmentBytes: 1, Sync: false})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	for i := 1; i <= 4; i++ {
		b, _ := l.Append([][]byte{[]byte(fmt.Sprintf("segment-test-%d", i))})
		if _, err := l.Commit(b); err != nil {
			t.Fatal(err)
		}
	}
	segs := l.Segments()
	if len(segs) != 4 {
		t.Fatalf("segments = %d, want 4", len(segs))
	}
	names := make([]string, len(segs))
	var total int64
	for i, s := range segs {
		if s.FirstSeq != uint64(i+1) || s.LastSeq != uint64(i+1) {
			t.Fatalf("segment %d range = %d..%d, want %d..%d", i+1, s.FirstSeq, s.LastSeq, i+1, i+1)
		}
		st, err := os.Stat(s.File)
		if err != nil {
			t.Fatal(err)
		}
		if s.Bytes != st.Size() {
			t.Fatalf("segment %d Bytes = %d, file size = %d", i+1, s.Bytes, st.Size())
		}
		total += s.Bytes
		names[i] = filepath.Base(s.File)
	}
	if !sort.StringsAreSorted(names) {
		t.Fatalf("segment files not sorted: %v", names)
	}
	if total <= 0 {
		t.Fatal("total bytes should be positive")
	}
}

func TestOpenCreatesDirAndEmptyLog(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "nested", "log")
	l, err := Open(dir, Options{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer l.Close()
	if segs := l.Segments(); len(segs) != 0 {
		t.Fatalf("empty log has %d segments", len(segs))
	}
	if err := l.Scan(1, func(Batch) error {
		t.Fatal("empty scan")
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// A first commit after an empty open creates segment 1.
	b, _ := l.Append([][]byte{[]byte("first")})
	if seq, err := l.Commit(b); err != nil || seq != 1 {
		t.Fatalf("Commit = %d, %v, want 1", seq, err)
	}
}

func TestOpenRejectsFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "notadir")
	if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(path, Options{}); err == nil {
		t.Fatal("Open on a file should fail")
	}
	if _, err := Open("", Options{}); err == nil {
		t.Fatal("Open with empty dir should fail")
	}
}

func TestEmptyTornSegmentRemoved(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, segmentName(1)), []byte(magic+"B\x00\x00\x00\x05"), 0o644); err != nil {
		t.Fatal(err)
	}
	l, err := Open(dir, Options{Sync: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer l.Close()
	if segs := l.Segments(); len(segs) != 0 {
		t.Fatalf("expected empty torn segment removed, got %d", len(segs))
	}
	if _, err := os.Stat(filepath.Join(dir, segmentName(1))); !os.IsNotExist(err) {
		t.Fatalf("expected segment gone, stat err = %v", err)
	}
	b, _ := l.Append([][]byte{[]byte("fresh")})
	if seq, err := l.Commit(b); err != nil || seq != 1 {
		t.Fatalf("Commit = %d, %v, want 1", seq, err)
	}
}
