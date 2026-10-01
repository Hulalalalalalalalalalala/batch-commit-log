package log

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// writeBatches closes a fresh log after committing n batches of rec.
func writeBatches(t *testing.T, dir string, opts Options, n int, rec []byte) {
	t.Helper()
	l, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		b, err := l.Append([][]byte{rec})
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

// frameOffsets returns the byte offset of every frame in the only
// segment file.
func frameOffsets(t *testing.T, dir string) ([]int64, []byte, string) {
	t.Helper()
	segs, err := listSegmentFiles(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(segs) != 1 {
		t.Fatalf("segments = %d, want 1", len(segs))
	}
	path := filepath.Join(dir, segs[0].Name())
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var offs []int64
	off := int64(0)
	for off < int64(len(data)) {
		offs = append(offs, off)
		plen := binaryBigEndianUint32(data[off+magicSize : off+prefixSize])
		off += int64(prefixSize) + int64(plen) + crcSize
	}
	return offs, data, path
}

func TestCorruptMiddleLength(t *testing.T) {
	dir := t.TempDir()
	rec := bytes.Repeat([]byte("z"), 40)
	writeBatches(t, dir, Options{Sync: true}, 5, rec)
	offs, data, path := frameOffsets(t, dir)

	// Smash frame 2's length (offset 1, 0-based) to run past EOF.
	bad := append([]byte(nil), data...)
	for i := offs[1] + magicSize; i < offs[1]+prefixSize; i++ {
		bad[i] = 0xff
	}
	if err := os.WriteFile(path, bad, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir, Options{}); !errors.Is(err, ErrCorruptSegment) {
		t.Fatalf("Open with blown middle length = %v, want ErrCorruptSegment", err)
	}
}

func TestCorruptMiddleMagic(t *testing.T) {
	dir := t.TempDir()
	rec := bytes.Repeat([]byte("m"), 40)
	writeBatches(t, dir, Options{Sync: true}, 5, rec)
	offs, data, path := frameOffsets(t, dir)

	bad := append([]byte(nil), data...)
	bad[offs[2]] ^= 0x01 // flip a magic byte in frame 3
	if err := os.WriteFile(path, bad, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir, Options{}); !errors.Is(err, ErrCorruptSegment) {
		t.Fatalf("Open with damaged middle magic = %v, want ErrCorruptSegment", err)
	}
}

func TestTailDamageIsTruncated(t *testing.T) {
	dir := t.TempDir()
	rec := bytes.Repeat([]byte("t"), 40)
	writeBatches(t, dir, Options{Sync: true}, 4, rec)
	offs, data, path := frameOffsets(t, dir)

	cases := []struct {
		name    string
		wantSeq uint64
		wantSz  int64
		mut     func([]byte) []byte
	}{
		{
			name:    "torn-header-few-bytes",
			wantSeq: 4,
			wantSz:  int64(len(data)),
			mut: func(b []byte) []byte {
				return append(b, frameMagic[0], frameMagic[1], frameMagic[2])
			},
		},
		{
			name:    "torn-full-header",
			wantSeq: 4,
			wantSz:  int64(len(data)),
			mut: func(b []byte) []byte {
				return append(b, frameMagic[:]...)
			},
		},
		{
			name:    "torn-header-plus-len",
			wantSeq: 4,
			wantSz:  int64(len(data)),
			mut: func(b []byte) []byte {
				b = append(b, frameMagic[:]...)
				return append(b, 0x00, 0x00, 0x00, 0xff) // claims 255 bytes
			},
		},
		{
			name:    "last-frame-len-blown",
			wantSeq: 3,
			wantSz:  offs[3],
			mut: func(b []byte) []byte {
				b = append([]byte(nil), b...)
				for i := offs[3] + magicSize; i < offs[3]+prefixSize; i++ {
					b[i] = 0xff
				}
				return b
			},
		},
		{
			name:    "garbage-tail",
			wantSeq: 4,
			wantSz:  int64(len(data)),
			mut: func(b []byte) []byte {
				return append(b, 0xde, 0xad, 0xbe, 0xef, 0x00, 0x11)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := os.WriteFile(path, tc.mut(data), 0o644); err != nil {
				t.Fatal(err)
			}
			l, err := Open(dir, Options{Sync: true})
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			if l.lastSeq != tc.wantSeq {
				t.Fatalf("lastSeq = %d, want %d", l.lastSeq, tc.wantSeq)
			}
			st, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			if st.Size() != tc.wantSz {
				t.Fatalf("size = %d, want truncated to %d", st.Size(), tc.wantSz)
			}
			var seqs []uint64
			if err := l.Scan(0, func(b Batch) error {
				seqs = append(seqs, b.Sequence)
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if len(seqs) != int(tc.wantSeq) || seqs[0] != 1 ||
				(len(seqs) > 0 && seqs[len(seqs)-1] != tc.wantSeq) {
				t.Fatalf("seqs = %v", seqs)
			}
			// Committing continues at wantSeq+1.
			b, _ := l.Append([][]byte{rec})
			seq, err := l.Commit(b)
			if err != nil || seq != tc.wantSeq+1 {
				t.Fatalf("commit = %d, %v, want %d", seq, err, tc.wantSeq+1)
			}
			l.Close()

			// Rebuild a clean 4-frame file for the next case.
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
			writeBatches(t, dir, Options{Sync: true}, 4, rec)
			_, data, path = frameOffsets(t, dir)
		})
	}
}

func TestPartialPayloadTailTruncated(t *testing.T) {
	dir := t.TempDir()
	rec := bytes.Repeat([]byte("p"), 50)
	writeBatches(t, dir, Options{Sync: true}, 3, rec)
	_, data, path := frameOffsets(t, dir)

	// Append the full prefix of a 4th frame plus only half its payload.
	frame4, err := encodeFrame(4, [][]byte{rec})
	if err != nil {
		t.Fatal(err)
	}
	bad := append(append([]byte(nil), data...), frame4[:prefixSize+10]...)
	if err := os.WriteFile(path, bad, 0o644); err != nil {
		t.Fatal(err)
	}
	l, err := Open(dir, Options{Sync: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer l.Close()
	if l.lastSeq != 3 {
		t.Fatalf("lastSeq = %d, want 3", l.lastSeq)
	}
	st, _ := os.Stat(path)
	if st.Size() != int64(len(data)) {
		t.Fatalf("size = %d, want %d", st.Size(), len(data))
	}
}

func TestOpaqueRecordsContainMagic(t *testing.T) {
	// Records holding bytes identical to the frame magic must not fool
	// recovery or reads.
	dir := t.TempDir()
	l, err := Open(dir, Options{Sync: true})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	magicRecord := append(append([]byte(nil), frameMagic[:]...), bytes.Repeat(frameMagic[:], 3)...)
	for i := 0; i < 6; i++ {
		b, _ := l.Append([][]byte{magicRecord, []byte("other")})
		if _, err := l.Commit(b); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	l2, err := Open(dir, Options{Sync: true})
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Close()
	if l2.lastSeq != 6 {
		t.Fatalf("lastSeq = %d, want 6", l2.lastSeq)
	}
	got, err := l2.Read(4)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got[0], magicRecord) || string(got[1]) != "other" {
		t.Fatalf("records mismatch: %q / %q", got[0], got[1])
	}
}

func TestSyncFalseNormalExitPreserves(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, Options{Sync: false})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		b, _ := l.Append([][]byte{[]byte(fmt.Sprintf("n-%d", i))})
		if _, err := l.Commit(b); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.Close(); err != nil { // normal exit: flush kernel buffers
		t.Fatal(err)
	}
	l2, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Close()
	if l2.lastSeq != 3 {
		t.Fatalf("lastSeq = %d, want 3", l2.lastSeq)
	}
	got, err := l2.Read(2)
	if err != nil || string(got[0]) != "n-1" {
		t.Fatalf("Read(2) = %q, %v", got, err)
	}
}
