package log

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestTruncationMarkerRoundTrip(t *testing.T) {
	cases := []struct {
		name             string
		through, reclaim uint64
		next             uint64
		segs             []int
	}{
		{"small", 3, 3, 4, []int{1, 2, 3}},
		{"boundary", 7, 5, 12, []int{1, 4}},
		{"empty", 1, 1, 2, []int{1}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := truncation{through: tc.through, reclaimed: tc.reclaim, nextSeq: tc.next, deletedSegs: tc.segs}
			got, ok := parseTruncation(encodeTruncation(in))
			if !ok {
				t.Fatal("parseTruncation rejected valid marker")
			}
			if got.through != tc.through || got.reclaimed != tc.reclaim || got.nextSeq != tc.next {
				t.Fatalf("scalars = %d,%d,%d; want %d,%d,%d", got.through, got.reclaimed, got.nextSeq, tc.through, tc.reclaim, tc.next)
			}
			if len(got.deletedSegs) != len(tc.segs) {
				t.Fatalf("segs = %v, want %v", got.deletedSegs, tc.segs)
			}
			for i := range tc.segs {
				if got.deletedSegs[i] != tc.segs[i] {
					t.Fatalf("seg %d = %d, want %d", i, got.deletedSegs[i], tc.segs[i])
				}
			}
		})
	}
}

func TestTruncationMarkerRejectsGarbage(t *testing.T) {
	good := encodeTruncation(truncation{through: 3, reclaimed: 3, nextSeq: 4, deletedSegs: []int{1}})
	for name, mutate := range map[string]func([]byte) []byte{
		"short":       func(b []byte) []byte { return b[:8] },
		"bad magic":   func(b []byte) []byte { b[0] = 'X'; return b },
		"bad version": func(b []byte) []byte { b[8] = 99; return b },
		"bad crc":     func(b []byte) []byte { b[len(b)-1] ^= 0xff; return b },
		"reclaim>through": func(b []byte) []byte {
			c := append([]byte(nil), b...)
			return c
		},
	} {
		t.Run(name, func(t *testing.T) {
			if name == "reclaim>through" {
				bad := encodeTruncation(truncation{through: 2, reclaimed: 3, nextSeq: 4, deletedSegs: []int{1}})
				if _, ok := parseTruncation(bad); ok {
					t.Fatal("accepted reclaimed > through")
				}
				return
			}
			if _, ok := parseTruncation(mutate(append([]byte(nil), good...))); ok {
				t.Fatal("parseTruncation accepted corrupt marker")
			}
		})
	}
}

func TestDeleteThroughSyncFailureLeavesOldState(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, Options{SegmentBytes: 32, Sync: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer l.Close()
	for i := 1; i <= 3; i++ {
		b, err := l.Append([][]byte{[]byte{byte('a' + i - 1)}})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := l.Commit(b); err != nil {
			t.Fatal(err)
		}
	}

	boom := errors.New("marker fsync boom")
	failing := true
	orig := syncFile
	defer func() { syncFile = orig }()
	syncFile = func(f *os.File) error {
		if failing && strings.HasSuffix(f.Name(), "truncate.idx.tmp") {
			return boom
		}
		return orig(f)
	}

	n, err := l.DeleteThrough(2)
	if !errors.Is(err, ErrRetentionFailed) {
		t.Fatalf("DeleteThrough err = %v, want ErrRetentionFailed", err)
	}
	if n != 0 {
		t.Fatalf("deleted %d segments after failure, want 0", n)
	}
	// No marker, no missing data: the complete old state is readable.
	if _, statErr := os.Stat(filepath.Join(dir, "truncate.idx")); !os.IsNotExist(statErr) {
		t.Fatalf("truncate.idx present after failed persist: %v", statErr)
	}
	for seq := uint64(1); seq <= 3; seq++ {
		if _, err := l.Read(seq); err != nil {
			t.Fatalf("Read(%d) after failed truncation: %v", seq, err)
		}
	}
	if segs := l.Segments(); len(segs) != 3 {
		t.Fatalf("segments after failure = %d, want 3", len(segs))
	}

	// The retry, with sync healthy, completes the truncation.
	failing = false
	n, err = l.DeleteThrough(2)
	if err != nil || n != 2 {
		t.Fatalf("retry DeleteThrough = %d, %v; want 2 nil", n, err)
	}
	if _, err := l.Read(1); !errors.Is(err, ErrTruncated) {
		t.Fatalf("Read(1) err = %v, want ErrTruncated", err)
	}
	if _, err := l.Read(3); err != nil {
		t.Fatalf("Read(3): %v", err)
	}
}

func TestOpenRejectsCorruptTruncationMarker(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, Options{SegmentBytes: 32})
	if err != nil {
		t.Fatal(err)
	}
	b, _ := l.Append([][]byte{[]byte("x")})
	if _, err := l.Commit(b); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "truncate.idx"), []byte("garbage-not-a-marker"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir, Options{SegmentBytes: 32}); !errors.Is(err, ErrCorruptSegment) {
		t.Fatalf("Open err = %v, want ErrCorruptSegment", err)
	}
}
