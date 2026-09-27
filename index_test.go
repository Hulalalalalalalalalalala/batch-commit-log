package log

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// openInternal opens a fresh synced log for crash-window tests.
func openInternal(t *testing.T, dir string) *Log {
	t.Helper()
	l, err := Open(dir, Options{SegmentBytes: 1 << 20, Sync: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { l.Close() })
	return l
}

func appendRaw(t *testing.T, dir string, n int, b []byte) {
	t.Helper()
	p := filepath.Join(dir, fmt.Sprintf("%06d.seg", n))
	f, err := os.OpenFile(p, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(b); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

func readIndexFile(t *testing.T, dir string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, "index.idx"))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func writeIndexFile(t *testing.T, dir string, b []byte) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, "index.idx"), b, 0o644); err != nil {
		t.Fatal(err)
	}
}

// commitOne synchronously appends and commits one record, returning seq.
func commitOne(t *testing.T, l *Log, s string) uint64 {
	t.Helper()
	b, err := l.Append([][]byte{[]byte(s)})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	seq, err := l.Commit(b)
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	return seq
}

// TestSideFileFramesTrackCommits checks the durable side file gains one
// frame per commit and a reopen adopts it incrementally.
func TestSideFileFramesTrackCommits(t *testing.T) {
	dir := t.TempDir()
	l := openInternal(t, dir)
	for _, s := range []string{"a", "b", "c"} {
		commitOne(t, l, s)
	}
	l.Close()

	raw := readIndexFile(t, dir)
	frames, ok := parseIndex(raw)
	if !ok {
		t.Fatalf("side file failed to parse")
	}
	if len(frames) != 3 {
		t.Fatalf("frames = %d, want 3", len(frames))
	}
	var seqs []uint64
	for _, f := range frames {
		for _, m := range f.members {
			seqs = append(seqs, m.seq)
		}
	}
	if !reflect.DeepEqual(seqs, []uint64{1, 2, 3}) {
		t.Fatalf("frame seqs = %v, want [1 2 3]", seqs)
	}

	// Reopen adopts the side file; reads and replay are identical to a
	// fresh rebuild.
	l2 := openInternal(t, dir)
	for seq := uint64(1); seq <= 3; seq++ {
		got, err := l2.Read(seq)
		if err != nil || string(got[0]) != string(rune('a'+seq-1)) {
			t.Fatalf("Read(%d) = %q, %v", seq, got, err)
		}
	}
}

// TestCrashAfterSegmentSyncBeforeIndex: the segment entry is complete
// and synced but the index never advertised it. It must be invisible
// after reopen and its sequence is a permanent hole.
func TestCrashAfterSegmentSyncBeforeIndex(t *testing.T) {
	dir := t.TempDir()
	l := openInternal(t, dir)
	commitOne(t, l, "one")
	l.Close()

	// A fully synced entry for seq 2 with no matching index frame:
	// exactly the state of a crash between the segment sync and the
	// index frame write.
	appendRaw(t, dir, 1, encodeBatch(2, [][]byte{[]byte("two")}))
	// index.idx stays with just seq 1's frame (a clean boundary).

	l = openInternal(t, dir)
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
		t.Fatalf("next seq = %d, want 3", b.Seq())
	}
	if _, err := l.Commit(b); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	var seen []uint64
	if err := l.Scan(1, func(b Batch) error { seen = append(seen, b.Seq()); return nil }); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if !reflect.DeepEqual(seen, []uint64{1, 3}) {
		t.Fatalf("scan = %v, want [1 3]", seen)
	}
	l.Close()

	// Hole persists across another reopen.
	l = openInternal(t, dir)
	if _, err := l.Read(2); !errors.Is(err, ErrNotCommitted) {
		t.Fatalf("Read(2) after reopen err = %v, want ErrNotCommitted", err)
	}
}

// TestCrashMidIndexFrame: the frame for a complete synced entry was
// only half written. The salvaged frame prefix is the commit watermark,
// so the entry stays invisible and its sequences become holes.
func TestCrashMidIndexFrame(t *testing.T) {
	dir := t.TempDir()
	l := openInternal(t, dir)
	commitOne(t, l, "one")
	l.Close()

	// Complete synced group [2,3] in the segment...
	entry, _, _ := encodeGroup([]groupMember{
		{seq: 2, records: [][]byte{[]byte("g2")}},
		{seq: 3, records: [][]byte{[]byte("g3")}},
	})
	appendRaw(t, dir, 1, entry)
	// ...and a torn group frame in the side file: valid header/body up
	// to a few bytes, no trailing checksum.
	good := readIndexFile(t, dir)
	frame := encodeFrame(&indexFrame{kind: kindData, seg: 1, endOff: uint64(len(entry))})
	torn := append(append([]byte{}, good...), frame[:len(frame)-6]...)
	writeIndexFile(t, dir, torn)

	l = openInternal(t, dir)
	if _, err := l.Read(1); err != nil {
		t.Fatalf("Read(1): %v", err)
	}
	for seq := uint64(2); seq <= 3; seq++ {
		if _, err := l.Read(seq); !errors.Is(err, ErrNotCommitted) {
			t.Fatalf("Read(%d) err = %v, want ErrNotCommitted", seq, err)
		}
	}
	b, _ := l.Append([][]byte{[]byte("four")})
	if b.Seq() != 4 {
		t.Fatalf("next seq = %d, want 4", b.Seq())
	}
	if _, err := l.Commit(b); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	l.Close()

	// After repair the side file is whole again and replay skips both
	// holes.
	l = openInternal(t, dir)
	var seen []uint64
	l.Scan(1, func(b Batch) error { seen = append(seen, b.Seq()); return nil })
	if !reflect.DeepEqual(seen, []uint64{1, 4}) {
		t.Fatalf("scan = %v, want [1 4]", seen)
	}
}

// TestBadIndexChecksumFallsBackToRebuild: a fully present frame with a
// bad checksum is content tampering of the side file, not a crash. The
// full structural rebuild decides from the segments: the complete
// checksum-valid entry is committed.
func TestBadIndexChecksumFallsBackToRebuild(t *testing.T) {
	dir := t.TempDir()
	l := openInternal(t, dir)
	commitOne(t, l, "one")
	l.Close()

	raw := readIndexFile(t, dir)
	// Flip a byte inside the first frame's body (after the 8-byte
	// header) while keeping the file length intact.
	raw[len(raw)-5] ^= 0xff
	writeIndexFile(t, dir, raw)

	l = openInternal(t, dir)
	if got, err := l.Read(1); err != nil || string(got[0]) != "one" {
		t.Fatalf("Read(1) after bad frame checksum = %q, %v", got, err)
	}
}

// TestIndexAheadOfSegmentRebuilds: the side file advertises more bytes
// than the segment holds; adoption is rejected and the rebuild treats
// the torn segment as a crash tail.
func TestIndexAheadOfSegmentRebuilds(t *testing.T) {
	dir := t.TempDir()
	l := openInternal(t, dir)
	commitOne(t, l, "one")
	l.Close()

	// Append a torn entry to the segment but keep the side file claiming
	// the larger committed prefix it would have had.
	good := readIndexFile(t, dir)
	bigEntry := encodeBatch(2, [][]byte{[]byte("two")})
	appendRaw(t, dir, 1, bigEntry)
	// Forge a frame covering the big entry, then truncate the segment
	// partway so the side file is ahead.
	segData, _ := os.ReadFile(filepath.Join(dir, "000001.seg"))
	baseLen := len(segData) - len(bigEntry)
	forged := encodeFrame(&indexFrame{
		kind:   kindData,
		seg:    1,
		endOff: uint64(len(segData)),
		segCRC: 0,
		members: []frameMember{{
			seq: 2, off: uint64(baseLen + 4), length: uint64(len(bigEntry) - 8),
		}},
	})
	writeIndexFile(t, dir, append(good, forged...))
	if err := os.Truncate(filepath.Join(dir, "000001.seg"), int64(len(segData)-3)); err != nil {
		t.Fatal(err)
	}

	l = openInternal(t, dir) // must not fail
	if _, err := l.Read(1); err != nil {
		t.Fatalf("Read(1): %v", err)
	}
}

// TestAdoptionAndRebuildEquivalent: reads, replay and segment listing
// are identical whether the side file is adopted intact or deleted and
// fully rebuilt.
func TestAdoptionAndRebuildEquivalent(t *testing.T) {
	mk := func(t *testing.T) string {
		dir := t.TempDir()
		l, _ := Open(dir, Options{SegmentBytes: 48, Sync: true})
		t.Cleanup(func() { l.Close() })
		commitOne(t, l, "one")
		var bs []Batch
		for _, s := range []string{"g1", "g2"} {
			b, _ := l.Append([][]byte{[]byte(s)})
			bs = append(bs, b)
		}
		if _, err := l.CommitGroup(bs); err != nil {
			t.Fatalf("CommitGroup: %v", err)
		}
		commitOne(t, l, "four")
		l.Close()
		return dir
	}

	snapshot := func(t *testing.T, dir string) (map[uint64][][]byte, []uint64, []Segment) {
		l := openInternal(t, dir)
		contents := map[uint64][][]byte{}
		var scan []uint64
		l.Scan(1, func(b Batch) error {
			scan = append(scan, b.Seq())
			contents[b.Seq()] = b.Records()
			return nil
		})
		return contents, scan, l.Segments()
	}

	adoptDir := mk(t)
	rebuildDir := mk(t)
	// rebuildDir gets an identical segment set; remove its side file.
	if err := os.Remove(filepath.Join(rebuildDir, "index.idx")); err != nil {
		t.Fatal(err)
	}

	c1, s1, g1 := snapshot(t, adoptDir)
	c2, s2, g2 := snapshot(t, rebuildDir)
	if !reflect.DeepEqual(s1, s2) || !reflect.DeepEqual(c1, c2) || !reflect.DeepEqual(g1, g2) {
		t.Fatalf("adopt vs rebuild differ:\nscan %v vs %v\nsegs %+v vs %+v", s1, s2, g1, g2)
	}
}

// TestAdoptionDoesNotRewriteIntactSideFile: a clean reopen that finds
// the side file fully covering the segments leaves its bytes untouched.
func TestAdoptionDoesNotRewriteIntactSideFile(t *testing.T) {
	dir := t.TempDir()
	l := openInternal(t, dir)
	commitOne(t, l, "one")
	commitOne(t, l, "two")
	l.Close()

	before := readIndexFile(t, dir)
	l = openInternal(t, dir)
	commitOne(t, l, "three")
	l.Close()
	after := readIndexFile(t, dir)
	if len(after) <= len(before) {
		t.Fatalf("side file did not grow: before=%d after=%d", len(before), len(after))
	}
	if !reflect.DeepEqual(after[:len(before)], before) {
		t.Fatalf("existing side file prefix was rewritten")
	}
}

// TestSideFileVersionMismatchRebuilds: an unknown version forces a full
// rebuild from the segments.
func TestSideFileVersionMismatchRebuilds(t *testing.T) {
	dir := t.TempDir()
	l := openInternal(t, dir)
	commitOne(t, l, "one")
	l.Close()

	raw := readIndexFile(t, dir)
	raw[4] = 0xff // bogus version
	writeIndexFile(t, dir, raw)

	l = openInternal(t, dir)
	if got, err := l.Read(1); err != nil || string(got[0]) != "one" {
		t.Fatalf("Read(1) after version mismatch = %q, %v", got, err)
	}
}

// TestGroupCrashWindowAllOrNothing: a crash after the group entry is
// synced but before its single index frame lands hides the whole group.
func TestGroupCrashWindowAllOrNothing(t *testing.T) {
	dir := t.TempDir()
	l := openInternal(t, dir)
	commitOne(t, l, "one")
	l.Close()

	entry, _, _ := encodeGroup([]groupMember{
		{seq: 2, records: [][]byte{[]byte("g2")}},
		{seq: 3, records: [][]byte{[]byte("g3")}},
	})
	appendRaw(t, dir, 1, entry) // synced group, no index frame

	l = openInternal(t, dir)
	for seq := uint64(2); seq <= 3; seq++ {
		if _, err := l.Read(seq); !errors.Is(err, ErrNotCommitted) {
			t.Fatalf("Read(%d) err = %v, want ErrNotCommitted", seq, err)
		}
	}
	if got := l.Segments(); len(got) != 1 || got[0].FirstSeq != 1 || got[0].LastSeq != 1 {
		t.Fatalf("Segments = %+v, want only seq 1", got)
	}
	var seen []uint64
	l.Scan(1, func(b Batch) error { seen = append(seen, b.Seq()); return nil })
	if !reflect.DeepEqual(seen, []uint64{1}) {
		t.Fatalf("scan = %v, want [1]", seen)
	}
}

// TestIndexSyncFailureRollsBackSegment ensures a failed index fsync
// rolls the synced segment prefix back so the retry publishes exactly
// once.
func TestIndexSyncFailureRollsBackSegment(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, Options{SegmentBytes: 1 << 20, Sync: true})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	orig := syncFile
	calls := 0
	failed := false
	syncFile = func(f *os.File) error {
		calls++
		// Fail exactly the index fsync (2nd fsync) of the first attempt.
		if !failed && calls == 2 {
			failed = true
			return errors.New("index fsync boom")
		}
		return orig(f)
	}
	defer func() { syncFile = orig }()

	b, _ := l.Append([][]byte{[]byte("x")})
	if _, err := l.Commit(b); !errors.Is(err, ErrSyncFailed) {
		t.Fatalf("Commit err = %v, want ErrSyncFailed", err)
	}
	if l.fileSize != 0 {
		t.Fatalf("fileSize = %d, want 0 after rollback", l.fileSize)
	}
	seq, err := l.Commit(b)
	if err != nil || seq != 1 {
		t.Fatalf("retry = %d, %v", seq, err)
	}
	l.Close()

	l2, _ := Open(dir, Options{SegmentBytes: 1 << 20, Sync: true})
	defer l2.Close()
	count := 0
	l2.Scan(1, func(Batch) error { count++; return nil })
	if count != 1 {
		t.Fatalf("replayed %d batches, want 1 (no duplicate)", count)
	}
}

// TestEmptyLogSideFileHeader: an empty log creates a header-only side
// file and still reports no segments.
func TestEmptyLogSideFileHeader(t *testing.T) {
	dir := t.TempDir()
	l := openInternal(t, dir)
	if segs := l.Segments(); len(segs) != 0 {
		t.Fatalf("Segments = %+v, want none", segs)
	}
	l.Close()
	raw := readIndexFile(t, dir)
	if len(raw) != 8 {
		t.Fatalf("header-only side file = %d bytes, want 8", len(raw))
	}
	// Reopen of an empty log works.
	l = openInternal(t, dir)
	if seq := commitOne(t, l, "first"); seq != 1 {
		t.Fatalf("seq = %d, want 1", seq)
	}
}
