package log_test

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	log "github.com/Hulalalalalalalalalalala/batch-commit-log"
)

func indexPath(dir string) string { return filepath.Join(dir, "index.idx") }

// seedLog writes a spread of commits: singles, a group, holes-free,
// across multiple segments, and returns the directory + options.
func seedLog(t *testing.T, segBytes int) (string, log.Options) {
	t.Helper()
	dir := t.TempDir()
	opts := log.Options{SegmentBytes: segBytes, Sync: true}
	l := open(t, dir, opts)
	commit(t, l, []byte("one"))
	commit(t, l, []byte("two"), []byte("three"))
	var gs []log.Batch
	for _, s := range []string{"g1", "g2"} {
		b, err := l.Append([][]byte{[]byte(s)})
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
		gs = append(gs, b)
	}
	if _, err := l.CommitGroup(gs); err != nil {
		t.Fatalf("CommitGroup: %v", err)
	}
	eb, err := l.Append(nil) // genuinely empty batch
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if _, err := l.Commit(eb); err != nil {
		t.Fatalf("Commit empty: %v", err)
	}
	l.Close()
	return dir, opts
}

func assertSeedState(t *testing.T, l *log.Log) {
	t.Helper()
	want := map[uint64][][]byte{
		1: {[]byte("one")},
		2: {[]byte("two"), []byte("three")},
		3: {[]byte("g1")},
		4: {[]byte("g2")},
		5: {},
	}
	for seq, recs := range want {
		got, err := l.Read(seq)
		if err != nil || !reflect.DeepEqual(got, recs) {
			t.Fatalf("Read(%d) = %q, %v; want %q", seq, got, err, recs)
		}
	}
	var seqs []uint64
	if err := l.Scan(1, func(b log.Batch) error {
		seqs = append(seqs, b.Seq())
		return nil
	}); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if !reflect.DeepEqual(seqs, []uint64{1, 2, 3, 4, 5}) {
		t.Fatalf("scan seqs = %v, want [1 2 3 4 5]", seqs)
	}
	segs := l.Segments()
	if len(segs) == 0 || segs[0].FirstSeq != 1 {
		t.Fatalf("segments = %+v, want non-empty starting at 1", segs)
	}
	// Listing ends at 5 regardless of how the segments were rolled.
	if segs[len(segs)-1].LastSeq != 5 {
		t.Fatalf("last segment = %+v, want LastSeq 5", segs[len(segs)-1])
	}
	// Sorted by file order, non-overlapping ranges.
	var prev uint64
	for i, s := range segs {
		if i > 0 && s.FirstSeq <= prev {
			t.Fatalf("segments out of order: %+v", segs)
		}
		prev = s.LastSeq
	}
}

func TestSidecarCreatedAndAdopted(t *testing.T) {
	dir, opts := seedLog(t, 1<<20)

	st, err := os.Stat(indexPath(dir))
	if err != nil {
		t.Fatalf("index sidecar missing: %v", err)
	}
	if st.Size() <= 8 {
		t.Fatalf("sidecar size = %d, want records", st.Size())
	}

	// Reopen: incremental adoption must serve the identical state.
	l := open(t, dir, opts)
	assertSeedState(t, l)
	l.Close()

	// Drop the sidecar: the full rebuild path gives the same state.
	if err := os.Remove(indexPath(dir)); err != nil {
		t.Fatal(err)
	}
	l = open(t, dir, opts)
	assertSeedState(t, l)
	// And committing after a rebuild extends the same sequence space.
	if seq := commit(t, l, []byte("six")); seq != 6 {
		t.Fatalf("seq after rebuild = %d, want 6", seq)
	}
	l.Close()

	// A third reopen adopts the freshly rebuilt sidecar.
	l = open(t, dir, opts)
	if got, err := l.Read(6); err != nil || string(got[0]) != "six" {
		t.Fatalf("Read(6) = %q, %v", got, err)
	}
}

func TestSidecarDefectsFallBackToRebuild(t *testing.T) {
	mutate := func(t *testing.T, fn func([]byte) []byte) {
		t.Helper()
		dir, opts := seedLog(t, 1<<20)
		data, err := os.ReadFile(indexPath(dir))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(indexPath(dir), fn(data), 0o644); err != nil {
			t.Fatal(err)
		}
		l, err := log.Open(dir, opts)
		if err != nil {
			t.Fatalf("Open with defective sidecar: %v", err)
		}
		defer l.Close()
		assertSeedState(t, l)
	}

	t.Run("garbage", func(t *testing.T) {
		mutate(t, func([]byte) []byte { return []byte("not an index at all") })
	})
	t.Run("wrong version", func(t *testing.T) {
		mutate(t, func(d []byte) []byte {
			d[6] ^= 0xff
			d[7] ^= 0xff
			return d
		})
	})
	t.Run("bad record checksum", func(t *testing.T) {
		mutate(t, func(d []byte) []byte {
			d[len(d)-1] ^= 0xff
			return d
		})
	})
	t.Run("truncated final record", func(t *testing.T) {
		mutate(t, func(d []byte) []byte { return d[:len(d)-3] })
	})
	t.Run("header only", func(t *testing.T) {
		mutate(t, func(d []byte) []byte { return d[:8] })
	})
	t.Run("empty file", func(t *testing.T) {
		mutate(t, func(d []byte) []byte { return nil })
	})
}

func TestSidecarNeverAdvertisesAhead(t *testing.T) {
	dir, opts := seedLog(t, 1<<20)

	// One committed batch per segment, then drop the last segment's
	// bytes while the sidecar still claims the batch: adoption must
	// reject the contradiction and rebuild must expose exactly what the
	// segments hold — never the indexed-but-missing batch 1.
	dir2 := t.TempDir()
	small := log.Options{SegmentBytes: 32, Sync: true}
	l := open(t, dir2, small)
	for _, s := range []string{"one", "two", "three"} {
		commit(t, l, []byte(s))
	}
	l.Close()

	// Sanity: three segments, each with one batch.
	files, _ := filepath.Glob(filepath.Join(dir2, "*.seg"))
	sort.Strings(files)
	if len(files) != 3 {
		t.Fatalf("segments = %d, want 3", len(files))
	}
	if err := os.Truncate(files[2], 0); err != nil {
		t.Fatal(err)
	}
	l, err := log.Open(dir2, small)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer l.Close()
	if _, err := l.Read(3); !errors.Is(err, log.ErrUnknownBatch) {
		t.Fatalf("Read(3) err = %v, want ErrUnknownBatch (index ahead of segments must be ignored)", err)
	}
	if got, err := l.Read(2); err != nil || string(got[0]) != "two" {
		t.Fatalf("Read(2) = %q, %v", got, err)
	}
	// Next append continues after reality, not after the stale index.
	if seq := commit(t, l, []byte("four")); seq != 3 {
		t.Fatalf("seq = %d, want 3", seq)
	}

	// Appending junk records to the sidecar (a stale/future writer) can
	// never publish a batch either.
	data, err := os.ReadFile(indexPath(dir))
	if err != nil {
		t.Fatal(err)
	}
	data = append(data, 'X', 'X', 'X', 'X', 'X')
	if err := os.WriteFile(indexPath(dir), data, 0o644); err != nil {
		t.Fatal(err)
	}
	l2, err := log.Open(dir, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer l2.Close()
	assertSeedState(t, l2)
}

// appendCompleteEntry appends a valid, checksummed single-batch entry
// for seq with one record, simulating a crash after the segment entry
// is durable but before its index record lands.
func appendCompleteEntry(t *testing.T, path string, seq uint64, payload string) {
	t.Helper()
	var entry []byte
	entry = append(entry, 'B', 'C', 'L', '1')
	var tmp [8]byte
	binary.LittleEndian.PutUint64(tmp[:], seq)
	entry = append(entry, tmp[:]...)
	binary.LittleEndian.PutUint32(tmp[:4], 1)
	entry = append(entry, tmp[:4]...)
	binary.LittleEndian.PutUint32(tmp[:4], uint32(len(payload)))
	entry = append(entry, tmp[:4]...)
	entry = append(entry, payload...)
	var crc [4]byte
	binary.LittleEndian.PutUint32(crc[:], crc32.ChecksumIEEE(entry))
	entry = append(entry, crc[:]...)
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

func TestCrashBetweenSegmentAndIndexAdoptsSuffix(t *testing.T) {
	dir, opts := seedLog(t, 1<<20)

	// Segment contains a complete committed entry the sidecar does not
	// know about: adoption parses just the suffix and publishes it.
	files, _ := filepath.Glob(filepath.Join(dir, "*.seg"))
	sort.Strings(files)
	appendCompleteEntry(t, files[len(files)-1], 6, "late")

	l, err := log.Open(dir, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer l.Close()
	if got, err := l.Read(6); err != nil || string(got[0]) != "late" {
		t.Fatalf("Read(6) = %q, %v", got, err)
	}
	l.Close()

	// The suffix record was persisted: a second reopen adopts it.
	l, err = log.Open(dir, opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer l.Close()
	if got, err := l.Read(6); err != nil || string(got[0]) != "late" {
		t.Fatalf("Read(6) after second reopen = %q, %v", got, err)
	}
	var seqs []uint64
	if err := l.Scan(1, func(b log.Batch) error { seqs = append(seqs, b.Seq()); return nil }); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if !reflect.DeepEqual(seqs, []uint64{1, 2, 3, 4, 5, 6}) {
		t.Fatalf("scan = %v", seqs)
	}
}

func TestTornTailRecoveredOnBothOpenPaths(t *testing.T) {
	for _, removeSidecar := range []bool{false, true} {
		t.Run(map[bool]string{false: "adopt", true: "rebuild"}[removeSidecar], func(t *testing.T) {
			dir, opts := seedLog(t, 1<<20)
			// Torn group reserving 6,7 in the suffix the index lacks.
			appendTornGroup(t, dir, 6, 7)
			if removeSidecar {
				if err := os.Remove(indexPath(dir)); err != nil {
					t.Fatal(err)
				}
			}
			l, err := log.Open(dir, opts)
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			defer l.Close()
			for seq := uint64(6); seq <= 7; seq++ {
				if _, err := l.Read(seq); !errors.Is(err, log.ErrNotCommitted) {
					t.Fatalf("Read(%d) err = %v, want ErrNotCommitted", seq, err)
				}
			}
			if seq := commit(t, l, []byte("eight")); seq != 8 {
				t.Fatalf("seq = %d, want 8", seq)
			}
			l.Close()

			// Holes persist through another reopen.
			l, err = log.Open(dir, opts)
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			defer l.Close()
			var seqs []uint64
			if err := l.Scan(1, func(b log.Batch) error { seqs = append(seqs, b.Seq()); return nil }); err != nil {
				t.Fatalf("Scan: %v", err)
			}
			if !reflect.DeepEqual(seqs, []uint64{1, 2, 3, 4, 5, 8}) {
				t.Fatalf("scan = %v, want [1 2 3 4 5 8]", seqs)
			}
		})
	}
}

func TestTamperedSegmentWithSidecarIsCorrupt(t *testing.T) {
	dir, opts := seedLog(t, 1<<20)
	files, _ := filepath.Glob(filepath.Join(dir, "*.seg"))
	sort.Strings(files)
	data, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatal(err)
	}
	// Flip a payload byte in the first segment: its disk CRC no longer
	// matches, and adoption must not paper over that with a rebuild —
	// both paths agree it is ErrCorruptSegment.
	data[20] ^= 0xff
	if err := os.WriteFile(files[0], data, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := log.Open(dir, opts); !errors.Is(err, log.ErrCorruptSegment) {
		t.Fatalf("Open err = %v, want ErrCorruptSegment", err)
	}
}

func TestAdoptSuffixInNewSegment(t *testing.T) {
	// A commit that rolls into a fresh segment and is durable there
	// while its index record is missing: adoption must parse the whole
	// later segment as suffix, not treat the gap as a contradiction.
	dir := t.TempDir()
	opts := log.Options{SegmentBytes: 32, Sync: true}
	l := open(t, dir, opts)
	commit(t, l, []byte("one")) // segment 1
	commit(t, l, []byte("two")) // rolls to segment 2
	l.Close()

	// Find the sidecar offset just past the record for seq 1: the last
	// record covers seq 2 in the later segment.
	idx, err := os.ReadFile(indexPath(dir))
	if err != nil {
		t.Fatal(err)
	}
	// Records: header(8) + rec(seq1) + rec(seq2). The first record's
	// length field sits at offset 9; cut exactly the final record.
	bodyLen := binary.LittleEndian.Uint32(idx[9:13])
	cut := 8 + 5 + int(bodyLen) + 4
	if cut >= len(idx) {
		t.Fatalf("cut %d not within sidecar of %d bytes", cut, len(idx))
	}
	if err := os.Truncate(indexPath(dir), int64(cut)); err != nil {
		t.Fatal(err)
	}

	l, err = log.Open(dir, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer l.Close()
	if got, err := l.Read(1); err != nil || string(got[0]) != "one" {
		t.Fatalf("Read(1) = %q, %v", got, err)
	}
	if got, err := l.Read(2); err != nil || string(got[0]) != "two" {
		t.Fatalf("Read(2) = %q, %v (whole-segment suffix must be adopted)", got, err)
	}
	segs := l.Segments()
	if len(segs) != 2 {
		t.Fatalf("segments = %+v, want 2", segs)
	}
	l.Close()

	// Second reopen adopts the persisted suffix record.
	l, err = log.Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	count := 0
	if err := l.Scan(1, func(b log.Batch) error { count++; return nil }); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("scan count = %d, want 2", count)
	}
}

func TestRebuiltAndAdoptedReadsMatchRecordByRecord(t *testing.T) {
	dir, opts := seedLog(t, 64) // force several segments

	adopt, err := log.Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	adoptSegs := adopt.Segments()
	var adoptRecs [][][]byte
	for seq := uint64(1); seq <= 5; seq++ {
		r, err := adopt.Read(seq)
		if err != nil {
			t.Fatal(err)
		}
		adoptRecs = append(adoptRecs, r)
	}
	adopt.Close()

	if err := os.Remove(indexPath(dir)); err != nil {
		t.Fatal(err)
	}
	reb, err := log.Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer reb.Close()
	if !reflect.DeepEqual(reb.Segments(), adoptSegs) {
		t.Fatalf("segments differ: adopt=%+v rebuild=%+v", adoptSegs, reb.Segments())
	}
	for seq := uint64(1); seq <= 5; seq++ {
		r, err := reb.Read(seq)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(r, adoptRecs[seq-1]) {
			t.Fatalf("seq %d differs: adopt=%q rebuild=%q", seq, adoptRecs[seq-1], r)
		}
	}
}
