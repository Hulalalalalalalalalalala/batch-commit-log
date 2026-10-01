package log_test

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	log "github.com/Hulalalalalalalalalalala/batch-commit-log"
)

func TestAppendIdempotentStagedReturnsSameBatch(t *testing.T) {
	l := open(t, t.TempDir(), log.Options{SegmentBytes: 1 << 20})

	recs := [][]byte{[]byte("alpha"), []byte("beta")}
	b1, err := l.AppendIdempotent("key-1", recs)
	if err != nil {
		t.Fatalf("AppendIdempotent: %v", err)
	}
	if b1.Seq() != 1 || b1.ID() != "key-1" {
		t.Fatalf("first batch = seq %d id %q, want 1/key-1", b1.Seq(), b1.ID())
	}
	// A repeat before commit returns the very same staged batch and Seq,
	// reserving nothing new.
	b2, err := l.AppendIdempotent("key-1", recs)
	if err != nil {
		t.Fatalf("retry AppendIdempotent: %v", err)
	}
	if b2.Seq() != b1.Seq() || b2.ID() != b1.ID() {
		t.Fatalf("retry batch = %d/%q, want same as %d/%q", b2.Seq(), b2.ID(), b1.Seq(), b1.ID())
	}
	if !reflect.DeepEqual(b2.Records(), recs) {
		t.Fatalf("retry records = %q, want %q", b2.Records(), recs)
	}
	// A concurrent anonymous append shows the retry reserved no sequence.
	anon, err := l.Append([][]byte{[]byte("anon")})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if anon.Seq() != 2 {
		t.Fatalf("anonymous seq after keyed retry = %d, want 2", anon.Seq())
	}
	if _, err := l.Read(1); !errors.Is(err, log.ErrNotCommitted) {
		t.Fatalf("Read(1) err = %v, want ErrNotCommitted", err)
	}
}

func TestAppendIdempotentCommittedDedupAndRecommit(t *testing.T) {
	l := open(t, t.TempDir(), log.Options{SegmentBytes: 1 << 20, Sync: true})

	recs := [][]byte{[]byte("one"), []byte("two")}
	b1, err := l.AppendIdempotent("order-7", recs)
	if err != nil {
		t.Fatalf("AppendIdempotent: %v", err)
	}
	if seq, err := l.Commit(b1); err != nil || seq != 1 {
		t.Fatalf("Commit = %d, %v; want 1", seq, err)
	}
	// Repeat after commit: same Seq/ID/records, no new write.
	b2, err := l.AppendIdempotent("order-7", recs)
	if err != nil {
		t.Fatalf("committed retry: %v", err)
	}
	if b2.Seq() != 1 || b2.ID() != "order-7" {
		t.Fatalf("committed retry = %d/%q, want 1/order-7", b2.Seq(), b2.ID())
	}
	if !reflect.DeepEqual(b2.Records(), recs) {
		t.Fatalf("committed retry records = %q, want %q", b2.Records(), recs)
	}
	// The next fresh key keeps reserving sequences after the first one.
	b3, err := l.AppendIdempotent("order-8", [][]byte{[]byte("x")})
	if err != nil {
		t.Fatalf("AppendIdempotent other: %v", err)
	}
	if b3.Seq() != 2 {
		t.Fatalf("fresh key seq = %d, want 2", b3.Seq())
	}
	// Committing the returned dedup Batch is committing an already
	// committed batch: ErrUnknownBatch.
	if _, err := l.Commit(b2); !errors.Is(err, log.ErrUnknownBatch) {
		t.Fatalf("Commit of dedup batch err = %v, want ErrUnknownBatch", err)
	}
}

func TestAppendIdempotentContentConflict(t *testing.T) {
	l := open(t, t.TempDir(), log.Options{SegmentBytes: 1 << 20})

	first := [][]byte{[]byte("a"), []byte("b")}
	b, err := l.AppendIdempotent("k", first)
	if err != nil {
		t.Fatalf("AppendIdempotent: %v", err)
	}
	variants := [][][]byte{
		{[]byte("a")},                           // fewer records
		{[]byte("a"), []byte("b"), []byte("c")}, // more records
		{[]byte("a"), []byte("different")},      // different bytes
		{[]byte("b"), []byte("a")},              // different order
	}
	for i, v := range variants {
		if _, err := l.AppendIdempotent("k", v); !errors.Is(err, log.ErrBatchIDConflict) {
			t.Fatalf("staged conflict case %d: err = %v, want ErrBatchIDConflict", i, err)
		}
	}
	// Conflicts reserve nothing and change nothing: the original still
	// commits at seq 1, and a repeat with the original content dedups.
	again, err := l.AppendIdempotent("k", first)
	if err != nil || again.Seq() != 1 {
		t.Fatalf("matching retry after conflicts = %d, %v", again.Seq(), err)
	}
	if _, err := l.Commit(b); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	// After commit, mismatching content still conflicts, matching still
	// dedups.
	if _, err := l.AppendIdempotent("k", [][]byte{[]byte("a"), []byte("B")}); !errors.Is(err, log.ErrBatchIDConflict) {
		t.Fatalf("committed conflict: err = %v, want ErrBatchIDConflict", err)
	}
	got, err := l.AppendIdempotent("k", first)
	if err != nil || got.Seq() != 1 {
		t.Fatalf("committed dedup = %d, %v", got.Seq(), err)
	}
}

func TestAppendIdempotentInvalidKeys(t *testing.T) {
	l := open(t, t.TempDir(), log.Options{SegmentBytes: 1 << 20})

	good := strings.Repeat("a", 256)
	bad := []string{
		"",
		"with\x00nul",
		"\xff\xfe", // invalid UTF-8
		strings.Repeat("z", 257),
	}
	for _, k := range bad {
		if _, err := l.AppendIdempotent(k, [][]byte{[]byte("r")}); !errors.Is(err, log.ErrInvalidBatchID) {
			t.Fatalf("key %q: err = %v, want ErrInvalidBatchID", k, err)
		}
	}
	// Exactly 256 bytes is accepted.
	b, err := l.AppendIdempotent(good, [][]byte{[]byte("r")})
	if err != nil {
		t.Fatalf("256-byte key: %v", err)
	}
	if b.Seq() != 1 {
		t.Fatalf("valid key seq = %d, want 1 (rejected keys must reserve nothing)", b.Seq())
	}
	// Valid UTF-8 multibyte content and a unicode key are fine.
	ub, err := l.AppendIdempotent("héllo-世界", [][]byte{[]byte("rec")})
	if err != nil || ub.Seq() != 2 {
		t.Fatalf("unicode key = %d, %v", ub.Seq(), err)
	}
}

func TestKeyedBatchReadAndScanIDs(t *testing.T) {
	dir := t.TempDir()
	opts := log.Options{SegmentBytes: 1 << 20, Sync: true}
	l := open(t, dir, opts)

	anon, _ := l.Append([][]byte{[]byte("anon")})
	k1, _ := l.AppendIdempotent("key-a", [][]byte{[]byte("r1"), []byte("r2")})
	k2, _ := l.AppendIdempotent("key-b", nil) // empty record set, keyed
	if _, err := l.CommitGroup([]log.Batch{anon, k1, k2}); err != nil {
		t.Fatalf("CommitGroup: %v", err)
	}

	if got, err := l.Read(1); err != nil || string(got[0]) != "anon" {
		t.Fatalf("Read(1) = %q, %v", got, err)
	}
	if got, err := l.Read(2); err != nil || !reflect.DeepEqual(got, [][]byte{[]byte("r1"), []byte("r2")}) {
		t.Fatalf("Read(2) = %q, %v", got, err)
	}
	if got, err := l.Read(3); err != nil || len(got) != 0 {
		t.Fatalf("Read(3) = %q, %v; want empty", got, err)
	}

	type seen struct {
		seq uint64
		id  string
	}
	var all []seen
	if err := l.Scan(1, func(b log.Batch) error {
		all = append(all, seen{b.Seq(), b.ID()})
		if b.Seq() == 2 && !reflect.DeepEqual(b.Records(), [][]byte{[]byte("r1"), []byte("r2")}) {
			t.Fatalf("Scan records for seq 2 = %q", b.Records())
		}
		return nil
	}); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	want := []seen{{1, ""}, {2, "key-a"}, {3, "key-b"}}
	if !reflect.DeepEqual(all, want) {
		t.Fatalf("scan = %+v, want %+v", all, want)
	}
}

func TestCommitGroupMixesAnonymousAndKeyed(t *testing.T) {
	l := open(t, t.TempDir(), log.Options{SegmentBytes: 1 << 20, Sync: true})

	a1, _ := l.Append([][]byte{[]byte("a1")})
	k1, _ := l.AppendIdempotent("g-key", [][]byte{[]byte("k1")})
	a2, _ := l.Append([][]byte{[]byte("a2")})
	seqs, err := l.CommitGroup([]log.Batch{a1, k1, a2})
	if err != nil {
		t.Fatalf("CommitGroup: %v", err)
	}
	if !reflect.DeepEqual(seqs, []uint64{1, 2, 3}) {
		t.Fatalf("group seqs = %v, want [1 2 3]", seqs)
	}
	for seq, want := range map[uint64]string{1: "a1", 2: "k1", 3: "a2"} {
		got, err := l.Read(seq)
		if err != nil || string(got[0]) != want {
			t.Fatalf("Read(%d) = %q, %v; want %q", seq, got, err, want)
		}
	}
	// The keyed member dedups after commit despite sharing the group.
	b, err := l.AppendIdempotent("g-key", [][]byte{[]byte("k1")})
	if err != nil || b.Seq() != 2 {
		t.Fatalf("group key dedup = %d, %v", b.Seq(), err)
	}
	// Anonymous members never grew a key.
	var ids []string
	l.Scan(1, func(b log.Batch) error { ids = append(ids, b.ID()); return nil })
	if !reflect.DeepEqual(ids, []string{"", "g-key", ""}) {
		t.Fatalf("scan ids = %v, want [\"\" g-key \"\"]", ids)
	}
}

func TestIdempotentReopenDedup(t *testing.T) {
	dir := t.TempDir()
	opts := log.Options{SegmentBytes: 1 << 20, Sync: true}
	recs := [][]byte{[]byte("durable"), {0, 1, 2, 255}}

	l := open(t, dir, opts)
	b, err := l.AppendIdempotent("reopen-key", recs)
	if err != nil {
		t.Fatalf("AppendIdempotent: %v", err)
	}
	if _, err := l.Commit(b); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	// Also a mixed group, to recover BCLK keys.
	g1, _ := l.Append([][]byte{[]byte("g-anon")})
	g2, _ := l.AppendIdempotent("group-key", [][]byte{[]byte("g-keyed")})
	if _, err := l.CommitGroup([]log.Batch{g1, g2}); err != nil {
		t.Fatalf("CommitGroup: %v", err)
	}
	l.Close()

	for _, removeSidecar := range []bool{false, true} {
		name := "adopt"
		if removeSidecar {
			name = "rebuild"
			if err := os.Remove(filepath.Join(dir, "index.idx")); err != nil {
				t.Fatal(err)
			}
		}
		t.Run(name, func(t *testing.T) {
			l2, err := log.Open(dir, opts)
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			defer l2.Close()
			again, err := l2.AppendIdempotent("reopen-key", recs)
			if err != nil {
				t.Fatalf("dedup after reopen: %v", err)
			}
			if again.Seq() != 1 || again.ID() != "reopen-key" {
				t.Fatalf("dedup = %d/%q, want 1/reopen-key", again.Seq(), again.ID())
			}
			if !reflect.DeepEqual(again.Records(), recs) {
				t.Fatalf("dedup records = %q, want %q", again.Records(), recs)
			}
			g, err := l2.AppendIdempotent("group-key", [][]byte{[]byte("g-keyed")})
			if err != nil || g.Seq() != 3 {
				t.Fatalf("group key dedup = %d, %v", g.Seq(), err)
			}
			if _, err := l2.AppendIdempotent("reopen-key", [][]byte{[]byte("nope")}); !errors.Is(err, log.ErrBatchIDConflict) {
				t.Fatalf("conflict after reopen: %v", err)
			}
			// Same-key retry commits nothing new; fresh keys continue at 4.
			fresh, err := l2.AppendIdempotent("brand-new", [][]byte{[]byte("z")})
			if err != nil || fresh.Seq() != 4 {
				t.Fatalf("fresh seq = %d, %v", fresh.Seq(), err)
			}
		})
	}
}

// appendKeyedCompleteEntry appends a valid, checksummed BCLI entry for
// seq with key and one record, simulating a crash after the segment
// entry is durable but before its index record lands.
func appendKeyedCompleteEntry(t *testing.T, path string, seq uint64, key, payload string) {
	t.Helper()
	var entry []byte
	entry = append(entry, 'B', 'C', 'L', 'I')
	var tmp [8]byte
	binary.LittleEndian.PutUint64(tmp[:], seq)
	entry = append(entry, tmp[:]...)
	binary.LittleEndian.PutUint16(tmp[:2], uint16(len(key)))
	entry = append(entry, tmp[:2]...)
	entry = append(entry, key...)
	binary.LittleEndian.PutUint32(tmp[:4], 1)
	entry = append(entry, tmp[:4]...)
	binary.LittleEndian.PutUint32(tmp[:4], uint32(len(payload)))
	entry = append(entry, tmp[:4]...)
	entry = append(entry, payload...)
	var crc [4]byte
	binary.LittleEndian.PutUint32(crc[:], crc32.ChecksumIEEE(entry))
	entry = append(entry, crc[:]...)
	if err := os.WriteFile(path, append(mustReadFile(t, path), entry...), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestKeyedSuffixAdoptedAndIndexed(t *testing.T) {
	// A complete BCLI entry durable in the segment while the sidecar
	// lacks it: the adoption suffix parse must recover the key, and a
	// second reopen must adopt the persisted keyed index record.
	dir := t.TempDir()
	opts := log.Options{SegmentBytes: 1 << 20, Sync: true}
	l := open(t, dir, opts)
	commit(t, l, []byte("first"))
	l.Close()

	files, _ := filepath.Glob(filepath.Join(dir, "*.seg"))
	sort.Strings(files)
	appendKeyedCompleteEntry(t, files[len(files)-1], 2, "late-key", "late")

	l = open(t, dir, opts)
	if b, err := l.AppendIdempotent("late-key", [][]byte{[]byte("late")}); err != nil || b.Seq() != 2 {
		t.Fatalf("dedup adopted suffix = %d, %v", b.Seq(), err)
	}
	if got, err := l.Read(2); err != nil || string(got[0]) != "late" {
		t.Fatalf("Read(2) = %q, %v", got, err)
	}
	l.Close()

	l = open(t, dir, opts)
	defer l.Close()
	if b, err := l.AppendIdempotent("late-key", [][]byte{[]byte("late")}); err != nil || b.Seq() != 2 {
		t.Fatalf("dedup after second reopen = %d, %v", b.Seq(), err)
	}
	var id string
	l.Scan(1, func(b log.Batch) error {
		if b.Seq() == 2 {
			id = b.ID()
		}
		return nil
	})
	if id != "late-key" {
		t.Fatalf("scan id of seq 2 = %q, want late-key", id)
	}
}

// appendTornKeyedEntry appends a half-finished BCLI entry: header, key
// and count are whole, the record is cut off, no checksum.
func appendTornKeyedEntry(t *testing.T, dir string, seq uint64, key string) {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(dir, "*.seg"))
	if err != nil || len(files) == 0 {
		t.Fatalf("segment files = %v, %v", files, err)
	}
	last := files[len(files)-1]
	var entry []byte
	entry = append(entry, 'B', 'C', 'L', 'I')
	var tmp [8]byte
	binary.LittleEndian.PutUint64(tmp[:], seq)
	entry = append(entry, tmp[:]...)
	binary.LittleEndian.PutUint16(tmp[:2], uint16(len(key)))
	entry = append(entry, tmp[:2]...)
	entry = append(entry, key...)
	binary.LittleEndian.PutUint32(tmp[:4], 1) // one record
	entry = append(entry, tmp[:4]...)
	binary.LittleEndian.PutUint32(tmp[:4], 100)
	entry = append(entry, tmp[:4]...)
	entry = append(entry, []byte("only-a-prefix")...)
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

func TestTornKeyedTailForgetsKeyAndLeavesHole(t *testing.T) {
	for _, removeSidecar := range []bool{false, true} {
		t.Run(map[bool]string{false: "adopt", true: "rebuild"}[removeSidecar], func(t *testing.T) {
			dir := t.TempDir()
			opts := log.Options{SegmentBytes: 1 << 20, Sync: true}
			l := open(t, dir, opts)
			commit(t, l, []byte("one"))
			l.Close()

			// Crash halfway through the BCLI entry for seq 2/key "torn".
			appendTornKeyedEntry(t, dir, 2, "torn")
			if removeSidecar {
				if err := os.Remove(filepath.Join(dir, "index.idx")); err != nil {
					t.Fatal(err)
				}
			}

			l = open(t, dir, opts)
			// Seq 2 is a permanent hole: staged, never committed.
			if _, err := l.Read(2); !errors.Is(err, log.ErrNotCommitted) {
				t.Fatalf("Read(2) err = %v, want ErrNotCommitted", err)
			}
			// The incomplete key is forgotten: retrying it establishes a
			// brand new batch, which must take seq 3 (past the hole).
			b, err := l.AppendIdempotent("torn", [][]byte{[]byte("retried")})
			if err != nil {
				t.Fatalf("retry forgotten key: %v", err)
			}
			if b.Seq() != 3 || b.ID() != "torn" {
				t.Fatalf("retried batch = %d/%q, want 3/torn", b.Seq(), b.ID())
			}
			if _, err := l.Commit(b); err != nil {
				t.Fatalf("Commit: %v", err)
			}
			if got, err := l.Read(3); err != nil || string(got[0]) != "retried" {
				t.Fatalf("Read(3) = %q, %v", got, err)
			}
			// Dedup now resolves at 3.
			again, err := l.AppendIdempotent("torn", [][]byte{[]byte("retried")})
			if err != nil || again.Seq() != 3 {
				t.Fatalf("dedup = %d, %v", again.Seq(), err)
			}
			l.Close()

			// Hole and the new batch both survive another reopen.
			l = open(t, dir, opts)
			defer l.Close()
			if _, err := l.Read(2); !errors.Is(err, log.ErrNotCommitted) {
				t.Fatalf("hole after reopen: %v", err)
			}
			if b, err := l.AppendIdempotent("torn", [][]byte{[]byte("retried")}); err != nil || b.Seq() != 3 {
				t.Fatalf("dedup after reopen = %d, %v", b.Seq(), err)
			}
		})
	}
}

// appendTornKeyedGroup writes a half-finished BCLK entry: an anonymous
// member and a keyed member are complete, the final keyed member is cut
// off mid-record and the checksum is missing.
func appendTornKeyedGroup(t *testing.T, dir string, finalSeq uint64, finalKey string) {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(dir, "*.seg"))
	if err != nil || len(files) == 0 {
		t.Fatalf("segment files = %v, %v", files, err)
	}
	last := files[len(files)-1]
	var entry []byte
	entry = append(entry, 'B', 'C', 'L', 'K')
	var tmp [8]byte
	binary.LittleEndian.PutUint32(tmp[:4], 3) // three members
	entry = append(entry, tmp[:4]...)
	// Complete anonymous member with seq 2.
	binary.LittleEndian.PutUint64(tmp[:], 2)
	entry = append(entry, tmp[:]...)
	binary.LittleEndian.PutUint16(tmp[:2], 0)
	entry = append(entry, tmp[:2]...)
	binary.LittleEndian.PutUint32(tmp[:4], 1)
	entry = append(entry, tmp[:4]...)
	binary.LittleEndian.PutUint32(tmp[:4], 2)
	entry = append(entry, tmp[:4]...)
	entry = append(entry, 'x', 'y')
	// Complete keyed member with seq 3 and key "gk-1".
	binary.LittleEndian.PutUint64(tmp[:], 3)
	entry = append(entry, tmp[:]...)
	binary.LittleEndian.PutUint16(tmp[:2], uint16(len("gk-1")))
	entry = append(entry, tmp[:2]...)
	entry = append(entry, "gk-1"...)
	binary.LittleEndian.PutUint32(tmp[:4], 1)
	entry = append(entry, tmp[:4]...)
	binary.LittleEndian.PutUint32(tmp[:4], 2)
	entry = append(entry, tmp[:4]...)
	entry = append(entry, 'o', 'k')
	// Final keyed member: header complete, record declared 100 bytes but
	// only a prefix present, no checksum.
	binary.LittleEndian.PutUint64(tmp[:], finalSeq)
	entry = append(entry, tmp[:]...)
	binary.LittleEndian.PutUint16(tmp[:2], uint16(len(finalKey)))
	entry = append(entry, tmp[:2]...)
	entry = append(entry, finalKey...)
	binary.LittleEndian.PutUint32(tmp[:4], 1)
	entry = append(entry, tmp[:4]...)
	binary.LittleEndian.PutUint32(tmp[:4], 100)
	entry = append(entry, tmp[:4]...)
	entry = append(entry, []byte("only-a-prefix")...)
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

func TestTornKeyedGroupForgetsAllKeys(t *testing.T) {
	dir := t.TempDir()
	opts := log.Options{SegmentBytes: 1 << 20, Sync: true}
	l := open(t, dir, opts)
	commit(t, l, []byte("one"))
	l.Close()

	// Crash mid-BCLK reserving seqs 2,3,4 with keys gk-1 and gk-2.
	appendTornKeyedGroup(t, dir, 4, "gk-2")

	l = open(t, dir, opts)
	for seq := uint64(2); seq <= 4; seq++ {
		if _, err := l.Read(seq); !errors.Is(err, log.ErrNotCommitted) {
			t.Fatalf("Read(%d) err = %v, want ErrNotCommitted", seq, err)
		}
	}
	// Keys from the torn, never-durable group are forgotten: retrying
	// either establishes a fresh batch past the holes.
	b, err := l.AppendIdempotent("gk-1", [][]byte{[]byte("a")})
	if err != nil {
		t.Fatalf("retry gk-1: %v", err)
	}
	if b.Seq() != 5 {
		t.Fatalf("gk-1 retry seq = %d, want 5", b.Seq())
	}
	b2, err := l.AppendIdempotent("gk-2", [][]byte{[]byte("b")})
	if err != nil {
		t.Fatalf("retry gk-2: %v", err)
	}
	if b2.Seq() != 6 {
		t.Fatalf("gk-2 retry seq = %d, want 6", b2.Seq())
	}
}

func TestKeyedBatchesAcrossSegments(t *testing.T) {
	dir := t.TempDir()
	l := open(t, dir, log.Options{SegmentBytes: 48})

	var keys []string
	for i := 0; i < 4; i++ {
		key := "key-" + string(rune('a'+i))
		b, err := l.AppendIdempotent(key, [][]byte{[]byte("payload-" + string(rune('a'+i)))})
		if err != nil {
			t.Fatalf("AppendIdempotent: %v", err)
		}
		if _, err := l.Commit(b); err != nil {
			t.Fatalf("Commit: %v", err)
		}
		keys = append(keys, key)
	}
	l.Close()

	l = open(t, dir, log.Options{SegmentBytes: 48})
	defer l.Close()
	for i, key := range keys {
		want := []byte("payload-" + string(rune('a'+i)))
		b, err := l.AppendIdempotent(key, [][]byte{want})
		if err != nil {
			t.Fatalf("dedup %q: %v", key, err)
		}
		if b.Seq() != uint64(i+1) {
			t.Fatalf("dedup %q seq = %d, want %d", key, b.Seq(), i+1)
		}
	}
}

// seedKeyedLog writes single keyed, anonymous and mixed-group keyed
// batches, returning a dir whose every key must survive a rebuild.
func seedKeyedLog(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	l := open(t, dir, log.Options{SegmentBytes: 1 << 20, Sync: true})
	b1, err := l.AppendIdempotent("k-single", [][]byte{[]byte("solo")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := l.Commit(b1); err != nil {
		t.Fatal(err)
	}
	a, _ := l.Append([][]byte{[]byte("anon")})
	g, err := l.AppendIdempotent("k-group", [][]byte{[]byte("grp")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := l.CommitGroup([]log.Batch{a, g}); err != nil {
		t.Fatal(err)
	}
	l.Close()
	return dir
}

func assertKeyedState(t *testing.T, l *log.Log) {
	t.Helper()
	want := map[uint64]struct {
		id  string
		rec []byte
	}{
		1: {"k-single", []byte("solo")},
		2: {"", []byte("anon")},
		3: {"k-group", []byte("grp")},
	}
	for seq, w := range want {
		got, err := l.Read(seq)
		if err != nil || string(got[0]) != string(w.rec) {
			t.Fatalf("Read(%d) = %q, %v; want %q", seq, got, err, w.rec)
		}
	}
	var ids []string
	if err := l.Scan(1, func(b log.Batch) error { ids = append(ids, b.ID()); return nil }); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if !reflect.DeepEqual(ids, []string{"k-single", "", "k-group"}) {
		t.Fatalf("scan ids = %v", ids)
	}
	for key, seq := range map[string]uint64{"k-single": 1, "k-group": 3} {
		b, err := l.AppendIdempotent(key, [][]byte{wantRec(seq)})
		if err != nil {
			t.Fatalf("dedup %q after rebuild: %v", key, err)
		}
		if b.Seq() != seq {
			t.Fatalf("dedup %q seq = %d, want %d", key, b.Seq(), seq)
		}
	}
}

func wantRec(seq uint64) []byte {
	if seq == 1 {
		return []byte("solo")
	}
	return []byte("grp")
}

func TestKeyedSidecarDefectsRebuildKeys(t *testing.T) {
	mutate := func(t *testing.T, fn func([]byte) []byte) {
		t.Helper()
		dir := seedKeyedLog(t)
		path := indexPath(dir)
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		out := fn(data)
		if out == nil {
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
		} else if err := os.WriteFile(path, out, 0o644); err != nil {
			t.Fatal(err)
		}
		l, err := log.Open(dir, log.Options{SegmentBytes: 1 << 20, Sync: true})
		if err != nil {
			t.Fatalf("Open with defective sidecar: %v", err)
		}
		defer l.Close()
		assertKeyedState(t, l)
	}

	t.Run("missing", func(t *testing.T) { mutate(t, func([]byte) []byte { return nil }) })
	t.Run("garbage", func(t *testing.T) { mutate(t, func([]byte) []byte { return []byte("not an index at all") }) })
	t.Run("wrong version", func(t *testing.T) {
		mutate(t, func(d []byte) []byte { d[6] ^= 0xff; d[7] ^= 0xff; return d })
	})
	t.Run("bad checksum", func(t *testing.T) {
		mutate(t, func(d []byte) []byte { d[len(d)-1] ^= 0xff; return d })
	})
	t.Run("truncated", func(t *testing.T) {
		mutate(t, func(d []byte) []byte { return d[:len(d)-3] })
	})
}

// TestKeyedSidecarContradictionRebuildsKeys verifies the "index ahead
// of segments" contradiction: truncating the last segment makes adopt
// reject the sidecar and rebuild from reality, without losing earlier
// keyed batches' keys.
func TestKeyedSidecarContradictionRebuildsKeys(t *testing.T) {
	dir := seedKeyedLog(t)
	small := log.Options{SegmentBytes: 32, Sync: true}
	// seedKeyedLog used 1MiB segments; reopen with tiny capacity so a new
	// keyed commit rolls to its own segment, then drop that segment.
	l := open(t, dir, small)
	b, err := l.AppendIdempotent("k-late", [][]byte{[]byte("late")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := l.Commit(b); err != nil {
		t.Fatal(err)
	}
	l.Close()

	files, _ := filepath.Glob(filepath.Join(dir, "*.seg"))
	sort.Strings(files)
	if err := os.Truncate(files[len(files)-1], 0); err != nil {
		t.Fatal(err)
	}
	l2, err := log.Open(dir, small)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer l2.Close()
	// The indexed-but-missing late batch is gone; earlier keys survive.
	if _, err := l2.Read(4); !errors.Is(err, log.ErrUnknownBatch) {
		t.Fatalf("Read(4) err = %v, want ErrUnknownBatch", err)
	}
	for key, seq := range map[string]uint64{"k-single": 1, "k-group": 3} {
		bb, err := l2.AppendIdempotent(key, [][]byte{wantRec(seq)})
		if err != nil {
			t.Fatalf("dedup %q: %v", key, err)
		}
		if bb.Seq() != seq {
			t.Fatalf("dedup %q = %d, want %d", key, bb.Seq(), seq)
		}
	}
}
