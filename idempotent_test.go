package log_test

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	log "github.com/Hulalalalalalalalalalala/batch-commit-log"
)

// appendKeyed is the idempotent counterpart of commit: stage with key
// and commit immediately.
func appendKeyed(t *testing.T, l *log.Log, key string, records ...[]byte) uint64 {
	t.Helper()
	b, err := l.AppendIdempotent(key, records)
	if err != nil {
		t.Fatalf("AppendIdempotent(%q): %v", key, err)
	}
	seq, err := l.Commit(b)
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	return seq
}

func TestAppendIdempotentStagesOnceAndReturnsSameBatch(t *testing.T) {
	l := open(t, t.TempDir(), log.Options{SegmentBytes: 1 << 20})

	recs := [][]byte{[]byte("a"), []byte("b")}
	b1, err := l.AppendIdempotent("k1", recs)
	if err != nil {
		t.Fatalf("AppendIdempotent: %v", err)
	}
	if b1.Seq() != 1 || b1.ID() != "k1" {
		t.Fatalf("first batch = seq %d id %q, want 1/k1", b1.Seq(), b1.ID())
	}
	// Retry while staged: identical records return the same staged
	// batch and reserve no new sequence.
	b2, err := l.AppendIdempotent("k1", [][]byte{[]byte("a"), []byte("b")})
	if err != nil {
		t.Fatalf("retry AppendIdempotent: %v", err)
	}
	if b2.Seq() != 1 || b2.ID() != "k1" {
		t.Fatalf("retry batch = seq %d id %q, want 1/k1", b2.Seq(), b2.ID())
	}
	if !reflect.DeepEqual(b2.Records(), recs) {
		t.Fatalf("retry records = %q, want %q", b2.Records(), recs)
	}
	// Another, distinct key reserves the next sequence.
	b3, err := l.AppendIdempotent("k2", [][]byte{[]byte("c")})
	if err != nil {
		t.Fatalf("AppendIdempotent k2: %v", err)
	}
	if b3.Seq() != 2 {
		t.Fatalf("k2 seq = %d, want 2", b3.Seq())
	}
	// Committing either handle for k1 publishes it once.
	seq, err := l.Commit(b2)
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if seq != 1 {
		t.Fatalf("commit seq = %d, want 1", seq)
	}
}

func TestAppendIdempotentCommittedDedup(t *testing.T) {
	l := open(t, t.TempDir(), log.Options{SegmentBytes: 1 << 20})

	recs := [][]byte{[]byte("payload"), {}}
	b, err := l.AppendIdempotent("order-7", recs)
	if err != nil {
		t.Fatalf("AppendIdempotent: %v", err)
	}
	if _, err := l.Commit(b); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	// A later anonymous batch sits at seq 2, proving the dedup call
	// reserves nothing.
	anon := commit(t, l, []byte("later"))
	if anon != 2 {
		t.Fatalf("anonymous seq = %d, want 2", anon)
	}

	again, err := l.AppendIdempotent("order-7", recs)
	if err != nil {
		t.Fatalf("retry after commit: %v", err)
	}
	if again.Seq() != 1 || again.ID() != "order-7" {
		t.Fatalf("dedup batch = seq %d id %q, want 1/order-7", again.Seq(), again.ID())
	}
	if !reflect.DeepEqual(again.Records(), recs) {
		t.Fatalf("dedup records = %q, want %q", again.Records(), recs)
	}
	// The returned batch is already committed: committing it again is
	// ErrUnknownBatch, and nothing was written.
	if _, err := l.Commit(again); !errors.Is(err, log.ErrUnknownBatch) {
		t.Fatalf("recommit err = %v, want ErrUnknownBatch", err)
	}
	got, err := l.Read(1)
	if err != nil || !reflect.DeepEqual(got, recs) {
		t.Fatalf("Read(1) = %q, %v", got, err)
	}
}

func TestAppendIdempotentConflict(t *testing.T) {
	l := open(t, t.TempDir(), log.Options{SegmentBytes: 1 << 20})

	// Conflict with a still-staged batch: record count differs.
	b, err := l.AppendIdempotent("k", [][]byte{[]byte("one"), []byte("two")})
	if err != nil {
		t.Fatalf("AppendIdempotent: %v", err)
	}
	if _, err := l.AppendIdempotent("k", [][]byte{[]byte("one")}); !errors.Is(err, log.ErrBatchIDConflict) {
		t.Fatalf("count mismatch err = %v, want ErrBatchIDConflict", err)
	}
	// Same count, a byte differs.
	if _, err := l.AppendIdempotent("k", [][]byte{[]byte("one"), []byte("TWO")}); !errors.Is(err, log.ErrBatchIDConflict) {
		t.Fatalf("byte mismatch err = %v, want ErrBatchIDConflict", err)
	}
	// Order matters.
	if _, err := l.AppendIdempotent("k", [][]byte{[]byte("two"), []byte("one")}); !errors.Is(err, log.ErrBatchIDConflict) {
		t.Fatalf("order mismatch err = %v, want ErrBatchIDConflict", err)
	}
	// The rejected calls must not have moved the reservation: the
	// original staged batch still owns seq 1 and commits fine.
	if b.Seq() != 1 {
		t.Fatalf("seq = %d, want 1", b.Seq())
	}
	if _, err := l.Commit(b); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	// Conflict with a committed batch.
	if _, err := l.AppendIdempotent("k", [][]byte{[]byte("different")}); !errors.Is(err, log.ErrBatchIDConflict) {
		t.Fatalf("post-commit conflict err = %v, want ErrBatchIDConflict", err)
	}
	// And the identical request still dedups after the conflicts.
	again, err := l.AppendIdempotent("k", [][]byte{[]byte("one"), []byte("two")})
	if err != nil || again.Seq() != 1 {
		t.Fatalf("dedup after conflict = seq %d, %v; want seq 1", again.Seq(), err)
	}
}

func TestAppendIdempotentInvalidKeys(t *testing.T) {
	l := open(t, t.TempDir(), log.Options{SegmentBytes: 1 << 20})

	valid := []string{
		"x",
		"a b/c:d",
		strings.Repeat("k", 256),
		"héllo-世界",
		"tab\tnl\n",
	}
	for i, key := range valid {
		if _, err := l.AppendIdempotent(key, [][]byte{[]byte("r")}); err != nil {
			t.Fatalf("valid key #%d %q rejected: %v", i, key, err)
		}
	}

	invalid := []string{
		"",
		"nul\x00",
		"a\x00b",
		"\x00",
		strings.Repeat("k", 257),
		string([]byte{0xff}),      // not valid UTF-8
		string([]byte{'o', 0xff}), // invalid UTF-8 in the middle
	}
	for i, key := range invalid {
		if _, err := l.AppendIdempotent(key, [][]byte{[]byte("r")}); !errors.Is(err, log.ErrInvalidBatchID) {
			t.Fatalf("invalid key #%d (%q) err = %v, want ErrInvalidBatchID", i, key, err)
		}
	}
}

func TestReadAndScanExposeKeyedID(t *testing.T) {
	dir := t.TempDir()
	opts := log.Options{SegmentBytes: 1 << 20, Sync: true}
	l := open(t, dir, opts)

	anon, _ := l.Append([][]byte{[]byte("anon")})
	keyed, err := l.AppendIdempotent("keyed", [][]byte{[]byte("k1"), []byte("k2")})
	if err != nil {
		t.Fatalf("AppendIdempotent: %v", err)
	}
	if _, err := l.CommitGroup([]log.Batch{anon, keyed}); err != nil {
		t.Fatalf("CommitGroup: %v", err)
	}
	if anon.ID() != "" {
		t.Fatalf("anonymous ID = %q, want empty", anon.ID())
	}
	if keyed.ID() != "keyed" {
		t.Fatalf("keyed ID = %q, want keyed", keyed.ID())
	}

	type seen struct {
		seq uint64
		id  string
	}
	var got []seen
	if err := l.Scan(1, func(b log.Batch) error {
		got = append(got, seen{b.Seq(), b.ID()})
		return nil
	}); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	want := []seen{{1, ""}, {2, "keyed"}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("scan = %+v, want %+v", got, want)
	}

	got2, err := l.Read(keyed.Seq())
	if err != nil || !reflect.DeepEqual(got2, [][]byte{[]byte("k1"), []byte("k2")}) {
		t.Fatalf("Read keyed = %q, %v", got2, err)
	}

	// Only segments and the one sidecar live in the directory: no extra
	// on-disk files were introduced for keys.
	l.Close()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".seg") && name != "index.idx" {
			t.Fatalf("unexpected file in log dir: %q", name)
		}
	}
}

func TestMixedGroupCommitAndDedup(t *testing.T) {
	l := open(t, t.TempDir(), log.Options{SegmentBytes: 1 << 20, Sync: true})

	a, _ := l.Append([][]byte{[]byte("a")})
	k1, err := l.AppendIdempotent("g-key-1", [][]byte{[]byte("g1")})
	if err != nil {
		t.Fatalf("AppendIdempotent: %v", err)
	}
	b, _ := l.Append([][]byte{[]byte("b")})
	k2, err := l.AppendIdempotent("g-key-2", [][]byte{[]byte("g2a"), []byte("g2b")})
	if err != nil {
		t.Fatalf("AppendIdempotent: %v", err)
	}

	seqs, err := l.CommitGroup([]log.Batch{a, k1, b, k2})
	if err != nil {
		t.Fatalf("CommitGroup: %v", err)
	}
	if !reflect.DeepEqual(seqs, []uint64{1, 2, 3, 4}) {
		t.Fatalf("group seqs = %v, want [1 2 3 4]", seqs)
	}
	// Every member reads back with its records and ID.
	for _, c := range []struct {
		b    log.Batch
		want [][]byte
	}{
		{a, [][]byte{[]byte("a")}},
		{k1, [][]byte{[]byte("g1")}},
		{b, [][]byte{[]byte("b")}},
		{k2, [][]byte{[]byte("g2a"), []byte("g2b")}},
	} {
		got, err := l.Read(c.b.Seq())
		if err != nil || !reflect.DeepEqual(got, c.want) {
			t.Fatalf("Read(%d) = %q, %v; want %q", c.b.Seq(), got, err, c.want)
		}
	}
	// Recommitting any group member is ErrUnknownBatch.
	if _, err := l.Commit(k1); !errors.Is(err, log.ErrUnknownBatch) {
		t.Fatalf("recommit keyed member err = %v, want ErrUnknownBatch", err)
	}
	// The keys dedup with the committed content.
	d1, err := l.AppendIdempotent("g-key-1", [][]byte{[]byte("g1")})
	if err != nil || d1.Seq() != 2 {
		t.Fatalf("dedup g-key-1 = seq %d, %v; want 2", d1.Seq(), err)
	}
	if _, err := l.AppendIdempotent("g-key-2", [][]byte{[]byte("other")}); !errors.Is(err, log.ErrBatchIDConflict) {
		t.Fatalf("g-key-2 conflict err = %v, want ErrBatchIDConflict", err)
	}
}

func TestKeyedDedupSurvivesReopenAndRebuild(t *testing.T) {
	for _, mode := range []string{"adopt", "rebuild-missing", "rebuild-truncated"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			opts := log.Options{SegmentBytes: 1 << 20, Sync: true}
			l := open(t, dir, opts)
			seq := appendKeyed(t, l, "durable-key", []byte("one"), []byte("two"))
			if seq != 1 {
				t.Fatalf("seq = %d, want 1", seq)
			}
			// Also commit a mixed group so its keys rebuild too.
			a, _ := l.Append([][]byte{[]byte("a")})
			k, _ := l.AppendIdempotent("group-key", [][]byte{[]byte("g")})
			if _, err := l.CommitGroup([]log.Batch{a, k}); err != nil {
				t.Fatalf("CommitGroup: %v", err)
			}
			l.Close()

			switch mode {
			case "adopt":
			case "rebuild-missing":
				if err := os.Remove(indexPath(dir)); err != nil {
					t.Fatal(err)
				}
			case "rebuild-truncated":
				p := indexPath(dir)
				data, err := os.ReadFile(p)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.Truncate(p, int64(len(data)-3)); err != nil {
					t.Fatal(err)
				}
			}

			l2 := open(t, dir, opts)
			for _, c := range []struct {
				key  string
				seq  uint64
				recs [][]byte
			}{
				{"durable-key", 1, [][]byte{[]byte("one"), []byte("two")}},
				{"group-key", 3, [][]byte{[]byte("g")}},
			} {
				b, err := l2.AppendIdempotent(c.key, c.recs)
				if err != nil {
					t.Fatalf("dedup %q: %v", c.key, err)
				}
				if b.Seq() != c.seq || b.ID() != c.key {
					t.Fatalf("dedup %q = %d/%q, want %d", c.key, b.Seq(), b.ID(), c.seq)
				}
				if _, err := l2.AppendIdempotent(c.key, [][]byte{[]byte("different")}); !errors.Is(err, log.ErrBatchIDConflict) {
					t.Fatalf("conflict %q: want ErrBatchIDConflict", c.key)
				}
			}
			// Scan exposes the same IDs.
			var ids []string
			if err := l2.Scan(1, func(b log.Batch) error { ids = append(ids, b.ID()); return nil }); err != nil {
				t.Fatalf("Scan: %v", err)
			}
			if !reflect.DeepEqual(ids, []string{"durable-key", "", "group-key"}) {
				t.Fatalf("scan ids = %v", ids)
			}
			// New keys keep allocating fresh sequences after the max.
			n := appendKeyed(t, l2, "brand-new", []byte("x"))
			if n != 4 {
				t.Fatalf("new key seq = %d, want 4", n)
			}
		})
	}
}

// tornKeyedEntry appends a half-finished BCLK entry for seq carrying
// key, simulating a crash mid-write of a keyed single batch.
func tornKeyedEntry(t *testing.T, dir string, seq uint64, key string) {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(dir, "*.seg"))
	if err != nil || len(files) == 0 {
		t.Fatalf("segment files = %v, %v", files, err)
	}
	sort.Strings(files)
	var entry []byte
	entry = append(entry, 'B', 'C', 'L', 'K')
	var tmp [8]byte
	binary.LittleEndian.PutUint64(tmp[:], seq)
	entry = append(entry, tmp[:]...)
	binary.LittleEndian.PutUint32(tmp[:4], uint32(len(key)))
	entry = append(entry, tmp[:4]...)
	entry = append(entry, key...)
	binary.LittleEndian.PutUint32(tmp[:4], 1) // one record
	entry = append(entry, tmp[:4]...)
	binary.LittleEndian.PutUint32(tmp[:4], 100)
	entry = append(entry, tmp[:4]...)
	entry = append(entry, []byte("only-a-prefix")...)
	f, err := os.OpenFile(files[len(files)-1], os.O_WRONLY|os.O_APPEND, 0)
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

func TestTornKeyedTailForgetsKey(t *testing.T) {
	for _, removeSidecar := range []bool{false, true} {
		t.Run(map[bool]string{false: "adopt", true: "rebuild"}[removeSidecar], func(t *testing.T) {
			dir := t.TempDir()
			opts := log.Options{SegmentBytes: 1 << 20, Sync: true}
			l := open(t, dir, opts)
			appendKeyed(t, l, "committed", []byte("ok"))
			l.Close()

			// Crash halfway through a keyed batch for seq 2.
			tornKeyedEntry(t, dir, 2, "lost-key")
			if removeSidecar {
				if err := os.Remove(indexPath(dir)); err != nil {
					t.Fatal(err)
				}
			}

			l2 := open(t, dir, opts)
			// The torn sequence is a permanent hole...
			if _, err := l2.Read(2); !errors.Is(err, log.ErrNotCommitted) {
				t.Fatalf("Read(2) err = %v, want ErrNotCommitted", err)
			}
			// ...and the incomplete key was forgotten: retrying it
			// establishes a fresh batch with a fresh sequence.
			b, err := l2.AppendIdempotent("lost-key", [][]byte{[]byte("retried")})
			if err != nil {
				t.Fatalf("retry forgotten key: %v", err)
			}
			if b.Seq() != 3 {
				t.Fatalf("retry seq = %d, want 3 (hole 2 kept)", b.Seq())
			}
			if _, err := l2.Commit(b); err != nil {
				t.Fatalf("Commit: %v", err)
			}
			got, err := l2.Read(3)
			if err != nil || string(got[0]) != "retried" {
				t.Fatalf("Read(3) = %q, %v", got, err)
			}
			// The already-complete key survives and still dedups.
			d, err := l2.AppendIdempotent("committed", [][]byte{[]byte("ok")})
			if err != nil || d.Seq() != 1 {
				t.Fatalf("complete key dedup = %d, %v; want 1", d.Seq(), err)
			}
			l2.Close()

			// Hole and the new batch persist across another reopen.
			l3 := open(t, dir, opts)
			if _, err := l3.Read(2); !errors.Is(err, log.ErrNotCommitted) {
				t.Fatalf("hole after reopen: %v", err)
			}
			d2, err := l3.AppendIdempotent("lost-key", [][]byte{[]byte("retried")})
			if err != nil || d2.Seq() != 3 {
				t.Fatalf("lost-key after reopen = %d, %v; want 3", d2.Seq(), err)
			}
			var seqs []uint64
			if err := l3.Scan(1, func(b log.Batch) error { seqs = append(seqs, b.Seq()); return nil }); err != nil {
				t.Fatalf("Scan: %v", err)
			}
			if !reflect.DeepEqual(seqs, []uint64{1, 3}) {
				t.Fatalf("scan = %v, want [1 3]", seqs)
			}
		})
	}
}

func TestTornMixedGroupTailForgetsKey(t *testing.T) {
	dir := t.TempDir()
	opts := log.Options{SegmentBytes: 1 << 20, Sync: true}
	l := open(t, dir, opts)
	commit(t, l, []byte("one")) // seq 1 committed
	l.Close()

	// A torn BCLM reserving seq 2 (anonymous, complete) and seq 3
	// (keyed "gk", cut off mid-record).
	files, _ := filepath.Glob(filepath.Join(dir, "*.seg"))
	sort.Strings(files)
	var e []byte
	e = append(e, 'B', 'C', 'L', 'M')
	var tmp [8]byte
	binary.LittleEndian.PutUint32(tmp[:4], 2)
	e = append(e, tmp[:4]...)
	// member 1: anonymous flag 0, seq 2, one record "a"
	e = append(e, 0)
	binary.LittleEndian.PutUint64(tmp[:], 2)
	e = append(e, tmp[:]...)
	binary.LittleEndian.PutUint32(tmp[:4], 1)
	e = append(e, tmp[:4]...)
	binary.LittleEndian.PutUint32(tmp[:4], 1)
	e = append(e, tmp[:4]...)
	e = append(e, 'a')
	// member 2: keyed flag 1, key "gk", seq 3, one record torn
	e = append(e, 1)
	binary.LittleEndian.PutUint32(tmp[:4], 2)
	e = append(e, tmp[:4]...)
	e = append(e, 'g', 'k')
	binary.LittleEndian.PutUint64(tmp[:], 3)
	e = append(e, tmp[:]...)
	binary.LittleEndian.PutUint32(tmp[:4], 1)
	e = append(e, tmp[:4]...)
	binary.LittleEndian.PutUint32(tmp[:4], 100)
	e = append(e, tmp[:4]...)
	e = append(e, []byte("only-a-prefix")...)
	f, err := os.OpenFile(files[len(files)-1], os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(e); err != nil {
		t.Fatal(err)
	}
	f.Close()

	l2 := open(t, dir, opts)
	for seq := uint64(2); seq <= 3; seq++ {
		if _, err := l2.Read(seq); !errors.Is(err, log.ErrNotCommitted) {
			t.Fatalf("Read(%d) err = %v, want ErrNotCommitted", seq, err)
		}
	}
	// The incomplete group's key is forgotten: a retry makes a new batch.
	b, err := l2.AppendIdempotent("gk", [][]byte{[]byte("fresh")})
	if err != nil {
		t.Fatalf("retry forgotten group key: %v", err)
	}
	if b.Seq() != 4 {
		t.Fatalf("retry seq = %d, want 4 (holes 2,3 kept)", b.Seq())
	}
	if _, err := l2.Commit(b); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	var seqs []uint64
	if err := l2.Scan(1, func(b log.Batch) error { seqs = append(seqs, b.Seq()); return nil }); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if !reflect.DeepEqual(seqs, []uint64{1, 4}) {
		t.Fatalf("scan = %v, want [1 4]", seqs)
	}
}

func TestKeyedAndAnonymousInterleaveSequences(t *testing.T) {
	l := open(t, t.TempDir(), log.Options{SegmentBytes: 1 << 20})

	if s := commit(t, l, []byte("a")); s != 1 {
		t.Fatalf("anon seq = %d, want 1", s)
	}
	if s := appendKeyed(t, l, "k", []byte("b")); s != 2 {
		t.Fatalf("keyed seq = %d, want 2", s)
	}
	if s := commit(t, l, []byte("c")); s != 3 {
		t.Fatalf("anon seq = %d, want 3", s)
	}
	// Same key again reserves nothing.
	d, err := l.AppendIdempotent("k", [][]byte{[]byte("b")})
	if err != nil || d.Seq() != 2 {
		t.Fatalf("dedup = %d, %v; want 2", d.Seq(), err)
	}
	if s := appendKeyed(t, l, "k2", []byte("d")); s != 4 {
		t.Fatalf("keyed seq = %d, want 4", s)
	}
}

func TestKeyedEmptyBatch(t *testing.T) {
	dir := t.TempDir()
	opts := log.Options{SegmentBytes: 1 << 20, Sync: true}
	l := open(t, dir, opts)

	b, err := l.AppendIdempotent("empty", nil)
	if err != nil {
		t.Fatalf("AppendIdempotent: %v", err)
	}
	if _, err := l.Commit(b); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	got, err := l.Read(b.Seq())
	if err != nil || len(got) != 0 {
		t.Fatalf("Read keyed empty = %d records, %v; want 0", len(got), err)
	}
	l.Close()

	// Empty content dedups across a reopen, and a request carrying one
	// record conflicts.
	l2 := open(t, dir, opts)
	d, err := l2.AppendIdempotent("empty", nil)
	if err != nil || d.Seq() != 1 {
		t.Fatalf("empty dedup = seq %d, %v; want 1", d.Seq(), err)
	}
	if _, err := l2.AppendIdempotent("empty", [][]byte{{}}); !errors.Is(err, log.ErrBatchIDConflict) {
		t.Fatalf("empty vs one-record err = %v, want ErrBatchIDConflict", err)
	}
}

func TestKeyLengthBoundaryRoundTrip(t *testing.T) {
	dir := t.TempDir()
	opts := log.Options{SegmentBytes: 1 << 20, Sync: true}
	l := open(t, dir, opts)
	key := strings.Repeat("界", 64) // 192 bytes... use an ASCII 256
	key = strings.Repeat("z", 256)
	if s := appendKeyed(t, l, key, []byte("edge")); s != 1 {
		t.Fatalf("256-byte key seq = %d, want 1", s)
	}
	l.Close()

	l2 := open(t, dir, opts)
	d, err := l2.AppendIdempotent(key, [][]byte{[]byte("edge")})
	if err != nil || d.Seq() != 1 || d.ID() != key {
		t.Fatalf("boundary dedup = %d/%q, %v", d.Seq(), d.ID(), err)
	}
	var id string
	if err := l2.Scan(1, func(b log.Batch) error { id = b.ID(); return nil }); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if id != key {
		t.Fatalf("scan id len = %d, want 256", len(id))
	}
}

func TestV1SidecarRebuildsAndRecoversKeys(t *testing.T) {
	dir := t.TempDir()
	opts := log.Options{SegmentBytes: 1 << 20, Sync: true}
	l := open(t, dir, opts)
	appendKeyed(t, l, "keyed-1", []byte("one"))
	commit(t, l, []byte("two"))
	l.Close()

	// Downgrade the sidecar's version field to 1, as an older release
	// would have left it. Adoption must reject the foreign version and
	// rebuild; keys are recovered from the segments themselves.
	p := indexPath(dir)
	data, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	data[6], data[7] = 1, 0
	if err := os.WriteFile(p, data, 0o644); err != nil {
		t.Fatal(err)
	}

	l2 := open(t, dir, opts)
	d, err := l2.AppendIdempotent("keyed-1", [][]byte{[]byte("one")})
	if err != nil || d.Seq() != 1 {
		t.Fatalf("dedup after v1-sidecar rebuild = seq %d, %v; want 1", d.Seq(), err)
	}
	if got, err := l2.Read(2); err != nil || string(got[0]) != "two" {
		t.Fatalf("Read(2) = %q, %v", got, err)
	}
}

func TestTamperedKeyedSegmentIsCorrupt(t *testing.T) {
	dir := t.TempDir()
	opts := log.Options{SegmentBytes: 1 << 20}
	l := open(t, dir, opts)
	appendKeyed(t, l, "k", []byte("payload"))
	l.Close()

	files, _ := filepath.Glob(filepath.Join(dir, "*.seg"))
	data, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatal(err)
	}
	// Flip a byte inside the key (offset 16). The CRC must fail on both
	// open paths.
	data[16] ^= 0xff
	if err := os.WriteFile(files[0], data, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := log.Open(dir, opts); !errors.Is(err, log.ErrCorruptSegment) {
		t.Fatalf("Open err = %v, want ErrCorruptSegment", err)
	}
	if err := os.Remove(indexPath(dir)); err != nil {
		t.Fatal(err)
	}
	if _, err := log.Open(dir, opts); !errors.Is(err, log.ErrCorruptSegment) {
		t.Fatalf("rebuild Open err = %v, want ErrCorruptSegment", err)
	}
}

func TestSidecarKeyTamperRebuildsToSegmentTruth(t *testing.T) {
	dir := t.TempDir()
	opts := log.Options{SegmentBytes: 1 << 20, Sync: true}
	l := open(t, dir, opts)
	appendKeyed(t, l, "real-key", []byte("payload"))
	l.Close()

	// Flip a byte of the key inside the sidecar body. Its record CRC
	// fails, adoption gives up, and the rebuild from the segment restores
	// the real key.
	p := indexPath(dir)
	data, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	// Locate "real-key" in the sidecar and corrupt its first byte.
	idx := strings.Index(string(data), "real-key")
	if idx < 0 {
		t.Fatalf("key not present in sidecar")
	}
	data[idx] ^= 0xff
	if err := os.WriteFile(p, data, 0o644); err != nil {
		t.Fatal(err)
	}

	l2 := open(t, dir, opts)
	d, err := l2.AppendIdempotent("real-key", [][]byte{[]byte("payload")})
	if err != nil || d.Seq() != 1 {
		t.Fatalf("dedup after sidecar tamper = seq %d, %v; want 1", d.Seq(), err)
	}
}

func TestKeyedBatchesRollSegmentsLikeAnonymous(t *testing.T) {
	dir := t.TempDir()
	l := open(t, dir, log.Options{SegmentBytes: 48})
	for i := 0; i < 3; i++ {
		appendKeyed(t, l, fmt.Sprintf("key-%d", i), []byte(fmt.Sprintf("rec-%d", i)))
	}
	if segs := l.Segments(); len(segs) != 3 {
		t.Fatalf("segments = %d, want 3: %+v", len(segs), segs)
	}
	l.Close()

	l2 := open(t, dir, log.Options{SegmentBytes: 48})
	for i := 0; i < 3; i++ {
		d, err := l2.AppendIdempotent(fmt.Sprintf("key-%d", i), [][]byte{[]byte(fmt.Sprintf("rec-%d", i))})
		if err != nil || d.Seq() != uint64(i+1) {
			t.Fatalf("dedup key-%d = seq %d, %v; want %d", i, d.Seq(), err, i+1)
		}
	}
}
