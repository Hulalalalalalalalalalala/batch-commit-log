package log

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

func openInternal(t *testing.T, dir string, opts Options) *Log {
	t.Helper()
	l, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { l.Close() })
	return l
}

func commitInternal(t *testing.T, l *Log, body string) uint64 {
	t.Helper()
	b, err := l.Append([][]byte{[]byte(body)})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	seq, err := l.Commit(b)
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	return seq
}

// TestAckForcesDataSyncEvenWhenSyncFalse verifies that a successful
// checkpoint fsyncs the committed data before consumers.idx lands, even
// when commits themselves run with Options.Sync false.
func TestAckForcesDataSyncEvenWhenSyncFalse(t *testing.T) {
	dir := t.TempDir()
	l := openInternal(t, dir, Options{SegmentBytes: 1 << 20})

	orig := syncFile
	defer func() { syncFile = orig }()
	var synced []string
	syncFile = func(f *os.File) error {
		synced = append(synced, filepath.Base(f.Name()))
		return nil
	}

	commitInternal(t, l, "one")
	if len(synced) != 0 {
		t.Fatalf("Sync:false commit synced %v, want nothing", synced)
	}
	if !l.segDirty[1] {
		t.Fatalf("segment 1 not marked dirty after Sync:false commit")
	}

	// Registration at 0 persists metadata only: no segment barrier.
	if err := l.AckConsumer("c", 0); err != nil {
		t.Fatalf("register: %v", err)
	}
	for _, name := range synced {
		if strings.HasSuffix(name, ".seg") {
			t.Fatalf("registration synced segment %s", name)
		}
	}
	synced = nil

	if err := l.AckConsumer("c", 1); err != nil {
		t.Fatalf("ack: %v", err)
	}
	// The data sync must come before the metadata image sync (directory
	// syncs around both are allowed).
	idxMeta := -1
	for i, name := range synced {
		if name == "consumers.idx.tmp" {
			idxMeta = i
		}
	}
	if idxMeta <= 0 || synced[0] != "000001.seg" {
		t.Fatalf("sync order = %v, want segment ... consumers.idx.tmp", synced)
	}
	if l.segDirty[1] {
		t.Fatalf("segment still dirty after barrier")
	}

	// Repeating the current value writes and syncs nothing at all.
	synced = nil
	if err := l.AckConsumer("c", 1); err != nil {
		t.Fatalf("repeat ack: %v", err)
	}
	if len(synced) != 0 {
		t.Fatalf("repeat ack synced %v, want nothing", synced)
	}
}

func TestAckBarrierSyncsRolledSegments(t *testing.T) {
	dir := t.TempDir()
	l := openInternal(t, dir, Options{SegmentBytes: 1}) // every commit rolls

	orig := syncFile
	defer func() { syncFile = orig }()
	var synced []string
	syncFile = func(f *os.File) error {
		synced = append(synced, filepath.Base(f.Name()))
		return nil
	}

	commitInternal(t, l, "one")
	commitInternal(t, l, "two")
	if !l.segDirty[1] || !l.segDirty[2] {
		t.Fatalf("dirty = %v, want both 1 and 2", l.segDirty)
	}
	if err := l.AckConsumer("c", 0); err != nil {
		t.Fatalf("register: %v", err)
	}
	synced = nil
	if err := l.AckConsumer("c", 2); err != nil {
		t.Fatalf("ack: %v", err)
	}
	sort.Strings(synced)
	if len(synced) != 3 || synced[0] != "000001.seg" || synced[1] != "000002.seg" {
		t.Fatalf("syncs = %v, want both rolled segments then metadata", synced)
	}
	if len(l.segDirty) != 0 {
		t.Fatalf("dirty = %v, want empty after barrier", l.segDirty)
	}
}

// TestAckDataSyncFailureIsAtomic fails the forced segment fsync: the
// checkpoint must not register, nothing reaches consumers.idx and the
// retry succeeds after the fault clears.
func TestAckDataSyncFailureIsAtomic(t *testing.T) {
	dir := t.TempDir()
	l := openInternal(t, dir, Options{SegmentBytes: 1 << 20})
	commitInternal(t, l, "one")

	orig := syncFile
	defer func() { syncFile = orig }()
	syncFile = func(f *os.File) error {
		if filepath.Base(f.Name()) == "000001.seg" {
			return errors.New("disk boom")
		}
		return nil
	}

	if err := l.AckConsumer("c", 0); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := l.AckConsumer("c", 1); !errors.Is(err, ErrCheckpointFailed) {
		t.Fatalf("ack = %v, want ErrCheckpointFailed", err)
	}
	if got, err := l.ConsumerSeq("c"); err != nil || got != 0 {
		t.Fatalf("seq after failure = %d, %v; want 0", got, err)
	}
	// The image must still show the registered consumer at 0.
	probe := &Log{dir: dir}
	entries, ok, err := probe.readConsumers()
	if err != nil || !ok || len(entries) != 1 || entries[0].name != "c" || entries[0].seq != 0 {
		t.Fatalf("on-disk state = %+v ok=%v err=%v, want c@0 (old state)", entries, ok, err)
	}

	// Fault clears; the exact same call now succeeds.
	syncFile = func(f *os.File) error { return nil }
	if err := l.AckConsumer("c", 1); err != nil {
		t.Fatalf("retry ack: %v", err)
	}
	if got, _ := l.ConsumerSeq("c"); got != 1 {
		t.Fatalf("seq after retry = %d, want 1", got)
	}
}

// TestAckMetadataSyncFailureIsAtomic fails the fsync of the consumers
// temp image: the rename never happens and the old state survives.
func TestAckMetadataSyncFailureIsAtomic(t *testing.T) {
	dir := t.TempDir()
	l := openInternal(t, dir, Options{SegmentBytes: 1 << 20, Sync: true})
	commitInternal(t, l, "one")
	commitInternal(t, l, "two")
	if err := l.AckConsumer("c", 0); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := l.AckConsumer("c", 1); err != nil {
		t.Fatalf("ack 1: %v", err)
	}

	orig := syncFile
	defer func() { syncFile = orig }()
	syncFile = func(f *os.File) error {
		if filepath.Base(f.Name()) == "consumers.idx.tmp" {
			return errors.New("meta boom")
		}
		return nil
	}
	if err := l.AckConsumer("c", 2); !errors.Is(err, ErrCheckpointFailed) {
		t.Fatalf("ack = %v, want ErrCheckpointFailed", err)
	}
	if got, _ := l.ConsumerSeq("c"); got != 1 {
		t.Fatalf("seq after failure = %d, want 1", got)
	}
	syncFile = func(f *os.File) error { return nil }
	if err := l.AckConsumer("c", 2); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if got, _ := l.ConsumerSeq("c"); got != 2 {
		t.Fatalf("seq = %d, want 2", got)
	}
}

// TestOpenCorruptConsumersMetadata covers every fatal metadata shape:
// truncation, a bad checksum and an unsupported version.
func TestOpenCorruptConsumersMetadata(t *testing.T) {
	for _, tc := range []struct {
		name string
		mut  func([]byte) []byte
	}{
		{"truncated", func(b []byte) []byte { return b[:len(b)-1] }},
		{"bad checksum", func(b []byte) []byte {
			b[len(b)-1] ^= 0xff
			return b
		}},
		{"bad magic", func(b []byte) []byte {
			b[0] = 'X'
			return b
		}},
		{"short garbage", func(b []byte) []byte { return []byte("BCLCN") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			l := openInternal(t, dir, Options{SegmentBytes: 1 << 20, Sync: true})
			commitInternal(t, l, "one")
			if err := l.AckConsumer("c", 0); err != nil {
				t.Fatalf("register: %v", err)
			}
			if err := l.AckConsumer("c", 1); err != nil {
				t.Fatalf("ack: %v", err)
			}
			if err := l.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			path := filepath.Join(dir, "consumers.idx")
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, tc.mut(append([]byte(nil), data...)), 0o644); err != nil {
				t.Fatal(err)
			}
			if _, err := Open(dir, Options{SegmentBytes: 1 << 20}); !errors.Is(err, ErrCorruptCheckpoint) {
				t.Fatalf("Open = %v, want ErrCorruptCheckpoint", err)
			}
		})
	}
}

func TestOpenUnsupportedConsumersVersion(t *testing.T) {
	dir := t.TempDir()
	l := openInternal(t, dir, Options{SegmentBytes: 1 << 20, Sync: true})
	commitInternal(t, l, "one")
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	data := encodeConsumers([]consumerEntry{{name: "c", seq: 1}})
	data[6] = 99
	data[7] = 0
	// Recompute the checksum so framing otherwise passes: only the
	// version mismatch must be fatal.
	checksum := crc32.ChecksumIEEE(data[:len(data)-4])
	binary.LittleEndian.PutUint32(data[len(data)-4:], checksum)
	if err := os.WriteFile(filepath.Join(dir, "consumers.idx"), data, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir, Options{SegmentBytes: 1 << 20}); !errors.Is(err, ErrCorruptCheckpoint) {
		t.Fatalf("Open = %v, want ErrCorruptCheckpoint", err)
	}
}

// TestOpenCheckpointSemanticallyImpossible verifies the post-load
// cross-checks: the confirmed batch must be committed and retained, and
// no live staged batch may sit below it.
func TestOpenCheckpointSemanticallyImpossible(t *testing.T) {
	dir := t.TempDir()
	opts := Options{SegmentBytes: 1 << 20, Sync: true}
	l := openInternal(t, dir, opts)
	commitInternal(t, l, "one")
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	writeMeta := func(entries []consumerEntry) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, "consumers.idx"), encodeConsumers(entries), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	// A checkpoint past anything ever committed.
	writeMeta([]consumerEntry{{name: "c", seq: 99}})
	if _, err := Open(dir, opts); !errors.Is(err, ErrCorruptCheckpoint) {
		t.Fatalf("unknown target Open = %v, want ErrCorruptCheckpoint", err)
	}

	// A checkpoint at a permanent hole.
	writeMeta([]consumerEntry{{name: "c", seq: 2}})
	if _, err := Open(dir, opts); !errors.Is(err, ErrCorruptCheckpoint) {
		t.Fatalf("hole target Open = %v, want ErrCorruptCheckpoint", err)
	}

	// After a legal truncation, a checkpoint at or below the truncation
	// point is ordinary state (the consumer confirmed history that has
	// since been reclaimed), not corruption.
	if err := os.Remove(filepath.Join(dir, "consumers.idx")); err != nil {
		t.Fatal(err)
	}
	l = openInternal(t, dir, Options{SegmentBytes: 32, Sync: true})
	commitInternal(t, l, "two")
	if n, err := l.DeleteThrough(1); err != nil || n != 1 {
		t.Fatalf("DeleteThrough = %d, %v", n, err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	// Fabricate a checkpoint at the reclaimed prefix and reopen.
	if err := os.WriteFile(filepath.Join(dir, "consumers.idx"),
		encodeConsumers([]consumerEntry{{name: "c", seq: 1}}), 0o644); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(dir, Options{SegmentBytes: 32, Sync: true})
	if err != nil {
		t.Fatalf("Open with checkpoint at through: %v", err)
	}
	if got, err := reopened.ConsumerSeq("c"); err != nil || got != 1 {
		t.Fatalf("ConsumerSeq = %d, %v; want 1", got, err)
	}
	reopened.Close()
}

func TestOpenCheckpointDuplicateName(t *testing.T) {
	dir := t.TempDir()
	opts := Options{SegmentBytes: 1 << 20, Sync: true}
	l := openInternal(t, dir, opts)
	commitInternal(t, l, "one")
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	// Hand-built image with the same name twice: the strict parser
	// rejects it rather than silently folding the entries.
	if err := os.WriteFile(filepath.Join(dir, "consumers.idx"),
		encodeConsumers([]consumerEntry{{name: "c", seq: 1}, {name: "c", seq: 1}}), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir, opts); !errors.Is(err, ErrCorruptCheckpoint) {
		t.Fatalf("Open = %v, want ErrCorruptCheckpoint", err)
	}
}

// TestAckAfterReopenBarriersOldSegments verifies a Sync:false writer
// that dies before a checkpoint forces the barrier on the next open.
func TestAckAfterReopenBarriersOldSegments(t *testing.T) {
	dir := t.TempDir()
	opts := Options{SegmentBytes: 1 << 20}
	l := openInternal(t, dir, opts)
	commitInternal(t, l, "one")
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	l = openInternal(t, dir, opts)
	if !l.segDirty[1] {
		t.Fatalf("reopened segment not conservatively dirty")
	}
	if err := l.AckConsumer("c", 0); err != nil {
		t.Fatalf("register: %v", err)
	}
	orig := syncFile
	defer func() { syncFile = orig }()
	var sawSegment bool
	syncFile = func(f *os.File) error {
		if filepath.Base(f.Name()) == "000001.seg" {
			sawSegment = true
		}
		return nil
	}
	if err := l.AckConsumer("c", 1); err != nil {
		t.Fatalf("ack: %v", err)
	}
	if !sawSegment {
		t.Fatalf("post-reopen ack did not barrier the old segment")
	}
}
