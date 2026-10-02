package log

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
)

// encodeConsumersV1 builds a version 1 image exactly the format the
// previous release wrote: no state field, entries of
// seq(8) + nameLen(2) + name.
func encodeConsumersV1(entries []consumerEntry) []byte {
	buf := make([]byte, 0, consumersHeaderLen+4)
	buf = append(buf, consumersMagic...)
	var tmp [8]byte
	binary.LittleEndian.PutUint16(tmp[:2], consumersV1)
	buf = append(buf, tmp[:2]...)
	binary.LittleEndian.PutUint32(tmp[:4], uint32(len(entries)))
	buf = append(buf, tmp[:4]...)
	for _, e := range entries {
		binary.LittleEndian.PutUint64(tmp[:], e.seq)
		buf = append(buf, tmp[:]...)
		binary.LittleEndian.PutUint16(tmp[:2], uint16(len(e.name)))
		buf = append(buf, tmp[:2]...)
		buf = append(buf, e.name...)
	}
	binary.LittleEndian.PutUint32(tmp[:4], crc32.ChecksumIEEE(buf))
	return append(buf, tmp[:4]...)
}

// TestV1CheckpointImageReadsEmptyState: an image written by the old
// release (no state field) opens cleanly with an empty replay context,
// and queries neither fail nor rewrite the file. The first actual write
// upgrades it to version 2 without losing progress.
func TestV1CheckpointImageReadsEmptyState(t *testing.T) {
	dir := t.TempDir()
	opts := Options{SegmentBytes: 1 << 20, Sync: true}
	l := openInternal(t, dir, opts)
	commitInternal(t, l, "one")
	commitInternal(t, l, "two")
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "consumers.idx"),
		encodeConsumersV1([]consumerEntry{{name: "alpha", seq: 1}, {name: "beta", seq: 2}}),
		0o644); err != nil {
		t.Fatal(err)
	}

	l = openInternal(t, dir, opts)
	if got, st, err := l.ConsumerCheckpoint("alpha"); err != nil || got != 1 || st != nil {
		t.Fatalf("alpha checkpoint = %d, %q, %v; want 1, nil state", got, st, err)
	}
	if got, st, err := l.ConsumerCheckpoint("beta"); err != nil || got != 2 || len(st) != 0 {
		t.Fatalf("beta checkpoint = %d, %q, %v; want 2, empty state", got, st, err)
	}
	if got, err := l.ConsumerSeq("alpha"); err != nil || got != 1 {
		t.Fatalf("ConsumerSeq = %d, %v; want 1", got, err)
	}

	// A query must not upgrade or otherwise rewrite the old file: its
	// bytes (including the version field) stay exactly as on disk.
	path := filepath.Join(dir, "consumers.idx")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := l.ConsumerCheckpoint("alpha"); err != nil {
		t.Fatal(err)
	}
	if _, err := l.ConsumerSeq("beta"); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) || binary.LittleEndian.Uint16(after[6:8]) != consumersV1 {
		t.Fatalf("query rewrote the v1 image")
	}

	// Advancing one consumer rewrites the whole table as version 2,
	// carries every other consumer along and leaves the v1-only consumer
	// with an empty context.
	if err := l.AckConsumerState("alpha", 2, []byte("ctx-2")); err != nil {
		t.Fatalf("AckConsumerState: %v", err)
	}
	onDisk, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if binary.LittleEndian.Uint16(onDisk[6:8]) != consumersVersion {
		t.Fatalf("image after write version = %d, want 2", binary.LittleEndian.Uint16(onDisk[6:8]))
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	l = openInternal(t, dir, opts)
	if got, st, err := l.ConsumerCheckpoint("alpha"); err != nil || got != 2 || string(st) != "ctx-2" {
		t.Fatalf("alpha after upgrade = %d, %q, %v", got, st, err)
	}
	if got, st, err := l.ConsumerCheckpoint("beta"); err != nil || got != 2 || st != nil {
		t.Fatalf("beta after upgrade = %d, %q, %v; want empty state carried", got, st, err)
	}
}

// TestV1ImageSemanticCrossChecksStillApply makes sure a v1 checkpoint
// that contradicts the log is still fatal after loading.
func TestV1ImageSemanticCrossChecksStillApply(t *testing.T) {
	dir := t.TempDir()
	opts := Options{SegmentBytes: 1 << 20, Sync: true}
	l := openInternal(t, dir, opts)
	commitInternal(t, l, "one")
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "consumers.idx"),
		encodeConsumersV1([]consumerEntry{{name: "c", seq: 42}}), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir, opts); !errors.Is(err, ErrCorruptCheckpoint) {
		t.Fatalf("Open = %v, want ErrCorruptCheckpoint", err)
	}
}

// TestCheckpointStateRoundTripAndAliasing verifies the context survives
// a reopen, nil and empty are the same context, and neither the argument
// nor the returned slice aliases stored bytes.
func TestCheckpointStateRoundTripAndAliasing(t *testing.T) {
	dir := t.TempDir()
	opts := Options{SegmentBytes: 1 << 20, Sync: true}
	l := openInternal(t, dir, opts)
	commitInternal(t, l, "one")
	commitInternal(t, l, "two")

	in := []byte("replay-context")
	if err := l.AckConsumerState("c", 0, in); err != nil {
		t.Fatalf("register with state: %v", err)
	}
	in[0] = 'X' // mutating the argument must not change the save
	if _, st, err := l.ConsumerCheckpoint("c"); err != nil || string(st) != "replay-context" {
		t.Fatalf("state after arg mutation = %q, %v", st, err)
	}
	if err := l.AckConsumerState("c", 1, []byte("ctx-1")); err != nil {
		t.Fatalf("ack 1: %v", err)
	}
	out, err := func() ([]byte, error) {
		_, st, e := l.ConsumerCheckpoint("c")
		return st, e
	}()
	if err != nil || string(out) != "ctx-1" {
		t.Fatalf("checkpoint = %q, %v", out, err)
	}
	out[0] = 'Y' // mutating the returned copy must not change the save
	if _, st, err := l.ConsumerCheckpoint("c"); err != nil || string(st) != "ctx-1" {
		t.Fatalf("state after returned-slice mutation = %q, %v", st, err)
	}

	// Advancing with an empty slice must clear the context; nil and
	// []byte{} are the same save.
	if err := l.AckConsumerState("c", 2, []byte{}); err != nil {
		t.Fatalf("ack 2 empty: %v", err)
	}
	if _, st, err := l.ConsumerCheckpoint("c"); err != nil || st != nil {
		t.Fatalf("state = %q, want nil", st)
	}
	if err := l.AckConsumerState("c", 2, nil); err != nil {
		t.Fatalf("same seq, nil vs empty: %v, want nil error", err)
	}

	// Reopen: the cleared context stays empty.
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	l = openInternal(t, dir, opts)
	if got, st, err := l.ConsumerCheckpoint("c"); err != nil || got != 2 || st != nil {
		t.Fatalf("after reopen = %d, %q, %v; want 2, nil", got, st, err)
	}
}

// TestCheckpointStateSizeBoundary exercises exactly the 1 MiB limit.
func TestCheckpointStateSizeBoundary(t *testing.T) {
	dir := t.TempDir()
	l := openInternal(t, dir, Options{SegmentBytes: 1 << 20, Sync: true})
	commitInternal(t, l, "one")
	if err := l.AckConsumerState("c", 0, make([]byte, MaxCheckpointState)); err != nil {
		t.Fatalf("state at limit: %v", err)
	}
	if err := l.AckConsumerState("c", 1, make([]byte, MaxCheckpointState+1)); !errors.Is(err, ErrInvalidCheckpointState) {
		t.Fatalf("state over limit = %v, want ErrInvalidCheckpointState", err)
	}
	// The rejected call changed neither seq nor state nor the file.
	if got, _ := l.ConsumerSeq("c"); got != 0 {
		t.Fatalf("seq = %d, want 0", got)
	}
}

// TestAckConsumerStateValidationOrder: name is judged before state size,
// which is judged before registration and progress.
func TestAckConsumerStateValidationOrder(t *testing.T) {
	dir := t.TempDir()
	l := openInternal(t, dir, Options{SegmentBytes: 1 << 20, Sync: true})
	commitInternal(t, l, "one")
	big := make([]byte, MaxCheckpointState+1)

	// Bad name beats oversized state and an unregistered/non-zero seq.
	if err := l.AckConsumerState("", 5, big); !errors.Is(err, ErrInvalidConsumer) {
		t.Fatalf("bad name = %v, want ErrInvalidConsumer", err)
	}
	if err := l.AckConsumerState("has\x00nul", 5, big); !errors.Is(err, ErrInvalidConsumer) {
		t.Fatalf("bad name = %v, want ErrInvalidConsumer", err)
	}
	// Valid name: state size beats unknown-registration (non-zero seq).
	if err := l.AckConsumerState("new", 1, big); !errors.Is(err, ErrInvalidCheckpointState) {
		t.Fatalf("oversized before registration = %v, want ErrInvalidCheckpointState", err)
	}
	// Valid name, valid size: registration rule comes last.
	if err := l.AckConsumerState("new", 1, []byte("x")); !errors.Is(err, ErrUnknownConsumer) {
		t.Fatalf("non-zero register = %v, want ErrUnknownConsumer", err)
	}
	if err := l.AckConsumerState("new", 0, big); !errors.Is(err, ErrInvalidCheckpointState) {
		t.Fatalf("oversized register at 0 = %v, want ErrInvalidCheckpointState", err)
	}
	// Registered consumer: size beats a backwards/forwards progress error.
	if err := l.AckConsumerState("new", 0, nil); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := l.AckConsumerState("new", 0, big); !errors.Is(err, ErrInvalidCheckpointState) {
		t.Fatalf("oversized same-seq = %v, want ErrInvalidCheckpointState", err)
	}
	if err := l.AckConsumerState("new", 99, big); !errors.Is(err, ErrInvalidCheckpointState) {
		t.Fatalf("oversized ahead = %v, want ErrInvalidCheckpointState", err)
	}
	if _, _, err := l.ConsumerCheckpoint(""); !errors.Is(err, ErrInvalidConsumer) {
		t.Fatalf("ConsumerCheckpoint bad name = %v, want ErrInvalidConsumer", err)
	}
	if _, _, err := l.ConsumerCheckpoint("ghost"); !errors.Is(err, ErrUnknownConsumer) {
		t.Fatalf("ConsumerCheckpoint unknown = %v, want ErrUnknownConsumer", err)
	}
}

// TestSameSeqStateConflictWritesNothing checks the idempotent retry and
// the conflict path, including that no sync happens on either.
func TestSameSeqStateConflictWritesNothing(t *testing.T) {
	dir := t.TempDir()
	l := openInternal(t, dir, Options{SegmentBytes: 1 << 20, Sync: true})
	commitInternal(t, l, "one")
	if err := l.AckConsumerState("c", 0, nil); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := l.AckConsumerState("c", 1, []byte("A")); err != nil {
		t.Fatalf("ack: %v", err)
	}

	orig := syncFile
	defer func() { syncFile = orig }()
	var synced []string
	syncFile = func(f *os.File) error {
		synced = append(synced, filepath.Base(f.Name()))
		return nil
	}

	// Same seq, same state: idempotent success with no I/O.
	if err := l.AckConsumerState("c", 1, []byte("A")); err != nil {
		t.Fatalf("same state retry: %v", err)
	}
	if err := l.AckConsumerState("c", 1, []byte{}); !errors.Is(err, ErrCheckpointConflict) {
		t.Fatalf("conflict = %v, want ErrCheckpointConflict", err)
	}
	if err := l.AckConsumerState("c", 1, []byte("B")); !errors.Is(err, ErrCheckpointConflict) {
		t.Fatalf("conflict = %v, want ErrCheckpointConflict", err)
	}
	if len(synced) != 0 {
		t.Fatalf("retry/conflict synced %v, want nothing", synced)
	}
	if _, st, err := l.ConsumerCheckpoint("c"); err != nil || string(st) != "A" {
		t.Fatalf("state after conflict = %q, %v; want A", st, err)
	}
}

// TestAckConsumerPreservesState verifies a plain AckConsumer advance and
// same-seq retry never alter the context committed by AckConsumerState.
func TestAckConsumerPreservesState(t *testing.T) {
	dir := t.TempDir()
	opts := Options{SegmentBytes: 1 << 20, Sync: true}
	l := openInternal(t, dir, opts)
	commitInternal(t, l, "one")
	commitInternal(t, l, "two")
	if err := l.AckConsumerState("c", 0, nil); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := l.AckConsumerState("c", 1, []byte("keep-me")); err != nil {
		t.Fatalf("ack state: %v", err)
	}
	if err := l.AckConsumer("c", 2); err != nil {
		t.Fatalf("plain advance: %v", err)
	}
	if _, st, err := l.ConsumerCheckpoint("c"); err != nil || string(st) != "keep-me" {
		t.Fatalf("state after plain advance = %q, %v", st, err)
	}
	// Same-seq plain retry stays a no-op and still preserves state.
	if err := l.AckConsumer("c", 2); err != nil {
		t.Fatalf("plain retry: %v", err)
	}

	orig := syncFile
	defer func() { syncFile = orig }()
	synced := 0
	syncFile = func(f *os.File) error {
		synced++
		return nil
	}
	if err := l.AckConsumer("c", 2); err != nil {
		t.Fatalf("plain retry under fault: %v", err)
	}
	if synced != 0 {
		t.Fatalf("plain same-seq retry synced %d files", synced)
	}

	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	l = openInternal(t, dir, opts)
	if got, st, err := l.ConsumerCheckpoint("c"); err != nil || got != 2 || string(st) != "keep-me" {
		t.Fatalf("after reopen = %d, %q, %v", got, st, err)
	}
}

// TestAckConsumerStateBarriersDataLikeAck verifies the forced data sync
// precedes the metadata image sync even with Options.Sync false, and a
// same-seq retry does neither.
func TestAckConsumerStateBarriersDataLikeAck(t *testing.T) {
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
	synced = nil

	if err := l.AckConsumerState("c", 0, []byte("boot")); err != nil {
		t.Fatalf("register: %v", err)
	}
	for _, name := range synced {
		if filepath.Ext(name) == ".seg" {
			t.Fatalf("registration synced segment %s", name)
		}
	}
	synced = nil
	if err := l.AckConsumerState("c", 1, []byte("ctx")); err != nil {
		t.Fatalf("ack: %v", err)
	}
	if len(synced) < 2 || synced[0] != "000001.seg" {
		t.Fatalf("syncs = %v, want segment before consumers image", synced)
	}
	found := false
	for _, name := range synced {
		if name == "consumers.idx.tmp" {
			found = true
		}
	}
	if !found {
		t.Fatalf("syncs = %v, missing consumers.idx.tmp", synced)
	}
	synced = nil
	if err := l.AckConsumerState("c", 1, []byte("ctx")); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if len(synced) != 0 {
		t.Fatalf("same-seq retry synced %v", synced)
	}
}

// TestCheckpointStateMetaFailureIsAtomic fails the image fsync during a
// state-bearing advance: the old seq and state survive and the retry
// lands.
func TestCheckpointStateMetaFailureIsAtomic(t *testing.T) {
	dir := t.TempDir()
	l := openInternal(t, dir, Options{SegmentBytes: 1 << 20, Sync: true})
	commitInternal(t, l, "one")
	commitInternal(t, l, "two")
	if err := l.AckConsumerState("c", 0, nil); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := l.AckConsumerState("c", 1, []byte("old")); err != nil {
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
	if err := l.AckConsumerState("c", 2, []byte("new")); !errors.Is(err, ErrCheckpointFailed) {
		t.Fatalf("failing write = %v, want ErrCheckpointFailed", err)
	}
	if got, st, err := l.ConsumerCheckpoint("c"); err != nil || got != 1 || string(st) != "old" {
		t.Fatalf("after failure = %d, %q, %v; want 1/old", got, st, err)
	}
	// Other consumers are untouched too.
	probe := &Log{dir: dir}
	entries, ok, err := probe.readConsumers()
	if err != nil || !ok || len(entries) != 1 || entries[0].seq != 1 || string(entries[0].state) != "old" {
		t.Fatalf("on-disk table = %+v ok=%v err=%v", entries, ok, err)
	}

	syncFile = func(f *os.File) error { return nil }
	if err := l.AckConsumerState("c", 2, []byte("new")); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if got, st, _ := l.ConsumerCheckpoint("c"); got != 2 || string(st) != "new" {
		t.Fatalf("after retry = %d, %q; want 2/new", got, st)
	}
}

// TestCorruptV2StateLength rejects a v2 image whose state length runs
// past the entry framing, and an oversized-but-framed state.
func TestCorruptV2StateLength(t *testing.T) {
	dir := t.TempDir()
	opts := Options{SegmentBytes: 1 << 20, Sync: true}
	l := openInternal(t, dir, opts)
	commitInternal(t, l, "one")
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	write := func(t *testing.T, b []byte) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, "consumers.idx"), b, 0o644); err != nil {
			t.Fatal(err)
		}
	}

	// State length claims more bytes than the entry actually holds.
	bad := encodeConsumers([]consumerEntry{{name: "c", seq: 1, state: []byte("ok")}})
	// Locate the 4-byte state length (right after header(12) + seq(8)).
	binary.LittleEndian.PutUint32(bad[20:24], 999)
	binary.LittleEndian.PutUint32(bad[len(bad)-4:], crc32.ChecksumIEEE(bad[:len(bad)-4]))
	write(t, bad)
	if _, err := Open(dir, opts); !errors.Is(err, ErrCorruptCheckpoint) {
		t.Fatalf("Open overlong state = %v, want ErrCorruptCheckpoint", err)
	}

	// State within framing but over the API limit is structural damage.
	huge := encodeConsumers([]consumerEntry{{name: "c", seq: 1, state: make([]byte, MaxCheckpointState+1)}})
	write(t, huge)
	if _, err := Open(dir, opts); !errors.Is(err, ErrCorruptCheckpoint) {
		t.Fatalf("Open oversized state = %v, want ErrCorruptCheckpoint", err)
	}
}

// TestDropConsumerClearsState verifies the context is gone after a drop
// and never revived by a reopen or a fresh registration.
func TestDropConsumerClearsState(t *testing.T) {
	dir := t.TempDir()
	opts := Options{SegmentBytes: 1 << 20, Sync: true}
	l := openInternal(t, dir, opts)
	commitInternal(t, l, "one")
	if err := l.AckConsumerState("c", 0, nil); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := l.AckConsumerState("c", 1, []byte("secret")); err != nil {
		t.Fatalf("ack: %v", err)
	}
	if err := l.DropConsumer("c"); err != nil {
		t.Fatalf("drop: %v", err)
	}
	probe := &Log{dir: dir}
	entries, ok, err := probe.readConsumers()
	if err != nil || !ok || len(entries) != 0 {
		t.Fatalf("on-disk table after drop = %+v ok=%v err=%v", entries, ok, err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	l = openInternal(t, dir, opts)
	if _, _, err := l.ConsumerCheckpoint("c"); !errors.Is(err, ErrUnknownConsumer) {
		t.Fatalf("revived after reopen: %v", err)
	}
	// A fresh registration starts with no context.
	if err := l.AckConsumerState("c", 0, nil); err != nil {
		t.Fatalf("re-register: %v", err)
	}
	if got, st, err := l.ConsumerCheckpoint("c"); err != nil || got != 0 || st != nil {
		t.Fatalf("re-registered = %d, %q, %v; want 0, nil", got, st, err)
	}
}
