package log

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
)

// encodeConsumersV1 serialises the legacy version 1 image (no state
// blobs) so the compatibility path can be exercised against real bytes.
func encodeConsumersV1(entries []consumerEntry) []byte {
	size := consumersHeaderLen + 4
	for _, e := range entries {
		size += 8 + 2 + len(e.name)
	}
	buf := make([]byte, 0, size)
	buf = append(buf, consumersMagic...)
	var tmp [8]byte
	binary.LittleEndian.PutUint16(tmp[:2], 1)
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

// TestV1CheckpointReadsEmptyStateAndIsNotRewritten: an old directory's
// version 1 consumers.idx loads with empty state per consumer, a plain
// query leaves the file bytes untouched, and the first checkpoint change
// re-encodes it as version 2 without losing progress.
func TestV1CheckpointReadsEmptyStateAndIsNotRewritten(t *testing.T) {
	dir := t.TempDir()
	opts := Options{SegmentBytes: 1 << 20, Sync: true}
	l := openInternal(t, dir, opts)
	commitInternal(t, l, "one")
	commitInternal(t, l, "two")
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	v1 := encodeConsumersV1([]consumerEntry{{name: "alpha", seq: 1}, {name: "beta", seq: 0}})
	if err := os.WriteFile(filepath.Join(dir, "consumers.idx"), v1, 0o644); err != nil {
		t.Fatal(err)
	}

	l = openInternal(t, dir, opts)
	if seq, st, err := l.ConsumerCheckpoint("alpha"); err != nil || seq != 1 || len(st) != 0 {
		t.Fatalf("alpha v1 checkpoint = %d %v len=%d; want 1 empty", seq, err, len(st))
	}
	if seq, err := l.ConsumerSeq("beta"); err != nil || seq != 0 {
		t.Fatalf("beta v1 seq = %d, %v; want 0", seq, err)
	}
	// Read-only access must not upgrade or rewrite the old image.
	got, err := os.ReadFile(filepath.Join(dir, "consumers.idx"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(v1) {
		t.Fatalf("consumers.idx rewritten by a query\n got %x\nwant %x", got, v1)
	}

	// A stateless advance keeps the empty state and moves to version 2.
	if err := l.AckConsumer("alpha", 2); err != nil {
		t.Fatalf("v1 upgrade advance: %v", err)
	}
	if _, st, _ := l.ConsumerCheckpoint("alpha"); len(st) != 0 {
		t.Fatalf("state after stateless advance = %q, want empty", st)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	l2, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("reopen v2 image: %v", err)
	}
	defer l2.Close()
	if seq, st, err := l2.ConsumerCheckpoint("alpha"); err != nil || seq != 2 || len(st) != 0 {
		t.Fatalf("alpha after upgrade = %d %q %v; want 2 empty", seq, st, err)
	}
	if seq, _ := l2.ConsumerSeq("beta"); seq != 0 {
		t.Fatalf("beta lost on upgrade: %d", seq)
	}
}

// TestV1CheckpointStatefulRegistration saves the input state on a new
// consumer even though the on-disk image was version 1.
func TestV1CheckpointStatefulRegistration(t *testing.T) {
	dir := t.TempDir()
	opts := Options{SegmentBytes: 1 << 20, Sync: true}
	l := openInternal(t, dir, opts)
	commitInternal(t, l, "one")
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "consumers.idx"),
		encodeConsumersV1([]consumerEntry{{name: "old", seq: 0}}), 0o644); err != nil {
		t.Fatal(err)
	}
	l = openInternal(t, dir, opts)
	if err := l.AckConsumerState("new", 0, []byte("born-on-v2")); err != nil {
		t.Fatalf("register on v1 image: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	l2, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer l2.Close()
	if seq, st, err := l2.ConsumerCheckpoint("new"); err != nil || seq != 0 || string(st) != "born-on-v2" {
		t.Fatalf("new = %d %q %v; want 0 born-on-v2", seq, st, err)
	}
	if seq, st, err := l2.ConsumerCheckpoint("old"); err != nil || seq != 0 || len(st) != 0 {
		t.Fatalf("old = %d len=%d %v; want 0 empty", seq, len(st), err)
	}
}

// TestOpenV2OversizedStateIsCorrupt: a structurally complete v2 image
// whose state length exceeds the cap is fatal corruption, not a reset or
// a silent truncation.
func TestOpenV2OversizedStateIsCorrupt(t *testing.T) {
	dir := t.TempDir()
	opts := Options{SegmentBytes: 1 << 20, Sync: true}
	l := openInternal(t, dir, opts)
	commitInternal(t, l, "one")
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	good := encodeConsumers([]consumerEntry{{name: "c", seq: 1, state: []byte("ok")}})
	// Patch the 4-byte state length of the only entry to 1048577; its
	// declared blob runs past the checksum, so the strict parser must
	// reject the image.
	pos := consumersHeaderLen + 8 + 2 + 1
	bad := append([]byte(nil), good...)
	binary.LittleEndian.PutUint32(bad[pos:pos+4], 1048577)
	checksum := crc32.ChecksumIEEE(bad[:len(bad)-4])
	binary.LittleEndian.PutUint32(bad[len(bad)-4:], checksum)
	if err := os.WriteFile(filepath.Join(dir, "consumers.idx"), bad, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir, opts); !errors.Is(err, ErrCorruptCheckpoint) {
		t.Fatalf("Open = %v, want ErrCorruptCheckpoint", err)
	}
}

// TestV2RoundTripPreservesBinaryState ensures opaque bytes survive a
// full write/reopen cycle unchanged, including NULs and 0xff.
func TestV2RoundTripPreservesBinaryState(t *testing.T) {
	dir := t.TempDir()
	opts := Options{SegmentBytes: 1 << 20, Sync: true}
	l := openInternal(t, dir, opts)
	commitInternal(t, l, "one")
	want := []byte{0x00, 0x01, 0xfe, 0xff, 0x00}
	if err := l.AckConsumerState("bin", 0, want); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	l2, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Close()
	_, got, err := l2.ConsumerCheckpoint("bin")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) {
		t.Fatalf("state len = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("state byte %d = %#x, want %#x (got %x)", i, got[i], want[i], got)
		}
	}
}

// TestAckConsumerStateSyncOrderAndIdempotency: a stateful advance fsyncs
// the data before the metadata image even with Sync:false; an identical
// same-seq retry and a same-seq conflict touch neither.
func TestAckConsumerStateSyncOrderAndIdempotency(t *testing.T) {
	dir := t.TempDir()
	l := openInternal(t, dir, Options{SegmentBytes: 1 << 20})
	commitInternal(t, l, "one")
	commitInternal(t, l, "two")
	if err := l.AckConsumerState("c", 0, []byte("z")); err != nil {
		t.Fatalf("register: %v", err)
	}

	orig := syncFile
	defer func() { syncFile = orig }()
	var synced []string
	syncFile = func(f *os.File) error {
		synced = append(synced, filepath.Base(f.Name()))
		return nil
	}
	if err := l.AckConsumerState("c", 2, []byte("zz")); err != nil {
		t.Fatalf("advance: %v", err)
	}
	if len(synced) == 0 || synced[0] != "000001.seg" {
		t.Fatalf("syncs = %v, want segment first", synced)
	}
	found := false
	for _, n := range synced {
		if n == "consumers.idx.tmp" {
			found = true
		}
	}
	if !found {
		t.Fatalf("syncs = %v, want consumers.idx.tmp", synced)
	}

	synced = nil
	if err := l.AckConsumerState("c", 2, []byte("zz")); err != nil {
		t.Fatalf("identical retry: %v", err)
	}
	if len(synced) != 0 {
		t.Fatalf("identical retry synced %v, want nothing", synced)
	}
	if err := l.AckConsumerState("c", 2, []byte("different")); !errors.Is(err, ErrCheckpointConflict) {
		t.Fatalf("conflict = %v, want ErrCheckpointConflict", err)
	}
	if len(synced) != 0 {
		t.Fatalf("conflict synced %v, want nothing", synced)
	}
}
