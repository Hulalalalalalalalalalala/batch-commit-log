package log_test

import (
	"bytes"
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

// v1ConsumersImage hand-builds a release-1 consumers.idx image (magic
// "BCLCNM", version 1, entries of seq + nameLen + name, trailing CRC)
// from outside the package so the compatibility test cannot borrow any
// internal encoder.
func v1ConsumersImage(t *testing.T, consumers map[string]uint64) []byte {
	t.Helper()
	names := make([]string, 0, len(consumers))
	for n := range consumers {
		names = append(names, n)
	}
	sort.Strings(names)
	buf := []byte{'B', 'C', 'L', 'C', 'N', 'M'}
	var tmp [8]byte
	binary.LittleEndian.PutUint16(tmp[:2], 1)
	buf = append(buf, tmp[:2]...)
	binary.LittleEndian.PutUint32(tmp[:4], uint32(len(names)))
	buf = append(buf, tmp[:4]...)
	for _, n := range names {
		binary.LittleEndian.PutUint64(tmp[:], consumers[n])
		buf = append(buf, tmp[:]...)
		binary.LittleEndian.PutUint16(tmp[:2], uint16(len(n)))
		buf = append(buf, tmp[:2]...)
		buf = append(buf, n...)
	}
	binary.LittleEndian.PutUint32(tmp[:4], crc32.ChecksumIEEE(buf))
	return append(buf, tmp[:4]...)
}

// TestCheckpointStatePersistsAcrossReopenAndIndexLoss is the headline
// flow: commit a replay context with the sequence, reopen, and read both
// back; deleting or rebuilding index.idx must not touch the context.
func TestCheckpointStatePersistsAcrossReopenAndIndexLoss(t *testing.T) {
	dir := t.TempDir()
	opts := log.Options{SegmentBytes: 1 << 20, Sync: true}
	l := open(t, dir, opts)
	commit(t, l, []byte("one"))
	commit(t, l, []byte("two"))
	if err := l.AckConsumerState("svc", 0, []byte("boot")); err != nil {
		t.Fatalf("register with state: %v", err)
	}
	if err := l.AckConsumerState("svc", 2, []byte("cursor@2")); err != nil {
		t.Fatalf("ack with state: %v", err)
	}

	l = reopen(t, l, dir, opts)
	got, st, err := l.ConsumerCheckpoint("svc")
	if err != nil || got != 2 || string(st) != "cursor@2" {
		t.Fatalf("after reopen = %d, %q, %v; want 2, cursor@2", got, st, err)
	}
	// ConsumerSeq and ConsumerCheckpoint always agree on the sequence.
	if seq, err := l.ConsumerSeq("svc"); err != nil || seq != got {
		t.Fatalf("ConsumerSeq = %d, %v; want %d", seq, err, got)
	}

	// Lose the rebuildable sidecar: the authoritative checkpoint keeps
	// both its sequence and context.
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, "index.idx")); err != nil {
		t.Fatal(err)
	}
	l = open(t, dir, opts)
	if got, st, err := l.ConsumerCheckpoint("svc"); err != nil || got != 2 || string(st) != "cursor@2" {
		t.Fatalf("after index loss = %d, %q, %v", got, st, err)
	}
}

// TestCheckpointStateOpaqueAndCopied checks binary round-trip fidelity
// and the no-alias guarantee on both argument and return sides.
func TestCheckpointStateOpaqueAndCopied(t *testing.T) {
	l := open(t, t.TempDir(), log.Options{SegmentBytes: 1 << 20, Sync: true})
	commit(t, l, []byte("x"))
	payload := []byte{0x00, 0x01, 0xff, 'a', 0x80}
	if err := l.AckConsumerState("c", 0, payload); err != nil {
		t.Fatalf("register: %v", err)
	}
	payload[0] = 0x42
	if _, st, err := l.ConsumerCheckpoint("c"); err != nil || !bytes.Equal(st, []byte{0x00, 0x01, 0xff, 'a', 0x80}) {
		t.Fatalf("argument aliased stored state: %x", st)
	}
	if err := l.AckConsumerState("c", 1, payload); err != nil {
		t.Fatalf("ack: %v", err)
	}
	_, st, err := l.ConsumerCheckpoint("c")
	if err != nil || !bytes.Equal(st, []byte{0x42, 0x01, 0xff, 'a', 0x80}) {
		t.Fatalf("roundtrip = %x, %v", st, err)
	}
	st[0] = 0x00
	if _, st2, err := l.ConsumerCheckpoint("c"); err != nil || st2[0] != 0x42 {
		t.Fatalf("returned slice aliased stored state: %x", st2)
	}
}

// TestCheckpointStateNilEmptyEquivalent covers the nil/empty identity at
// registration, on retry and after a reopen.
func TestCheckpointStateNilEmptyEquivalent(t *testing.T) {
	dir := t.TempDir()
	opts := log.Options{SegmentBytes: 1 << 20, Sync: true}
	l := open(t, dir, opts)
	commit(t, l, []byte("x"))
	if err := l.AckConsumerState("a", 0, nil); err != nil {
		t.Fatalf("register nil: %v", err)
	}
	if err := l.AckConsumerState("a", 0, []byte{}); err != nil {
		t.Fatalf("same seq empty vs nil = %v, want success", err)
	}
	if err := l.AckConsumerState("b", 0, []byte{}); err != nil {
		t.Fatalf("register empty: %v", err)
	}
	if err := l.AckConsumerState("b", 0, nil); err != nil {
		t.Fatalf("same seq nil vs empty = %v, want success", err)
	}
	l = reopen(t, l, dir, opts)
	for _, name := range []string{"a", "b"} {
		if got, st, err := l.ConsumerCheckpoint(name); err != nil || got != 0 || len(st) != 0 {
			t.Fatalf("%s = %d, %x, %v; want 0 with empty state", name, got, st, err)
		}
	}
}

// TestCheckpointStateTooLarge rejects a context over the bound before
// anything is registered or written, and the boundary size is accepted.
func TestCheckpointStateTooLarge(t *testing.T) {
	dir := t.TempDir()
	opts := log.Options{SegmentBytes: 1 << 20, Sync: true}
	l := open(t, dir, opts)
	commit(t, l, []byte("x"))
	if err := l.AckConsumerState("c", 0, make([]byte, 1<<20)); err != nil {
		t.Fatalf("state of %d bytes: %v", 1<<20, err)
	}
	if err := l.AckConsumerState("d", 0, make([]byte, (1<<20)+1)); !errors.Is(err, log.ErrInvalidCheckpointState) {
		t.Fatalf("state of %d bytes = %v, want ErrInvalidCheckpointState", (1<<20)+1, err)
	}
	// No file for the rejected registration, no consumer visible.
	if _, _, err := l.ConsumerCheckpoint("d"); !errors.Is(err, log.ErrUnknownConsumer) {
		t.Fatalf("rejected registration visible: %v", err)
	}
	if entries, _ := filepath.Glob(filepath.Join(dir, "consumers.idx.tmp")); len(entries) != 0 {
		t.Fatalf("temp image left behind: %v", entries)
	}
}

// TestCheckpointStateRegistrationRules mirrors AckConsumer for names:
// non-zero first call is unknown, seq-0 registration saves the state.
func TestCheckpointStateRegistrationRules(t *testing.T) {
	l := open(t, t.TempDir(), log.Options{SegmentBytes: 1 << 20, Sync: true})
	commit(t, l, []byte("x"))
	if err := l.AckConsumerState("new", 1, []byte("s")); !errors.Is(err, log.ErrUnknownConsumer) {
		t.Fatalf("non-zero registration = %v, want ErrUnknownConsumer", err)
	}
	if err := l.AckConsumerState("new", 0, []byte("s0")); err != nil {
		t.Fatalf("register at 0: %v", err)
	}
	if got, st, err := l.ConsumerCheckpoint("new"); err != nil || got != 0 || string(st) != "s0" {
		t.Fatalf("registered = %d, %q, %v", got, st, err)
	}
}

// TestCheckpointStateProgressValidation reuses AckConsumer's rules:
// rollback, hole target, reclaimed target, jump over a staged batch.
func TestCheckpointStateProgressValidation(t *testing.T) {
	dir := t.TempDir()
	l := open(t, dir, log.Options{SegmentBytes: 1 << 20, Sync: true})
	s1 := commit(t, l, []byte("one"))
	if s1 != 1 {
		t.Fatalf("seq = %d, want 1", s1)
	}
	if err := l.AckConsumerState("c", 0, nil); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := l.AckConsumerState("c", 1, []byte("ok")); err != nil {
		t.Fatalf("ack 1: %v", err)
	}
	// Rollback.
	if err := l.AckConsumerState("c", 0, []byte("back")); !errors.Is(err, log.ErrInvalidAck) {
		t.Fatalf("rollback = %v, want ErrInvalidAck", err)
	}
	// Unknown target.
	if err := l.AckConsumerState("c", 42, []byte("far")); !errors.Is(err, log.ErrInvalidAck) {
		t.Fatalf("unknown target = %v, want ErrInvalidAck", err)
	}
	// Smaller staged batch below the target.
	b2, _ := l.Append([][]byte{[]byte("two")})
	b3, _ := l.Append([][]byte{[]byte("three")})
	if _, err := l.Commit(b3); err != nil {
		t.Fatalf("commit b3: %v", err)
	}
	if err := l.AckConsumerState("c", 3, []byte("jump")); !errors.Is(err, log.ErrInvalidAck) {
		t.Fatalf("jump over staged 2 = %v, want ErrInvalidAck", err)
	}
	if _, err := l.Commit(b2); err != nil {
		t.Fatalf("commit b2: %v", err)
	}
	if err := l.AckConsumerState("c", 3, []byte("at-3")); err != nil {
		t.Fatalf("ack 3 after b2 commits: %v", err)
	}
	if got, st, _ := l.ConsumerCheckpoint("c"); got != 3 || string(st) != "at-3" {
		t.Fatalf("checkpoint = %d, %q", got, st)
	}

	// Reclaimed target.
	l2 := open(t, t.TempDir(), log.Options{SegmentBytes: 32, Sync: true})
	commit(t, l2, []byte("one"))
	commit(t, l2, []byte("two"))
	if n, err := l2.DeleteThrough(1); err != nil || n != 1 {
		t.Fatalf("DeleteThrough = %d, %v", n, err)
	}
	if err := l2.AckConsumerState("r", 0, nil); err != nil {
		t.Fatalf("register r: %v", err)
	}
	if err := l2.AckConsumerState("r", 1, []byte("gone")); !errors.Is(err, log.ErrInvalidAck) {
		t.Fatalf("reclaimed target = %v, want ErrInvalidAck", err)
	}
	if err := l2.AckConsumerState("r", 2, []byte("kept")); err != nil {
		t.Fatalf("retained target: %v", err)
	}
}

// TestCheckpointConflictAcrossReopen confirms a state mismatch at the
// stored sequence is rejected even after a reopen, while byte-identical
// state retries cleanly and writes nothing observable.
func TestCheckpointConflictAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	opts := log.Options{SegmentBytes: 1 << 20, Sync: true}
	l := open(t, dir, opts)
	commit(t, l, []byte("x"))
	if err := l.AckConsumerState("c", 0, nil); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := l.AckConsumerState("c", 1, []byte("v1")); err != nil {
		t.Fatalf("ack: %v", err)
	}
	l = reopen(t, l, dir, opts)
	if err := l.AckConsumerState("c", 1, []byte("v1")); err != nil {
		t.Fatalf("same seq same state after reopen: %v", err)
	}
	if err := l.AckConsumerState("c", 1, []byte("v2")); !errors.Is(err, log.ErrCheckpointConflict) {
		t.Fatalf("same seq different state = %v, want ErrCheckpointConflict", err)
	}
	// Saved state survives the rejected call.
	if _, st, err := l.ConsumerCheckpoint("c"); err != nil || string(st) != "v1" {
		t.Fatalf("state after conflict = %q, %v", st, err)
	}
}

// TestPlainAckConsumerCarriesStateAcrossAdvances interleaves the two
// ack entry points: state committed by one survives plain advances and
// plain registrations stay empty.
func TestPlainAckConsumerCarriesStateAcrossAdvances(t *testing.T) {
	dir := t.TempDir()
	opts := log.Options{SegmentBytes: 1 << 20, Sync: true}
	l := open(t, dir, opts)
	for _, b := range []string{"a", "b", "c", "d"} {
		commit(t, l, []byte(b))
	}
	// Plain registration: empty context.
	if err := l.AckConsumer("plain", 0); err != nil {
		t.Fatalf("register plain: %v", err)
	}
	if _, st, err := l.ConsumerCheckpoint("plain"); err != nil || st != nil {
		t.Fatalf("plain state = %q, want nil", st)
	}
	// Stateful consumer.
	if err := l.AckConsumerState("rich", 0, []byte("s0")); err != nil {
		t.Fatalf("register rich: %v", err)
	}
	if err := l.AckConsumerState("rich", 1, []byte("s1")); err != nil {
		t.Fatalf("rich ack 1: %v", err)
	}
	// Plain advance on the rich consumer preserves state.
	if err := l.AckConsumer("rich", 2); err != nil {
		t.Fatalf("plain advance rich: %v", err)
	}
	if got, st, err := l.ConsumerCheckpoint("rich"); err != nil || got != 2 || string(st) != "s1" {
		t.Fatalf("rich after plain ack = %d, %q, %v", got, st, err)
	}
	// Plain same-seq retry is still a silent no-op (exercises the
	// preserve path; would fail under a forced sync if it wrote).
	if err := l.AckConsumer("rich", 2); err != nil {
		t.Fatalf("plain retry: %v", err)
	}
	// A further stateful advance replaces the context.
	if err := l.AckConsumerState("rich", 4, []byte("s4")); err != nil {
		t.Fatalf("rich ack 4: %v", err)
	}
	l = reopen(t, l, dir, opts)
	if got, st, err := l.ConsumerCheckpoint("rich"); err != nil || got != 4 || string(st) != "s4" {
		t.Fatalf("rich after reopen = %d, %q, %v", got, st, err)
	}
	if got, st, err := l.ConsumerCheckpoint("plain"); err != nil || got != 0 || st != nil {
		t.Fatalf("plain after reopen = %d, %q, %v", got, st, err)
	}
}

// TestDropConsumerRemovesState checks the context is deleted with the
// registration and a re-registration starts empty, including across a
// reopen.
func TestDropConsumerRemovesState(t *testing.T) {
	dir := t.TempDir()
	opts := log.Options{SegmentBytes: 1 << 20, Sync: true}
	l := open(t, dir, opts)
	commit(t, l, []byte("x"))
	if err := l.AckConsumerState("c", 0, []byte("ctx")); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := l.DropConsumer("c"); err != nil {
		t.Fatalf("drop: %v", err)
	}
	if _, _, err := l.ConsumerCheckpoint("c"); !errors.Is(err, log.ErrUnknownConsumer) {
		t.Fatalf("after drop = %v, want ErrUnknownConsumer", err)
	}
	if err := l.AckConsumerState("c", 0, nil); err != nil {
		t.Fatalf("re-register: %v", err)
	}
	if _, st, err := l.ConsumerCheckpoint("c"); err != nil || st != nil {
		t.Fatalf("re-registered state = %q, want nil", st)
	}
	l = reopen(t, l, dir, opts)
	if _, st, err := l.ConsumerCheckpoint("c"); err != nil || st != nil {
		t.Fatalf("state after reopen = %q, %v", st, err)
	}
}

// TestOldV1CheckpointReadsEmptyState is the compatibility contract from
// outside the package using a hand-built release-1 image.
func TestOldV1CheckpointReadsEmptyState(t *testing.T) {
	dir := t.TempDir()
	opts := log.Options{SegmentBytes: 1 << 20, Sync: true}
	l := open(t, dir, opts)
	commit(t, l, []byte("x"))
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	img := v1ConsumersImage(t, map[string]uint64{"legacy": 1})
	if err := os.WriteFile(filepath.Join(dir, "consumers.idx"), img, 0o644); err != nil {
		t.Fatal(err)
	}
	l = open(t, dir, opts)
	got, st, err := l.ConsumerCheckpoint("legacy")
	if err != nil || got != 1 || len(st) != 0 {
		t.Fatalf("legacy = %d, %x, %v; want 1 with empty state", got, st, err)
	}
	// File untouched by the query.
	if onDisk, err := os.ReadFile(filepath.Join(dir, "consumers.idx")); err != nil || !reflect.DeepEqual(onDisk, img) {
		t.Fatalf("query rewrote the v1 image: %v", err)
	}
	// Stateful advance upgrades and round-trips.
	if err := l.AckConsumerState("legacy", 1, []byte("")); err != nil {
		t.Fatalf("same seq empty state: %v", err)
	}
	// A state change at the same seq conflicts; advance instead after a
	// second commit.
	commit(t, l, []byte("y"))
	if err := l.AckConsumerState("legacy", 2, []byte("now-v2")); err != nil {
		t.Fatalf("stateful advance: %v", err)
	}
	l = reopen(t, l, dir, opts)
	if got, st, err := l.ConsumerCheckpoint("legacy"); err != nil || got != 2 || string(st) != "now-v2" {
		t.Fatalf("after upgrade = %d, %q, %v", got, st, err)
	}
}
