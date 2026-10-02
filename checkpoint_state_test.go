package log_test

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	log "github.com/Hulalalalalalalalalalala/batch-commit-log"
)

// TestCheckpointStateRoundTrip registers a consumer with state, advances
// with new state and reopens: both sequence and state come back from
// ConsumerCheckpoint, and ConsumerSeq always agrees on the sequence.
func TestCheckpointStateRoundTrip(t *testing.T) {
	dir := t.TempDir()
	opts := log.Options{SegmentBytes: 1 << 20}
	l := open(t, dir, opts)
	commit(t, l, []byte("one"))
	commit(t, l, []byte("two"))

	if err := l.AckConsumerState("c", 0, []byte("boot")); err != nil {
		t.Fatalf("register with state: %v", err)
	}
	if seq, st, err := l.ConsumerCheckpoint("c"); err != nil || seq != 0 || string(st) != "boot" {
		t.Fatalf("checkpoint = %d %q %v; want 0 boot", seq, st, err)
	}
	if seq, err := l.ConsumerSeq("c"); err != nil || seq != 0 {
		t.Fatalf("ConsumerSeq = %d, %v; want 0", seq, err)
	}
	if err := l.AckConsumerState("c", 2, []byte("snapshot-2")); err != nil {
		t.Fatalf("ack with state: %v", err)
	}

	l = reopen(t, l, dir, opts)
	seq, st, err := l.ConsumerCheckpoint("c")
	if err != nil || seq != 2 || string(st) != "snapshot-2" {
		t.Fatalf("after reopen = %d %q %v; want 2 snapshot-2", seq, st, err)
	}
	if got, err := l.ConsumerSeq("c"); err != nil || got != seq {
		t.Fatalf("ConsumerSeq = %d, %v; want %d", got, err, seq)
	}

	// Deleting index.idx must not affect the checkpoint state either.
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, "index.idx")); err != nil {
		t.Fatal(err)
	}
	l = open(t, dir, opts)
	if seq, st, err := l.ConsumerCheckpoint("c"); err != nil || seq != 2 || string(st) != "snapshot-2" {
		t.Fatalf("after index loss = %d %q %v; want 2 snapshot-2", seq, st, err)
	}
}

// TestCheckpointNilAndEmptyStateEquivalent covers nil/empty equivalence
// and the no-copy isolation guarantee in both directions.
func TestCheckpointNilAndEmptyStateEquivalent(t *testing.T) {
	l := open(t, t.TempDir(), log.Options{SegmentBytes: 1 << 20})

	if err := l.AckConsumerState("nil", 0, nil); err != nil {
		t.Fatalf("nil register: %v", err)
	}
	if _, st, err := l.ConsumerCheckpoint("nil"); err != nil || len(st) != 0 {
		t.Fatalf("nil state = %v (%d), want empty", err, len(st))
	}
	if err := l.AckConsumerState("nil", 0, []byte{}); err != nil {
		t.Fatalf("empty repeat of nil must be the same state: %v", err)
	}
	if err := l.AckConsumerState("nil", 0, nil); err != nil {
		t.Fatalf("nil repeat: %v", err)
	}

	if err := l.AckConsumerState("emp", 0, []byte{}); err != nil {
		t.Fatalf("empty register: %v", err)
	}
	if err := l.AckConsumerState("emp", 0, nil); err != nil {
		t.Fatalf("nil repeat of empty must be the same state: %v", err)
	}

	// Mutating the caller's slice after the call must not change saved
	// state.
	in := []byte("abc")
	if err := l.AckConsumerState("iso", 0, in); err != nil {
		t.Fatalf("register: %v", err)
	}
	in[0] = 'Z'
	if _, st, _ := l.ConsumerCheckpoint("iso"); string(st) != "abc" {
		t.Fatalf("saved state changed via input alias: %q", st)
	}
	// Mutating the returned slice must not change saved state.
	_, out, _ := l.ConsumerCheckpoint("iso")
	out[0] = 'Q'
	if _, st, _ := l.ConsumerCheckpoint("iso"); string(st) != "abc" {
		t.Fatalf("saved state changed via output alias: %q", st)
	}
}

// TestCheckpointStateSizeLimit: exactly 1 MiB is accepted, one byte more
// is ErrInvalidCheckpointState and nothing is written.
func TestCheckpointStateSizeLimit(t *testing.T) {
	l := open(t, t.TempDir(), log.Options{SegmentBytes: 1 << 20})
	commit(t, l, []byte("one"))
	if err := l.AckConsumerState("c", 0, make([]byte, 1048576)); err != nil {
		t.Fatalf("1 MiB state: %v", err)
	}
	big := make([]byte, 1048576)
	big[0] = 'x'
	if err := l.AckConsumerState("c", 1, big); err != nil {
		t.Fatalf("1 MiB state on advance: %v", err)
	}
	tooBig := make([]byte, 1048577)
	if err := l.AckConsumerState("c", 1, tooBig); !errors.Is(err, log.ErrInvalidCheckpointState) {
		t.Fatalf("1 MiB+1 = %v, want ErrInvalidCheckpointState", err)
	}
	if seq, st, _ := l.ConsumerCheckpoint("c"); seq != 1 || len(st) != 1048576 {
		t.Fatalf("after oversize call = %d len=%d, want 1 / 1048576", seq, len(st))
	}
}

// TestCheckpointValidationOrder: name first, then state size, then
// registration/progress.
func TestCheckpointValidationOrder(t *testing.T) {
	l := open(t, t.TempDir(), log.Options{SegmentBytes: 1 << 20})
	commit(t, l, []byte("one"))
	huge := make([]byte, 1048577)

	// Bad name beats the oversized state.
	if err := l.AckConsumerState("", 1, huge); !errors.Is(err, log.ErrInvalidConsumer) {
		t.Fatalf("bad name + huge state = %v, want ErrInvalidConsumer", err)
	}
	if _, _, err := l.ConsumerCheckpoint(""); !errors.Is(err, log.ErrInvalidConsumer) {
		t.Fatalf("ConsumerCheckpoint bad name = %v, want ErrInvalidConsumer", err)
	}
	// Valid-but-unregistered name with a valid state and a non-zero seq
	// would be ErrUnknownConsumer; the size limit is checked first.
	if err := l.AckConsumerState("new", 1, huge); !errors.Is(err, log.ErrInvalidCheckpointState) {
		t.Fatalf("unregistered + huge state = %v, want ErrInvalidCheckpointState", err)
	}
	// Size fine, still unregistered, non-zero seq: unknown consumer.
	if err := l.AckConsumerState("new", 1, []byte("x")); !errors.Is(err, log.ErrUnknownConsumer) {
		t.Fatalf("unregistered non-zero = %v, want ErrUnknownConsumer", err)
	}
	// A legal zero registration saves the state.
	if err := l.AckConsumerState("new", 0, []byte("x")); err != nil {
		t.Fatalf("register: %v", err)
	}
}

// TestCheckpointSameSeqRules: identical state retried is a diskless
// no-op; different state at the same sequence is a conflict; a stateless
// AckConsumer at the current sequence stays a plain no-op and preserves
// the saved state.
func TestCheckpointSameSeqRules(t *testing.T) {
	dir := t.TempDir()
	l := open(t, dir, log.Options{SegmentBytes: 1 << 20})
	commit(t, l, []byte("one"))

	if err := l.AckConsumerState("c", 0, []byte("s0")); err != nil {
		t.Fatalf("register: %v", err)
	}
	// Re-registering the same sequence 0 with different state conflicts.
	if err := l.AckConsumerState("c", 0, []byte("other")); !errors.Is(err, log.ErrCheckpointConflict) {
		t.Fatalf("seq 0 different state = %v, want ErrCheckpointConflict", err)
	}
	if err := l.AckConsumerState("c", 0, []byte("s0")); err != nil {
		t.Fatalf("seq 0 same state: %v", err)
	}
	if err := l.AckConsumer("c", 0); err != nil {
		t.Fatalf("stateless repeat at 0: %v", err)
	}

	if err := l.AckConsumerState("c", 1, []byte("s1")); err != nil {
		t.Fatalf("advance: %v", err)
	}
	if err := l.AckConsumerState("c", 1, []byte("s1")); err != nil {
		t.Fatalf("same seq same state: %v", err)
	}
	if err := l.AckConsumerState("c", 1, []byte("s1-other")); !errors.Is(err, log.ErrCheckpointConflict) {
		t.Fatalf("same seq different state = %v, want ErrCheckpointConflict", err)
	}
	if err := l.AckConsumerState("c", 1, nil); !errors.Is(err, log.ErrCheckpointConflict) {
		t.Fatalf("same seq nil vs saved state = %v, want ErrCheckpointConflict", err)
	}
	// The stateless API still repeats at the current sequence and the
	// saved state survives.
	if err := l.AckConsumer("c", 1); err != nil {
		t.Fatalf("stateless repeat at 1: %v", err)
	}
	if seq, st, _ := l.ConsumerCheckpoint("c"); seq != 1 || string(st) != "s1" {
		t.Fatalf("checkpoint = %d %q, want 1 s1", seq, st)
	}
}

// TestAckConsumerAdvancePreservesState: progress made through the old
// API republishes (never clears) the state saved by the stateful API.
func TestAckConsumerAdvancePreservesState(t *testing.T) {
	dir := t.TempDir()
	opts := log.Options{SegmentBytes: 1 << 20}
	l := open(t, dir, opts)
	commit(t, l, []byte("one"))
	commit(t, l, []byte("two"))
	if err := l.AckConsumerState("c", 0, []byte("keep")); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := l.AckConsumer("c", 1); err != nil {
		t.Fatalf("stateless advance: %v", err)
	}
	if _, st, _ := l.ConsumerCheckpoint("c"); string(st) != "keep" {
		t.Fatalf("state after AckConsumer advance = %q, want keep", st)
	}
	l = reopen(t, l, dir, opts)
	if seq, st, err := l.ConsumerCheckpoint("c"); err != nil || seq != 1 || string(st) != "keep" {
		t.Fatalf("after reopen = %d %q %v; want 1 keep", seq, st, err)
	}
	// A consumer created entirely through AckConsumer reads empty state.
	if err := l.AckConsumer("plain", 0); err != nil {
		t.Fatalf("register plain: %v", err)
	}
	if err := l.AckConsumer("plain", 2); err != nil {
		t.Fatalf("advance plain: %v", err)
	}
	if seq, st, err := l.ConsumerCheckpoint("plain"); err != nil || seq != 2 || len(st) != 0 {
		t.Fatalf("plain = %d %v len=%d, want 2 empty", seq, err, len(st))
	}
}

// TestAckConsumerStateProgressRules mirrors AckConsumer's progress
// validation: rollback, uncommitted/hole/reclaimed targets and smaller
// staged batches are all ErrInvalidAck through the stateful API.
func TestAckConsumerStateProgressRules(t *testing.T) {
	dir := t.TempDir()
	opts := log.Options{SegmentBytes: 32, Sync: true}
	l := open(t, dir, opts)
	s1 := commit(t, l, []byte("one"))
	commit(t, l, []byte("two"))
	if err := l.AckConsumerState("c", 0, []byte("z")); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := l.AckConsumerState("c", s1, []byte("a")); err != nil {
		t.Fatalf("ack 1: %v", err)
	}
	if err := l.AckConsumerState("c", 0, []byte("back")); !errors.Is(err, log.ErrInvalidAck) {
		t.Fatalf("rollback = %v, want ErrInvalidAck", err)
	}
	if err := l.AckConsumerState("c", 99, []byte("x")); !errors.Is(err, log.ErrInvalidAck) {
		t.Fatalf("unknown target = %v, want ErrInvalidAck", err)
	}
	// Repeating the stored sequence with different state conflicts even
	// once that prefix has been reclaimed underneath the consumer.
	if _, err := l.DeleteThrough(1); err != nil {
		t.Fatalf("DeleteThrough: %v", err)
	}
	if err := l.AckConsumerState("c", 1, []byte("x")); !errors.Is(err, log.ErrCheckpointConflict) {
		t.Fatalf("same seq different state after reclaim = %v, want ErrCheckpointConflict", err)
	}

	// A lagging consumer confirming an already-reclaimed target gets
	// ErrInvalidAck: the fast consumer unlocks DeleteThrough while the
	// slow one is still at zero.
	lr := open(t, t.TempDir(), log.Options{SegmentBytes: 32, Sync: true})
	commit(t, lr, []byte("early"))
	commit(t, lr, []byte("later"))
	if err := lr.AckConsumer("fast", 0); err != nil {
		t.Fatalf("register fast: %v", err)
	}
	if err := lr.AckConsumer("fast", 1); err != nil {
		t.Fatalf("fast ack 1: %v", err)
	}
	if n, err := lr.DeleteThrough(1); err != nil || n != 1 {
		t.Fatalf("DeleteThrough(1) = %d, %v", n, err)
	}
	if err := lr.AckConsumerState("slow", 0, nil); err != nil {
		t.Fatalf("register slow: %v", err)
	}
	if err := lr.AckConsumerState("slow", 1, []byte("late")); !errors.Is(err, log.ErrInvalidAck) {
		t.Fatalf("lagging reclaimed target = %v, want ErrInvalidAck", err)
	}

	// Smaller staged batch blocks a stateful advance.
	l2 := open(t, t.TempDir(), log.Options{SegmentBytes: 1 << 20, Sync: true})
	commit(t, l2, []byte("a"))
	staged, err := l2.Append([][]byte{[]byte("stuck")})
	if err != nil {
		t.Fatal(err)
	}
	next, err := l2.Append([][]byte{[]byte("goes-first")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := l2.Commit(next); err != nil {
		t.Fatal(err)
	}
	if err := l2.AckConsumerState("w", 0, nil); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := l2.AckConsumerState("w", next.Seq(), []byte("x")); !errors.Is(err, log.ErrInvalidAck) {
		t.Fatalf("over staged = %v, want ErrInvalidAck", err)
	}
	if _, err := l2.Commit(staged); err != nil {
		t.Fatal(err)
	}
	if err := l2.AckConsumerState("w", next.Seq(), []byte("x")); err != nil {
		t.Fatalf("advance after staged commits: %v", err)
	}
}

// TestCheckpointDropClearsState: DropConsumer removes the state too, and
// a later registration starts fresh.
func TestCheckpointDropClearsState(t *testing.T) {
	dir := t.TempDir()
	opts := log.Options{SegmentBytes: 1 << 20}
	l := open(t, dir, opts)
	commit(t, l, []byte("one"))
	if err := l.AckConsumerState("c", 0, []byte("secret")); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := l.AckConsumerState("c", 1, []byte("secret-1")); err != nil {
		t.Fatalf("advance: %v", err)
	}
	if err := l.DropConsumer("c"); err != nil {
		t.Fatalf("drop: %v", err)
	}
	if _, _, err := l.ConsumerCheckpoint("c"); !errors.Is(err, log.ErrUnknownConsumer) {
		t.Fatalf("after drop = %v, want ErrUnknownConsumer", err)
	}
	if err := l.AckConsumerState("c", 0, []byte("fresh")); err != nil {
		t.Fatalf("re-register: %v", err)
	}
	l = reopen(t, l, dir, opts)
	if seq, st, err := l.ConsumerCheckpoint("c"); err != nil || seq != 0 || string(st) != "fresh" {
		t.Fatalf("after reopen = %d %q %v; want 0 fresh", seq, st, err)
	}
	if err := l.DropConsumer("c"); err != nil {
		t.Fatalf("drop 2: %v", err)
	}
	l = reopen(t, l, dir, opts)
	if _, _, err := l.ConsumerCheckpoint("c"); !errors.Is(err, log.ErrUnknownConsumer) {
		t.Fatalf("dropped state revived after reopen: %v", err)
	}
}

// TestCheckpointStateReopenMultipleConsumers checks the full sorted
// table image keeps every consumer's state independently.
func TestCheckpointStateReopenMultipleConsumers(t *testing.T) {
	dir := t.TempDir()
	opts := log.Options{SegmentBytes: 1 << 20}
	l := open(t, dir, opts)
	for i := 0; i < 3; i++ {
		commit(t, l, []byte("x"))
	}
	want := map[string]struct {
		seq   uint64
		state []byte
	}{
		"alpha": {2, []byte("A-A-A")},
		"beta":  {1, []byte("B")},
		"gamma": {3, []byte("g0")}, // AckConsumer advances preserve state
	}
	if err := l.AckConsumerState("alpha", 0, []byte("a0")); err != nil {
		t.Fatal(err)
	}
	if err := l.AckConsumerState("beta", 0, nil); err != nil {
		t.Fatal(err)
	}
	if err := l.AckConsumerState("gamma", 0, []byte("g0")); err != nil {
		t.Fatal(err)
	}
	if err := l.AckConsumerState("alpha", 2, want["alpha"].state); err != nil {
		t.Fatal(err)
	}
	if err := l.AckConsumerState("beta", 1, want["beta"].state); err != nil {
		t.Fatal(err)
	}
	if err := l.AckConsumer("gamma", 3); err != nil { // stateless clears nothing
		t.Fatal(err)
	}
	l = reopen(t, l, dir, opts)
	for name, w := range want {
		seq, st, err := l.ConsumerCheckpoint(name)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if seq != w.seq || !bytes.Equal(st, w.state) {
			t.Fatalf("%s = %d %q, want %d %q", name, seq, st, w.seq, w.state)
		}
	}
}
