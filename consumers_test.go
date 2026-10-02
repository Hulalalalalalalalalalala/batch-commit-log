package log_test

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	log "github.com/Hulalalalalalalalalalala/batch-commit-log"
)

func reopen(t *testing.T, l *log.Log, dir string, opts log.Options) *log.Log {
	t.Helper()
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	l2, err := log.Open(dir, opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { l2.Close() })
	return l2
}

func TestConsumerRegisterAndQuery(t *testing.T) {
	dir := t.TempDir()
	l := open(t, dir, log.Options{SegmentBytes: 1 << 20, Sync: true})

	// A legal but unregistered name is unknown, for every operation.
	if _, err := l.ConsumerSeq("a"); !errors.Is(err, log.ErrUnknownConsumer) {
		t.Fatalf("ConsumerSeq unknown = %v, want ErrUnknownConsumer", err)
	}
	if err := l.AckConsumer("a", 1); !errors.Is(err, log.ErrUnknownConsumer) {
		t.Fatalf("AckConsumer(a,1) = %v, want ErrUnknownConsumer", err)
	}
	if err := l.DropConsumer("a"); !errors.Is(err, log.ErrUnknownConsumer) {
		t.Fatalf("DropConsumer = %v, want ErrUnknownConsumer", err)
	}

	// Registration only happens through AckConsumer(name, 0).
	if err := l.AckConsumer("a", 0); err != nil {
		t.Fatalf("register: %v", err)
	}
	if got, err := l.ConsumerSeq("a"); err != nil || got != 0 {
		t.Fatalf("ConsumerSeq = %d, %v; want 0, nil", got, err)
	}
	if _, err := os.Stat(filepath.Join(dir, "consumers.idx")); err != nil {
		t.Fatalf("consumers.idx not persisted: %v", err)
	}

	// Illegal names, sharing the idempotency-key rules but not namespace.
	for _, bad := range []string{"", strings.Repeat("x", 257), "a\x00b", string([]byte{0xff, 0xfe})} {
		if err := l.AckConsumer(bad, 0); !errors.Is(err, log.ErrInvalidConsumer) {
			t.Fatalf("AckConsumer(%q) = %v, want ErrInvalidConsumer", bad, err)
		}
		if _, err := l.ConsumerSeq(bad); !errors.Is(err, log.ErrInvalidConsumer) {
			t.Fatalf("ConsumerSeq(%q) = %v, want ErrInvalidConsumer", bad, err)
		}
		if err := l.DropConsumer(bad); !errors.Is(err, log.ErrInvalidConsumer) {
			t.Fatalf("DropConsumer(%q) = %v, want ErrInvalidConsumer", bad, err)
		}
	}
}

func TestConsumerNamespaceIndependentFromKeys(t *testing.T) {
	dir := t.TempDir()
	l := open(t, dir, log.Options{SegmentBytes: 1 << 20, Sync: true})

	// The same string is legal as both an idempotency key and a consumer.
	b, err := l.AppendIdempotent("dup", [][]byte{[]byte("r")})
	if err != nil {
		t.Fatalf("AppendIdempotent: %v", err)
	}
	if _, err := l.Commit(b); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if err := l.AckConsumer("dup", 0); err != nil {
		t.Fatalf("AckConsumer(dup,0): %v", err)
	}
	if err := l.AckConsumer("dup", 1); err != nil {
		t.Fatalf("AckConsumer(dup,1): %v", err)
	}
	if got, err := l.ConsumerSeq("dup"); err != nil || got != 1 {
		t.Fatalf("ConsumerSeq = %d, %v; want 1", got, err)
	}
	// The keyed batch keeps its dedup semantics independently.
	if rb, err := l.AppendIdempotent("dup", [][]byte{[]byte("r")}); err != nil || rb.Seq() != 1 {
		t.Fatalf("idempotent retry = seq %d, %v; want 1, nil", rb.Seq(), err)
	}
}

func TestAckMonotonicAndRepeatedNoOp(t *testing.T) {
	dir := t.TempDir()
	l := open(t, dir, log.Options{SegmentBytes: 1 << 20, Sync: true})
	s1 := commit(t, l, []byte("a"))
	s2 := commit(t, l, []byte("b"))

	if err := l.AckConsumer("c", 0); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := l.AckConsumer("c", s1); err != nil {
		t.Fatalf("ack %d: %v", s1, err)
	}
	if got, _ := l.ConsumerSeq("c"); got != s1 {
		t.Fatalf("point = %d, want %d", got, s1)
	}
	// Repeating the current value succeeds.
	if err := l.AckConsumer("c", s1); err != nil {
		t.Fatalf("re-ack: %v", err)
	}
	// Moving backwards is refused.
	if err := l.AckConsumer("c", 0); !errors.Is(err, log.ErrInvalidAck) {
		t.Fatalf("rollback ack = %v, want ErrInvalidAck", err)
	}
	if err := l.AckConsumer("c", s2); err != nil {
		t.Fatalf("ack %d: %v", s2, err)
	}
}

func TestAckInvalidTargets(t *testing.T) {
	dir := t.TempDir()
	l := open(t, dir, log.Options{SegmentBytes: 1 << 20, Sync: true})
	if err := l.AckConsumer("c", 0); err != nil {
		t.Fatalf("register: %v", err)
	}

	// Unknown sequence.
	if err := l.AckConsumer("c", 1); !errors.Is(err, log.ErrInvalidAck) {
		t.Fatalf("ack unknown = %v, want ErrInvalidAck", err)
	}
	b1, err := l.Append([][]byte{[]byte("a")})
	if err != nil {
		t.Fatal(err)
	}
	// Staged target.
	if err := l.AckConsumer("c", b1.Seq()); !errors.Is(err, log.ErrInvalidAck) {
		t.Fatalf("ack staged = %v, want ErrInvalidAck", err)
	}
	if _, err := l.Commit(b1); err != nil {
		t.Fatal(err)
	}
	if err := l.AckConsumer("c", 1); err != nil {
		t.Fatalf("ack committed: %v", err)
	}
	// Beyond the reserved range.
	if err := l.AckConsumer("c", 99); !errors.Is(err, log.ErrInvalidAck) {
		t.Fatalf("ack beyond = %v, want ErrInvalidAck", err)
	}

	// After reclaim, repeating the current point is still the cheap
	// no-op success; advancing into the reclaimed prefix is refused.
	l2 := reopen(t, l, dir, log.Options{SegmentBytes: 1 << 20, Sync: true})
	if n, err := l2.DeleteThrough(1); err != nil || n != 1 {
		t.Fatalf("DeleteThrough = %d, %v", n, err)
	}
	if err := l2.AckConsumer("c", 1); err != nil {
		t.Fatalf("repeat current point = %v, want nil no-op", err)
	}
}

func TestAckStagedBeforeBlocksHoleBeforeAllowed(t *testing.T) {
	dir := t.TempDir()
	opts := log.Options{SegmentBytes: 1 << 20, Sync: true}
	l := open(t, dir, opts)
	commit(t, l, []byte("one")) // seq 1
	l.Close()

	// A torn entry reserving seq 2 becomes a permanent hole on reopen.
	appendTornEntry(t, dir, 2)
	l = open(t, dir, opts)
	if err := l.AckConsumer("c", 0); err != nil {
		t.Fatalf("register: %v", err)
	}
	b3, err := l.Append([][]byte{[]byte("three")})
	if err != nil {
		t.Fatal(err)
	}
	if b3.Seq() != 3 {
		t.Fatalf("staged seq = %d, want 3", b3.Seq())
	}
	// seq 3 is merely staged, so it cannot anchor an ack.
	if err := l.AckConsumer("c", 3); !errors.Is(err, log.ErrInvalidAck) {
		t.Fatalf("ack staged target = %v, want ErrInvalidAck", err)
	}
	b4, err := l.Append([][]byte{[]byte("four")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := l.Commit(b3); err != nil {
		t.Fatal(err)
	}
	// Batch 4 is staged *after* the target: the processed prefix ends at
	// 3, so this advance is fine even though the permanent hole at 2 is
	// crossed.
	if err := l.AckConsumer("c", 3); err != nil {
		t.Fatalf("ack 3 over hole with 4 staged after = %v", err)
	}
	// ...but 4 itself cannot be acknowledged while staged.
	if err := l.AckConsumer("c", 4); !errors.Is(err, log.ErrInvalidAck) {
		t.Fatalf("ack staged 4 = %v, want ErrInvalidAck", err)
	}
	if _, err := l.Commit(b4); err != nil {
		t.Fatal(err)
	}

	// Commit out of order: stage 5, stage 6, commit 6 first. A staged
	// batch (5) still sitting below the target (6) blocks the ack.
	b5, err := l.Append([][]byte{[]byte("five")})
	if err != nil {
		t.Fatal(err)
	}
	b6, err := l.Append([][]byte{[]byte("six")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := l.Commit(b6); err != nil {
		t.Fatal(err)
	}
	if err := l.AckConsumer("c", 6); !errors.Is(err, log.ErrInvalidAck) {
		t.Fatalf("ack 6 with 5 staged = %v, want ErrInvalidAck", err)
	}
	if _, err := l.Commit(b5); err != nil {
		t.Fatal(err)
	}
	if err := l.AckConsumer("c", 6); err != nil {
		t.Fatalf("ack 6 after 5 committed = %v", err)
	}
	if got, _ := l.ConsumerSeq("c"); got != 6 {
		t.Fatalf("point = %d, want 6", got)
	}
}

func TestAckInsideGroup(t *testing.T) {
	dir := t.TempDir()
	l := open(t, dir, log.Options{SegmentBytes: 1 << 20, Sync: true})
	var bs []log.Batch
	for _, s := range []string{"g1", "g2", "g3"} {
		b, _ := l.Append([][]byte{[]byte(s)})
		bs = append(bs, b)
	}
	if _, err := l.CommitGroup(bs); err != nil {
		t.Fatalf("CommitGroup: %v", err)
	}
	if err := l.AckConsumer("c", 0); err != nil {
		t.Fatal(err)
	}
	// A group member in the middle is a valid prefix point.
	if err := l.AckConsumer("c", 2); err != nil {
		t.Fatalf("ack group member = %v", err)
	}
	if got, _ := l.ConsumerSeq("c"); got != 2 {
		t.Fatalf("point = %d, want 2", got)
	}
}

func TestConsumerScanFromAckAndRestart(t *testing.T) {
	dir := t.TempDir()
	opts := log.Options{SegmentBytes: 32, Sync: true}
	l := open(t, dir, opts)
	for _, s := range []string{"a", "b", "c", "d"} {
		commit(t, l, []byte(s))
	}
	if err := l.AckConsumer("w1", 0); err != nil {
		t.Fatal(err)
	}
	if err := l.AckConsumer("w2", 0); err != nil {
		t.Fatal(err)
	}
	if err := l.AckConsumer("w1", 2); err != nil {
		t.Fatal(err)
	}

	// Reopen: both consumers and their points survive.
	l = reopen(t, l, dir, opts)
	if got, err := l.ConsumerSeq("w1"); err != nil || got != 2 {
		t.Fatalf("w1 = %d, %v; want 2", got, err)
	}
	if got, err := l.ConsumerSeq("w2"); err != nil || got != 0 {
		t.Fatalf("w2 = %d, %v; want 0", got, err)
	}

	// Each consumer replays from point+1.
	var seen []uint64
	start, _ := l.ConsumerSeq("w1")
	if err := l.Scan(start+1, func(b log.Batch) error {
		seen = append(seen, b.Seq())
		return nil
	}); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if !reflect.DeepEqual(seen, []uint64{3, 4}) {
		t.Fatalf("w1 scan = %v, want [3 4]", seen)
	}

	// Deleting the rebuildable index cache must not lose checkpoints.
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, "index.idx")); err != nil {
		t.Fatal(err)
	}
	l2, err := log.Open(dir, opts)
	if err != nil {
		t.Fatalf("open after index removal: %v", err)
	}
	t.Cleanup(func() { l2.Close() })
	if got, err := l2.ConsumerSeq("w1"); err != nil || got != 2 {
		t.Fatalf("w1 after index rebuild = %d, %v; want 2", got, err)
	}
}

func TestDropConsumer(t *testing.T) {
	dir := t.TempDir()
	opts := log.Options{SegmentBytes: 1 << 20, Sync: true}
	l := open(t, dir, opts)
	commit(t, l, []byte("a"))
	if err := l.AckConsumer("c", 0); err != nil {
		t.Fatal(err)
	}
	if err := l.AckConsumer("c", 1); err != nil {
		t.Fatal(err)
	}
	if err := l.DropConsumer("c"); err != nil {
		t.Fatalf("drop: %v", err)
	}
	if _, err := l.ConsumerSeq("c"); !errors.Is(err, log.ErrUnknownConsumer) {
		t.Fatalf("after drop = %v, want ErrUnknownConsumer", err)
	}
	if err := l.DropConsumer("c"); !errors.Is(err, log.ErrUnknownConsumer) {
		t.Fatalf("drop again = %v, want ErrUnknownConsumer", err)
	}

	// Dropping is durable: no resurrection on reopen, re-register from 0.
	l = reopen(t, l, dir, opts)
	if _, err := l.ConsumerSeq("c"); !errors.Is(err, log.ErrUnknownConsumer) {
		t.Fatalf("reopen = %v, want ErrUnknownConsumer", err)
	}
	if err := l.AckConsumer("c", 0); err != nil {
		t.Fatal(err)
	}
	if got, _ := l.ConsumerSeq("c"); got != 0 {
		t.Fatalf("re-registered point = %d, want 0", got)
	}
}

func TestDeleteThroughBlockedByConsumer(t *testing.T) {
	dir := t.TempDir()
	opts := log.Options{SegmentBytes: 32, Sync: true}
	l := open(t, dir, opts)
	commit(t, l, []byte("one"))
	commit(t, l, []byte("two")) // two segments, first fully deletable at seq 1
	if err := l.AckConsumer("slow", 0); err != nil {
		t.Fatal(err)
	}
	if err := l.AckConsumer("fast", 0); err != nil {
		t.Fatal(err)
	}
	if err := l.AckConsumer("fast", 1); err != nil {
		t.Fatal(err)
	}

	before, _ := filepath.Glob(filepath.Join(dir, "*"))
	n, err := l.DeleteThrough(1)
	if !errors.Is(err, log.ErrRetentionBlocked) {
		t.Fatalf("DeleteThrough = %d, %v; want ErrRetentionBlocked", n, err)
	}
	if n != 0 {
		t.Fatalf("deleted = %d, want 0", n)
	}
	after, _ := filepath.Glob(filepath.Join(dir, "*"))
	sort.Strings(before)
	sort.Strings(after)
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("directory changed: before=%v after=%v", before, after)
	}
	if _, err := os.Stat(filepath.Join(dir, "truncate.idx")); !os.IsNotExist(err) {
		t.Fatalf("truncate.idx should not exist, stat = %v", err)
	}
	if got, err := l.Read(1); err != nil || string(got[0]) != "one" {
		t.Fatalf("Read(1) = %q, %v; want untouched", got, err)
	}

	// Once the slow consumer catches up, the original reclaim rule runs.
	if err := l.AckConsumer("slow", 1); err != nil {
		t.Fatal(err)
	}
	n, err = l.DeleteThrough(1)
	if err != nil || n != 1 {
		t.Fatalf("DeleteThrough after ack = %d, %v; want 1, nil", n, err)
	}
	if segs := l.Segments(); len(segs) != 1 {
		t.Fatalf("segments = %+v, want 1", segs)
	}
}

func TestDropConsumerUnblocksRetentionAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	opts := log.Options{SegmentBytes: 32, Sync: true}
	l := open(t, dir, opts)
	commit(t, l, []byte("one"))
	commit(t, l, []byte("two"))
	if err := l.AckConsumer("c", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := l.DeleteThrough(1); !errors.Is(err, log.ErrRetentionBlocked) {
		t.Fatalf("expected blocked")
	}
	if err := l.DropConsumer("c"); err != nil {
		t.Fatal(err)
	}
	l = reopen(t, l, dir, opts)
	n, err := l.DeleteThrough(1)
	if err != nil || n != 1 {
		t.Fatalf("DeleteThrough after drop+reopen = %d, %v; want 1, nil", n, err)
	}
}

func TestNoConsumersKeepsLegacyRetention(t *testing.T) {
	dir := t.TempDir()
	l := open(t, dir, log.Options{SegmentBytes: 32, Sync: true})
	commit(t, l, []byte("one"))
	commit(t, l, []byte("two"))
	// No consumer registered at all: original semantics, including a
	// mid-segment point that deletes nothing.
	n, err := l.DeleteThrough(1)
	if err != nil || n != 1 {
		t.Fatalf("DeleteThrough = %d, %v; want 1, nil", n, err)
	}
}

func TestAckAtTruncationPointInKeptSegment(t *testing.T) {
	// seg 1 holds seq 1; seg 2 holds a group with seqs 2,3,4. Deleting
	// through 3 removes only seg 1 (the group's segment overhangs), so
	// seq 3 is at the persisted truncation point yet still indexed and
	// readable: acknowledging it must succeed.
	dir := t.TempDir()
	l := open(t, dir, log.Options{SegmentBytes: 64, Sync: true})
	commit(t, l, []byte("one"))
	var bs []log.Batch
	for _, s := range []string{"g1", "g2", "g3"} {
		b, _ := l.Append([][]byte{[]byte(s)})
		bs = append(bs, b)
	}
	if _, err := l.CommitGroup(bs); err != nil {
		t.Fatalf("CommitGroup: %v", err)
	}
	if err := l.AckConsumer("c", 0); err != nil {
		t.Fatal(err)
	}
	if err := l.AckConsumer("c", 1); err != nil {
		t.Fatal(err)
	}
	n, err := l.DeleteThrough(3)
	if err != nil || n != 1 {
		t.Fatalf("DeleteThrough(3) = %d, %v; want 1, nil", n, err)
	}
	if err := l.AckConsumer("c", 3); err != nil {
		t.Fatalf("ack retained member at truncation point: %v", err)
	}
	if got, _ := l.ConsumerSeq("c"); got != 3 {
		t.Fatalf("point = %d, want 3", got)
	}
	if err := l.AckConsumer("c", 3); err != nil {
		t.Fatalf("repeat ack = %v", err)
	}
	// Restart keeps the point together with the retention marker.
	l = reopen(t, l, dir, log.Options{SegmentBytes: 64, Sync: true})
	if got, _ := l.ConsumerSeq("c"); got != 3 {
		t.Fatalf("point after reopen = %d, want 3", got)
	}
	if _, err := l.Read(1); !errors.Is(err, log.ErrTruncated) {
		t.Fatalf("Read(1) = %v, want ErrTruncated", err)
	}
	if got, err := l.Read(3); err != nil || string(got[0]) != "g2" {
		t.Fatalf("Read(3) = %q, %v", got, err)
	}
}

func TestCorruptCheckpointFailsOpen(t *testing.T) {
	dir := t.TempDir()
	opts := log.Options{SegmentBytes: 1 << 20, Sync: true}
	l := open(t, dir, opts)
	commit(t, l, []byte("a"))
	if err := l.AckConsumer("c", 0); err != nil {
		t.Fatal(err)
	}
	if err := l.AckConsumer("c", 1); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(dir, "consumers.idx")
	pristine, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	corrupt := func(mutate func([]byte) []byte) {
		t.Helper()
		bad := mutate(append([]byte(nil), pristine...))
		if err := os.WriteFile(path, bad, 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := log.Open(dir, opts); !errors.Is(err, log.ErrCorruptCheckpoint) {
			t.Fatalf("Open = %v, want ErrCorruptCheckpoint", err)
		}
	}
	corrupt(func(b []byte) []byte { return b[:len(b)-1] })              // truncated
	corrupt(func(b []byte) []byte { b[len(b)-1] ^= 0xff; return b })    // bad crc
	corrupt(func(b []byte) []byte { b[6] ^= 0xff; return b })           // bad version byte
	corrupt(func(b []byte) []byte { return append(b, 0) })              // trailing byte
	corrupt(func(b []byte) []byte { return []byte("garbage garbage") }) // foreign

	// A missing checkpoint is the historical directory: opens with no
	// consumers, progress is not invented.
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	l2, err := log.Open(dir, opts)
	if err != nil {
		t.Fatalf("Open without checkpoint: %v", err)
	}
	defer l2.Close()
	if _, err := l2.ConsumerSeq("c"); !errors.Is(err, log.ErrUnknownConsumer) {
		t.Fatalf("ConsumerSeq = %v, want ErrUnknownConsumer", err)
	}
	if got, err := l2.Read(1); err != nil || string(got[0]) != "a" {
		t.Fatalf("Read(1) = %q, %v", got, err)
	}
}
