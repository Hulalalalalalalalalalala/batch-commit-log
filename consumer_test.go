package log_test

import (
	"encoding/binary"
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
	return open(t, dir, opts)
}

func TestConsumerRegistrationAndLookup(t *testing.T) {
	l := open(t, t.TempDir(), log.Options{SegmentBytes: 1 << 20})

	if _, err := l.ConsumerSeq("svc-a"); !errors.Is(err, log.ErrUnknownConsumer) {
		t.Fatalf("ConsumerSeq unknown = %v, want ErrUnknownConsumer", err)
	}
	if err := l.DropConsumer("svc-a"); !errors.Is(err, log.ErrUnknownConsumer) {
		t.Fatalf("DropConsumer unknown = %v, want ErrUnknownConsumer", err)
	}
	if err := l.AckConsumer("svc-a", 1); !errors.Is(err, log.ErrUnknownConsumer) {
		t.Fatalf("AckConsumer new name at 1 = %v, want ErrUnknownConsumer", err)
	}
	if err := l.AckConsumer("svc-a", 0); err != nil {
		t.Fatalf("AckConsumer register: %v", err)
	}
	if got, err := l.ConsumerSeq("svc-a"); err != nil || got != 0 {
		t.Fatalf("ConsumerSeq after register = %d, %v; want 0", got, err)
	}
	// Registering again with 0 is the same value: success, no progress.
	if err := l.AckConsumer("svc-a", 0); err != nil {
		t.Fatalf("repeat AckConsumer(0): %v", err)
	}
	if got, _ := l.ConsumerSeq("svc-a"); got != 0 {
		t.Fatalf("seq = %d, want 0", got)
	}
}

func TestConsumerInvalidNames(t *testing.T) {
	l := open(t, t.TempDir(), log.Options{SegmentBytes: 1 << 20})
	bad := []string{
		"",
		"has\x00nul",
		string([]byte{0xff, 0xfe}), // invalid UTF-8
		strings.Repeat("x", 257),
	}
	for _, name := range bad {
		if err := l.AckConsumer(name, 0); !errors.Is(err, log.ErrInvalidConsumer) {
			t.Errorf("AckConsumer(%q) = %v, want ErrInvalidConsumer", name, err)
		}
		if _, err := l.ConsumerSeq(name); !errors.Is(err, log.ErrInvalidConsumer) {
			t.Errorf("ConsumerSeq(%q) = %v, want ErrInvalidConsumer", name, err)
		}
		if err := l.DropConsumer(name); !errors.Is(err, log.ErrInvalidConsumer) {
			t.Errorf("DropConsumer(%q) = %v, want ErrInvalidConsumer", name, err)
		}
	}
}

func TestConsumerNamespaceIndependentFromIdempotencyKeys(t *testing.T) {
	l := open(t, t.TempDir(), log.Options{SegmentBytes: 1 << 20, Sync: true})
	b, err := l.AppendIdempotent("dup-name", [][]byte{[]byte("r")})
	if err != nil {
		t.Fatalf("AppendIdempotent: %v", err)
	}
	if _, err := l.Commit(b); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	// The same string is independently a legal, initially unknown
	// consumer name.
	if _, err := l.ConsumerSeq("dup-name"); !errors.Is(err, log.ErrUnknownConsumer) {
		t.Fatalf("ConsumerSeq = %v, want ErrUnknownConsumer", err)
	}
	if err := l.AckConsumer("dup-name", 1); !errors.Is(err, log.ErrUnknownConsumer) {
		t.Fatalf("Ack before register = %v, want ErrUnknownConsumer", err)
	}
	if err := l.AckConsumer("dup-name", 0); err != nil {
		t.Fatalf("register consumer: %v", err)
	}
	if err := l.AckConsumer("dup-name", 1); err != nil {
		t.Fatalf("AckConsumer(1): %v", err)
	}
}

func TestAckAdvanceRules(t *testing.T) {
	dir := t.TempDir()
	l := open(t, dir, log.Options{SegmentBytes: 1 << 20, Sync: true})
	s1 := commit(t, l, []byte("one"))
	if s1 != 1 {
		t.Fatalf("seq = %d, want 1", s1)
	}
	if err := l.AckConsumer("c", 0); err != nil {
		t.Fatalf("register: %v", err)
	}

	if err := l.AckConsumer("c", 42); !errors.Is(err, log.ErrInvalidAck) {
		t.Fatalf("ack unknown seq = %v, want ErrInvalidAck", err)
	}
	if err := l.AckConsumer("c", 1); err != nil {
		t.Fatalf("ack 1: %v", err)
	}
	if got, _ := l.ConsumerSeq("c"); got != 1 {
		t.Fatalf("seq = %d, want 1", got)
	}
	// Repeating the current value succeeds.
	if err := l.AckConsumer("c", 1); err != nil {
		t.Fatalf("repeat ack 1: %v", err)
	}
	// Checkpoints never move backwards, not even to zero.
	if err := l.AckConsumer("c", 0); !errors.Is(err, log.ErrInvalidAck) {
		t.Fatalf("rollback to 0 = %v, want ErrInvalidAck", err)
	}

	// A staged batch with a lower sequence than the target blocks the
	// prefix claim until it commits. Commit 3 first while 2 stays staged.
	b2, err := l.Append([][]byte{[]byte("two")})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	b3, err := l.Append([][]byte{[]byte("three")})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if _, err := l.Commit(b3); err != nil {
		t.Fatalf("Commit b3: %v", err)
	}
	if err := l.AckConsumer("c", 3); !errors.Is(err, log.ErrInvalidAck) {
		t.Fatalf("ack 3 past staged 2 = %v, want ErrInvalidAck", err)
	}
	if _, err := l.Commit(b2); err != nil {
		t.Fatalf("Commit b2: %v", err)
	}
	if err := l.AckConsumer("c", 3); err != nil {
		t.Fatalf("ack 3 after 2 commits: %v", err)
	}
}

func TestAckCrossesPermanentHoleAndConfirmsGroup(t *testing.T) {
	dir := t.TempDir()
	opts := log.Options{SegmentBytes: 1 << 20}
	l := open(t, dir, opts)
	commit(t, l, []byte("one")) // seq 1
	l.Close()

	// Tear seq 2 into a permanent hole; reopen and commit seq 3.
	appendTornEntry(t, dir, 2)
	l = open(t, dir, opts)
	b3, err := l.Append([][]byte{[]byte("three")})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if b3.Seq() != 3 {
		t.Fatalf("seq = %d, want 3", b3.Seq())
	}
	if _, err := l.Commit(b3); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if err := l.AckConsumer("c", 0); err != nil {
		t.Fatalf("register: %v", err)
	}
	// The hole itself cannot anchor a prefix.
	if err := l.AckConsumer("c", 2); !errors.Is(err, log.ErrInvalidAck) {
		t.Fatalf("ack hole = %v, want ErrInvalidAck", err)
	}
	// A committed target past the permanent hole confirms the prefix.
	if err := l.AckConsumer("c", 3); err != nil {
		t.Fatalf("ack across hole: %v", err)
	}

	// A whole group may be confirmed by its highest member.
	g1, _ := l.Append([][]byte{[]byte("g1")})
	g2, _ := l.Append([][]byte{[]byte("g2")})
	seqs, err := l.CommitGroup([]log.Batch{g1, g2})
	if err != nil {
		t.Fatalf("CommitGroup: %v", err)
	}
	if !reflect.DeepEqual(seqs, []uint64{4, 5}) {
		t.Fatalf("group seqs = %v, want [4 5]", seqs)
	}
	// A member in the middle of the group is itself a committed prefix
	// anchor; the consumer is free to stop there.
	if err := l.AckConsumer("mid", 0); err != nil {
		t.Fatalf("register mid: %v", err)
	}
	if err := l.AckConsumer("mid", 4); err != nil {
		t.Fatalf("ack mid-group member 4: %v", err)
	}
	if got, _ := l.ConsumerSeq("mid"); got != 4 {
		t.Fatalf("mid seq = %d, want 4", got)
	}
	if err := l.AckConsumer("c", 5); err != nil {
		t.Fatalf("ack group end: %v", err)
	}
	if got, _ := l.ConsumerSeq("c"); got != 5 {
		t.Fatalf("seq = %d, want 5", got)
	}
}

func TestAckReclaimedTargetRejected(t *testing.T) {
	dir := t.TempDir()
	l := open(t, dir, log.Options{SegmentBytes: 32, Sync: true})
	commit(t, l, []byte("one"))
	commit(t, l, []byte("two"))
	n, err := l.DeleteThrough(1)
	if err != nil || n != 1 {
		t.Fatalf("DeleteThrough = %d, %v", n, err)
	}
	if err := l.AckConsumer("c", 0); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := l.AckConsumer("c", 1); !errors.Is(err, log.ErrInvalidAck) {
		t.Fatalf("ack reclaimed = %v, want ErrInvalidAck", err)
	}
	if err := l.AckConsumer("c", 2); err != nil {
		t.Fatalf("ack retained target: %v", err)
	}
}

func TestConsumerScanFromConfirmedPlusOne(t *testing.T) {
	l := open(t, t.TempDir(), log.Options{SegmentBytes: 1 << 20, Sync: true})
	for _, body := range []string{"a", "b", "c"} {
		commit(t, l, []byte(body))
	}
	if err := l.AckConsumer("worker", 0); err != nil {
		t.Fatalf("register: %v", err)
	}
	var got []string
	from, err := l.ConsumerSeq("worker")
	if err != nil {
		t.Fatalf("ConsumerSeq: %v", err)
	}
	if err := l.Scan(from+1, func(b log.Batch) error {
		got = append(got, string(b.Records()[0]))
		return nil
	}); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if !reflect.DeepEqual(got, []string{"a", "b", "c"}) {
		t.Fatalf("first scan = %v, want a b c", got)
	}
	if err := l.AckConsumer("worker", 2); err != nil {
		t.Fatalf("ack 2: %v", err)
	}
	got = nil
	from, _ = l.ConsumerSeq("worker")
	if err := l.Scan(from+1, func(b log.Batch) error {
		got = append(got, string(b.Records()[0]))
		return nil
	}); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if !reflect.DeepEqual(got, []string{"c"}) {
		t.Fatalf("second scan = %v, want [c]", got)
	}
}

func TestConsumerCheckpointPersistsAcrossReopenAndIndexLoss(t *testing.T) {
	dir := t.TempDir()
	opts := log.Options{SegmentBytes: 1 << 20}
	l := open(t, dir, opts)
	commit(t, l, []byte("one"))
	commit(t, l, []byte("two"))
	if err := l.AckConsumer("alpha", 0); err != nil {
		t.Fatalf("register alpha: %v", err)
	}
	if err := l.AckConsumer("beta", 0); err != nil {
		t.Fatalf("register beta: %v", err)
	}
	if err := l.AckConsumer("beta", 2); err != nil {
		t.Fatalf("ack beta: %v", err)
	}
	if err := l.AckConsumer("alpha", 1); err != nil {
		t.Fatalf("ack alpha: %v", err)
	}

	// Plain reopen keeps every value.
	l = reopen(t, l, dir, opts)
	if got, err := l.ConsumerSeq("alpha"); err != nil || got != 1 {
		t.Fatalf("alpha after reopen = %d, %v", got, err)
	}
	if got, err := l.ConsumerSeq("beta"); err != nil || got != 2 {
		t.Fatalf("beta after reopen = %d, %v", got, err)
	}

	// Deleting the rebuildable index sidecar must not lose checkpoints:
	// consumers.idx is authoritative metadata.
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := os.Remove(filepath.Join(dir, "index.idx")); err != nil {
		t.Fatalf("remove index: %v", err)
	}
	l = open(t, dir, opts)
	if got, _ := l.ConsumerSeq("alpha"); got != 1 {
		t.Fatalf("alpha after index loss = %d, want 1", got)
	}
	if got, _ := l.ConsumerSeq("beta"); got != 2 {
		t.Fatalf("beta after index loss = %d, want 2", got)
	}
}

func TestOldDirectoryWithoutConsumerMetadata(t *testing.T) {
	dir := t.TempDir()
	opts := log.Options{SegmentBytes: 1 << 20}
	l := open(t, dir, opts)
	commit(t, l, []byte("one"))
	l = reopen(t, l, dir, opts)
	if _, err := l.ConsumerSeq("any"); !errors.Is(err, log.ErrUnknownConsumer) {
		t.Fatalf("ConsumerSeq = %v, want ErrUnknownConsumer", err)
	}
	if entries, _ := filepath.Glob(filepath.Join(dir, "consumers.idx")); len(entries) != 0 {
		t.Fatalf("consumers.idx appeared without a consumer: %v", entries)
	}
}

func TestDropConsumerReleasesRegistration(t *testing.T) {
	dir := t.TempDir()
	opts := log.Options{SegmentBytes: 1 << 20}
	l := open(t, dir, opts)
	commit(t, l, []byte("one"))
	if err := l.AckConsumer("c", 0); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := l.AckConsumer("c", 1); err != nil {
		t.Fatalf("AckConsumer: %v", err)
	}
	if err := l.DropConsumer("c"); err != nil {
		t.Fatalf("DropConsumer: %v", err)
	}
	if _, err := l.ConsumerSeq("c"); !errors.Is(err, log.ErrUnknownConsumer) {
		t.Fatalf("ConsumerSeq after drop = %v, want ErrUnknownConsumer", err)
	}
	if err := l.DropConsumer("c"); !errors.Is(err, log.ErrUnknownConsumer) {
		t.Fatalf("drop again = %v, want ErrUnknownConsumer", err)
	}
	// Re-registering starts from zero.
	if err := l.AckConsumer("c", 0); err != nil {
		t.Fatalf("re-register: %v", err)
	}
	if got, _ := l.ConsumerSeq("c"); got != 0 {
		t.Fatalf("re-registered seq = %d, want 0", got)
	}
	if err := l.AckConsumer("c", 1); err != nil {
		t.Fatalf("ack after re-register: %v", err)
	}
	// A reopen must not revive the dropped registration nor lose the new
	// one.
	l = reopen(t, l, dir, opts)
	if got, err := l.ConsumerSeq("c"); err != nil || got != 1 {
		t.Fatalf("after reopen = %d, %v; want 1", got, err)
	}
	if err := l.DropConsumer("c"); err != nil {
		t.Fatalf("drop: %v", err)
	}
	l = reopen(t, l, dir, opts)
	if _, err := l.ConsumerSeq("c"); !errors.Is(err, log.ErrUnknownConsumer) {
		t.Fatalf("dropped consumer revived by reopen: %v", err)
	}
}

func TestDeleteThroughBlockedUntilAllConsumersConfirm(t *testing.T) {
	dir := t.TempDir()
	opts := log.Options{SegmentBytes: 32, Sync: true}
	l := open(t, dir, opts)
	commit(t, l, []byte("one"))
	commit(t, l, []byte("two"))
	commit(t, l, []byte("three"))

	listSegs := func() []string {
		files, err := filepath.Glob(filepath.Join(dir, "*.seg"))
		if err != nil {
			t.Fatal(err)
		}
		sort.Strings(files)
		return files
	}
	before := listSegs()
	if len(before) != 3 {
		t.Fatalf("segments = %v, want 3", before)
	}

	if err := l.AckConsumer("slow", 0); err != nil {
		t.Fatalf("register slow: %v", err)
	}
	if err := l.AckConsumer("fast", 0); err != nil {
		t.Fatalf("register fast: %v", err)
	}
	if err := l.AckConsumer("fast", 3); err != nil {
		t.Fatalf("fast ack: %v", err)
	}

	// The fast consumer alone does not unlock the prefix: slow is still
	// at 0 and segment 1 carries batch 1.
	n, err := l.DeleteThrough(1)
	if !errors.Is(err, log.ErrRetentionBlocked) {
		t.Fatalf("DeleteThrough = %d, %v; want ErrRetentionBlocked", n, err)
	}
	if after := listSegs(); !reflect.DeepEqual(after, before) {
		t.Fatalf("files changed on block: before=%v after=%v", before, after)
	}
	// Data, index and idempotency keys are untouched: reads still work.
	if got, err := l.Read(1); err != nil || string(got[0]) != "one" {
		t.Fatalf("Read(1) after block = %q, %v", got, err)
	}

	// Once the lagging consumer confirms, the same call reclaims.
	if err := l.AckConsumer("slow", 1); err != nil {
		t.Fatalf("slow ack: %v", err)
	}
	n, err = l.DeleteThrough(1)
	if err != nil || n != 1 {
		t.Fatalf("DeleteThrough after confirm = %d, %v", n, err)
	}
	if len(listSegs()) != 2 {
		t.Fatalf("segments after delete = %v, want 2", listSegs())
	}

	// Prefix up through 3 still contains batches above both checkpoints;
	// advancing fast only is not enough.
	n, err = l.DeleteThrough(3)
	if !errors.Is(err, log.ErrRetentionBlocked) {
		t.Fatalf("DeleteThrough(3) = %d, %v; want ErrRetentionBlocked", n, err)
	}
	// Dropping the lagging consumer removes its protection.
	if err := l.DropConsumer("slow"); err != nil {
		t.Fatalf("DropConsumer: %v", err)
	}
	n, err = l.DeleteThrough(3)
	if err != nil || n != 2 {
		t.Fatalf("DeleteThrough after drop = %d, %v; want 2", n, err)
	}
	if len(listSegs()) != 0 {
		t.Fatalf("segments = %v, want none", listSegs())
	}
}

func TestDeleteThroughValidationUnchangedWithConsumers(t *testing.T) {
	l := open(t, t.TempDir(), log.Options{SegmentBytes: 32, Sync: true})
	commit(t, l, []byte("one"))
	if err := l.AckConsumer("c", 0); err != nil {
		t.Fatalf("register: %v", err)
	}
	// seq 0 remains a no-op returning zero.
	if n, err := l.DeleteThrough(0); err != nil || n != 0 {
		t.Fatalf("DeleteThrough(0) = %d, %v", n, err)
	}
	// An unknown anchor keeps returning ErrInvalidRetention even though a
	// consumer protects nothing there.
	if _, err := l.DeleteThrough(99); !errors.Is(err, log.ErrInvalidRetention) {
		t.Fatalf("DeleteThrough(99) = %v, want ErrInvalidRetention", err)
	}
}

func TestDeleteThroughHoleOnlySegmentNotProtected(t *testing.T) {
	dir := t.TempDir()
	// A capacity of one byte makes the first commit after tail recovery
	// roll to segment 2, leaving segment 1 hole-only.
	opts := log.Options{SegmentBytes: 1}
	open(t, dir, opts).Close()
	// seq 1 dies in a torn first entry in a manually created segment 1:
	// the segment becomes hole-only on reopen, and the first successful
	// batch lands in segment 2 at seq 2.
	var entry []byte
	var tmp [8]byte
	entry = append(entry, 'B', 'C', 'L', '1')
	binary.LittleEndian.PutUint64(tmp[:], 1)
	entry = append(entry, tmp[:]...)
	binary.LittleEndian.PutUint32(tmp[:4], 1)
	entry = append(entry, tmp[:4]...)
	binary.LittleEndian.PutUint32(tmp[:4], 100)
	entry = append(entry, tmp[:4]...)
	entry = append(entry, []byte("only-a-prefix-of-the-record")...)
	f, err := os.OpenFile(filepath.Join(dir, "000001.seg"), os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(entry); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	l := open(t, dir, opts)
	if seq := commit(t, l, []byte("two")); seq != 2 {
		t.Fatalf("seq = %d, want 2", seq)
	}
	// A consumer that has confirmed batch 2 protects neither the
	// hole-only segment nor segment 2; the hole marker reserves no data.
	if err := l.AckConsumer("c", 0); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := l.AckConsumer("c", 2); err != nil {
		t.Fatalf("ack 2 across hole: %v", err)
	}
	files, _ := filepath.Glob(filepath.Join(dir, "*.seg"))
	sort.Strings(files)
	if len(files) != 2 {
		t.Fatalf("segments = %v, want 2", files)
	}
	n, err := l.DeleteThrough(2)
	if err != nil {
		t.Fatalf("DeleteThrough = %d, %v", n, err)
	}
	if n != 2 {
		t.Fatalf("deleted = %d, want 2 (hole-only + committed segment)", n)
	}
	if _, err := l.ConsumerSeq("c"); err != nil {
		t.Fatalf("consumer lost after reclaim: %v", err)
	}
	// Reopen: consumer state intact, seq 1 stays a permanent hole.
	l = reopen(t, l, dir, opts)
	if got, err := l.ConsumerSeq("c"); err != nil || got != 2 {
		t.Fatalf("seq after reopen = %d, %v; want 2", got, err)
	}
	if seq := commit(t, l, []byte("three")); seq != 3 {
		t.Fatalf("seq = %d, want 3 (hole 1 not reused)", seq)
	}
}
