package log_test

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	log "github.com/Hulalalalalalalalalalala/batch-commit-log"
)

func abort(t *testing.T, l *log.Log, b log.Batch) {
	t.Helper()
	if err := l.Abort(b); err != nil {
		t.Fatalf("Abort: %v", err)
	}
}

func TestAbortMakesPermanentHole(t *testing.T) {
	dir := t.TempDir()
	opts := log.Options{SegmentBytes: 1 << 20}
	l := open(t, dir, opts)

	commit(t, l, []byte("one")) // seq 1
	b2, err := l.Append([][]byte{[]byte("two")})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	abort(t, l, b2)

	// The aborted sequence is a permanent hole: it reads as not
	// committed and is never handed out again.
	if _, err := l.Read(2); !errors.Is(err, log.ErrNotCommitted) {
		t.Fatalf("Read(2) err = %v, want ErrNotCommitted", err)
	}
	b3, err := l.Append([][]byte{[]byte("three")})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if b3.Seq() != 3 {
		t.Fatalf("seq after abort = %d, want 3 (hole not reused)", b3.Seq())
	}
	if _, err := l.Commit(b3); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	var seqs []uint64
	if err := l.Scan(1, func(b log.Batch) error {
		seqs = append(seqs, b.Seq())
		return nil
	}); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if !reflect.DeepEqual(seqs, []uint64{1, 3}) {
		t.Fatalf("scan seqs = %v, want [1 3]", seqs)
	}

	// The aborted batch is dead to every write entry.
	if _, err := l.Commit(b2); !errors.Is(err, log.ErrUnknownBatch) {
		t.Fatalf("Commit aborted err = %v, want ErrUnknownBatch", err)
	}
	if _, err := l.CommitGroup([]log.Batch{b2}); !errors.Is(err, log.ErrUnknownBatch) {
		t.Fatalf("CommitGroup aborted err = %v, want ErrUnknownBatch", err)
	}
	if err := l.Abort(b2); !errors.Is(err, log.ErrUnknownBatch) {
		t.Fatalf("re-Abort err = %v, want ErrUnknownBatch", err)
	}
}

func TestAbortRejectsUnknownBatches(t *testing.T) {
	dir := t.TempDir()
	opts := log.Options{SegmentBytes: 1 << 20}
	l := open(t, dir, opts)

	committed, err := l.Append([][]byte{[]byte("done")})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if _, err := l.Commit(committed); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	staged, err := l.Append([][]byte{[]byte("pending")})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}

	// A batch from another log and one from a previous open of the same
	// directory are both foreign.
	other := open(t, t.TempDir(), opts)
	foreign, err := other.Append([][]byte{[]byte("elsewhere")})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	l.Close()
	stale := staged
	l = open(t, dir, opts)

	cases := []struct {
		name string
		b    log.Batch
	}{
		{"zero batch", log.Batch{}},
		{"committed batch", committed},
		{"other log's batch", foreign},
		{"previous open's batch", stale},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if err := l.Abort(c.b); !errors.Is(err, log.ErrUnknownBatch) {
				t.Fatalf("Abort err = %v, want ErrUnknownBatch", err)
			}
		})
	}
	// The log is unchanged: seq 1 is still committed and readable, and
	// the next append continues the sequence.
	if got, err := l.Read(1); err != nil || string(got[0]) != "done" {
		t.Fatalf("Read(1) = %q, %v", got, err)
	}
	b, err := l.Append([][]byte{[]byte("next")})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if b.Seq() != 2 {
		t.Fatalf("seq = %d, want 2", b.Seq())
	}
}

func TestAbortEmptyBatch(t *testing.T) {
	l := open(t, t.TempDir(), log.Options{SegmentBytes: 1 << 20})
	b, err := l.Append(nil)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	abort(t, l, b)
	if _, err := l.Read(b.Seq()); !errors.Is(err, log.ErrNotCommitted) {
		t.Fatalf("Read err = %v, want ErrNotCommitted", err)
	}
	next, err := l.Append(nil)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if next.Seq() != b.Seq()+1 {
		t.Fatalf("seq = %d, want %d", next.Seq(), b.Seq()+1)
	}
}

func TestAbortReleasesIdempotencyKey(t *testing.T) {
	dir := t.TempDir()
	opts := log.Options{SegmentBytes: 1 << 20}
	l := open(t, dir, opts)

	k1, err := l.AppendIdempotent("key", [][]byte{[]byte("v1")})
	if err != nil {
		t.Fatalf("AppendIdempotent: %v", err)
	}
	// Another keyed batch is untouched by the abort.
	k2, err := l.AppendIdempotent("other", [][]byte{[]byte("v2")})
	if err != nil {
		t.Fatalf("AppendIdempotent: %v", err)
	}
	abort(t, l, k1)

	// The released key starts a fresh batch — same records or different
	// ones both reserve a new sequence, never the aborted one.
	re1, err := l.AppendIdempotent("key", [][]byte{[]byte("v1")})
	if err != nil {
		t.Fatalf("AppendIdempotent same records: %v", err)
	}
	if re1.Seq() == k1.Seq() {
		t.Fatalf("reused key got aborted seq %d", re1.Seq())
	}
	abort(t, l, re1)
	re2, err := l.AppendIdempotent("key", [][]byte{[]byte("different")})
	if err != nil {
		t.Fatalf("AppendIdempotent different records: %v", err)
	}
	if re2.Seq() == k1.Seq() || re2.Seq() == re1.Seq() {
		t.Fatalf("reused key got aborted seq %d", re2.Seq())
	}

	// The other key still dedups against its original staged batch.
	again, err := l.AppendIdempotent("other", [][]byte{[]byte("v2")})
	if err != nil {
		t.Fatalf("AppendIdempotent other: %v", err)
	}
	if again.Seq() != k2.Seq() {
		t.Fatalf("other key seq = %d, want %d", again.Seq(), k2.Seq())
	}
	if _, err := l.AppendIdempotent("other", [][]byte{[]byte("changed")}); !errors.Is(err, log.ErrBatchIDConflict) {
		t.Fatalf("other key conflict err = %v, want ErrBatchIDConflict", err)
	}
	if _, err := l.Commit(k2); err != nil {
		t.Fatalf("Commit other: %v", err)
	}
}

func TestAbortDurableAcrossReopenAndIndexRebuild(t *testing.T) {
	dir := t.TempDir()
	// Sync false: the abort must still be durable.
	opts := log.Options{SegmentBytes: 1 << 20, Sync: false}
	l := open(t, dir, opts)
	commit(t, l, []byte("one")) // seq 1
	b2, err := l.AppendIdempotent("gone", [][]byte{[]byte("two")})
	if err != nil {
		t.Fatalf("AppendIdempotent: %v", err)
	}
	abort(t, l, b2) // seq 2 becomes a hole
	commit(t, l, []byte("three"))
	l.Close()

	check := func(t *testing.T, l *log.Log, wantNext uint64) {
		t.Helper()
		if _, err := l.Read(2); !errors.Is(err, log.ErrNotCommitted) {
			t.Fatalf("Read(2) err = %v, want ErrNotCommitted", err)
		}
		var seqs []uint64
		if err := l.Scan(1, func(b log.Batch) error {
			seqs = append(seqs, b.Seq())
			return nil
		}); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		if !reflect.DeepEqual(seqs, []uint64{1, 3}) {
			t.Fatalf("scan seqs = %v, want [1 3]", seqs)
		}
		// The released key reserves a fresh sequence after a reopen too.
		k, err := l.AppendIdempotent("gone", [][]byte{[]byte("two")})
		if err != nil {
			t.Fatalf("AppendIdempotent: %v", err)
		}
		if k.Seq() != wantNext {
			t.Fatalf("seq = %d, want %d", k.Seq(), wantNext)
		}
	}

	l = open(t, dir, opts)
	check(t, l, 4)
	l.Close()

	// Losing the index sidecar changes nothing: the hole and the
	// sequence allocation are rebuilt from the segments.
	if err := os.Remove(filepath.Join(dir, "index.idx")); err != nil {
		t.Fatal(err)
	}
	l = open(t, dir, opts)
	check(t, l, 4)
}

func TestAbortHoleOnlySegmentNotListed(t *testing.T) {
	dir := t.TempDir()
	// Tight segments: the abort marker rolls into its own segment.
	l := open(t, dir, log.Options{SegmentBytes: 32, Sync: true})
	commit(t, l, []byte("one")) // seq 1 in segment 1
	b2, err := l.Append([][]byte{[]byte("two")})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	abort(t, l, b2) // hole marker alone in segment 2

	segs := l.Segments()
	if !reflect.DeepEqual(segs, []log.Segment{{FirstSeq: 1, LastSeq: 1}}) {
		t.Fatalf("Segments = %v, want [{1 1}]", segs)
	}
	l.Close()

	l = open(t, dir, log.Options{SegmentBytes: 32, Sync: true})
	if segs := l.Segments(); !reflect.DeepEqual(segs, []log.Segment{{FirstSeq: 1, LastSeq: 1}}) {
		t.Fatalf("Segments after reopen = %v, want [{1 1}]", segs)
	}
}

func TestAbortGroupMemberLeavesOthersStaged(t *testing.T) {
	l := open(t, t.TempDir(), log.Options{SegmentBytes: 1 << 20})
	b1, _ := l.Append([][]byte{[]byte("one")})
	b2, _ := l.Append([][]byte{[]byte("two")})
	b3, _ := l.Append([][]byte{[]byte("three")})
	abort(t, l, b2)

	// The group fails on the aborted member, but the valid members stay
	// staged and commit afterwards.
	if _, err := l.CommitGroup([]log.Batch{b1, b2, b3}); !errors.Is(err, log.ErrUnknownBatch) {
		t.Fatalf("CommitGroup err = %v, want ErrUnknownBatch", err)
	}
	seqs, err := l.CommitGroup([]log.Batch{b1, b3})
	if err != nil {
		t.Fatalf("CommitGroup valid members: %v", err)
	}
	if !reflect.DeepEqual(seqs, []uint64{1, 3}) {
		t.Fatalf("seqs = %v, want [1 3]", seqs)
	}
}

func TestAbortConsumerInteraction(t *testing.T) {
	dir := t.TempDir()
	opts := log.Options{SegmentBytes: 1 << 20}
	l := open(t, dir, opts)
	commit(t, l, []byte("one")) // seq 1
	b2, _ := l.Append([][]byte{[]byte("two")})
	abort(t, l, b2) // seq 2 hole
	commit(t, l, []byte("three"))

	if err := l.AckConsumer("c", 0); err != nil {
		t.Fatalf("register: %v", err)
	}
	// The aborted sequence anchors neither a checkpoint nor a
	// truncation.
	if err := l.AckConsumer("c", 2); !errors.Is(err, log.ErrInvalidAck) {
		t.Fatalf("ack aborted seq = %v, want ErrInvalidAck", err)
	}
	if _, err := l.DeleteThrough(2); !errors.Is(err, log.ErrInvalidRetention) {
		t.Fatalf("DeleteThrough(aborted) = %v, want ErrInvalidRetention", err)
	}
	// The consumer crosses the hole to confirm the later commit.
	if err := l.AckConsumer("c", 3); err != nil {
		t.Fatalf("ack across abort hole: %v", err)
	}

	// A smaller still-staged batch blocks the ack; aborting it lifts
	// the block.
	b4, _ := l.Append([][]byte{[]byte("four")})
	commit(t, l, []byte("five"))
	if err := l.AckConsumer("c", 5); !errors.Is(err, log.ErrInvalidAck) {
		t.Fatalf("ack over staged seq 4 = %v, want ErrInvalidAck", err)
	}
	abort(t, l, b4)
	if err := l.AckConsumer("c", 5); err != nil {
		t.Fatalf("ack across aborted seq 4: %v", err)
	}
	if got, _ := l.ConsumerSeq("c"); got != 5 {
		t.Fatalf("checkpoint = %d, want 5", got)
	}
}

func TestAbortDoesNotTouchConsumers(t *testing.T) {
	l := open(t, t.TempDir(), log.Options{SegmentBytes: 1 << 20})
	commit(t, l, []byte("one"))
	if err := l.AckConsumer("c", 0); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := l.AckConsumer("c", 1); err != nil {
		t.Fatalf("ack: %v", err)
	}
	b, _ := l.Append([][]byte{[]byte("two")})
	abort(t, l, b)
	// No checkpoint moved and no consumer appeared.
	if got, _ := l.ConsumerSeq("c"); got != 1 {
		t.Fatalf("checkpoint = %d, want 1", got)
	}
	if _, err := l.ConsumerSeq("abort"); !errors.Is(err, log.ErrUnknownConsumer) {
		t.Fatalf("ConsumerSeq(abort) = %v, want ErrUnknownConsumer", err)
	}
}

func TestAbortHoleReclaimedByTruncation(t *testing.T) {
	dir := t.TempDir()
	opts := log.Options{SegmentBytes: 32, Sync: true}
	l := open(t, dir, opts)
	commit(t, l, []byte("one")) // seq 1, segment 1
	b2, _ := l.Append([][]byte{[]byte("two")})
	abort(t, l, b2) // seq 2 hole, segment 2
	commit(t, l, []byte("three"))

	// The hole pins its segment: truncating through 1 reclaims only the
	// first segment.
	if n, err := l.DeleteThrough(1); err != nil || n != 1 {
		t.Fatalf("DeleteThrough(1) = %d, %v; want 1", n, err)
	}
	if _, err := l.Read(2); !errors.Is(err, log.ErrNotCommitted) {
		t.Fatalf("Read(2) err = %v, want ErrNotCommitted", err)
	}
	// Truncating past the hole reclaims its segment; the sequence then
	// reads as truncated and stays reserved across a reopen.
	if n, err := l.DeleteThrough(3); err != nil || n != 2 {
		t.Fatalf("DeleteThrough(3) = %d, %v; want 2", n, err)
	}
	if _, err := l.Read(2); !errors.Is(err, log.ErrTruncated) {
		t.Fatalf("Read(2) err = %v, want ErrTruncated", err)
	}
	l.Close()

	l = open(t, dir, opts)
	if _, err := l.Read(2); !errors.Is(err, log.ErrTruncated) {
		t.Fatalf("Read(2) after reopen err = %v, want ErrTruncated", err)
	}
	b, err := l.Append([][]byte{[]byte("four")})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if b.Seq() != 4 {
		t.Fatalf("seq = %d, want 4 (reclaimed hole not reused)", b.Seq())
	}
}

func TestAbortAfterClose(t *testing.T) {
	dir := t.TempDir()
	l := open(t, dir, log.Options{SegmentBytes: 1 << 20})
	b, _ := l.Append([][]byte{[]byte("x")})
	l.Close()
	if err := l.Abort(b); err == nil || err.Error() != "log: closed" {
		t.Fatalf("Abort after Close = %v, want log: closed", err)
	}
}
