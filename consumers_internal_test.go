package log

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// hookSync installs a syncFile stub that reports the base name of the
// file being synced to synced and lets the caller fail chosen files.
func hookSync(t *testing.T, synced *[]string, failBase string) (restore func()) {
	t.Helper()
	orig := syncFile
	syncFile = func(f *os.File) error {
		*synced = append(*synced, filepath.Base(f.Name()))
		if filepath.Base(f.Name()) == failBase {
			return errors.New("injected sync failure")
		}
		return orig(f)
	}
	return func() { syncFile = orig }
}

func openAckLog(t *testing.T, dir string, segBytes int, sync bool) *Log {
	t.Helper()
	l, err := Open(dir, Options{SegmentBytes: segBytes, Sync: sync})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { l.Close() })
	return l
}

// Even with Sync:false, an acknowledgement fsyncs the committed data up
// to the target; a data-sync failure is ErrCheckpointFailed and leaves
// every consumer state untouched, retryable.
func TestAckForcesDataSyncWithoutSyncOption(t *testing.T) {
	dir := t.TempDir()
	l := openAckLog(t, dir, 1<<20, false)
	s1 := commitOne(t, l, "a")
	s2 := commitOne(t, l, "b")
	if err := l.AckConsumer("c", 0); err != nil {
		t.Fatalf("register: %v", err)
	}

	var synced []string
	restore := hookSync(t, &synced, "000001.seg")
	err := l.AckConsumer("c", s1)
	restore()
	if !errors.Is(err, ErrCheckpointFailed) {
		t.Fatalf("AckConsumer with data sync failing = %v, want ErrCheckpointFailed", err)
	}
	// State unchanged: point still 0 and no partial checkpoint left.
	if got, _ := l.ConsumerSeq("c"); got != 0 {
		t.Fatalf("point after failure = %d, want 0", got)
	}
	if _, err := os.Stat(filepath.Join(dir, "consumers.idx.tmp")); !os.IsNotExist(err) {
		t.Fatalf("stale tmp: %v", err)
	}

	// Retry fsyncs the segment, then the checkpoint, and advances.
	synced = nil
	if err := l.AckConsumer("c", s1); err != nil {
		t.Fatalf("retry ack: %v", err)
	}
	if got, _ := l.ConsumerSeq("c"); got != s1 {
		t.Fatalf("point = %d, want %d", got, s1)
	}

	// The second batch landed in the same still-live segment, whose tail
	// keeps growing: the next ack must fsync it again even though its
	// number was synced before — sealed files are the ones that get the
	// once-only treatment.
	synced = nil
	restore = hookSync(t, &synced, "")
	if err := l.AckConsumer("c", s2); err != nil {
		t.Fatalf("ack %d: %v", s2, err)
	}
	restore()
	found := false
	for _, name := range synced {
		if name == "000001.seg" {
			found = true
		}
	}
	if !found {
		t.Fatalf("live segment not re-synced on second ack: %v", synced)
	}
}

// Once a segment is sealed, later acknowledgements never fsync it again:
// its bytes were covered by the earlier ack and immutable ever since.
func TestAckDoesNotResyncSealedSegments(t *testing.T) {
	dir := t.TempDir()
	l := openAckLog(t, dir, 32, false) // tiny capacity: one batch per segment
	seqs := make([]uint64, 4)
	for i, s := range []string{"a", "bb", "ccc", "dddd"} {
		seqs[i] = commitOne(t, l, s)
	}
	if err := l.AckConsumer("c", 0); err != nil {
		t.Fatal(err)
	}
	var synced []string
	restore := hookSync(t, &synced, "")
	if err := l.AckConsumer("c", seqs[1]); err != nil {
		t.Fatalf("ack: %v", err)
	}
	restore()
	counts := map[string]int{}
	for _, name := range synced {
		counts[name]++
	}
	for _, seg := range []string{"000001.seg", "000002.seg"} {
		if counts[seg] != 1 {
			t.Fatalf("syncs of %s = %d (%v), want 1", seg, counts[seg], synced)
		}
	}

	// Advance again after a roll: the two previously sealed files are
	// not reopened, the newly sealed/live file is synced once.
	synced = nil
	restore = hookSync(t, &synced, "")
	if err := l.AckConsumer("c", seqs[3]); err != nil {
		t.Fatalf("ack: %v", err)
	}
	restore()
	for _, name := range synced {
		if name == "000001.seg" || name == "000002.seg" {
			t.Fatalf("sealed segment re-synced: %v", synced)
		}
	}
}

// Every surviving segment file at or before the target's segment is
// synced on an ack, including sealed files reopened read-only.
func TestAckSyncsAllEarlierSegments(t *testing.T) {
	dir := t.TempDir()
	l := openAckLog(t, dir, 32, false)
	for _, s := range []string{"a", "bb", "ccc"} {
		commitOne(t, l, s)
	}
	if err := l.AckConsumer("c", 0); err != nil {
		t.Fatal(err)
	}
	var synced []string
	restore := hookSync(t, &synced, "")
	err := l.AckConsumer("c", 3)
	restore()
	if err != nil {
		t.Fatalf("ack: %v", err)
	}
	counts := map[string]int{}
	for _, name := range synced {
		counts[name]++
	}
	for _, seg := range []string{"000001.seg", "000002.seg", "000003.seg"} {
		if counts[seg] != 1 {
			t.Fatalf("syncs of %s = %d (all: %v), want 1", seg, counts[seg], synced)
		}
	}
}

// A checkpoint persist failure (here the temp-file fsync) changes no
// consumer state either in memory or on disk, and the call retries.
func TestAckCheckpointSyncFailureKeepsOldState(t *testing.T) {
	dir := t.TempDir()
	l := openAckLog(t, dir, 1<<20, true)
	s1 := commitOne(t, l, "a")
	if err := l.AckConsumer("c", 0); err != nil {
		t.Fatal(err)
	}
	var synced []string
	restore := hookSync(t, &synced, "consumers.idx.tmp")
	err := l.AckConsumer("c", s1)
	restore()
	if !errors.Is(err, ErrCheckpointFailed) {
		t.Fatalf("ack = %v, want ErrCheckpointFailed", err)
	}
	if got, _ := l.ConsumerSeq("c"); got != 0 {
		t.Fatalf("in-memory point = %d, want 0", got)
	}
	if _, err := os.Stat(filepath.Join(dir, "consumers.idx.tmp")); !os.IsNotExist(err) {
		t.Fatalf("tmp not cleaned up: %v", err)
	}

	// The durable image on disk still describes the complete old state.
	l.Close()
	l2, err := Open(dir, Options{SegmentBytes: 1 << 20, Sync: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { l2.Close() })
	if got, err := l2.ConsumerSeq("c"); err != nil || got != 0 {
		t.Fatalf("durable point = %d, %v; want 0, nil", got, err)
	}
	// Retry on the reopened log converges to the new state.
	if err := l2.AckConsumer("c", s1); err != nil {
		t.Fatalf("retry: %v", err)
	}
}

// A failed registration leaves the name unknown and the call retryable.
func TestRegisterCheckpointFailureIsAtomic(t *testing.T) {
	dir := t.TempDir()
	l := openAckLog(t, dir, 1<<20, true)
	var synced []string
	restore := hookSync(t, &synced, "consumers.idx.tmp")
	err := l.AckConsumer("new", 0)
	restore()
	if !errors.Is(err, ErrCheckpointFailed) {
		t.Fatalf("register = %v, want ErrCheckpointFailed", err)
	}
	if _, err := l.ConsumerSeq("new"); !errors.Is(err, ErrUnknownConsumer) {
		t.Fatalf("ConsumerSeq after failed register = %v, want ErrUnknownConsumer", err)
	}
	if err := l.AckConsumer("new", 0); err != nil {
		t.Fatalf("retry register: %v", err)
	}
	if got, _ := l.ConsumerSeq("new"); got != 0 {
		t.Fatalf("point = %d, want 0", got)
	}
}
