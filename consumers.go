package log

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"unicode/utf8"
)

// Named consumer checkpoints live in their own sidecar
// ("consumers.idx"), deliberately separate from both the rebuildable
// index.idx (pure cache) and truncate.idx (the retention journal). It is
// authoritative state, not derived data: deleting index.idx must leave
// every consumer's confirmed sequence untouched, and a reopen must
// resume exactly the progress that was durable.
//
// Layout, all integers little-endian:
//
//	header  8 bytes  "BCLCNM" (6 bytes) + uint16 version
//	count   4 bytes  number of registered consumers
//	entries count × …
//	crc32   4 bytes  IEEE checksum of every preceding byte
//
// Version 1 entry:
//
//	8-byte confirmed sequence, 2-byte name length, name bytes
//
// Version 2 additionally carries one opaque replay-state blob per
// consumer:
//
//	8-byte confirmed sequence, 2-byte name length, name bytes,
//	4-byte state length, state bytes (length 0 = empty state)
//
// Version 1 images stay readable forever: a consumer without a state
// blob simply reads back an empty state. Reads never rewrite the file;
// the first actual checkpoint change re-encodes the table as version 2.
//
// The whole image is replaced atomically (fsynced temp + rename,
// directory synced best effort like the other sidecars), so a crash can
// only ever leave the complete previous image or the complete new one —
// never a torn or half-applied checkpoint. Unlike index.idx, any defect
// (short file, bad magic, an unknown version, a bad checksum, a framing
// error, an oversized state blob or a duplicate name) is fatal: Open
// returns ErrCorruptCheckpoint rather than guessing at progress or
// resetting a consumer to zero.
var (
	consumersMagic   = []byte{'B', 'C', 'L', 'C', 'N', 'M'}
	consumersVersion = uint16(2)

	consumersHeaderLen = 8 + 4

	// maxCheckpointStateLen bounds one consumer's opaque replay state.
	maxCheckpointStateLen = 1048576
)

// consumerEntry is one registered consumer's confirmed prefix and the
// opaque replay state published together with it.
type consumerEntry struct {
	name  string
	seq   uint64
	state []byte
}

// consumerState is the in-memory checkpoint: a confirmed sequence plus
// its owned state bytes. Callers never alias state: every value stored
// here is an independent copy, and every value handed out is copied.
type consumerState struct {
	seq   uint64
	state []byte
}

func (l *Log) consumersPath() string {
	return filepath.Join(l.dir, "consumers.idx")
}

// validConsumerName reports whether name is an acceptable consumer
// name. The rule is the idempotency-key rule — non-empty, valid UTF-8,
// NUL-free, at most 256 bytes — but the two namespaces are independent:
// a consumer may share a name with an idempotency key.
func validConsumerName(name string) bool {
	if name == "" || len(name) > maxBatchIDLen {
		return false
	}
	if strings.IndexByte(name, 0) >= 0 {
		return false
	}
	return utf8.ValidString(name)
}

// readConsumers loads and strictly validates consumers.idx. No file (the
// common case for a directory written before consumers existed) reports
// ok=false with no entries; an old directory simply has no consumers.
// Version 1 images load with empty state; any other present-but-defective
// image is ErrCorruptCheckpoint.
func (l *Log) readConsumers() (entries []consumerEntry, ok bool, err error) {
	data, rerr := os.ReadFile(l.consumersPath())
	if errors.Is(rerr, os.ErrNotExist) {
		return nil, false, nil
	}
	if rerr != nil {
		return nil, false, rerr
	}
	if len(data) < consumersHeaderLen+4 {
		return nil, false, ErrCorruptCheckpoint
	}
	if string(data[:6]) != string(consumersMagic) {
		return nil, false, ErrCorruptCheckpoint
	}
	version := binary.LittleEndian.Uint16(data[6:8])
	if version != 1 && version != consumersVersion {
		return nil, false, ErrCorruptCheckpoint
	}
	stored := binary.LittleEndian.Uint32(data[len(data)-4:])
	if crc32.ChecksumIEEE(data[:len(data)-4]) != stored {
		return nil, false, ErrCorruptCheckpoint
	}
	count := int(binary.LittleEndian.Uint32(data[8:12]))
	pos := consumersHeaderLen
	end := len(data) - 4
	seen := make(map[string]bool, count)
	entries = make([]consumerEntry, 0, count)
	for i := 0; i < count; i++ {
		if end-pos < 8+2 {
			return nil, false, ErrCorruptCheckpoint
		}
		seq := binary.LittleEndian.Uint64(data[pos : pos+8])
		pos += 8
		nlen := int(binary.LittleEndian.Uint16(data[pos : pos+2]))
		pos += 2
		if nlen <= 0 || nlen > maxBatchIDLen || end-pos < nlen {
			return nil, false, ErrCorruptCheckpoint
		}
		raw := data[pos : pos+nlen]
		pos += nlen
		if strings.IndexByte(string(raw), 0) >= 0 || !utf8.Valid(raw) {
			return nil, false, ErrCorruptCheckpoint
		}
		name := string(raw)
		if seen[name] {
			return nil, false, ErrCorruptCheckpoint
		}
		seen[name] = true
		var state []byte
		if version == consumersVersion {
			if end-pos < 4 {
				return nil, false, ErrCorruptCheckpoint
			}
			slen := int(binary.LittleEndian.Uint32(data[pos : pos+4]))
			pos += 4
			if slen < 0 || slen > maxCheckpointStateLen || end-pos < slen {
				return nil, false, ErrCorruptCheckpoint
			}
			if slen > 0 {
				// An independent copy keeps the loaded file bytes from
				// being mutated through the in-memory checkpoint table.
				state = append(make([]byte, 0, slen), data[pos:pos+slen]...)
				pos += slen
			}
		}
		entries = append(entries, consumerEntry{name: name, seq: seq, state: state})
	}
	if pos != end {
		// Trailing bytes between the last entry and the checksum: never a
		// legal image.
		return nil, false, ErrCorruptCheckpoint
	}
	return entries, true, nil
}

// encodeConsumers serialises the full consumer table in name order as a
// version 2 image with its trailing checksum.
func encodeConsumers(entries []consumerEntry) []byte {
	sorted := make([]consumerEntry, len(entries))
	copy(sorted, entries)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].name < sorted[j].name })
	size := consumersHeaderLen + 4
	for _, e := range sorted {
		size += 8 + 2 + len(e.name) + 4 + len(e.state)
	}
	buf := make([]byte, 0, size)
	buf = append(buf, consumersMagic...)
	var tmp [8]byte
	binary.LittleEndian.PutUint16(tmp[:2], consumersVersion)
	buf = append(buf, tmp[:2]...)
	binary.LittleEndian.PutUint32(tmp[:4], uint32(len(sorted)))
	buf = append(buf, tmp[:4]...)
	for _, e := range sorted {
		binary.LittleEndian.PutUint64(tmp[:], e.seq)
		buf = append(buf, tmp[:]...)
		binary.LittleEndian.PutUint16(tmp[:2], uint16(len(e.name)))
		buf = append(buf, tmp[:2]...)
		buf = append(buf, e.name...)
		binary.LittleEndian.PutUint32(tmp[:4], uint32(len(e.state)))
		buf = append(buf, tmp[:4]...)
		buf = append(buf, e.state...)
	}
	binary.LittleEndian.PutUint32(tmp[:4], crc32.ChecksumIEEE(buf))
	return append(buf, tmp[:4]...)
}

// writeConsumers durably replaces consumers.idx with the full table. The
// bytes land in a synced temp file before the atomic rename, exactly
// like writeTruncateMarker, so a crash leaves one complete image or the
// other. Every write is forced through here — checkpoint changes are
// durable even when Options.Sync is false.
func (l *Log) writeConsumers(entries []consumerEntry) error {
	path := l.consumersPath()
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(encodeConsumers(entries)); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := syncFile(f); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return err
	}
	_ = syncDir(l.dir)
	return nil
}

// consumersList snapshots the registration table as on-disk entries. The
// state slices are owned copies of the saved bytes.
func (l *Log) consumersList() []consumerEntry {
	entries := make([]consumerEntry, 0, len(l.consumers))
	for name, c := range l.consumers {
		entries = append(entries, consumerEntry{
			name:  name,
			seq:   c.seq,
			state: append([]byte(nil), c.state...),
		})
	}
	return entries
}

// cloneState returns an independent copy of state, normalising nil and
// the empty slice to nil: nil and empty are the same saved state.
func cloneState(state []byte) []byte {
	if len(state) == 0 {
		return nil
	}
	return append(make([]byte, 0, len(state)), state...)
}

// checkAck validates and applies a consumer checkpoint. With state nil
// (an AckConsumer call) the consumer's saved state is preserved; with
// state non-nil (an AckConsumerState call, using *[]byte so an explicit
// empty slice is distinguishable) the saved state is replaced by a copy
// of what is passed — and repeating the current sequence with the same
// state is the only no-op form.
//
// Validation order: caller has already handled the closed case; here the
// name is checked first, then the state-size limit (stateful calls), and
// only then registration and progress.
func (l *Log) checkAck(name string, seq uint64, state *[]byte) error {
	if !validConsumerName(name) {
		return ErrInvalidConsumer
	}
	if state != nil && len(*state) > maxCheckpointStateLen {
		return ErrInvalidCheckpointState
	}
	cur, registered := l.consumers[name]
	stateful := state != nil
	if !registered {
		if seq != 0 {
			return ErrUnknownConsumer
		}
	} else {
		switch {
		case seq == cur.seq:
			if stateful && !bytes.Equal(*state, cur.state) {
				// Same progress may only be republished with the exact same
				// state. This is a conflict, never a silent overwrite.
				return ErrCheckpointConflict
			}
			// Confirming the already-confirmed prefix (with the same state,
			// if any was given) is an idempotent no-op: no disk write and no
			// extra fsync.
			return nil
		case seq < cur.seq:
			// Checkpoints only move forward.
			return ErrInvalidAck
		}
	}
	if seq > 0 {
		if _, committed := l.index[seq]; !committed {
			// Unknown, a permanent hole, still staged, or already
			// reclaimed: reclaimed batches leave the index like unknown
			// ones (and Read distinguishes them as ErrTruncated), but for
			// a checkpoint every non-committed target is the same
			// ErrInvalidAck. Note a group that straddled a truncation
			// boundary can keep a batch with seq <= through readable, so
			// membership, not through, is the test.
			return ErrInvalidAck
		}
		for s, recs := range l.staged {
			if recs != nil && s < seq {
				// A staged batch ahead of the target would be claimed as
				// processed by the "prefix through seq" statement; its
				// reservation must commit first.
				return ErrInvalidAck
			}
		}
	}

	// Make the data durable before claiming it: a plain registration at
	// seq 0 persists the table only, while an advance must barrier the
	// target and every earlier committed batch even when Options.Sync is
	// false. The current segment goes through its live write handle;
	// rolled segments are closed and reopened just for the fsync. A
	// failure leaves the in-memory checkpoint untouched and the whole
	// call retryable.
	if seq > 0 {
		if err := l.syncDirtySegments(); err != nil {
			return err
		}
	}

	next := l.consumersList()
	nextState := cur.state
	if stateful {
		nextState = cloneState(*state)
	}
	if !registered {
		next = append(next, consumerEntry{name: name, seq: seq, state: nextState})
	} else {
		for i := range next {
			if next[i].name == name {
				next[i].seq = seq
				next[i].state = append([]byte(nil), nextState...)
				break
			}
		}
	}
	if err := l.writeConsumers(next); err != nil {
		// The new image never landed: the table on disk and in memory is
		// the previous one, and the segment syncs already performed only
		// made data more durable. Retry freely.
		return fmt.Errorf("%w: %v", ErrCheckpointFailed, err)
	}
	l.consumers[name] = consumerState{seq: seq, state: nextState}
	return nil
}

// AckConsumer confirms consumer name's replay progress through seq and
// persists it durably, leaving the consumer's saved replay state (if
// any) untouched; a first registration starts with empty state.
// Consumers Scan from ConsumerSeq(name)+1, process, and call AckConsumer
// when the prefix is handled; seq therefore means "every batch with a
// sequence up to seq has been processed".
//
// A name is registered only by its first AckConsumer(name, 0); the
// initial confirmed sequence is zero. After that:
//
//   - seq equal to the stored value succeeds without writing.
//   - a greater seq advances the checkpoint; seq must name a committed
//     batch (a permanent hole may be skipped over, a group may be
//     confirmed as a unit), the batch must not already have been
//     reclaimed by DeleteThrough, and no still-staged batch may hold a
//     sequence below seq that the prefix claim would overrun.
//   - a smaller seq, an unknown/hole/staged/reclaimed target, or a
//     staged batch earlier in the sequence space returns ErrInvalidAck
//     and changes nothing.
//
// A syntactically invalid name returns ErrInvalidConsumer. A legal but
// unregistered name returns ErrUnknownConsumer (registration is only
// AckConsumer(name, 0)). A returned success is durable regardless of
// Options.Sync: the target and every earlier committed batch are
// fsynced first, then consumers.idx is atomically replaced and synced.
// An fsync or metadata write failure returns ErrCheckpointFailed, all
// consumer state stays at its previous value and the call retries.
func (l *Log) AckConsumer(name string, seq uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return errClosed
	}
	return l.checkAck(name, seq, nil)
}

// AckConsumerState confirms consumer name's replay progress through seq
// and atomically republishes an opaque replay-state blob with it; after
// a successful return, a reopen restores both the sequence and the
// state via ConsumerCheckpoint. state is opaque to the log and may be
// nil — nil and an empty slice are the same state — and must be at most
// 1048576 bytes; a larger blob returns ErrInvalidCheckpointState. The
// caller may mutate state (or the bytes returned later) at any time:
// the log keeps its own copy.
//
// A new name is registered only with seq == 0, and the registration is
// persisted together with the given state; a non-zero seq for an
// unregistered name returns ErrUnknownConsumer, exactly like
// AckConsumer. Progress validation is identical to AckConsumer: besides
// retrying the current sequence, a rollback, an uncommitted, hole or
// reclaimed target, and an advance over a smaller staged batch return
// ErrInvalidAck.
//
// Retrying the stored sequence with the identical stored state succeeds
// without touching disk; the same sequence with different state returns
// ErrCheckpointConflict and writes nothing. A legal advance first makes
// the target and every earlier committed batch durable (even when
// Options.Sync is false) and then publishes the sequence and state in a
// single atomic consumers.idx image. A write, close or sync failure
// returns ErrCheckpointFailed; every consumer keeps its previous value
// and the call is retryable, and a crash during the update restores
// either the complete old table or the complete new one.
//
// Validation order is the name, then the state-size limit, then
// registration and progress. A syntactically invalid name returns
// ErrInvalidConsumer.
func (l *Log) AckConsumerState(name string, seq uint64, state []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return errClosed
	}
	s := cloneState(state)
	return l.checkAck(name, seq, &s)
}

// ConsumerSeq returns consumer name's last confirmed sequence, 0 for a
// freshly registered consumer. A syntactically invalid name returns
// ErrInvalidConsumer; a legal but unregistered name returns
// ErrUnknownConsumer. It always agrees with the sequence returned by
// ConsumerCheckpoint.
func (l *Log) ConsumerSeq(name string) (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.closed {
		return 0, errClosed
	}
	if !validConsumerName(name) {
		return 0, ErrInvalidConsumer
	}
	c, ok := l.consumers[name]
	if !ok {
		return 0, ErrUnknownConsumer
	}
	return c.seq, nil
}

// ConsumerCheckpoint returns consumer name's last confirmed sequence and
// the opaque state published with it. A consumer registered through
// AckConsumer, or one carried by an old version 1 consumers.idx, reads
// back an empty (nil) state. The returned slice is an independent copy:
// mutating it never changes the saved checkpoint. Name errors are the
// same as ConsumerSeq: ErrInvalidConsumer for a syntactically invalid
// name, ErrUnknownConsumer for a legal name that has never registered.
// The call only reads state; it never rewrites consumers.idx.
func (l *Log) ConsumerCheckpoint(name string) (uint64, []byte, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.closed {
		return 0, nil, errClosed
	}
	if !validConsumerName(name) {
		return 0, nil, ErrInvalidConsumer
	}
	c, ok := l.consumers[name]
	if !ok {
		return 0, nil, ErrUnknownConsumer
	}
	return c.seq, append([]byte(nil), c.state...), nil
}

// DropConsumer removes consumer name's registration durably, including
// its replay state, releasing any prefix protection it held. It returns
// ErrInvalidConsumer for a syntactically invalid name and
// ErrUnknownConsumer for a legal name that is not registered. A later
// AckConsumer(name, 0) or AckConsumerState(name, 0, …) registers the
// name again from zero with no trace of the old state; a reopen between
// the drop and the new registration never revives it.
func (l *Log) DropConsumer(name string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return errClosed
	}
	if !validConsumerName(name) {
		return ErrInvalidConsumer
	}
	if _, ok := l.consumers[name]; !ok {
		return ErrUnknownConsumer
	}
	next := l.consumersList()
	for i := range next {
		if next[i].name == name {
			next = append(next[:i], next[i+1:]...)
			break
		}
	}
	if err := l.writeConsumers(next); err != nil {
		return fmt.Errorf("%w: %v", ErrCheckpointFailed, err)
	}
	delete(l.consumers, name)
	return nil
}
