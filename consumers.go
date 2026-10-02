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

// MaxCheckpointState is the largest replay context AckConsumerState
// accepts: 1 MiB. A larger state returns ErrInvalidCheckpointState.
const MaxCheckpointState = 1 << 20

// Named consumer checkpoints live in their own sidecar
// ("consumers.idx"), deliberately separate from both the rebuildable
// index.idx (pure cache) and truncate.idx (the retention journal). It is
// authoritative state, not derived data: deleting index.idx must leave
// every consumer's confirmed sequence and replay context untouched, and
// a reopen must resume exactly the progress that was durable.
//
// Version 2 layout, all integers little-endian:
//
//	header  8 bytes  "BCLCNM" (6 bytes) + uint16 version
//	count   4 bytes  number of registered consumers
//	entries count × (8-byte confirmed sequence, 4-byte state length,
//	                state bytes, 2-byte name length, name bytes)
//	crc32   4 bytes  IEEE checksum of every preceding byte
//
// Version 1 images carried no per-consumer state; their entries framed
// the sequence, the 2-byte name length and the name with no state field
// (equivalent to an empty replay context). Such images are read and
// upgraded to version 2 in memory on the first write; a query never
// rewrites the file.
//
// The whole image is replaced atomically (fsynced temp + rename,
// directory synced best effort like the other sidecars), so a crash can
// only ever leave the complete previous image or the complete new one —
// never a torn or half-applied checkpoint. Unlike index.idx, any defect
// (short file, bad magic, an unknown version, a bad checksum, a framing
// error or a duplicate name) is fatal: Open returns
// ErrCorruptCheckpoint rather than guessing at progress or resetting a
// consumer to zero.
var (
	consumersMagic   = []byte{'B', 'C', 'L', 'C', 'N', 'M'}
	consumersVersion = uint16(2)
	consumersV1      = uint16(1)

	consumersHeaderLen = 8 + 4
)

// consumerEntry is one registered consumer's confirmed prefix and the
// opaque replay context committed together with it.
type consumerEntry struct {
	name  string
	seq   uint64
	state []byte
}

// consumerStateRec is the in-memory form of one registered consumer:
// the confirmed sequence with the replay context published in the same
// checkpoint. The bytes are owned by the log; copies cross the API.
type consumerStateRec struct {
	seq   uint64
	state []byte
}

// normalizeState returns the context to persist: nil and empty slices
// are the same empty context. It never aliases the caller's backing
// array, so later mutation of the argument cannot change saved state.
func normalizeState(state []byte) []byte {
	if len(state) == 0 {
		return nil
	}
	out := make([]byte, len(state))
	copy(out, state)
	return out
}

// cloneState returns a fresh copy of a stored context (nil when empty),
// so mutating the value handed back to a caller can never change the
// checkpoint.
func cloneState(state []byte) []byte {
	if len(state) == 0 {
		return nil
	}
	out := make([]byte, len(state))
	copy(out, state)
	return out
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

// publishCheckpoint validates a sequence/state submission, fsyncs the
// data it claims before publishing, then durably replaces the table,
// publishing seq and state together. It is the shared implementation of
// AckConsumer and AckConsumerState.
//
// When preserve is true (AckConsumer) a registered consumer's existing
// context is carried through unchanged on every call, including
// registrations' empty context; same-seq retries are always silent
// no-ops. When it is false (AckConsumerState) same seq with the same
// context succeeds without writing while same seq with a different
// context returns ErrCheckpointConflict, and an advance stores the new
// context.
//
// Validation order: name, then state size, then registration and
// progress. Parameter errors and conflicts neither write nor sync
// anything.
func (l *Log) publishCheckpoint(name string, seq uint64, state []byte, preserve bool) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return errClosed
	}
	// Fixed validation order: name, then state size, then registration
	// and progress.
	if !validConsumerName(name) {
		return ErrInvalidConsumer
	}
	if len(state) > MaxCheckpointState {
		return ErrInvalidCheckpointState
	}
	cur, registered := l.consumers[name]
	if !registered {
		if seq != 0 {
			return ErrUnknownConsumer
		}
		// A first call at seq 0 is a registration: it must persist the
		// table (and, for AckConsumerState, the initial context).
	} else if seq == cur.seq {
		// Confirming the already-confirmed prefix is an idempotent
		// no-op: no segment barrier and no disk write. AckConsumer keeps
		// that behavior no matter the (ignored) context; AckConsumerState
		// only stays a no-op when the context matches too.
		if preserve || bytes.Equal(cur.state, state) {
			return nil
		}
		// nil and the empty slice compare equal under bytes.Equal.
		// Nothing is written or synced, and the saved context wins:
		// same-seq calls cannot rewrite it.
		return ErrCheckpointConflict
	} else if seq < cur.seq {
		// Checkpoints only move forward.
		return ErrInvalidAck
	}

	// Cur holds the previous record of a registered consumer; the
	// progress checks reuse AckConsumer's exact rules so AckConsumerState
	// accepts and rejects exactly what AckConsumer would at this seq.
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

	// AckConsumer never edits the context: an advance keeps the bytes
	// already saved, and a registration stores the empty context. Only
	// AckConsumerState supplies fresh (copied) bytes.
	var saved []byte
	if preserve && registered {
		saved = cur.state
	} else {
		saved = normalizeState(state)
	}
	next := l.consumersList()
	if !registered {
		next = append(next, consumerEntry{name: name, seq: seq, state: saved})
	} else {
		for i := range next {
			if next[i].name == name {
				next[i].seq = seq
				next[i].state = saved
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
	l.consumers[name] = consumerStateRec{seq: seq, state: saved}
	return nil
}

// AckConsumer confirms consumer name's replay progress through seq and
// persists it durably, leaving any replay context committed with
// AckConsumerState untouched. Consumers Scan from ConsumerSeq(name)+1,
// process, and call AckConsumer when the prefix is handled; seq
// therefore means "every batch with a sequence up to seq has been
// processed".
//
// A name is registered only by its first AckConsumer(name, 0); the
// initial confirmed sequence is zero and the context is empty. After
// that:
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
	return l.publishCheckpoint(name, seq, nil, true)
}

// AckConsumerState confirms consumer name's replay progress through seq
// together with an opaque replay context, and persists both durably as
// one checkpoint. ConsumerCheckpoint reads both back after a reopen.
//
// State is opaque bytes of at most MaxCheckpointState (1 MiB); nil and
// an empty slice are equivalent. A larger context returns
// ErrInvalidCheckpointState. The log keeps its own copy, so a caller
// mutating state after a successful call cannot change the saved
// checkpoint; ConsumerCheckpoint likewise returns a copy.
//
// A new name is registered only at seq 0, and that first call saves the
// given context; registering with a non-zero seq returns
// ErrUnknownConsumer. Validation runs name first, then state size, then
// registration and progress — an invalid name always returns
// ErrInvalidConsumer even when the context is also too large or the
// name is unknown. Progress rules are exactly AckConsumer's: the
// current seq may be retried, while a rollback, an uncommitted or
// already-reclaimed target, a permanent hole as the target, or a jump
// over a smaller still-staged batch returns ErrInvalidAck.
//
// Calling at the stored seq with the stored context succeeds without
// writing; calling at the stored seq with a different context returns
// ErrCheckpointConflict and leaves the saved context in place. A legal
// advance publishes seq and context together after forcing the target
// and every earlier committed batch durable first, even when
// Options.Sync is false. A write, close or sync failure returns
// ErrCheckpointFailed; every consumer keeps its previous seq and
// context and the call retries. A crash during the update recovers
// either the complete old table or the complete new table.
func (l *Log) AckConsumerState(name string, seq uint64, state []byte) error {
	return l.publishCheckpoint(name, seq, state, false)
}

// ConsumerSeq returns consumer name's last confirmed sequence, 0 for a
// freshly registered consumer. It always agrees with the sequence
// ConsumerCheckpoint returns for the same name. A syntactically invalid
// name returns ErrInvalidConsumer; a legal but unregistered name
// returns ErrUnknownConsumer.
func (l *Log) ConsumerSeq(name string) (uint64, error) {
	seq, _, err := l.consumerRec(name)
	return seq, err
}

// ConsumerCheckpoint returns consumer name's last confirmed sequence and
// the opaque replay context committed with it. A consumer with no
// context — registered via AckConsumer, freshly registered with nil or
// read from a version 1 consumers.idx — reports an empty (nil) state.
// The returned byte slice is a copy: mutating it never changes the
// checkpoint. Name errors mirror ConsumerSeq: ErrInvalidConsumer for a
// syntactically invalid name, ErrUnknownConsumer for a legal but
// unregistered name; a query never rewrites consumers.idx, and deleting
// or rebuilding index.idx cannot affect the context.
func (l *Log) ConsumerCheckpoint(name string) (uint64, []byte, error) {
	return l.consumerRec(name)
}

// consumerRec is the shared read path behind ConsumerSeq and
// ConsumerCheckpoint so the two queries can never disagree.
func (l *Log) consumerRec(name string) (uint64, []byte, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.closed {
		return 0, nil, errClosed
	}
	if !validConsumerName(name) {
		return 0, nil, ErrInvalidConsumer
	}
	rec, ok := l.consumers[name]
	if !ok {
		return 0, nil, ErrUnknownConsumer
	}
	return rec.seq, cloneState(rec.state), nil
}

// DropConsumer removes consumer name's registration, its confirmed
// sequence and its replay context durably, releasing any prefix
// protection it held. It returns ErrInvalidConsumer for a syntactically
// invalid name and ErrUnknownConsumer for a legal name that is not
// registered. A later AckConsumer(name, 0) or
// AckConsumerState(name, 0, ...) registers the name again from zero
// with no surviving context; a reopen between the drop and the new
// registration never revives it.
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

// consumersList snapshots the registration table as on-disk entries.
func (l *Log) consumersList() []consumerEntry {
	entries := make([]consumerEntry, 0, len(l.consumers))
	for name, rec := range l.consumers {
		entries = append(entries, consumerEntry{name: name, seq: rec.seq, state: rec.state})
	}
	return entries
}

// readConsumers loads and strictly validates consumers.idx. No file (the
// common case for a directory written before consumers existed) reports
// ok=false with no entries; an old directory simply has no consumers.
// A version 1 image (entries without a state field) loads with an empty
// context; any other present-but-defective image is ErrCorruptCheckpoint.
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
	if version != consumersVersion && version != consumersV1 {
		return nil, false, ErrCorruptCheckpoint
	}
	stored := binary.LittleEndian.Uint32(data[len(data)-4:])
	if crc32.ChecksumIEEE(data[:len(data)-4]) != stored {
		return nil, false, ErrCorruptCheckpoint
	}
	count := int(binary.LittleEndian.Uint32(data[8:12]))
	pos := consumersHeaderLen
	end := len(data) - 4
	// Smallest framed entry: 8-byte seq, 4-byte (v2) state length,
	// 2-byte name length, one name byte. A count the remaining bytes
	// cannot hold is a structural error, not an allocation hint.
	minEntry := 8 + 4 + 2 + 1
	if version == consumersV1 {
		minEntry = 8 + 2 + 1
	}
	if count < 0 || (end-pos)/minEntry < count {
		return nil, false, ErrCorruptCheckpoint
	}
	seen := make(map[string]bool, count)
	entries = make([]consumerEntry, 0, count)
	for i := 0; i < count; i++ {
		if end-pos < 8 {
			return nil, false, ErrCorruptCheckpoint
		}
		seq := binary.LittleEndian.Uint64(data[pos : pos+8])
		pos += 8
		var state []byte
		if version == consumersVersion {
			if end-pos < 4 {
				return nil, false, ErrCorruptCheckpoint
			}
			slen := int(binary.LittleEndian.Uint32(data[pos : pos+4]))
			pos += 4
			// Reject the absurd before the length-driven bounds check so a
			// hostile length can never imply a gigantic allocation; a
			// context bigger than the API ever writes is itself a
			// structural defect.
			if slen < 0 || slen > MaxCheckpointState || slen > end-pos {
				return nil, false, ErrCorruptCheckpoint
			}
			if slen > 0 {
				state = bytes.Clone(data[pos : pos+slen])
			}
			pos += slen
		}
		if end-pos < 2 {
			return nil, false, ErrCorruptCheckpoint
		}
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
		size += 8 + 4 + len(e.state) + 2 + len(e.name)
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
		binary.LittleEndian.PutUint32(tmp[:4], uint32(len(e.state)))
		buf = append(buf, tmp[:4]...)
		buf = append(buf, e.state...)
		binary.LittleEndian.PutUint16(tmp[:2], uint16(len(e.name)))
		buf = append(buf, tmp[:2]...)
		buf = append(buf, e.name...)
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

func (l *Log) consumersPath() string {
	return filepath.Join(l.dir, "consumers.idx")
}
