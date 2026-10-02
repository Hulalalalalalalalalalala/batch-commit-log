package log

import (
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
//	entries count × (8-byte confirmed sequence, 2-byte name length,
//	                name bytes)
//	crc32   4 bytes  IEEE checksum of every preceding byte
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
	consumersVersion = uint16(1)

	consumersHeaderLen = 8 + 4
)

// consumerEntry is one registered consumer's confirmed prefix.
type consumerEntry struct {
	name string
	seq  uint64
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
// Any present-but-defective image is ErrCorruptCheckpoint.
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
	if binary.LittleEndian.Uint16(data[6:8]) != consumersVersion {
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
		entries = append(entries, consumerEntry{name: name, seq: seq})
	}
	if pos != end {
		// Trailing bytes between the last entry and the checksum: never a
		// legal image.
		return nil, false, ErrCorruptCheckpoint
	}
	return entries, true, nil
}

// encodeConsumers serialises the full consumer table in name order with
// its trailing checksum.
func encodeConsumers(entries []consumerEntry) []byte {
	sorted := make([]consumerEntry, len(entries))
	copy(sorted, entries)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].name < sorted[j].name })
	size := consumersHeaderLen + 4
	for _, e := range sorted {
		size += 8 + 2 + len(e.name)
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

// consumersList snapshots the registration table as on-disk entries.
func (l *Log) consumersList() []consumerEntry {
	entries := make([]consumerEntry, 0, len(l.consumers))
	for name, seq := range l.consumers {
		entries = append(entries, consumerEntry{name: name, seq: seq})
	}
	return entries
}

// AckConsumer confirms consumer name's replay progress through seq and
// persists it durably. Consumers Scan from ConsumerSeq(name)+1, process,
// and call AckConsumer when the prefix is handled; seq therefore means
// "every batch with a sequence up to seq has been processed".
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
	if !validConsumerName(name) {
		return ErrInvalidConsumer
	}
	cur, registered := l.consumers[name]
	if !registered {
		if seq != 0 {
			return ErrUnknownConsumer
		}
	} else {
		switch {
		case seq == cur:
			// Confirming the already-confirmed prefix is an idempotent
			// no-op: no disk write and no extra fsync.
			return nil
		case seq < cur:
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
	if !registered {
		next = append(next, consumerEntry{name: name, seq: seq})
	} else {
		for i := range next {
			if next[i].name == name {
				next[i].seq = seq
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
	l.consumers[name] = seq
	return nil
}

// ConsumerSeq returns consumer name's last confirmed sequence, 0 for a
// freshly registered consumer. A syntactically invalid name returns
// ErrInvalidConsumer; a legal but unregistered name returns
// ErrUnknownConsumer.
func (l *Log) ConsumerSeq(name string) (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.closed {
		return 0, errClosed
	}
	if !validConsumerName(name) {
		return 0, ErrInvalidConsumer
	}
	seq, ok := l.consumers[name]
	if !ok {
		return 0, ErrUnknownConsumer
	}
	return seq, nil
}

// DropConsumer removes consumer name's registration durably, releasing
// any prefix protection it held. It returns ErrInvalidConsumer for a
// syntactically invalid name and ErrUnknownConsumer for a legal name
// that is not registered. A later AckConsumer(name, 0) registers the
// name again from zero; a reopen between the drop and the new
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
