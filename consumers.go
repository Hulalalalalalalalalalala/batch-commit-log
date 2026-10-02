package log

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
)

// Persistent, named consumer acknowledgement points live in their own
// sidecar ("consumers.idx"), separate from both the rebuildable
// index.idx (pure cache) and truncate.idx (the retention marker):
// unlike index.idx it must never be rebuilt or reset, because it is the
// only record of how far each consumer has replayed.
//
// Layout, all integers little-endian:
//
//	magic   6 bytes  "BCLCON"
//	version 2 bytes  uint16
//	count   4 bytes  uint32 number of registered consumers
//	count × records:
//	  2 bytes uint16 name length
//	  n bytes name (the same legality rules as idempotency keys)
//	  8 bytes uint64 acknowledged sequence
//	crc32   4 bytes  IEEE checksum of every preceding byte
//
// The whole file is replaced atomically (fsynced temp + rename) on
// every registration, advance and removal, so a crash can only leave
// the complete previous image or the complete new one. A missing file
// means a log directory from before consumer metadata existed: zero
// consumers, not an error. A present file that is truncated,
// checksum-bad, from an unsupported version or otherwise malformed
// makes Open return ErrCorruptCheckpoint — progress is never reset to
// let consumption continue.
var (
	consumerMagic   = []byte{'B', 'C', 'L', 'C', 'O', 'N'}
	consumerVersion = uint16(1)

	consumerHeaderLen = 6 + 2 + 4
	consumerRecLen    = 2 + 8 // name-length prefix + ack sequence, name bytes follow
)

var (
	// ErrUnknownConsumer is returned by ConsumerSeq, AckConsumer and
	// DropConsumer for a name that was never registered. A name is
	// registered only with AckConsumer(name, 0).
	ErrUnknownConsumer = errors.New("log: unknown consumer")
	// ErrInvalidConsumer is returned for a consumer name that breaks the
	// idempotency-key legality rules: empty, longer than 256 bytes,
	// containing a NUL byte, or not valid UTF-8. The consumer namespace
	// is independent of the idempotency-key namespace.
	ErrInvalidConsumer = errors.New("log: invalid consumer name")
	// ErrInvalidAck is returned by AckConsumer when the target sequence
	// would move the point backwards, is staged rather than committed,
	// was never reserved, was already reclaimed by DeleteThrough, or a
	// still-staged batch precedes it. A permanent hole at or before the
	// target is fine and may be skipped.
	ErrInvalidAck = errors.New("log: invalid acknowledgement point")
	// ErrCheckpointFailed is returned when persisting consumer metadata
	// fails, or when syncing the committed data an acknowledgement must
	// guarantee durable. No consumer state changes when it is returned
	// and the call may be retried.
	ErrCheckpointFailed = errors.New("log: checkpoint failed")
	// ErrCorruptCheckpoint is returned by Open when consumers.idx is
	// present but truncated, checksum-bad, malformed or carries an
	// unsupported version. The log is not opened: a damaged checkpoint
	// can never reset progress.
	ErrCorruptCheckpoint = errors.New("log: corrupt consumer checkpoint")
	// ErrRetentionBlocked is returned by DeleteThrough when a
	// reclaimable prefix contains a committed batch above any
	// registered consumer's acknowledgement point. No file, index or
	// idempotency key changes.
	ErrRetentionBlocked = errors.New("log: retention blocked by consumer")
)

// consumersPath is the persistent consumer checkpoint sidecar.
func (l *Log) consumersPath() string {
	return filepath.Join(l.dir, "consumers.idx")
}

// encodeConsumers serialises the whole registration table, names in
// sorted order for a stable on-disk image, with a trailing checksum.
func encodeConsumers(m map[string]uint64) []byte {
	names := make([]string, 0, len(m))
	for name := range m {
		names = append(names, name)
	}
	sort.Strings(names)
	size := consumerHeaderLen + consumerRecLen*len(names)
	for _, name := range names {
		size += len(name)
	}
	buf := make([]byte, 0, size+4)
	buf = append(buf, consumerMagic...)
	var tmp [8]byte
	binary.LittleEndian.PutUint16(tmp[:2], consumerVersion)
	buf = append(buf, tmp[:2]...)
	binary.LittleEndian.PutUint32(tmp[:4], uint32(len(names)))
	buf = append(buf, tmp[:4]...)
	for _, name := range names {
		binary.LittleEndian.PutUint16(tmp[:2], uint16(len(name)))
		buf = append(buf, tmp[:2]...)
		buf = append(buf, name...)
		binary.LittleEndian.PutUint64(tmp[:], m[name])
		buf = append(buf, tmp[:8]...)
	}
	binary.LittleEndian.PutUint32(tmp[:4], crc32.ChecksumIEEE(buf))
	return append(buf, tmp[:4]...)
}

// parseConsumers validates and decodes one consumers.idx image. Every
// framing defect, a checksum mismatch, an unsupported version, a
// duplicated or illegal name, or trailing/short bytes are reported as
// ErrCorruptCheckpoint; the caller must never guess a table from a
// damaged file.
func parseConsumers(data []byte) (map[string]uint64, error) {
	if len(data) < consumerHeaderLen+4 {
		return nil, ErrCorruptCheckpoint
	}
	if string(data[:6]) != string(consumerMagic) ||
		binary.LittleEndian.Uint16(data[6:8]) != consumerVersion {
		return nil, ErrCorruptCheckpoint
	}
	count := int(binary.LittleEndian.Uint32(data[8:12]))
	body := data[consumerHeaderLen : len(data)-4]
	stored := binary.LittleEndian.Uint32(data[len(data)-4:])
	if crc32.ChecksumIEEE(data[:len(data)-4]) != stored {
		return nil, ErrCorruptCheckpoint
	}
	// Every record is at least 11 bytes (2-byte length, one name byte,
	// 8-byte seq); reject an implausible count before allocating for it.
	if count > len(body) {
		return nil, ErrCorruptCheckpoint
	}
	out := make(map[string]uint64, count)
	pos := 0
	for i := 0; i < count; i++ {
		if pos+2 > len(body) {
			return nil, ErrCorruptCheckpoint
		}
		nlen := int(binary.LittleEndian.Uint16(body[pos : pos+2]))
		pos += 2
		if pos+nlen+8 > len(body) {
			return nil, ErrCorruptCheckpoint
		}
		name := string(body[pos : pos+nlen])
		pos += nlen
		// The on-disk name must satisfy exactly the rules AckConsumer
		// enforces, so a tampered or foreign file cannot smuggle one in.
		if !validBatchID(name) {
			return nil, ErrCorruptCheckpoint
		}
		seq := binary.LittleEndian.Uint64(body[pos : pos+8])
		pos += 8
		if _, dup := out[name]; dup {
			return nil, ErrCorruptCheckpoint
		}
		out[name] = seq
	}
	if pos != len(body) {
		return nil, ErrCorruptCheckpoint
	}
	return out, nil
}

// loadConsumers reads the checkpoint at open. A missing file is the
// historical "no consumer metadata" directory and leaves the table
// empty; malformed bytes fail the open with ErrCorruptCheckpoint.
func (l *Log) loadConsumers() error {
	data, err := os.ReadFile(l.consumersPath())
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	m, err := parseConsumers(data)
	if err != nil {
		return err
	}
	// A stale temp file from a crash before the rename never replaces a
	// good checkpoint; best effort cleanup.
	_ = os.Remove(l.consumersPath() + ".tmp")
	l.consumers = m
	return nil
}

// cloneConsumers returns a copy of the live table so a failed persist
// leaves the in-memory registration table byte-for-byte unchanged.
func (l *Log) cloneConsumers() map[string]uint64 {
	next := make(map[string]uint64, len(l.consumers)+1)
	for name, seq := range l.consumers {
		next[name] = seq
	}
	return next
}

// persistConsumers durably replaces consumers.idx with the image of
// next (synced temp file, then rename) and only adopts it in memory
// once the rename has landed, so every error leaves all consumer state
// at its previous value and the call is retryable. Directory sync is
// best effort, exactly as for truncate.idx: the atomic rename already
// converges to old-or-new on reopen.
func (l *Log) persistConsumers(next map[string]uint64) error {
	path := l.consumersPath()
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrCheckpointFailed, err)
	}
	fail := func(err error) error {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("%w: %v", ErrCheckpointFailed, err)
	}
	if _, err := f.Write(encodeConsumers(next)); err != nil {
		return fail(err)
	}
	if err := syncFile(f); err != nil {
		return fail(err)
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("%w: %v", ErrCheckpointFailed, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("%w: %v", ErrCheckpointFailed, err)
	}
	_ = syncDir(l.dir)
	l.consumers = next
	return nil
}

// syncSegmentsThrough fsyncs every surviving segment file at or before
// seg that has not been fsynced by an acknowledgement already, so that
// when Sync is false a returned acknowledgement still guarantees the
// target batch and every committed byte before it durable. Sealed
// segment files are immutable once rolled, so each file number needs
// this at most once per process and ackSyncedSeg tracks the high-water
// mark; the live segment keeps receiving appends, so a previous fsync
// never covers its current tail and it is synced on every advance.
func (l *Log) syncSegmentsThrough(seg int) error {
	for i := range l.segs {
		s := &l.segs[i]
		if s.file > seg {
			break
		}
		if l.file != nil && s.file == l.segIndex {
			// Live segment: always sync through the current tail.
			if err := syncFile(l.file); err != nil {
				return err
			}
			continue
		}
		// Sealed segment: immutable, skip once it is behind the mark.
		if uint64(s.file) <= l.ackSyncedSeg {
			continue
		}
		if err := l.fsyncSegmentFile(s.file); err != nil {
			return err
		}
		l.ackSyncedSeg = uint64(s.file)
	}
	return nil
}

// fsyncSegmentFile fsyncs one sealed segment, opening it read-only
// because the live append handle, if any, names a different file.
func (l *Log) fsyncSegmentFile(n int) error {
	f, err := os.Open(l.segmentPath(n))
	if err != nil {
		return err
	}
	defer f.Close()
	return syncFile(f)
}

// AckConsumer registers a consumer or advances its acknowledgement
// point. A new name is registered only with AckConsumer(name, 0) and
// starts at zero; any other target for an unregistered name returns
// ErrUnknownConsumer. An illegal name returns ErrInvalidConsumer.
//
// The point only moves forward: acknowledging the current value again
// succeeds without writing anything, and a smaller value returns
// ErrInvalidAck. A larger target must itself be committed and still
// retained, and every staged (uncommitted, non-hole) sequence before
// it must have committed first; a target that is unknown, a permanent
// hole, merely staged or already reclaimed returns ErrInvalidAck.
// Permanent holes may be skipped, and a member in the middle of a
// group may be acknowledged — the point is the processed-by-sequence
// prefix.
//
// On success the registration and the new point are durable, and so is
// every committed batch up to and including the target, even when
// Options.Sync is false. A data sync or a checkpoint read, write or
// sync failure returns ErrCheckpointFailed and changes no consumer
// state; the call can be retried.
func (l *Log) AckConsumer(name string, seq uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return errClosed
	}
	if !validBatchID(name) {
		return ErrInvalidConsumer
	}
	cur, known := l.consumers[name]
	if !known {
		if seq != 0 {
			return ErrUnknownConsumer
		}
		next := l.cloneConsumers()
		next[name] = 0
		return l.persistConsumers(next)
	}
	if seq == cur {
		// Re-acknowledging the current point is a cheap no-op: durable
		// already and nothing is written.
		return nil
	}
	if seq < cur {
		return ErrInvalidAck
	}
	ref, ok := l.index[seq]
	if !ok {
		// Not a committed, retained batch: unknown, a permanent hole,
		// merely staged, or reclaimed by DeleteThrough. Note that a
		// batch in a segment kept across the boundary can sit at or
		// below the persisted truncation point and still be indexed —
		// the index, not through, is the authority on "still here".
		return ErrInvalidAck
	}
	for s, recs := range l.staged {
		// A nil entry is a permanent hole and may be skipped; a real
		// staged batch ahead of the prefix would be acknowledged without
		// having committed.
		if recs != nil && s < seq {
			return ErrInvalidAck
		}
	}
	// With Options.Sync every commit already fsynced these bytes as it
	// landed; the forced durability matters precisely when it is off.
	if !l.opts.Sync {
		if err := l.syncSegmentsThrough(ref.seg); err != nil {
			return fmt.Errorf("%w: %v", ErrCheckpointFailed, err)
		}
	}
	next := l.cloneConsumers()
	next[name] = seq
	return l.persistConsumers(next)
}

// ConsumerSeq returns the acknowledged sequence of a registered
// consumer. An illegal name returns ErrInvalidConsumer; a legal but
// unregistered name returns ErrUnknownConsumer.
func (l *Log) ConsumerSeq(name string) (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.closed {
		return 0, errClosed
	}
	if !validBatchID(name) {
		return 0, ErrInvalidConsumer
	}
	seq, ok := l.consumers[name]
	if !ok {
		return 0, ErrUnknownConsumer
	}
	return seq, nil
}

// DropConsumer removes a consumer's registration and, durably, its
// protection of unacknowledged history. A dropped consumer does not
// come back on reopen; registering the same name again starts it at
// zero. An illegal name returns ErrInvalidConsumer and a legal but
// unregistered name returns ErrUnknownConsumer. A checkpoint write or
// sync failure returns ErrCheckpointFailed and leaves the registration
// in place, retryable.
func (l *Log) DropConsumer(name string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return errClosed
	}
	if !validBatchID(name) {
		return ErrInvalidConsumer
	}
	if _, ok := l.consumers[name]; !ok {
		return ErrUnknownConsumer
	}
	next := l.cloneConsumers()
	delete(next, name)
	return l.persistConsumers(next)
}
