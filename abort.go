package log

import (
	"encoding/binary"
	"fmt"
)

// Abort abandons a staged batch — anonymous or keyed, including one with
// zero records — and turns its reserved sequence into a permanent hole,
// exactly like the hole a crash-recovered torn tail leaves behind: Read
// returns ErrNotCommitted for it, Scan skips it, and no later Append
// reuses the sequence, across restarts and index.idx rebuilds. A
// segment that carries only abort holes and no committed batch is not
// part of the Segments listing.
//
// Aborting a keyed batch releases its idempotency key: a later
// AppendIdempotent with the same key reserves a fresh sequence, whether
// the records match the aborted batch or not. Every other batch's key
// and records are untouched.
//
// The hole is published like a commit and is durable regardless of
// Options.Sync: a standard BCLH hole marker is appended to the current
// segment and fsynced, then one hole record is appended to the index
// sidecar and fsynced. A crash anywhere in that sequence is recovered by
// the ordinary tail rules on the next open — a recognizable aborted
// sequence is never handed out again. A write, close or sync failure
// returns ErrAbortFailed; the marker and the sidecar record are rolled
// back, the batch stays staged with its reserved sequence and key, and
// retrying Abort or committing instead cannot produce a duplicate
// record.
//
// A zero-value batch, a batch belonging to another Log or a previous
// open, and a batch already committed or already aborted all return
// ErrUnknownBatch and change nothing. Aborting a closed log returns the
// same "log: closed" error as the other write entry points. Abort never
// registers consumers or advances checkpoints.
func (l *Log) Abort(b Batch) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return errClosed
	}
	recs, ok := l.staged[b.seq]
	if !ok || recs == nil || b.owner != l {
		// Never staged here, a zero or foreign batch, already committed,
		// or already aborted (a nil-records hole marker).
		return ErrUnknownBatch
	}
	id := l.stagedID[b.seq]

	// Publish the hole exactly like a commit publishes its entry: marker
	// in the segment first, fsync, then one sidecar record, fsync. Both
	// syncs are unconditional so a returned abort survives a power loss
	// even when Options.Sync is false.
	marker := encodeHoles([]uint64{b.seq})
	idxBefore := l.idxSize
	off, err := l.writeEntry(marker)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrAbortFailed, err)
	}
	if err := syncFile(l.file); err != nil {
		// The batch stays staged and keeps its reserved sequence and
		// key; drop the un-synced marker so a retry cannot duplicate it.
		l.rollbackEntry(off, idxBefore)
		return fmt.Errorf("%w: %v", ErrAbortFailed, err)
	}
	rec := encodeHoleRecord(indexHole{
		seg:     l.segIndex,
		off:     off,
		length:  len(marker),
		diskCRC: binary.LittleEndian.Uint32(marker[len(marker)-4:]),
		seqs:    []uint64{b.seq},
	})
	if err := l.appendAbortRecord(rec); err != nil {
		// The sidecar never advertises a hole whose abort failed: roll
		// both files back and leave the batch staged for retry.
		l.rollbackEntry(off, idxBefore)
		return err
	}

	// The sequence is now a permanent hole: keep it in staged as a
	// nil-records marker, the same in-memory shape crash-recovered holes
	// have, so Read reports ErrNotCommitted and the retention and
	// checkpoint prefix checks skip it. The key is released; every other
	// key keeps resolving as before.
	delete(l.staged, b.seq)
	delete(l.stagedID, b.seq)
	if id != "" {
		delete(l.ids, id)
	}
	l.staged[b.seq] = nil
	l.recordAbortHole(l.segIndex, b.seq)
	// The fsync above also covered every committed byte already in this
	// segment, so it no longer needs a checkpoint barrier.
	delete(l.segDirty, l.segIndex)
	return nil
}

// appendAbortRecord appends one hole record to the live sidecar and
// fsyncs it unconditionally: an abort is durable regardless of
// Options.Sync, unlike a commit's index append. Any failure is wrapped
// as ErrAbortFailed so callers can match it with errors.Is.
func (l *Log) appendAbortRecord(rec []byte) error {
	if l.idxFile == nil {
		return fmt.Errorf("%w: %v", ErrAbortFailed, errIndexInvalid)
	}
	if _, err := l.idxFile.Write(rec); err != nil {
		return fmt.Errorf("%w: %v", ErrAbortFailed, err)
	}
	l.idxSize += len(rec)
	if err := syncFile(l.idxFile); err != nil {
		return fmt.Errorf("%w: %v", ErrAbortFailed, err)
	}
	return nil
}

// recordAbortHole records a permanent hole at seq inside segment file
// seg in the segment listing, so prefix truncation counts the
// reservation when bounding reclaimable segments. The current segment is
// always the highest-numbered file in use, so its entry — when it
// exists — is the last one; a segment with no committed batch yet gets
// a hole-only entry, which the first later commit in that file upgrades
// (see commitSpan).
func (l *Log) recordAbortHole(seg int, seq uint64) {
	if n := len(l.segs); n > 0 && l.segs[n-1].file == seg {
		s := &l.segs[n-1]
		if seq > s.maxHole {
			s.maxHole = seq
		}
		s.hasHoles = true
		return
	}
	l.segs = append(l.segs, segInfo{file: seg, maxHole: seq, hasHoles: true})
}
