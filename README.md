# batch-commit-log

Append only commit log that groups records into durable segments, so a writer publishes a batch atomically and a reader replays exactly the batches that were committed.

## Requirements

Go 1.22 or newer. Standard library only.

## Build

    go build ./...
    go run ./cmd/logctl --dir <path> stat

## Public interface

`log.Open(dir string, opts Options) (*Log, error)` opens the log directory. The committed set is taken from a persistent index sidecar (`index.idx`) when it can be adopted incrementally; if it is missing, truncated, version-mismatched, checksum-bad or contradictory to the segments, Open rebuilds it from the segment files, so the index is pure cache and opening a grown log no longer decodes every segment entry. A half-written tail of the last segment (crash mid-write) is discarded without error and its reserved sequence stays a permanent hole; any other inconsistency is `ErrCorruptSegment`.
- `(*Log).Append(records [][]byte) (Batch, error)` stages a batch.
- `(*Log).AppendIdempotent(key string, records [][]byte) (Batch, error)` stages a batch under a caller-chosen, directory-unique idempotency key and reserves its sequence. Retrying the same key after a network failure or a crash is safe: if the records are byte-for-byte identical (same length, order and bytes), it returns the first batch with the same `Seq` and `ID`, reserving no new sequence and writing nothing, whether the batch is still staged or already committed; committing the returned batch a second time is `ErrUnknownBatch`. A key already in use by a batch with different records returns `ErrBatchIDConflict`. An empty key, a key containing NUL, non-UTF-8 key or a key over 256 bytes returns `ErrInvalidBatchID`.
- `Batch.ID() string` returns the idempotency key, or an empty string for an anonymous batch staged with `Append`. A keyed batch keeps the same ID and records under `Scan` after commit and across reopens, and `Read` returns those identical records.
- `(*Log).Commit(batch Batch) (uint64, error)` makes a batch durable and returns its sequence. The segment entry is synced first, then one index sidecar record; with `Sync`, a failed fsync of either returns `ErrSyncFailed`, both files roll back and the batch stays staged for a retry that never duplicates the entry.
- `(*Log).CommitGroup(batches []Batch) ([]uint64, error)` commits several staged batches — anonymous, keyed, or a mix — as one atomic write and one fsync: all become durable together, or all stay staged for a retry. Each batch keeps its reserved sequence, and a crash never leaves half the group visible.
- `(*Log).Read(seq uint64) ([][]byte, error)` returns one committed batch, located directly through the persistent segment-level index (adopted from `index.idx` or rebuilt from the segment files on open).
- `(*Log).Scan(from uint64, fn func(Batch) error) error` replays committed batches in order.
- `(*Log).Segments() []Segment` lists segments with their first and last sequence.
- `type Options struct { SegmentBytes int; Sync bool }`.
- `log.ErrNotCommitted`, `log.ErrCorruptSegment`, `log.ErrUnknownBatch`, `log.ErrInvalidOptions`, `log.ErrSyncFailed`, `log.ErrBatchIDConflict`, `log.ErrInvalidBatchID` error values.

## Crash recovery and idempotency keys

Keys are stored in the segment entries themselves (a `BCLK` framing for a keyed single batch, `BCLM` for a group containing a keyed member) and in the `index.idx` v2 sidecar. A complete keyed batch stays dedup-able after reopen and after index rebuild; no extra on-disk file is involved. A half-written tail of the last segment still becomes a permanent sequence hole; if that tail belonged to a keyed batch, its incomplete key is forgotten, so a retry of the same key after recovery establishes a fresh batch. Old anonymous batches keep their `BCL1`/`BCLG` framing — keys are never inferred for them, and a v1 sidecar is rebuilt from the segments as usual.

## Tests

    go test ./...

## Limits

Single writer; concurrent appends are not serialised.
Records are opaque bytes.
No compaction and no replication.
