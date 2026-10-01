# batch-commit-log

Append only commit log that groups records into durable segments, so a writer publishes a batch atomically and a reader replays exactly the batches that were committed.

## Requirements

Go 1.22 or newer. Standard library only.

## Build

    go build ./...
    go run ./cmd/logctl --dir <path> stat

## Public interface

`log.Open(dir string, opts Options) (*Log, error)` opens the log directory. The committed set is taken from a persistent index sidecar (`index.idx`) when it can be adopted incrementally; if it is missing, truncated, version-mismatched, checksum-bad or contradictory to the segments, Open rebuilds it from the segment files, so the index is pure cache and opening a grown log no longer decodes every segment entry. A half-written tail of the last segment (crash mid-write) is discarded without error and its reserved sequence stays a permanent hole; any other inconsistency is `ErrCorruptSegment`.
- `(*Log).Append(records [][]byte) (Batch, error)` stages an anonymous batch.
- `(*Log).AppendIdempotent(key string, records [][]byte) (Batch, error)` stages a batch under a key that is unique within the log directory. A repeat with the same key and byte-identical records (same length, order and bytes) returns the first batch and its `Seq`, whether it is still staged or already committed, reserving no new sequence and writing nothing twice. A repeat with different records returns `ErrBatchIDConflict`. An empty key, a key containing a NUL byte, invalid UTF-8, or a key longer than 256 bytes returns `ErrInvalidBatchID`. The key is stored inside the batch's segment entry, so dedup survives process restarts and index rebuilds; a torn tail forgets its incomplete key and leaves the reserved sequence a permanent hole, letting the same key start a new batch.
- `Batch.ID() string` returns the idempotency key for a batch from `AppendIdempotent`, or `""` for an anonymous batch; batches handed to `Scan` report the same ID and records the segment stored.
- `(*Log).Commit(batch Batch) (uint64, error)` makes a batch durable and returns its sequence. The segment entry is synced first, then one index sidecar record; with `Sync`, a failed fsync of either returns `ErrSyncFailed`, both files roll back and the batch stays staged for a retry that never duplicates the entry. Committing the batch returned by a post-commit idempotent retry returns `ErrUnknownBatch` (the original batch is already committed).
- `(*Log).CommitGroup(batches []Batch) ([]uint64, error)` commits several staged batches as one atomic write and one fsync: all become durable together, or all stay staged for a retry. Groups may mix anonymous and keyed batches; each batch keeps its reserved sequence, and a crash never leaves half the group visible.
- `(*Log).Read(seq uint64) ([][]byte, error)` returns one committed batch, located directly through the persistent segment-level index (adopted from `index.idx` or rebuilt from the segment files on open).
- `(*Log).Scan(from uint64, fn func(Batch) error) error` replays committed batches in order. A `from` in a prefix reclaimed by `DeleteThrough` starts at the first retained batch; with no retained batch it ends normally.
- `(*Log).Segments() []Segment` lists the retained segments with their first and last sequence.
- `(*Log).DeleteThrough(seq uint64) (int, error)` reclaims the oldest **whole** segments whose last committed sequence is `<= seq` and returns the number of segment files removed. It never rewrites entries or splits a batch/group: a segment straddling `seq` is kept whole (one fewer segment removed). `DeleteThrough(0)` is a no-op returning 0; a never-reserved, staged or out-of-range `seq`, and a cut that would forget a reserved-but-uncommitted sequence, return `ErrInvalidRetention` and delete nothing. The truncation point is persisted in an authoritative `truncate.idx` marker before any segment is removed, so reclaimed sequences read as `ErrTruncated` (distinct from `ErrUnknownBatch`) after a reopen and even after `index.idx` is rebuilt; a crash during deletion is reconciled on the next open into either the complete old or complete new state. Persistence or sync failures return `ErrRetentionFailed` with the old state fully readable. Keys of deleted batches are released for reuse; retained keys keep their dedup and conflict semantics.
- `type Options struct { SegmentBytes int; Sync bool }`.
- `log.ErrNotCommitted`, `log.ErrCorruptSegment`, `log.ErrUnknownBatch`, `log.ErrInvalidOptions`, `log.ErrSyncFailed`, `log.ErrInvalidBatchID`, `log.ErrBatchIDConflict`, `log.ErrTruncated`, `log.ErrInvalidRetention`, `log.ErrRetentionFailed` error values.

## Tests

    go test ./...

## Limits

Single writer; concurrent appends are not serialised.
Records are opaque bytes.
No compaction and no replication.
