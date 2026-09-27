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
- `(*Log).Commit(batch Batch) (uint64, error)` makes a batch durable and returns its sequence. The segment entry is synced first, then one index sidecar record; with `Sync`, a failed fsync of either returns `ErrSyncFailed`, both files roll back and the batch stays staged for a retry that never duplicates the entry.
- `(*Log).CommitGroup(batches []Batch) ([]uint64, error)` commits several staged batches as one atomic write and one fsync: all become durable together, or all stay staged for a retry. Each batch keeps its reserved sequence, and a crash never leaves half the group visible.
- `(*Log).Read(seq uint64) ([][]byte, error)` returns one committed batch, located directly through the persistent segment-level index (adopted from `index.idx` or rebuilt from the segment files on open).
- `(*Log).Scan(from uint64, fn func(Batch) error) error` replays committed batches in order.
- `(*Log).Segments() []Segment` lists segments with their first and last sequence.
- `type Options struct { SegmentBytes int; Sync bool }`.
- `log.ErrNotCommitted`, `log.ErrCorruptSegment`, `log.ErrUnknownBatch`, `log.ErrInvalidOptions`, `log.ErrSyncFailed` error values.

## Tests

    go test ./...

## Limits

Single writer; concurrent appends are not serialised.
Records are opaque bytes.
No compaction and no replication.
