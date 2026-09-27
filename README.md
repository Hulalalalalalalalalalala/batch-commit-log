# batch-commit-log

Append only commit log that groups records into durable segments, so a writer publishes a batch atomically and a reader replays exactly the batches that were committed.

## Requirements

Go 1.22 or newer. Standard library only.

## Build

    go build ./...
    go run ./cmd/logctl --dir <path> stat

## Public interface

`log.Open(dir string, opts Options) (*Log, error)` opens the log directory. A half-written tail of the last segment (crash mid-write) is discarded without error and its reserved sequence stays a permanent hole; any other inconsistency is `ErrCorruptSegment`.
- `(*Log).Append(records [][]byte) (Batch, error)` stages a batch.
- `(*Log).Commit(batch Batch) (uint64, error)` makes a batch durable and returns its sequence. With `Sync`, a failed fsync returns `ErrSyncFailed` and the batch stays staged for a retry.
- `(*Log).CommitGroup(batches []Batch) ([]uint64, error)` commits several staged batches as one atomic write and one fsync: all become durable together, or all stay staged for a retry. Each batch keeps its reserved sequence, and a crash never leaves half the group visible.
- `(*Log).Read(seq uint64) ([][]byte, error)` returns one committed batch, located directly through a segment-level index that is rebuilt from the segment files on open.
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
