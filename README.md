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
- `(*Log).CommitGroup(batches []Batch) ([]uint64, error)` commits several staged batches as one durable group: a single write and, with `Sync`, a single fsync. Each batch keeps its reserved sequence and the group becomes visible in sequence order. The group is all-or-nothing — after a crash or a failed sync (`ErrSyncFailed`, bytes rolled back) every batch in it is either visible or still staged, and a retry cannot duplicate entries.
- `(*Log).Read(seq uint64) ([][]byte, error)` returns one committed batch, located directly through a segment-level index that `Open` rebuilds from the segment files; reads after a rebuild agree exactly with a full replay.
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
