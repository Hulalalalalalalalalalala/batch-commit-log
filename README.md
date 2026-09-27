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
- `(*Log).Read(seq uint64) ([][]byte, error)` returns one committed batch, located directly through a persistent segment-level index kept in a side file (`index.idx`). Open adopts the side file incrementally when it is intact and consistent with the segments (it never decodes their entries on that path), and automatically rebuilds it from the segment files when it is missing, truncated, the wrong version, fails a checksum, or contradicts the segments; both paths yield identical reads, replay, segment listing and errors.
- `(*Log).Scan(from uint64, fn func(Batch) error) error` replays committed batches in order.
- `(*Log).Segments() []Segment` lists segments with their first and last sequence.
- `type Options struct { SegmentBytes int; Sync bool }`.
- `log.ErrNotCommitted`, `log.ErrCorruptSegment`, `log.ErrUnknownBatch`, `log.ErrInvalidOptions`, `log.ErrSyncFailed` error values.

## Tests

    go test ./...

## Durability and crash windows

With `Sync`, each `Commit` and `CommitGroup` is a fixed ordering of two
durability steps: the segment bytes are written and fsynced first, then
one index frame is appended to `index.idx` and fsynced. The whole group
is covered by the single segment sync and the single index sync. A
batch becomes visible exactly when its index frame is durable, so a
crash after the segment sync but before the index sync — with the entry
structurally complete on disk — still leaves that commit, or the whole
group, invisible; its reserved sequences become permanent holes, never
reused. A failed fsync at either step returns `ErrSyncFailed`, rolls
both files back to their prior sizes, and keeps the batches staged for
a retry that cannot duplicate the entry.

The side file never advertises a batch ahead of its segment. When
opened, a missing, truncated, wrong-version, checksum-bad or
contradictory side file is discarded and rebuilt; only a half-written
tail of the last segment is ever discarded as a crash remnant, and all
other inconsistencies are `ErrCorruptSegment`.

## Limits

Single writer; concurrent appends are not serialised.
Records are opaque bytes.
No compaction and no replication.
