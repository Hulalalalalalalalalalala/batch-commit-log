# batch-commit-log

Append only commit log that groups records into durable segments, so a writer publishes a batch atomically and a reader replays exactly the batches that were committed.

## Requirements

Go 1.22 or newer. Standard library only.

## Build

    go build ./...
    go run ./cmd/logctl --dir <path> stat

## Public interface

`log.Open(dir string, opts Options) (*Log, error)` opens the log directory.
- `(*Log).Append(records [][]byte) (Batch, error)` stages a batch.
- `(*Log).Commit(batch Batch) (uint64, error)` makes a batch durable and returns its sequence.
- `(*Log).Read(seq uint64) ([][]byte, error)` returns one committed batch.
- `(*Log).Scan(from uint64, fn func(Batch) error) error` replays committed batches in order.
- `(*Log).Segments() []Segment` lists segments with their first and last sequence.
- `type Options struct { SegmentBytes int; Sync bool }`.
- `log.ErrNotCommitted`, `log.ErrCorruptSegment`, `log.ErrUnknownBatch`, `log.ErrSyncFailed` error values.

## Crash recovery

A crash can leave a half-written entry at the tail of the last segment. On the next open that torn tail is discarded: it is never replayed, its batch reads as staged (`ErrNotCommitted`), and its reserved sequence is not reused — the next append continues after it. A batch whose commit was interrupted is therefore either fully readable or fully absent after reopening, never half present. Any other malformed content, in any segment, fails `Open` with `ErrCorruptSegment`. With `Options.Sync` set, a returned `Commit` is durable; a failed fsync reports `ErrSyncFailed` and leaves the batch staged.

## Tests

    go test ./...

## Limits

Single writer; concurrent appends are not serialised.
Records are opaque bytes.
No compaction and no replication.
