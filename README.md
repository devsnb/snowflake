# snowflake

A high-performance unique ID generator based on Twitter's Snowflake algorithm, implemented in Go.

## Features

- Lock-free `Generate` and range-reserving `GenerateBatch` for high throughput workloads.
- Default epoch of **2025-01-01 UTC** (`DefaultEpoch`) to maximise lifespan, with opt-in overrides via `WithEpoch`.
- Helpers for extracting components (`ExtractTimestamp`, `ExtractWorkerID`, `ExtractSequence`) and validating IDs (`Validate`).
- Epoch-aware variants (`ExtractTimestampWithEpoch`, `ValidateWithEpoch`, `DecomposeWithEpoch`) for deployments that override the default epoch.

## Usage

```go
node, err := snowflake.NewNode(42)
if err != nil {
	log.Fatal(err)
}

id, err := node.Generate()
if err != nil {
	log.Fatal(err)
}

ts, worker, seq, err := snowflake.Decompose(id)
if err != nil {
	log.Fatal(err)
}

fmt.Printf("timestamp=%s worker=%d sequence=%d\n", ts, worker, seq)
```

### Custom epoch

If your deployment date precedes the default epoch, or you need IDs that map to an earlier timeline, pass an explicit epoch (milliseconds since Unix epoch) when building the node:

```go
node, err := snowflake.NewNode(
	7,
	snowflake.WithEpoch(time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli()),
)
```

When you override the epoch, use the corresponding epoch-aware helpers to decode/validate IDs:

```go
ts, err := snowflake.ExtractTimestampWithEpoch(id, customEpochMillis)
```

## Error handling

- `ErrTimeBeforeEpoch` indicates the current clock is earlier than the configured epoch. Supply an older epoch through `WithEpoch` or wait until the epoch is reached.
- `ErrClockBackwards` and `ErrTimestampOverflow` surface clock skew and exhausted timestamp space respectively.

## Testing & benchmarks

Run the full suite (tests and benchmarks):

```bash
go test ./...
go test -bench=. -benchmem
```
