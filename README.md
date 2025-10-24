# snowflake

A high-performance unique ID generator based on Twitter's Snowflake algorithm, implemented in Go.

## Features

- Lock-free `Generate` and range-reserving `GenerateBatch` for high throughput workloads.
- Default epoch of **2025-01-01 UTC** (`DefaultEpoch`) to maximise lifespan, with opt-in overrides via `WithEpoch`.
- Helpers for extracting components (`ExtractTimestamp`, `ExtractWorkerID`, `ExtractSequence`) and validating IDs (`Validate`).
- Epoch-aware variants (`ExtractTimestampWithEpoch`, `ValidateWithEpoch`, `DecomposeWithEpoch`) for deployments that override the default epoch.
- Pluggable time source (`WithTimeFunc`) and a best-effort worker ID allocator for environments without central coordination.
- Configurable backwards clock drift tolerance via `WithClockDriftTolerance` (defaults to 5ms).
- String helpers for generating (`GenerateString`, `GenerateBatchStrings`) and parsing (`ParseString`, `ParseStringWithEpoch`).
- Extensive test coverage, including concurrency, high-load, and custom epoch scenarios.

## Bit Layout & Encoding

Snowflake IDs are unsigned 64-bit integers composed as follows:

- **1 bit** sign (always `0`, keeps IDs positive when cast to `int64`).
- **41 bits** timestamp in milliseconds since the configured epoch.
- **10 bits** worker identifier (0-1023).
- **12 bits** per-millisecond sequence counter (0-4095).

Within the generator, the timestamp and sequence are also packed into a 64-bit atomic state to allow compare-and-swap updates without locks.

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

Generate textual IDs when integrating with string-first protocols:

```go
strID, err := node.GenerateString()
if err != nil {
	log.Fatal(err)
}

parsed, err := snowflake.ParseString(strID)
if err != nil {
	log.Fatal(err)
}

fmt.Printf("string=%s numeric=%d\n", strID, parsed)
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

### Concurrency & load

The generator is lock-free, so you can share a single `Node` across goroutines:

```go
const goroutines = 16
const idsPerGoroutine = 2000

ids := make(chan uint64, goroutines*idsPerGoroutine)

var wg sync.WaitGroup
for g := 0; g < goroutines; g++ {
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < idsPerGoroutine; i++ {
			id, err := node.Generate()
			if err != nil {
				log.Fatalf("generate failed: %v", err)
			}
			ids <- id
		}
	}()
}

wg.Wait()
close(ids)
```

For bursty workloads, reserve sequences upfront with `GenerateBatch`:

```go
ids, err := node.GenerateBatch(500)
if err != nil {
	log.Fatal(err)
}

// Use the batch of IDs
for _, id := range ids {
	process(id)
}
```

### Decomposing & validating IDs

Extract component parts from an ID for inspection or routing:

```go
id, _ := node.Generate()

// Decompose into all components at once
ts, worker, seq, err := snowflake.Decompose(id)
if err != nil {
	log.Fatal(err)
}
fmt.Printf("Generated at %s by worker %d (seq=%d)\n", time.UnixMilli(ts), worker, seq)

// Or extract individual components
worker, _ := snowflake.ExtractWorkerID(id)
seq, _ := snowflake.ExtractSequence(id)
```

Validate that an ID is well-formed and not too far in the future:

```go
if err := snowflake.Validate(id); err != nil {
	log.Printf("Invalid ID: %v", err)
}

// With a custom epoch
if err := snowflake.ValidateWithEpoch(id, customEpochMillis); err != nil {
	log.Printf("Invalid ID for custom epoch: %v", err)
}
```

### Server with distributed ID generation

Set up a lightweight HTTP service where each instance uses its worker ID:

```go
package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strconv"

	"github.com/devsnb/snowflake"
)

var node *snowflake.Node

func init() {
	workerID, _ := strconv.ParseInt(os.Getenv("WORKER_ID"), 10, 64)
	var err error
	node, err = snowflake.NewNode(workerID)
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	http.HandleFunc("/id", func(w http.ResponseWriter, r *http.Request) {
		id, err := node.Generate()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		json.NewEncoder(w).Encode(map[string]uint64{"id": id})
	})

	http.HandleFunc("/ids", func(w http.ResponseWriter, r *http.Request) {
		ids, err := node.GenerateBatch(100)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		json.NewEncoder(w).Encode(map[string][]uint64{"ids": ids})
	})

	log.Fatal(http.ListenAndServe(":8080", nil))
}
```

### Time sources & testing

Use `WithTimeFunc` to inject a deterministic clock—handy for simulation or unit tests:

```go
fixed := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli()
node, _ := snowflake.NewNode(1, snowflake.WithTimeFunc(func() int64 { return fixed }))
```

Pair `WithTimeFunc` with `WithEpoch` to reproduce historical streams or validate boundary conditions.

### String parsing

Use the parsing helpers when IDs traverse text-based protocols:

```go
raw := "182193810283812736"
id, err := snowflake.ParseString(raw)
if err != nil {
	log.Fatal(err)
}

customEpoch := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli()
id, err = snowflake.ParseStringWithEpoch(raw, customEpoch)
if err != nil {
	log.Fatal(err)
}

stringsBatch, err := node.GenerateBatchStrings(500)
if err != nil {
	log.Fatal(err)
}

for _, idStr := range stringsBatch {
	_ = idStr // use textual IDs directly
}
```

### Clock drift tolerance

Nodes tolerate up to 5ms of backwards clock movement by default, retrying ID generation after a short sleep. Adjust this behaviour with `WithClockDriftTolerance`:

```go
node, err := snowflake.NewNode(
	9,
	snowflake.WithClockDriftTolerance(15*time.Millisecond),
)
```

Pass `0` to disable tolerance entirely (any backwards drift returns `ErrClockBackwards`).

## Error handling

- `ErrTimeBeforeEpoch` indicates the current clock is earlier than the configured epoch. Supply an older epoch through `WithEpoch` or wait until the epoch is reached.
- `ErrClockBackwards` and `ErrTimestampOverflow` surface clock skew and exhausted timestamp space respectively.
- `ErrTimestampInFuture` is returned during extraction/validation when a decoded timestamp is more than one hour ahead of the local wall clock.

## Testing & benchmarks

Run the full suite (tests and benchmarks):

```bash
go test ./...
go test -bench=. -benchmem
```

The project ships with high-load and concurrency tests. Use `go test -run HighLoad -count 1` to focus on the stress cases, or `go test -short` to skip them when running in constrained environments.

## Benchmarks (how to run & example results)

Quick commands to reproduce benchmarks used during development:

```bash
# run all benchmarks (may take time)
go test -bench=. -benchmem

# run a focused concurrent benchmark for stability
go test -bench=BenchmarkConcurrentGenerate -benchmem -benchtime=5s -count=3
```

Representative results (your CPU and settings will vary):

```
BenchmarkGenerate-16                    ~288 ns/op    0 B/op    0 allocs/op
BenchmarkGenerateBatch-16               ~26.3 µs      896 B/op  1 allocs/op  (100 IDs)
BenchmarkExtractTimestamp-16            ~39 ns/op     0 B/op    0 allocs/op
BenchmarkConcurrentGenerate-16          ~267-275 ns/op 0 B/op   0 allocs/op
```

These numbers are representative from development runs on an AMD Ryzen laptop; expect higher or lower throughput depending on cores, CPU frequency scaling, and OS timer granularity.

## Performance overview — why this is fast

This library aims for predictable, low-latency ID generation. The main reasons for the strong performance are:

- Lock-free, atomic state packing: timestamp (upper bits) and sequence (lower bits) are stored in a single 64-bit atomic value. Updates use a Compare-And-Swap (CAS) on that single word, avoiding mutexes and reducing contention.

- Minimal allocations on the critical path: `Generate()` performs no heap allocations (benchmarks show 0 allocs/op). `GenerateBatch` allocates a single result slice per call; string helpers allocate only when you ask for textual IDs.

- Batched reservation: `GenerateBatch` atomically reserves ranges of sequences in the current millisecond, amortizing the cost of CAS and time reads across many IDs.

- Cheap bit operations: IDs are produced with a small number of bit shifts and ORs. The implementation also precomputes shifted timestamp/worker fields where possible to avoid repeating these operations in tight loops.

- Controlled spin/sleep strategy: When sequence numbers are exhausted or the clock drifts backwards a small amount, the generator uses short sleeps (or a hybrid sleep/poll) rather than busy-waiting. This reduces CPU spin and keeps latency predictable.

- Pluggable time source: `WithTimeFunc` allows you to inject a low-cost monotonic millisecond source (for example, based on a cached start timestamp plus monotonic elapsed time). That can reduce `time.Now()` overhead in extremely hot paths while preserving monotonicity.

### Safe tuning tips

- Prefer `GenerateBatch` for bulk ID needs — it reduces per-ID overhead by batching CAS and time checks.
- If you control the process lifecycle, consider supplying a monotonic millisecond `timeFunc` via `WithTimeFunc` for the highest throughput scenarios (benchmark first; correctness must be preserved).
- Use `WithClockDriftTolerance` to tune how the generator handles small backwards clock adjustments in your environment. A small positive tolerance avoids errors during NTP adjustments; `0` disables tolerance and surfaces backwards movement immediately.
- If your application demands zero allocations for large, repeated batches, consider reusing buffers (e.g., a `sync.Pool` for `[]uint64` or `[]string`) on the caller side; the library keeps default semantics simple and safe.
- Measure on your target hardware and under realistic loads; CPU pinning, governor settings, and virtualization can change results.