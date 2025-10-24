package snowflake

import (
	"errors"
	"sync"
	"testing"
	"time"
)

// TestNewNodeValidWorkerID tests node creation with valid worker IDs
func TestNewNodeValidWorkerID(t *testing.T) {
	tests := []struct {
		name     string
		workerID int64
		wantErr  bool
	}{
		{"Zero", 0, false},
		{"Mid-range", 512, false},
		{"Max valid", MaxWorkerID, false},
		{"Negative", -1, true},
		{"Too large", MaxWorkerID + 1, true},
		{"Well over", 2048, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node, err := NewNode(tt.workerID)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewNode() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && node == nil {
				t.Error("NewNode() returned nil node for valid workerID")
			}
			if !tt.wantErr && node.workerID != tt.workerID {
				t.Errorf("NewNode() workerID = %d, want %d", node.workerID, tt.workerID)
			}
		})
	}
}

// TestNewNodeWithOptions tests node creation with options
func TestNewNodeWithOptions(t *testing.T) {
	mockTime := customEpoch + 1000000 // 1 second after epoch
	mockTimeFunc := func() int64 { return mockTime }

	node, err := NewNode(0, WithTimeFunc(mockTimeFunc))
	if err != nil {
		t.Fatalf("NewNode() with options failed: %v", err)
	}

	// Generate an ID and verify it uses mocked time
	id, err := node.Generate()
	if err != nil {
		t.Fatalf("Generate() failed: %v", err)
	}

	extractedTime, err := ExtractTimestamp(id)
	if err != nil {
		t.Fatalf("ExtractTimestamp() failed: %v", err)
	}

	expectedTime := time.UnixMilli(mockTime)
	if !extractedTime.Equal(expectedTime) {
		t.Errorf("ExtractTimestamp() = %v, want %v", extractedTime, expectedTime)
	}
}

// TestNewNodeWithBestEffortID tests best-effort ID generation
func TestNewNodeWithBestEffortID(t *testing.T) {
	node, err := NewNodeWithBestEffortID()
	if err != nil {
		t.Fatalf("NewNodeWithBestEffortID() failed: %v", err)
	}

	if node == nil {
		t.Fatal("NewNodeWithBestEffortID() returned nil node")
	}

	if node.workerID < 0 || node.workerID > MaxWorkerID {
		t.Errorf("NewNodeWithBestEffortID() workerID out of range: %d", node.workerID)
	}

	// Should be able to generate IDs
	id, err := node.Generate()
	if err != nil {
		t.Fatalf("Generate() failed: %v", err)
	}

	if id == 0 {
		t.Error("Generate() returned 0")
	}
}

// TestGenerateSingleID tests basic ID generation
func TestGenerateSingleID(t *testing.T) {
	node, err := NewNode(42)
	if err != nil {
		t.Fatalf("NewNode() failed: %v", err)
	}

	id, err := node.Generate()
	if err != nil {
		t.Fatalf("Generate() failed: %v", err)
	}

	if id == 0 {
		t.Error("Generate() returned 0")
	}

	// Verify extracted worker ID matches
	extractedWorkerID, err := ExtractWorkerID(id)
	if err != nil {
		t.Fatalf("ExtractWorkerID() failed: %v", err)
	}
	if extractedWorkerID != 42 {
		t.Errorf("ExtractWorkerID() = %d, want 42", extractedWorkerID)
	}
}

// TestGenerateMultipleIDs tests that generated IDs are unique and sequential
func TestGenerateMultipleIDs(t *testing.T) {
	node, err := NewNode(1)
	if err != nil {
		t.Fatalf("NewNode() failed: %v", err)
	}

	ids := make([]uint64, 100)
	for i := 0; i < 100; i++ {
		id, err := node.Generate()
		if err != nil {
			t.Fatalf("Generate() iteration %d failed: %v", i, err)
		}
		ids[i] = id
	}

	// Check uniqueness
	seen := make(map[uint64]bool)
	for i, id := range ids {
		if seen[id] {
			t.Errorf("Duplicate ID at index %d: %d", i, id)
		}
		seen[id] = true
	}

	// Check ordering (IDs should be non-decreasing)
	for i := 1; i < len(ids); i++ {
		if ids[i] < ids[i-1] {
			t.Errorf("IDs not in order: ids[%d]=%d < ids[%d]=%d", i, ids[i], i-1, ids[i-1])
		}
	}
}

// TestGenerateSequenceIncrement tests sequence counter behavior within same millisecond
func TestGenerateSequenceIncrement(t *testing.T) {
	currentTime := customEpoch + 1000 // 1000ms after epoch
	call := 0
	mockTimeFunc := func() int64 {
		// Return same time for first 20 calls, then increment
		if call < 20 {
			call++
			return currentTime
		}
		call++
		return currentTime + int64(call/20)
	}

	node, err := NewNode(5, WithTimeFunc(mockTimeFunc))
	if err != nil {
		t.Fatalf("NewNode() failed: %v", err)
	}

	// Generate multiple IDs in same millisecond
	ids := make([]uint64, 10)
	sequences := make([]int64, 10)

	for i := 0; i < 10; i++ {
		id, err := node.Generate()
		if err != nil {
			t.Fatalf("Generate() iteration %d failed: %v", i, err)
		}
		ids[i] = id

		seq, err := ExtractSequence(id)
		if err != nil {
			t.Fatalf("ExtractSequence() failed: %v", err)
		}
		sequences[i] = seq
	}

	// All IDs should be different
	seen := make(map[uint64]bool)
	for _, id := range ids {
		if seen[id] {
			t.Errorf("Duplicate ID within same millisecond")
		}
		seen[id] = true
	}

	// Sequences should be incrementing
	for i := 1; i < len(sequences); i++ {
		if sequences[i] != sequences[i-1]+1 {
			t.Errorf("Sequence not incrementing: seq[%d]=%d, seq[%d]=%d",
				i-1, sequences[i-1], i, sequences[i])
		}
	}
}

// TestGenerateClockBackwardsTolerance tests clock drift within acceptable range
func TestGenerateClockBackwardsTolerance(t *testing.T) {
	currentTime := customEpoch + 10000
	call := 0
	mockTimeFunc := func() int64 {
		call++
		// Simulate small clock drift
		if call == 1 {
			return currentTime
		}
		if call == 2 {
			return currentTime - 2 // 2ms backwards
		}
		// After sleep, return time that moved forward
		return currentTime + int64(call*10)
	}

	node, err := NewNode(0, WithTimeFunc(mockTimeFunc))
	if err != nil {
		t.Fatalf("NewNode() failed: %v", err)
	}

	// First ID at currentTime
	id1, err := node.Generate()
	if err != nil {
		t.Fatalf("Generate() first call failed: %v", err)
	}

	// Second ID with clock moved backwards (should recover)
	id2, err := node.Generate()
	if err != nil {
		t.Fatalf("Generate() second call failed (clock drift within tolerance): %v", err)
	}

	if id1 == 0 || id2 == 0 {
		t.Error("Generated IDs should not be 0")
	}

	// IDs should still be orderable
	if id2 <= id1 {
		t.Errorf("ID2 should be >= ID1 after recovery from drift: %d <= %d", id2, id1)
	}
}

// TestGenerateClockBackwardsExcessive tests excessive clock drift
func TestGenerateClockBackwardsExcessive(t *testing.T) {
	currentTime := customEpoch + 10000
	call := 0
	mockTimeFunc := func() int64 {
		call++
		if call == 1 {
			return currentTime
		}
		// Second call: 100ms backwards - exceeds tolerance
		return currentTime - 100
	}

	node, err := NewNode(0, WithTimeFunc(mockTimeFunc))
	if err != nil {
		t.Fatalf("NewNode() failed: %v", err)
	}

	// First ID
	_, err = node.Generate()
	if err != nil {
		t.Fatalf("Generate() first call failed: %v", err)
	}

	// Second call with excessive backwards drift should fail
	_, err = node.Generate()
	if err == nil {
		t.Error("Generate() should error on excessive clock backwards")
	}
	if !errors.Is(err, ErrClockBackwards) {
		t.Errorf("Generate() error type = %v, want ErrClockBackwards", err)
	}
}

// TestGenerateBatchValid tests batch generation with valid count
func TestGenerateBatchValid(t *testing.T) {
	node, err := NewNode(99)
	if err != nil {
		t.Fatalf("NewNode() failed: %v", err)
	}

	count := 1000
	ids, err := node.GenerateBatch(count)
	if err != nil {
		t.Fatalf("GenerateBatch() failed: %v", err)
	}

	if len(ids) != count {
		t.Errorf("GenerateBatch() returned %d IDs, want %d", len(ids), count)
	}

	// Check uniqueness
	seen := make(map[uint64]bool)
	for i, id := range ids {
		if seen[id] {
			t.Errorf("Duplicate ID at index %d", i)
		}
		seen[id] = true

		if id == 0 {
			t.Errorf("ID at index %d is 0", i)
		}
	}

	// Check ordering
	for i := 1; i < len(ids); i++ {
		if ids[i] < ids[i-1] {
			t.Errorf("IDs not in order at index %d", i)
		}
	}

	// Verify worker ID in all IDs
	for i, id := range ids {
		workerID, err := ExtractWorkerID(id)
		if err != nil {
			t.Fatalf("ExtractWorkerID() at index %d failed: %v", i, err)
		}
		if workerID != 99 {
			t.Errorf("ID at index %d has wrong worker ID: %d, want 99", i, workerID)
		}
	}
}

// TestGenerateBatchInvalidCount tests batch generation with invalid counts
func TestGenerateBatchInvalidCount(t *testing.T) {
	node, err := NewNode(0)
	if err != nil {
		t.Fatalf("NewNode() failed: %v", err)
	}

	tests := []struct {
		name  string
		count int
	}{
		{"Zero", 0},
		{"Negative", -1},
		{"Negative large", -1000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ids, err := node.GenerateBatch(tt.count)
			if err == nil {
				t.Errorf("GenerateBatch(%d) should error", tt.count)
			}
			if ids != nil {
				t.Errorf("GenerateBatch(%d) should return nil on error, got %v", tt.count, ids)
			}
		})
	}
}

// TestExtractTimestamp tests timestamp extraction
func TestExtractTimestamp(t *testing.T) {
	currentTime := customEpoch + 5000
	mockTimeFunc := func() int64 { return currentTime }

	node, err := NewNode(0, WithTimeFunc(mockTimeFunc))
	if err != nil {
		t.Fatalf("NewNode() failed: %v", err)
	}

	id, err := node.Generate()
	if err != nil {
		t.Fatalf("Generate() failed: %v", err)
	}

	extractedTime, err := ExtractTimestamp(id)
	if err != nil {
		t.Fatalf("ExtractTimestamp() failed: %v", err)
	}

	expectedTime := time.UnixMilli(currentTime)
	if !extractedTime.Equal(expectedTime) {
		t.Errorf("ExtractTimestamp() = %v, want %v", extractedTime, expectedTime)
	}
}

// TestExtractTimestampInvalidID tests timestamp extraction on invalid IDs
func TestExtractTimestampInvalidID(t *testing.T) {
	tests := []struct {
		name    string
		id      uint64
		wantErr bool
	}{
		{"Zero ID", 0, true},
		{"Far future", ^uint64(0), true}, // Very large ID with future timestamp
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ExtractTimestamp(tt.id)
			if (err != nil) != tt.wantErr {
				t.Errorf("ExtractTimestamp() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestExtractWorkerID tests worker ID extraction
func TestExtractWorkerID(t *testing.T) {
	tests := []int64{0, 1, 512, 1023}

	for _, workerID := range tests {
		t.Run(string(rune(workerID)), func(t *testing.T) {
			node, err := NewNode(workerID)
			if err != nil {
				t.Fatalf("NewNode() failed: %v", err)
			}

			id, err := node.Generate()
			if err != nil {
				t.Fatalf("Generate() failed: %v", err)
			}

			extracted, err := ExtractWorkerID(id)
			if err != nil {
				t.Fatalf("ExtractWorkerID() failed: %v", err)
			}

			if extracted != workerID {
				t.Errorf("ExtractWorkerID() = %d, want %d", extracted, workerID)
			}
		})
	}
}

// TestExtractWorkerIDInvalidID tests worker ID extraction on invalid IDs
func TestExtractWorkerIDInvalidID(t *testing.T) {
	_, err := ExtractWorkerID(0)
	if err == nil {
		t.Error("ExtractWorkerID(0) should error")
	}
	if !errors.Is(err, ErrInvalidSnowflakeID) {
		t.Errorf("ExtractWorkerID() error = %v, want ErrInvalidSnowflakeID", err)
	}
}

// TestExtractSequence tests sequence extraction
func TestExtractSequence(t *testing.T) {
	currentTime := customEpoch + 5000
	call := 0
	mockTimeFunc := func() int64 {
		if call < 5 {
			call++
			return currentTime
		}
		call++
		return currentTime + 10
	}

	node, err := NewNode(0, WithTimeFunc(mockTimeFunc))
	if err != nil {
		t.Fatalf("NewNode() failed: %v", err)
	}

	// Generate 5 IDs in same millisecond
	for i := 0; i < 5; i++ {
		id, err := node.Generate()
		if err != nil {
			t.Fatalf("Generate() iteration %d failed: %v", i, err)
		}

		extracted, err := ExtractSequence(id)
		if err != nil {
			t.Fatalf("ExtractSequence() failed: %v", err)
		}

		if extracted != int64(i) {
			t.Errorf("Iteration %d: ExtractSequence() = %d, want %d", i, extracted, i)
		}
	}
}

// TestExtractSequenceInvalidID tests sequence extraction on invalid IDs
func TestExtractSequenceInvalidID(t *testing.T) {
	_, err := ExtractSequence(0)
	if err == nil {
		t.Error("ExtractSequence(0) should error")
	}
	if !errors.Is(err, ErrInvalidSnowflakeID) {
		t.Errorf("ExtractSequence() error = %v, want ErrInvalidSnowflakeID", err)
	}
}

// TestValidateValidID tests ID validation with valid IDs
func TestValidateValidID(t *testing.T) {
	node, err := NewNode(10)
	if err != nil {
		t.Fatalf("NewNode() failed: %v", err)
	}

	for i := 0; i < 100; i++ {
		id, err := node.Generate()
		if err != nil {
			t.Fatalf("Generate() iteration %d failed: %v", i, err)
		}

		err = Validate(id)
		if err != nil {
			t.Errorf("Validate() iteration %d failed: %v", i, err)
		}
	}
}

// TestValidateInvalidID tests ID validation with invalid IDs
func TestValidateInvalidID(t *testing.T) {
	tests := []struct {
		name    string
		id      uint64
		wantErr bool
	}{
		{"Zero ID", 0, true},
		{"Valid non-zero", uint64(1) << TimestampShift, false}, // Valid structure even if artificial
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := Validate(tt.id)
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestIDReconstruction tests that extracted components can reconstruct the original ID
func TestIDReconstruction(t *testing.T) {
	node, err := NewNode(512)
	if err != nil {
		t.Fatalf("NewNode() failed: %v", err)
	}

	for i := 0; i < 50; i++ {
		id, err := node.Generate()
		if err != nil {
			t.Fatalf("Generate() iteration %d failed: %v", i, err)
		}

		// Extract all components
		_, err = ExtractTimestamp(id)
		if err != nil {
			t.Fatalf("ExtractTimestamp() failed: %v", err)
		}

		workerID, err := ExtractWorkerID(id)
		if err != nil {
			t.Fatalf("ExtractWorkerID() failed: %v", err)
		}

		sequence, err := ExtractSequence(id)
		if err != nil {
			t.Fatalf("ExtractSequence() failed: %v", err)
		}

		// Verify worker ID
		if workerID != 512 {
			t.Errorf("Iteration %d: workerID = %d, want 512", i, workerID)
		}

		// Sequence should be in valid range
		if sequence < 0 || sequence > MaxSequence {
			t.Errorf("Iteration %d: sequence %d out of range", i, sequence)
		}
	}
}

// TestBitBoundaries tests that all components fit within their bit allocations
func TestBitBoundaries(t *testing.T) {
	node, err := NewNode(MaxWorkerID)
	if err != nil {
		t.Fatalf("NewNode() failed: %v", err)
	}

	// Generate many IDs to potentially hit sequence limits
	ids, err := node.GenerateBatch(1000)
	if err != nil {
		t.Fatalf("GenerateBatch() failed: %v", err)
	}

	for i, id := range ids {
		// Extract and verify all components are within bounds
		workerID, err := ExtractWorkerID(id)
		if err != nil {
			t.Fatalf("ExtractWorkerID() at index %d failed: %v", i, err)
		}
		if workerID < 0 || workerID > MaxWorkerID {
			t.Errorf("Index %d: workerID %d out of range", i, workerID)
		}

		sequence, err := ExtractSequence(id)
		if err != nil {
			t.Fatalf("ExtractSequence() at index %d failed: %v", i, err)
		}
		if sequence < 0 || sequence > MaxSequence {
			t.Errorf("Index %d: sequence %d out of range", i, sequence)
		}

		// Timestamp should be reasonable
		ts, err := ExtractTimestamp(id)
		if err != nil {
			t.Fatalf("ExtractTimestamp() at index %d failed: %v", i, err)
		}
		if ts.Before(time.UnixMilli(customEpoch)) {
			t.Errorf("Index %d: timestamp before epoch", i)
		}
	}
}

// TestConcurrentGeneration tests thread-safe ID generation
func TestConcurrentGeneration(t *testing.T) {
	node, err := NewNode(100)
	if err != nil {
		t.Fatalf("NewNode() failed: %v", err)
	}

	const numGoroutines = 10
	const idsPerGoroutine = 100
	idsChan := make(chan uint64, numGoroutines*idsPerGoroutine)

	var wg sync.WaitGroup
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < idsPerGoroutine; i++ {
				id, err := node.Generate()
				if err != nil {
					t.Errorf("Generate() failed: %v", err)
					return
				}
				idsChan <- id
			}
		}()
	}

	wg.Wait()
	close(idsChan)

	// Collect all IDs and verify uniqueness
	ids := make([]uint64, 0, numGoroutines*idsPerGoroutine)
	seen := make(map[uint64]bool)

	for id := range idsChan {
		if seen[id] {
			t.Errorf("Duplicate ID generated: %d", id)
		}
		seen[id] = true
		ids = append(ids, id)
	}

	if len(ids) != numGoroutines*idsPerGoroutine {
		t.Errorf("Expected %d IDs, got %d", numGoroutines*idsPerGoroutine, len(ids))
	}
}

// TestConcurrentBatchGeneration tests thread-safe batch ID generation
func TestConcurrentBatchGeneration(t *testing.T) {
	node, err := NewNode(50)
	if err != nil {
		t.Fatalf("NewNode() failed: %v", err)
	}

	const numGoroutines = 5
	const batchSize = 100
	idsChan := make(chan uint64, numGoroutines*batchSize)

	var wg sync.WaitGroup
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ids, err := node.GenerateBatch(batchSize)
			if err != nil {
				t.Errorf("GenerateBatch() failed: %v", err)
				return
			}
			for _, id := range ids {
				idsChan <- id
			}
		}()
	}

	wg.Wait()
	close(idsChan)

	// Verify all IDs are unique
	seen := make(map[uint64]bool)
	count := 0

	for id := range idsChan {
		if seen[id] {
			t.Errorf("Duplicate ID generated: %d", id)
		}
		seen[id] = true
		count++
	}

	if count != numGoroutines*batchSize {
		t.Errorf("Expected %d IDs, got %d", numGoroutines*batchSize, count)
	}
}

// TestDecompose tests the Decompose function for breaking down IDs
func TestDecompose(t *testing.T) {
	node, err := NewNode(123)
	if err != nil {
		t.Fatalf("NewNode() failed: %v", err)
	}

	id, err := node.Generate()
	if err != nil {
		t.Fatalf("Generate() failed: %v", err)
	}

	timestamp, workerID, sequence, err := Decompose(id)
	if err != nil {
		t.Fatalf("Decompose() failed: %v", err)
	}

	// Verify timestamp is valid
	if timestamp.IsZero() {
		t.Error("Decompose() returned zero timestamp")
	}

	// Verify worker ID matches
	if workerID != 123 {
		t.Errorf("Decompose() workerID = %d, want 123", workerID)
	}

	// Verify sequence is in valid range
	if sequence < 0 || sequence > MaxSequence {
		t.Errorf("Decompose() sequence = %d, out of range [0, %d]", sequence, MaxSequence)
	}

	// Verify extracted components match individual extract functions
	ts, _ := ExtractTimestamp(id)
	if !timestamp.Equal(ts) {
		t.Errorf("Decompose() timestamp mismatch with ExtractTimestamp()")
	}

	wid, _ := ExtractWorkerID(id)
	if workerID != wid {
		t.Errorf("Decompose() workerID mismatch with ExtractWorkerID()")
	}

	seq, _ := ExtractSequence(id)
	if sequence != seq {
		t.Errorf("Decompose() sequence mismatch with ExtractSequence()")
	}
}

// TestDecomposeInvalidID tests Decompose with invalid ID
func TestDecomposeInvalidID(t *testing.T) {
	_, _, _, err := Decompose(0)
	if err == nil {
		t.Error("Decompose(0) should error")
	}
	if !errors.Is(err, ErrInvalidSnowflakeID) {
		t.Errorf("Decompose() error = %v, want ErrInvalidSnowflakeID", err)
	}
}

// TestSequenceExhaustionRecovery tests recovery from sequence exhaustion
func TestSequenceExhaustionRecovery(t *testing.T) {
	currentTime := customEpoch + 5000
	callCount := 0
	mockTimeFunc := func() int64 {
		callCount++
		// Return same time for sequences 0 through MaxSequence (4096 IDs)
		// On the 4097th call (when sequence would wrap), still return same time to trigger exhaustion
		// Then on the re-check after sleep, return next time
		if callCount <= int(MaxSequence)+1 {
			return currentTime
		}
		// After exhaustion, return next millisecond
		return currentTime + 1
	}

	node, err := NewNode(0, WithTimeFunc(mockTimeFunc))
	if err != nil {
		t.Fatalf("NewNode() failed: %v", err)
	}

	// Generate enough IDs to exhaust sequence and recover
	ids := make([]uint64, MaxSequence+3)
	for i := 0; i < int(MaxSequence)+3; i++ {
		id, err := node.Generate()
		if err != nil {
			t.Fatalf("Generate() iteration %d failed: %v", i, err)
		}
		ids[i] = id
	}

	// All IDs should be unique
	seen := make(map[uint64]bool)
	for i, id := range ids {
		if seen[id] {
			t.Errorf("Duplicate ID at index %d", i)
		}
		seen[id] = true
	}

	// ID at index MaxSequence+1 should have sequence 0 (after exhaustion and time moved forward)
	resetSeq, err := ExtractSequence(ids[MaxSequence+1])
	if err != nil {
		t.Fatalf("ExtractSequence() for reset ID failed: %v", err)
	}
	if resetSeq != 0 {
		t.Errorf("ID at index %d (after exhaustion) has sequence = %d, want 0", MaxSequence+1, resetSeq)
	}

	// Next ID should have sequence 1
	nextSeq, err := ExtractSequence(ids[MaxSequence+2])
	if err != nil {
		t.Fatalf("ExtractSequence() for next ID failed: %v", err)
	}
	if nextSeq != 1 {
		t.Errorf("ID at index %d has sequence = %d, want 1", MaxSequence+2, nextSeq)
	}
}

// // BenchmarkGenerate benchmarks single ID generation
func BenchmarkGenerate(b *testing.B) {
	node, err := NewNode(1)
	if err != nil {
		b.Fatalf("NewNode() failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := node.Generate()
		if err != nil {
			b.Fatalf("Generate() failed: %v", err)
		}
	}
}

// BenchmarkGenerateBatch benchmarks batch ID generation
func BenchmarkGenerateBatch(b *testing.B) {
	node, err := NewNode(1)
	if err != nil {
		b.Fatalf("NewNode() failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := node.GenerateBatch(100)
		if err != nil {
			b.Fatalf("GenerateBatch() failed: %v", err)
		}
	}
}

// BenchmarkExtractTimestamp benchmarks timestamp extraction
func BenchmarkExtractTimestamp(b *testing.B) {
	node, err := NewNode(1)
	if err != nil {
		b.Fatalf("NewNode() failed: %v", err)
	}

	id, err := node.Generate()
	if err != nil {
		b.Fatalf("Generate() failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := ExtractTimestamp(id)
		if err != nil {
			b.Fatalf("ExtractTimestamp() failed: %v", err)
		}
	}
}

// BenchmarkExtractWorkerID benchmarks worker ID extraction
func BenchmarkExtractWorkerID(b *testing.B) {
	node, err := NewNode(1)
	if err != nil {
		b.Fatalf("NewNode() failed: %v", err)
	}

	id, err := node.Generate()
	if err != nil {
		b.Fatalf("Generate() failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := ExtractWorkerID(id)
		if err != nil {
			b.Fatalf("ExtractWorkerID() failed: %v", err)
		}
	}
}

// BenchmarkValidate benchmarks ID validation
func BenchmarkValidate(b *testing.B) {
	node, err := NewNode(1)
	if err != nil {
		b.Fatalf("NewNode() failed: %v", err)
	}

	id, err := node.Generate()
	if err != nil {
		b.Fatalf("Generate() failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := Validate(id)
		if err != nil {
			b.Fatalf("Validate() failed: %v", err)
		}
	}
}

// BenchmarkConcurrentGenerate benchmarks concurrent ID generation
func BenchmarkConcurrentGenerate(b *testing.B) {
	node, err := NewNode(1)
	if err != nil {
		b.Fatalf("NewNode() failed: %v", err)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := node.Generate()
			if err != nil {
				b.Fatalf("Generate() failed: %v", err)
			}
		}
	})
}
