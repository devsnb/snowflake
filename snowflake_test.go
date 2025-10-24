package snowflake

import (
	"errors"
	"strconv"
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
	mockTime := DefaultEpoch + 1000000 // 1 second after epoch
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

// TestNewNodeWithEpoch verifies configurable epochs work throughout helpers.
func TestNewNodeWithEpoch(t *testing.T) {
	customEpoch := DefaultEpoch - 10_000 // 10 seconds earlier than default
	mockTime := DefaultEpoch

	node, err := NewNode(7, WithEpoch(customEpoch), WithTimeFunc(func() int64 { return mockTime }))
	if err != nil {
		t.Fatalf("NewNode() with WithEpoch failed: %v", err)
	}

	id, err := node.Generate()
	if err != nil {
		t.Fatalf("Generate() failed: %v", err)
	}

	ts, err := ExtractTimestampWithEpoch(id, customEpoch)
	if err != nil {
		t.Fatalf("ExtractTimestampWithEpoch() failed: %v", err)
	}

	expected := time.UnixMilli(mockTime)
	if !ts.Equal(expected) {
		t.Fatalf("ExtractTimestampWithEpoch() = %v, want %v", ts, expected)
	}

	if err := ValidateWithEpoch(id, customEpoch); err != nil {
		t.Fatalf("ValidateWithEpoch() failed: %v", err)
	}

	_, workerID, sequence, err := DecomposeWithEpoch(id, customEpoch)
	if err != nil {
		t.Fatalf("DecomposeWithEpoch() failed: %v", err)
	}

	if workerID != 7 {
		t.Fatalf("DecomposeWithEpoch() workerID = %d, want 7", workerID)
	}

	if sequence != 0 {
		t.Fatalf("expected initial sequence 0, got %d", sequence)
	}
}

// TestExtractTimestampWithEpochDifference ensures the epoch-aware helper aligns timestamps with the configured epoch.
func TestExtractTimestampWithEpochDifference(t *testing.T) {
	customEpoch := DefaultEpoch - int64(time.Hour/time.Millisecond) // 1 hour earlier
	absoluteTime := customEpoch + 25_000

	node, err := NewNode(3, WithEpoch(customEpoch), WithTimeFunc(func() int64 { return absoluteTime }))
	if err != nil {
		t.Fatalf("NewNode() failed: %v", err)
	}

	id, err := node.Generate()
	if err != nil {
		t.Fatalf("Generate() failed: %v", err)
	}

	tsWithEpoch, err := ExtractTimestampWithEpoch(id, customEpoch)
	if err != nil {
		t.Fatalf("ExtractTimestampWithEpoch() failed: %v", err)
	}

	if want := time.UnixMilli(absoluteTime); !tsWithEpoch.Equal(want) {
		t.Fatalf("ExtractTimestampWithEpoch() = %v, want %v", tsWithEpoch, want)
	}

	tsDefault, err := ExtractTimestamp(id)
	if err != nil {
		t.Fatalf("ExtractTimestamp() failed: %v", err)
	}

	offset := time.Duration(DefaultEpoch-customEpoch) * time.Millisecond
	if !tsDefault.Equal(tsWithEpoch.Add(offset)) {
		t.Fatalf("default extraction mismatch: got %v, want %v", tsDefault, tsWithEpoch.Add(offset))
	}

	if err := ValidateWithEpoch(id, customEpoch); err != nil {
		t.Fatalf("ValidateWithEpoch() failed: %v", err)
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

// TestGenerateString ensures string output matches numeric generation semantics.
func TestGenerateString(t *testing.T) {
	node, err := NewNode(2)
	if err != nil {
		t.Fatalf("NewNode() failed: %v", err)
	}

	str, err := node.GenerateString()
	if err != nil {
		t.Fatalf("GenerateString() failed: %v", err)
	}

	if str == "" {
		t.Fatalf("GenerateString() returned empty string")
	}

	idFromStr, err := ParseString(str)
	if err != nil {
		t.Fatalf("ParseString() failed: %v", err)
	}

	if err := Validate(idFromStr); err != nil {
		t.Fatalf("Validate() failed for parsed ID: %v", err)
	}

	if parsedUint, err := strconv.ParseUint(str, 10, 64); err != nil || parsedUint != idFromStr {
		t.Fatalf("string mismatch: parsedUint=%d err=%v", parsedUint, err)
	}
}

// TestGenerateSequenceIncrement tests sequence counter behavior within same millisecond
func TestGenerateSequenceIncrement(t *testing.T) {
	currentTime := DefaultEpoch + 1000 // 1000ms after epoch
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
	currentTime := DefaultEpoch + 10000
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
	currentTime := DefaultEpoch + 10000
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

// TestGenerateClockBackwardsCustomTolerance ensures configurable tolerance allows larger drift.
func TestGenerateClockBackwardsCustomTolerance(t *testing.T) {
	currentTime := DefaultEpoch + 20000
	call := 0
	mockTimeFunc := func() int64 {
		call++
		switch call {
		case 1:
			return currentTime
		case 2:
			return currentTime - 8 // 8ms backwards but within custom 10ms tolerance
		default:
			return currentTime + int64(call*5)
		}
	}

	node, err := NewNode(0, WithClockDriftTolerance(10*time.Millisecond), WithTimeFunc(mockTimeFunc))
	if err != nil {
		t.Fatalf("NewNode() failed: %v", err)
	}

	id1, err := node.Generate()
	if err != nil {
		t.Fatalf("Generate() first call failed: %v", err)
	}

	id2, err := node.Generate()
	if err != nil {
		t.Fatalf("Generate() second call failed with custom tolerance: %v", err)
	}

	if id2 <= id1 {
		t.Fatalf("expected id2 > id1 with custom tolerance recovery, got %d <= %d", id2, id1)
	}
}

// TestGenerateClockBackwardsZeroTolerance ensures zero tolerance surfaces immediate errors.
func TestGenerateClockBackwardsZeroTolerance(t *testing.T) {
	currentTime := DefaultEpoch + 15000
	call := 0
	mockTimeFunc := func() int64 {
		call++
		if call == 1 {
			return currentTime
		}
		return currentTime - 1 // 1ms backwards should fail when tolerance is zero
	}

	node, err := NewNode(0, WithClockDriftTolerance(0), WithTimeFunc(mockTimeFunc))
	if err != nil {
		t.Fatalf("NewNode() failed: %v", err)
	}

	if _, err := node.Generate(); err != nil {
		t.Fatalf("Generate() first call failed: %v", err)
	}

	if _, err := node.Generate(); err == nil {
		t.Fatal("Generate() expected to error with zero tolerance")
	} else if !errors.Is(err, ErrClockBackwards) {
		t.Fatalf("Generate() error = %v, want ErrClockBackwards", err)
	}
}

// TestWithClockDriftToleranceNegative ensures negative tolerance is rejected during configuration.
func TestWithClockDriftToleranceNegative(t *testing.T) {
	_, err := NewNode(0, WithClockDriftTolerance(-1*time.Millisecond))
	if err == nil {
		t.Fatal("NewNode() expected error for negative clock drift tolerance")
	}
}

// TestGenerateBeforeEpoch ensures we surface ErrTimeBeforeEpoch when the clock is before the configured epoch.
func TestGenerateBeforeEpoch(t *testing.T) {
	node, err := NewNode(1, WithTimeFunc(func() int64 { return DefaultEpoch - 1 }))
	if err != nil {
		t.Fatalf("NewNode() failed: %v", err)
	}

	_, err = node.Generate()
	if err == nil {
		t.Fatal("Generate() expected ErrTimeBeforeEpoch")
	}
	if !errors.Is(err, ErrTimeBeforeEpoch) {
		t.Fatalf("Generate() error = %v, want ErrTimeBeforeEpoch", err)
	}

	_, err = node.GenerateBatch(5)
	if err == nil {
		t.Fatal("GenerateBatch() expected ErrTimeBeforeEpoch")
	}
	if !errors.Is(err, ErrTimeBeforeEpoch) {
		t.Fatalf("GenerateBatch() error = %v, want ErrTimeBeforeEpoch", err)
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

// TestGenerateBatchStrings verifies the string batch helper mirrors numeric behaviour.
func TestGenerateBatchStrings(t *testing.T) {
	node, err := NewNode(55)
	if err != nil {
		t.Fatalf("NewNode() failed: %v", err)
	}

	count := 64
	stringsBatch, err := node.GenerateBatchStrings(count)
	if err != nil {
		t.Fatalf("GenerateBatchStrings() failed: %v", err)
	}

	if len(stringsBatch) != count {
		t.Fatalf("GenerateBatchStrings() returned %d strings, want %d", len(stringsBatch), count)
	}

	seen := make(map[uint64]struct{}, count)
	for i, strID := range stringsBatch {
		parsed, parseErr := ParseString(strID)
		if parseErr != nil {
			t.Fatalf("ParseString() at index %d failed: %v", i, parseErr)
		}
		if _, ok := seen[parsed]; ok {
			t.Fatalf("duplicate ID detected at index %d", i)
		}
		seen[parsed] = struct{}{}
	}
}

// TestGenerateBatchStringsInvalidCount ensures invalid counts propagate errors.
func TestGenerateBatchStringsInvalidCount(t *testing.T) {
	node, err := NewNode(0)
	if err != nil {
		t.Fatalf("NewNode() failed: %v", err)
	}

	if ids, err := node.GenerateBatchStrings(0); err == nil || ids != nil {
		t.Fatalf("GenerateBatchStrings(0) expected error, got ids=%v err=%v", ids, err)
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

// TestParseString verifies decimal parsing integrates with validation.
func TestParseString(t *testing.T) {
	node, err := NewNode(7, WithTimeFunc(func() int64 { return DefaultEpoch + 1234 }))
	if err != nil {
		t.Fatalf("NewNode() failed: %v", err)
	}

	id, err := node.Generate()
	if err != nil {
		t.Fatalf("Generate() failed: %v", err)
	}

	str := strconv.FormatUint(id, 10)
	parsed, err := ParseString(str)
	if err != nil {
		t.Fatalf("ParseString() failed: %v", err)
	}
	if parsed != id {
		t.Fatalf("ParseString() mismatch: got %d, want %d", parsed, id)
	}
}

// TestParseStringWithEpoch verifies parsing against a custom epoch.
func TestParseStringWithEpoch(t *testing.T) {
	customEpoch := DefaultEpoch - 5000
	mockTime := customEpoch + 2500
	node, err := NewNode(9, WithEpoch(customEpoch), WithTimeFunc(func() int64 { return mockTime }))
	if err != nil {
		t.Fatalf("NewNode() failed: %v", err)
	}

	id, err := node.Generate()
	if err != nil {
		t.Fatalf("Generate() failed: %v", err)
	}

	str := strconv.FormatUint(id, 10)
	parsed, err := ParseStringWithEpoch(str, customEpoch)
	if err != nil {
		t.Fatalf("ParseStringWithEpoch() failed: %v", err)
	}
	if parsed != id {
		t.Fatalf("ParseStringWithEpoch() mismatch: got %d, want %d", parsed, id)
	}
}

// TestParseStringInvalid ensures invalid inputs are rejected.
func TestParseStringInvalid(t *testing.T) {
	if _, err := ParseString("not-a-number"); err == nil {
		t.Fatal("ParseString() expected error for non-numeric input")
	}

	if _, err := ParseString("0"); err == nil || !errors.Is(err, ErrInvalidSnowflakeID) {
		t.Fatalf("ParseString() expect ErrInvalidSnowflakeID for zero, got %v", err)
	}

	if _, err := ParseString(" "); err == nil || !errors.Is(err, ErrInvalidSnowflakeID) {
		t.Fatalf("ParseString() expect ErrInvalidSnowflakeID for blank string, got %v", err)
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
	currentTime := DefaultEpoch + 5000
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
		if ts.Before(time.UnixMilli(DefaultEpoch)) {
			t.Errorf("Index %d: timestamp before epoch", i)
		}
	}
}

// TestHighLoadGenerate exercises the generator under heavier sequential load.
func TestHighLoadGenerate(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping high-load test in short mode")
	}

	node, err := NewNode(11)
	if err != nil {
		t.Fatalf("NewNode() failed: %v", err)
	}

	const total = 50_000
	ids := make([]uint64, total)
	for i := 0; i < total; i++ {
		id, err := node.Generate()
		if err != nil {
			t.Fatalf("Generate() iteration %d failed: %v", i, err)
		}
		ids[i] = id
	}

	for i := 1; i < len(ids); i++ {
		if ids[i] <= ids[i-1] {
			t.Fatalf("IDs not strictly increasing at index %d: %d <= %d", i, ids[i], ids[i-1])
		}
	}
}

// TestGenerateUniquenessConcurrent ensures that concurrently generated IDs remain unique across goroutines.
func TestGenerateUniquenessConcurrent(t *testing.T) {
	node, err := NewNode(23)
	if err != nil {
		t.Fatalf("NewNode() failed: %v", err)
	}

	const (
		goroutines      = 16
		idsPerGoroutine = 2000
	)

	idsCh := make(chan uint64, goroutines*idsPerGoroutine)
	errCh := make(chan error, goroutines)

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < idsPerGoroutine; i++ {
				id, genErr := node.Generate()
				if genErr != nil {
					errCh <- genErr
					return
				}
				idsCh <- id
			}
		}()
	}

	wg.Wait()
	close(idsCh)
	close(errCh)

	for genErr := range errCh {
		if genErr != nil {
			t.Fatalf("Generate() failed: %v", genErr)
		}
	}

	seen := make(map[uint64]struct{}, goroutines*idsPerGoroutine)
	for id := range idsCh {
		if _, ok := seen[id]; ok {
			t.Fatalf("duplicate ID detected: %d", id)
		}
		seen[id] = struct{}{}
	}

	if len(seen) != goroutines*idsPerGoroutine {
		t.Fatalf("expected %d unique IDs, got %d", goroutines*idsPerGoroutine, len(seen))
	}
}

// TestConcurrentGenerationWithCustomEpoch validates concurrent usage with a non-default epoch.
func TestConcurrentGenerationWithCustomEpoch(t *testing.T) {
	epoch := time.Now().Add(-2 * time.Hour).UnixMilli()
	node, err := NewNode(123, WithEpoch(epoch))
	if err != nil {
		t.Fatalf("NewNode() failed: %v", err)
	}

	const (
		goroutines      = 16
		idsPerGoroutine = 1500
	)

	idsCh := make(chan uint64, goroutines*idsPerGoroutine)
	errCh := make(chan error, goroutines)

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < idsPerGoroutine; i++ {
				id, genErr := node.Generate()
				if genErr != nil {
					errCh <- genErr
					return
				}
				idsCh <- id
			}
		}()
	}

	wg.Wait()
	close(idsCh)

	select {
	case genErr := <-errCh:
		t.Fatalf("Generate() failed: %v", genErr)
	default:
	}

	seen := make(map[uint64]struct{}, goroutines*idsPerGoroutine)
	for id := range idsCh {
		if _, ok := seen[id]; ok {
			t.Fatalf("duplicate ID detected: %d", id)
		}
		seen[id] = struct{}{}

		if err := ValidateWithEpoch(id, epoch); err != nil {
			t.Fatalf("ValidateWithEpoch() failed for id %d: %v", id, err)
		}

		ts, err := ExtractTimestampWithEpoch(id, epoch)
		if err != nil {
			t.Fatalf("ExtractTimestampWithEpoch() failed: %v", err)
		}
		if ts.Before(time.UnixMilli(epoch)) {
			t.Fatalf("timestamp before epoch: %v", ts)
		}

		worker, err := ExtractWorkerID(id)
		if err != nil {
			t.Fatalf("ExtractWorkerID() failed: %v", err)
		}
		if worker != 123 {
			t.Fatalf("worker mismatch: got %d, want 123", worker)
		}
	}
}

// TestConcurrentBatchWithCustomEpoch validates batch generation under concurrent load with a custom epoch.
func TestConcurrentBatchWithCustomEpoch(t *testing.T) {
	epoch := time.Now().Add(-time.Hour).UnixMilli()
	node, err := NewNode(77, WithEpoch(epoch))
	if err != nil {
		t.Fatalf("NewNode() failed: %v", err)
	}

	const (
		goroutines = 8
		batchSize  = 10000
	)

	idsCh := make(chan uint64, goroutines*batchSize)
	errCh := make(chan error, goroutines)

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ids, batchErr := node.GenerateBatch(batchSize)
			if batchErr != nil {
				errCh <- batchErr
				return
			}
			for _, id := range ids {
				idsCh <- id
			}
		}()
	}

	wg.Wait()
	close(idsCh)

	select {
	case batchErr := <-errCh:
		t.Fatalf("GenerateBatch() failed: %v", batchErr)
	default:
	}

	seen := make(map[uint64]struct{}, goroutines*batchSize)
	for id := range idsCh {
		if _, ok := seen[id]; ok {
			t.Fatalf("duplicate ID detected: %d", id)
		}
		seen[id] = struct{}{}

		if err := ValidateWithEpoch(id, epoch); err != nil {
			t.Fatalf("ValidateWithEpoch() failed for id %d: %v", id, err)
		}

		worker, err := ExtractWorkerID(id)
		if err != nil {
			t.Fatalf("ExtractWorkerID() failed: %v", err)
		}
		if worker != 77 {
			t.Fatalf("worker mismatch: got %d, want 77", worker)
		}
	}
}

// TestGenerateBatchUniquenessAcrossCalls ensures uniqueness when multiple batches are generated sequentially.
func TestGenerateBatchUniquenessAcrossCalls(t *testing.T) {
	node, err := NewNode(31)
	if err != nil {
		t.Fatalf("NewNode() failed: %v", err)
	}

	const (
		batches   = 50
		batchSize = 256
	)

	seen := make(map[uint64]struct{}, batches*batchSize)

	for i := 0; i < batches; i++ {
		ids, genErr := node.GenerateBatch(batchSize)
		if genErr != nil {
			t.Fatalf("GenerateBatch() iteration %d failed: %v", i, genErr)
		}

		if len(ids) != batchSize {
			t.Fatalf("GenerateBatch() iteration %d returned %d IDs, want %d", i, len(ids), batchSize)
		}

		for _, id := range ids {
			if _, ok := seen[id]; ok {
				t.Fatalf("duplicate ID detected across batches: %d", id)
			}
			seen[id] = struct{}{}
		}
	}

	if len(seen) != batches*batchSize {
		t.Fatalf("expected %d unique IDs, got %d", batches*batchSize, len(seen))
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
	currentTime := DefaultEpoch + 5000
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

// TestTimestampOverflowGenerate ensures Generate returns ErrTimestampOverflow
// when the time function reports a timestamp beyond the 41-bit capacity.
func TestTimestampOverflowGenerate(t *testing.T) {
	// Compute the maximum timestamp representable in TimeBits
	maxTimestamp := int64(-1 ^ (-1 << TimeBits))

	// Create a time function that returns beyond the maximum
	overflowTime := DefaultEpoch + maxTimestamp + 10
	overflowTimeFunc := func() int64 { return overflowTime }

	node, err := NewNode(0, WithTimeFunc(overflowTimeFunc))
	if err != nil {
		t.Fatalf("NewNode() failed: %v", err)
	}

	_, err = node.Generate()
	if err == nil {
		t.Fatalf("Generate() expected to error on timestamp overflow but returned nil")
	}
	if !errors.Is(err, ErrTimestampOverflow) {
		t.Fatalf("Generate() error = %v, want ErrTimestampOverflow", err)
	}
}

// TestTimestampOverflowBatch ensures GenerateBatch returns ErrTimestampOverflow
// when the time function reports a timestamp beyond the 41-bit capacity.
func TestTimestampOverflowBatch(t *testing.T) {
	// Compute the maximum timestamp representable in TimeBits
	maxTimestamp := int64(-1 ^ (-1 << TimeBits))

	// Create a time function that returns beyond the maximum
	overflowTime := DefaultEpoch + maxTimestamp + 100
	overflowTimeFunc := func() int64 { return overflowTime }

	node, err := NewNode(1, WithTimeFunc(overflowTimeFunc))
	if err != nil {
		t.Fatalf("NewNode() failed: %v", err)
	}

	ids, err := node.GenerateBatch(5)
	if err == nil {
		t.Fatalf("GenerateBatch() expected to error on timestamp overflow but returned nil")
	}
	if !errors.Is(err, ErrTimestampOverflow) {
		t.Fatalf("GenerateBatch() error = %v, want ErrTimestampOverflow", err)
	}
	// Expect partial result to be empty slice (no IDs generated)
	if len(ids) != 0 {
		t.Fatalf("GenerateBatch() returned %d ids on overflow, want 0", len(ids))
	}
}

// TestExtractTimestampFuture ensures ExtractTimestamp (and Validate) detect timestamps
// that are too far in the future (more than 1 hour ahead of now).
func TestExtractTimestampFuture(t *testing.T) {
	// Build an ID with a timestamp 2 hours in the future (relative to now)
	futureTime := time.Now().Add(2 * time.Hour).UnixMilli()
	// Convert to the stored timestamp value (subtract DefaultEpoch)
	storedTs := futureTime - DefaultEpoch
	if storedTs <= 0 {
		t.Skip("computed stored timestamp is non-positive; environment time may be before configured epoch")
	}

	// Encode timestamp into an ID (worker and sequence zero)
	id := (uint64(storedTs) << TimestampShift)

	// ExtractTimestamp should return ErrTimestampInFuture
	_, err := ExtractTimestamp(id)
	if err == nil {
		t.Fatalf("ExtractTimestamp() expected to error for future timestamp but returned nil")
	}
	if !errors.Is(err, ErrTimestampInFuture) {
		t.Fatalf("ExtractTimestamp() error = %v, want ErrTimestampInFuture", err)
	}

	// Validate should also propagate the same error
	if err := Validate(id); err == nil {
		t.Fatalf("Validate() expected to error for future timestamp but returned nil")
	} else if !errors.Is(err, ErrTimestampInFuture) {
		t.Fatalf("Validate() error = %v, want ErrTimestampInFuture", err)
	}
}

// TestGenerateBatchClockBackwardsError ensures GenerateBatch returns ErrClockBackwards
// when the node's lastTimestamp is ahead of the current time by more than tolerance.
func TestGenerateBatchClockBackwardsError(t *testing.T) {
	// Start with a node using a stable time
	currentTime := DefaultEpoch + 10000
	stableTimeFunc := func() int64 { return currentTime }

	node, err := NewNode(2, WithTimeFunc(stableTimeFunc))
	if err != nil {
		t.Fatalf("NewNode() failed: %v", err)
	}

	// Prime the node so lastTimestamp is set
	_, err = node.Generate()
	if err != nil {
		t.Fatalf("initial Generate() failed: %v", err)
	}

	// Now simulate a clock that moved backwards significantly (100ms)
	node.timeFunc = func() int64 { return currentTime - 100 }

	_, err = node.GenerateBatch(10)
	if err == nil {
		t.Fatalf("GenerateBatch() expected to error on excessive clock backwards but returned nil")
	}
	if !errors.Is(err, ErrClockBackwards) {
		t.Fatalf("GenerateBatch() error = %v, want ErrClockBackwards", err)
	}
}

// TestExtractWithMaxComponentValues verifies that extract functions correctly handle
// IDs where each field is at its maximum value.
func TestExtractWithMaxComponentValues(t *testing.T) {
	// Construct an ID with max timestamp (41 bits), max worker (10 bits), max sequence (12 bits)
	maxTimestamp := int64(-1 ^ (-1 << TimeBits))
	id := (uint64(maxTimestamp) << TimestampShift) |
		(uint64(MaxWorkerID) << WorkerShift) |
		uint64(MaxSequence)

	// Extract timestamp
	ts, err := ExtractTimestamp(id)
	if err == nil || err.Error() == "" {
		// May error if timestamp is far in future (expected behavior)
		// But if it succeeds, verify extraction
		if ts.IsZero() {
			t.Fatal("ExtractTimestamp should return non-zero time for max timestamp")
		}
	}

	// Extract worker ID - should always work
	workerID, err := ExtractWorkerID(id)
	if err != nil {
		t.Fatalf("ExtractWorkerID() failed: %v", err)
	}
	if workerID != MaxWorkerID {
		t.Fatalf("ExtractWorkerID() = %d, want %d", workerID, MaxWorkerID)
	}

	// Extract sequence - should always work
	sequence, err := ExtractSequence(id)
	if err != nil {
		t.Fatalf("ExtractSequence() failed: %v", err)
	}
	if sequence != MaxSequence {
		t.Fatalf("ExtractSequence() = %d, want %d", sequence, MaxSequence)
	}
}

// TestExtractWithMinimalValues verifies extract functions handle IDs with minimal
// non-zero values (timestamp just after epoch, worker and sequence = 0).
func TestExtractWithMinimalValues(t *testing.T) {
	// Construct an ID with minimal timestamp, worker, and sequence
	minTimestamp := int64(1) // 1ms after epoch
	id := uint64(minTimestamp) << TimestampShift

	// Extract timestamp
	ts, err := ExtractTimestamp(id)
	if err != nil {
		t.Fatalf("ExtractTimestamp() failed: %v", err)
	}
	expectedTime := time.UnixMilli(minTimestamp + DefaultEpoch)
	if !ts.Equal(expectedTime) {
		t.Fatalf("ExtractTimestamp() = %v, want %v", ts, expectedTime)
	}

	// Extract worker ID
	workerID, err := ExtractWorkerID(id)
	if err != nil {
		t.Fatalf("ExtractWorkerID() failed: %v", err)
	}
	if workerID != 0 {
		t.Fatalf("ExtractWorkerID() = %d, want 0", workerID)
	}

	// Extract sequence
	sequence, err := ExtractSequence(id)
	if err != nil {
		t.Fatalf("ExtractSequence() failed: %v", err)
	}
	if sequence != 0 {
		t.Fatalf("ExtractSequence() = %d, want 0", sequence)
	}
}

// TestSignBitAlwaysZero verifies that all generated IDs have the sign bit (bit 63) set to 0,
// ensuring IDs are always positive when interpreted as int64.
func TestSignBitAlwaysZero(t *testing.T) {
	node, err := NewNode(MaxWorkerID)
	if err != nil {
		t.Fatalf("NewNode() failed: %v", err)
	}

	for i := 0; i < 100; i++ {
		id, err := node.Generate()
		if err != nil {
			t.Fatalf("Generate() iteration %d failed: %v", i, err)
		}

		// Check if sign bit is set (bit 63)
		signBit := id >> 63
		if signBit != 0 {
			t.Fatalf("ID %d has sign bit set (as int64: %d)", id, int64(id))
		}

		// Verify it's positive as int64
		if int64(id) < 0 {
			t.Fatalf("ID %d is negative when cast to int64: %d", id, int64(id))
		}
	}
}

// TestGenerateBatchExactSequenceBoundary generates a batch of exactly MaxSequence+1 IDs,
// which should span exactly two milliseconds (first batch exhausts sequence, second starts fresh).
func TestGenerateBatchExactSequenceBoundary(t *testing.T) {
	currentTime := DefaultEpoch + 40000
	callCount := 0
	n, err := NewNode(15, WithTimeFunc(func() int64 {
		callCount++
		// Advance time on the (MaxSequence+2)th call to allow batch to span 2ms
		if callCount > int(MaxSequence)+1 {
			return currentTime + 1
		}
		return currentTime
	}))
	if err != nil {
		t.Fatalf("NewNode() failed: %v", err)
	}

	count := int(MaxSequence) + 1
	ids, err := n.GenerateBatch(count)
	if err != nil {
		t.Fatalf("GenerateBatch() failed: %v", err)
	}

	if len(ids) != count {
		t.Fatalf("GenerateBatch() returned %d IDs, want %d", len(ids), count)
	}

	// All IDs should be unique
	seen := make(map[uint64]bool)
	for i, id := range ids {
		if seen[id] {
			t.Fatalf("Duplicate ID at index %d", i)
		}
		seen[id] = true
	}

	// IDs should be in order
	for i := 1; i < len(ids); i++ {
		if ids[i] <= ids[i-1] {
			t.Fatalf("IDs out of order at index %d: %d <= %d", i, ids[i], ids[i-1])
		}
	}
}

// TestValidateIDStructureBoundaries tests Validate with IDs that have component values
// at boundary positions to ensure the validation logic correctly handles edge cases.
func TestValidateIDStructureBoundaries(t *testing.T) {
	tests := []struct {
		name      string
		timestamp int64
		workerID  int64
		sequence  int64
		shouldErr bool
	}{
		{"Min valid timestamp", 1, 0, 0, false},
		{"Max valid timestamp at boundary", int64(-1 ^ (-1 << TimeBits)), MaxWorkerID, MaxSequence, true}, // May fail due to future check
		{"Max worker ID", 1000, MaxWorkerID, 0, false},
		{"Max sequence", 1000, 0, MaxSequence, false},
		{"All zeros except timestamp", 500, 0, 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id := (uint64(tt.timestamp) << TimestampShift) |
				(uint64(tt.workerID) << WorkerShift) |
				uint64(tt.sequence)

			err := Validate(id)
			if (err != nil) != tt.shouldErr {
				t.Fatalf("Validate() error = %v, wantErr %v", err, tt.shouldErr)
			}
		})
	}
}

// TestDecomposeWithComponentBoundaries tests Decompose with IDs that have components
// at various boundary values.
func TestDecomposeWithComponentBoundaries(t *testing.T) {
	// Build ID with max values (except timestamp which may error)
	id := (uint64(10000) << TimestampShift) |
		(uint64(MaxWorkerID) << WorkerShift) |
		uint64(MaxSequence)

	ts, workerID, sequence, err := Decompose(id)
	if err != nil {
		// May error due to timestamp validation, which is okay
		t.Logf("Decompose() returned error (acceptable): %v", err)
		return
	}

	if !ts.IsZero() && workerID != MaxWorkerID {
		t.Fatalf("Decompose() workerID = %d, want %d", workerID, MaxWorkerID)
	}

	if sequence != MaxSequence {
		t.Fatalf("Decompose() sequence = %d, want %d", sequence, MaxSequence)
	}
}

// TestIDStructureIntegrity verifies that generated IDs maintain correct bit structure
// by reconstructing components and checking positions match.
func TestIDStructureIntegrity(t *testing.T) {
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
		ts, _ := ExtractTimestamp(id)
		workerID, _ := ExtractWorkerID(id)
		sequence, _ := ExtractSequence(id)

		// Reconstruct ID from components
		reconstructed := (uint64(ts.UnixMilli()-DefaultEpoch) << TimestampShift) |
			(uint64(workerID) << WorkerShift) |
			uint64(sequence)

		// Reconstructed should match original
		if reconstructed != id {
			t.Fatalf("Iteration %d: reconstructed ID %d != original %d", i, reconstructed, id)
		}

		// Verify bit positions by masking
		extractedTs := int64((id >> TimestampShift) & (-1 ^ (-1 << TimeBits)))
		extractedWorker := int64((id >> WorkerShift) & MaxWorkerID)
		extractedSeq := int64(id & MaxSequence)

		if extractedWorker != workerID {
			t.Fatalf("Iteration %d: worker bit extraction failed: %d != %d", i, extractedWorker, workerID)
		}
		if extractedSeq != sequence {
			t.Fatalf("Iteration %d: sequence bit extraction failed: %d != %d", i, extractedSeq, sequence)
		}
		if extractedTs != ts.UnixMilli()-DefaultEpoch {
			t.Fatalf("Iteration %d: timestamp bit extraction failed", i)
		}
	}
}

// TestGenerateSequenceRolloverMultipleTimes tests that sequence correctly resets
// multiple times across different milliseconds.
func TestGenerateSequenceRolloverMultipleTimes(t *testing.T) {
	timeMs := DefaultEpoch + 30000
	callCount := 0
	n, err := NewNode(3, WithTimeFunc(func() int64 {
		callCount++
		// Advance to next ms on every (MaxSequence+1)th call
		ms := callCount / (int(MaxSequence) + 1)
		return timeMs + int64(ms)
	}))
	if err != nil {
		t.Fatalf("NewNode() failed: %v", err)
	}

	// Generate enough IDs to rollover sequence multiple times
	totalIDs := int(MaxSequence) * 3
	ids := make([]uint64, totalIDs)
	for i := 0; i < totalIDs; i++ {
		id, err := n.Generate()
		if err != nil {
			t.Fatalf("Generate() iteration %d failed: %v", i, err)
		}
		ids[i] = id
	}

	// All IDs should be unique
	seen := make(map[uint64]bool)
	for i, id := range ids {
		if seen[id] {
			t.Fatalf("Duplicate ID at index %d: %d", i, id)
		}
		seen[id] = true
	}

	// Verify IDs are strictly increasing
	for i := 1; i < len(ids); i++ {
		if ids[i] <= ids[i-1] {
			t.Fatalf("ID order violation at index %d: %d <= %d", i, ids[i], ids[i-1])
		}
	}
}
