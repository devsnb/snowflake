package snowflake

import (
	crypto_rand "crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

const (
	// Snowflake bit allocation (standard Twitter Snowflake format)
	// 1 bit: sign (always 0)
	// 41 bits: timestamp in milliseconds
	// 10 bits: worker/machine ID
	// 12 bits: sequence number
	SignBit      = 1
	TimeBits     = 41
	WorkerBits   = 10
	SequenceBits = 12

	// Maximum values
	MaxWorkerID = -1 ^ (-1 << WorkerBits)   // 1023
	MaxSequence = -1 ^ (-1 << SequenceBits) // 4095

	// Bit shifts
	WorkerShift    = SequenceBits
	TimestampShift = WorkerBits + SequenceBits

	// Clock drift tolerance (5ms should handle most NTP adjustments)
	defaultMaxBackwardDrift = 5 * time.Millisecond

	// Maximum timestamp value that fits in TimeBits (41 bits = ~69 years)
	maxTimestamp = int64(-1 ^ (-1 << TimeBits)) // 2199023255551
)

// DefaultEpoch is the default millisecond epoch (Jan 1, 2025 00:00:00 UTC).
// Callers can override this via WithEpoch when constructing a Node.
const DefaultEpoch = int64(1735689600000)

// Custom errors
var (
	ErrInvalidWorkerID    = errors.New("worker ID must be between 0 and 1023")
	ErrClockBackwards     = errors.New("clock moved backwards beyond acceptable drift")
	ErrInvalidSnowflakeID = errors.New("invalid snowflake ID")
	ErrTimestampInFuture  = errors.New("timestamp is in the future")
	ErrTimestampOverflow  = errors.New("timestamp overflow - epoch exhausted")
	ErrTimeBeforeEpoch    = errors.New("current time is before the configured epoch")
)

// Node represents a Snowflake ID generator node.
// Uses lock-free atomic operations for high-performance ID generation.
type Node struct {
	state            atomic.Uint64
	workerID         int64
	workerBits       uint64
	timeFunc         func() int64
	epoch            int64
	maxBackwardDrift int64 // Stored as milliseconds to avoid Duration conversion in hot path
}

// NodeOption defines function signature for node options
type NodeOption func(*Node)

// WithTimeFunc allows overriding the time function (useful for testing)
func WithTimeFunc(timeFunc func() int64) NodeOption {
	return func(n *Node) {
		n.timeFunc = timeFunc
	}
}

// NewNode creates a new Snowflake node with the specified worker ID.
// Worker ID must be between 0 and 1023 (inclusive).
func NewNode(workerID int64, opts ...NodeOption) (*Node, error) {
	if workerID < 0 || workerID > MaxWorkerID {
		return nil, fmt.Errorf("%w: got %d, max %d", ErrInvalidWorkerID, workerID, MaxWorkerID)
	}

	node := &Node{
		workerID:         workerID,
		workerBits:       uint64(workerID) << WorkerShift,
		timeFunc:         func() int64 { return time.Now().UnixMilli() },
		epoch:            DefaultEpoch,
		maxBackwardDrift: int64(defaultMaxBackwardDrift / time.Millisecond),
	}

	for _, opt := range opts {
		opt(node)
	}

	if node.timeFunc == nil {
		node.timeFunc = func() int64 { return time.Now().UnixMilli() }
	}

	if node.epoch < 0 {
		return nil, fmt.Errorf("epoch must be non-negative, got %d", node.epoch)
	}

	if node.maxBackwardDrift < 0 {
		return nil, fmt.Errorf("clock drift tolerance must be non-negative, got %d", node.maxBackwardDrift)
	}

	return node, nil
}

// WithEpoch overrides the default epoch (milliseconds since Unix epoch).
// The provided epoch must be non-negative and fit within a 64-bit signed integer.
func WithEpoch(epoch int64) NodeOption {
	return func(n *Node) {
		n.epoch = epoch
	}
}

// WithClockDriftTolerance overrides the maximum tolerated backwards clock drift.
// A zero duration disables tolerance (any backwards movement errors). Must be non-negative.
func WithClockDriftTolerance(d time.Duration) NodeOption {
	return func(n *Node) {
		n.maxBackwardDrift = int64(d / time.Millisecond)
	}
}

// NewNodeWithBestEffortID creates a new Snowflake node with a best-effort worker ID
// derived from the system's characteristics. This is NOT guaranteed to be unique
// across different machines and should only be used when proper coordination is not available.
//
// The worker ID is generated from:
// - Hostname
// - MAC addresses of network interfaces
// - Process ID
//
// Warning: This may produce collisions in environments with:
// - Cloned VMs or containers
// - Dynamic hostnames
// - Containerized environments without unique network interfaces
//
// For production use, always coordinate worker IDs externally.
func NewNodeWithBestEffortID(opts ...NodeOption) (*Node, error) {
	workerID, err := generateBestEffortWorkerID()
	if err != nil {
		return nil, fmt.Errorf("failed to generate worker ID: %w", err)
	}

	return NewNode(workerID, opts...)
}

// generateBestEffortWorkerID generates a worker ID from system characteristics
func generateBestEffortWorkerID() (int64, error) {
	hash := sha256.New()

	// Add hostname
	if hostname, err := os.Hostname(); err == nil {
		hash.Write([]byte(hostname))
	}

	// Add MAC addresses
	if interfaces, err := net.Interfaces(); err == nil {
		for _, iface := range interfaces {
			if len(iface.HardwareAddr) > 0 {
				hash.Write(iface.HardwareAddr)
			}
		}
	}

	// Add process ID for additional uniqueness
	pid := os.Getpid()
	pidBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(pidBytes, uint64(pid))
	hash.Write(pidBytes)

	// Add cryptographically secure random component for better distribution
	randomBytes := make([]byte, 16)
	if _, err := crypto_rand.Read(randomBytes); err == nil {
		hash.Write(randomBytes)
	}

	// Use more bits of the hash for better distribution
	hashBytes := hash.Sum(nil)
	hashValue := binary.BigEndian.Uint64(hashBytes[:8]) ^
		binary.BigEndian.Uint64(hashBytes[8:16])
	workerID := int64(hashValue % uint64(MaxWorkerID+1))

	return workerID, nil
}

// Generate creates a new Snowflake ID.
// It handles clock drift, sequence overflow, and ensures uniqueness.
// The implementation is optimized for minimal allocations and reduced branching.
func (n *Node) Generate() (uint64, error) {
	for {
		oldState := n.state.Load()
		oldSeq := oldState & uint64(MaxSequence)
		oldTs := int64(oldState >> SequenceBits)

		// Single time call with epoch subtraction
		now := n.timeFunc() - n.epoch

		if now < 0 {
			return 0, ErrTimeBeforeEpoch
		}

		if now > maxTimestamp {
			return 0, ErrTimestampOverflow
		}

		// Drift detection using direct int64 comparison
		if now < oldTs {
			drift := oldTs - now
			if drift > n.maxBackwardDrift {
				return 0, ErrClockBackwards
			}
			// Sleep slightly longer than the drift
			time.Sleep(time.Duration(drift+1) * time.Millisecond)
			continue
		}

		var newState uint64

		if now == oldTs {
			// Same millisecond - increment sequence
			if oldSeq == uint64(MaxSequence) {
				// Tight spin loop for sequence exhaustion (waits for next millisecond)
				for now <= oldTs {
					now = n.timeFunc() - n.epoch
					if now < 0 {
						return 0, ErrTimeBeforeEpoch
					}
				}
				continue
			}
			newState = oldState + 1
		} else {
			// New millisecond - reset sequence to 0
			newState = uint64(now) << SequenceBits
		}

		// Try CAS
		if n.state.CompareAndSwap(oldState, newState) {
			// Construct ID directly from packed state without intermediate variables
			seq := newState & uint64(MaxSequence)
			ts := newState >> SequenceBits
			return (ts << TimestampShift) | n.workerBits | seq, nil
		}
		// CAS failed, retry
	}
}

// GenerateString returns a decimal string representation of a generated Snowflake ID.
func (n *Node) GenerateString() (string, error) {
	id, err := n.Generate()
	if err != nil {
		return "", err
	}
	return strconv.FormatUint(id, 10), nil
}

// GenerateBatch generates multiple Snowflake IDs using lock-free atomic range reservation.
// This atomically reserves a range of sequence numbers and generates IDs from that range.
//
// The batch generation strategy:
// 1. Atomically reserve as many sequences as possible in the current millisecond
// 2. Generate IDs from the reserved range
// 3. If more IDs needed, wait for next millisecond and repeat
//
// Returns nil if any error occurs (no partial results).
func (n *Node) GenerateBatch(count int) ([]uint64, error) {
	if count <= 0 {
		return nil, errors.New("count must be positive")
	}

	ids := make([]uint64, 0, count)

	for len(ids) < count {
		oldState := n.state.Load()
		oldSeq := oldState & uint64(MaxSequence)
		oldTs := int64(oldState >> SequenceBits)

		now := n.timeFunc() - n.epoch

		if now < 0 {
			return nil, ErrTimeBeforeEpoch
		}

		if now > maxTimestamp {
			return nil, ErrTimestampOverflow
		}

		// Handle clock moving backwards
		if now < oldTs {
			drift := oldTs - now
			if drift > n.maxBackwardDrift {
				return nil, ErrClockBackwards
			}
			time.Sleep(time.Duration(drift+1) * time.Millisecond)
			continue
		}

		// Calculate how many IDs we can reserve
		var available int
		var startSeq uint64
		var useTs int64
		var newState uint64

		if now == oldTs {
			// Same millisecond
			available = int(uint64(MaxSequence) - oldSeq)
			if available == 0 {
				// Sequence exhausted
				for now <= oldTs {
					now = n.timeFunc() - n.epoch
					if now < 0 {
						return nil, ErrTimeBeforeEpoch
					}
				}
				continue
			}
			startSeq = oldSeq + 1
			useTs = oldTs

			remaining := count - len(ids)
			toReserve := remaining
			if toReserve > available {
				toReserve = available
			}
			newState = oldState + uint64(toReserve)
		} else {
			// New millisecond
			available = int(MaxSequence) + 1
			startSeq = 0
			useTs = now

			remaining := count - len(ids)
			toReserve := remaining
			if toReserve > available {
				toReserve = available
			}
			endSeq := startSeq + uint64(toReserve) - 1
			newState = (uint64(useTs) << SequenceBits) | (endSeq & uint64(MaxSequence))
		}

		// Calculate how many to reserve
		remaining := count - len(ids)
		toReserve := remaining
		if toReserve > available {
			toReserve = available
		}

		// Try to reserve atomically
		if n.state.CompareAndSwap(oldState, newState) {
			// Construct IDs directly in a tight loop, computing base once and combining with sequences
			base := (uint64(useTs) << TimestampShift) | n.workerBits
			for i := 0; i < toReserve; i++ {
				seq := (startSeq + uint64(i)) & uint64(MaxSequence)
				ids = append(ids, base|seq)
			}
		}
	}

	return ids, nil
}

// GenerateBatchStrings returns decimal string representations for a batch of IDs.
func (n *Node) GenerateBatchStrings(count int) ([]string, error) {
	ids, err := n.GenerateBatch(count)
	if err != nil {
		return nil, err
	}

	// Pre-allocate the output slice with exact size to avoid reallocation
	out := make([]string, len(ids))
	for i, id := range ids {
		out[i] = strconv.FormatUint(id, 10)
	}
	return out, nil
}

// ExtractTimestamp extracts the timestamp from a Snowflake ID and returns it as a time.Time.
// Returns an error if the ID is invalid or the timestamp is in the future.
func ExtractTimestamp(id uint64) (time.Time, error) {
	return ExtractTimestampWithEpoch(id, DefaultEpoch)
}

// ExtractTimestampWithEpoch extracts the timestamp using a specific epoch.
func ExtractTimestampWithEpoch(id uint64, epoch int64) (time.Time, error) {
	if id == 0 {
		return time.Time{}, ErrInvalidSnowflakeID
	}

	// Extract timestamp from the upper bits (optimized bit extraction)
	timestamp := int64(id >> TimestampShift)
	t := time.UnixMilli(timestamp + epoch)

	// Validate timestamp is not in the far future (allow 1 hour drift)
	if t.After(time.Now().Add(time.Hour)) {
		return time.Time{}, ErrTimestampInFuture
	}

	return t, nil
}

// ExtractWorkerID extracts the worker ID from a Snowflake ID.
// Returns an error if the ID is invalid.
func ExtractWorkerID(id uint64) (int64, error) {
	if id == 0 {
		return 0, ErrInvalidSnowflakeID
	}

	workerID := int64((id >> WorkerShift) & MaxWorkerID)
	return workerID, nil
}

// ExtractSequence extracts the sequence number from a Snowflake ID.
// Returns an error if the ID is invalid.
func ExtractSequence(id uint64) (int64, error) {
	if id == 0 {
		return 0, ErrInvalidSnowflakeID
	}

	sequence := int64(id & MaxSequence)
	return sequence, nil
}

// Validate checks if a Snowflake ID is structurally valid.
// It verifies:
// - ID is not zero
// - Timestamp is not in the future
// - Worker ID is within valid range
//
// Returns nil if the ID is valid, otherwise returns an error describing the issue.
func Validate(id uint64) error {
	return ValidateWithEpoch(id, DefaultEpoch)
}

// ValidateWithEpoch validates an ID using the supplied epoch.
func ValidateWithEpoch(id uint64, epoch int64) error {
	if id == 0 {
		return ErrInvalidSnowflakeID
	}

	// Check timestamp
	_, err := ExtractTimestampWithEpoch(id, epoch)
	if err != nil {
		return err
	}

	// Check worker ID
	workerID, err := ExtractWorkerID(id)
	if err != nil {
		return err
	}
	if workerID < 0 || workerID > MaxWorkerID {
		return fmt.Errorf("invalid worker ID: %d", workerID)
	}

	return nil
}

// Decompose breaks down a Snowflake ID into its components for debugging.
// Returns timestamp, worker ID, and sequence number.
func Decompose(id uint64) (timestamp time.Time, workerID int64, sequence int64, err error) {
	timestamp, err = ExtractTimestamp(id)
	if err != nil {
		return time.Time{}, 0, 0, err
	}

	workerID, err = ExtractWorkerID(id)
	if err != nil {
		return time.Time{}, 0, 0, err
	}

	sequence, err = ExtractSequence(id)
	if err != nil {
		return time.Time{}, 0, 0, err
	}

	return timestamp, workerID, sequence, nil
}

// DecomposeWithEpoch breaks down an ID using a specific epoch.
func DecomposeWithEpoch(id uint64, epoch int64) (timestamp time.Time, workerID int64, sequence int64, err error) {
	timestamp, err = ExtractTimestampWithEpoch(id, epoch)
	if err != nil {
		return time.Time{}, 0, 0, err
	}

	workerID, err = ExtractWorkerID(id)
	if err != nil {
		return time.Time{}, 0, 0, err
	}

	sequence, err = ExtractSequence(id)
	if err != nil {
		return time.Time{}, 0, 0, err
	}

	return timestamp, workerID, sequence, nil
}

// ParseString parses a decimal-encoded Snowflake ID string using the default epoch and validates it.
func ParseString(s string) (uint64, error) {
	return ParseStringWithEpoch(s, DefaultEpoch)
}

// ParseStringWithEpoch parses and validates a Snowflake ID string with a custom epoch.
func ParseStringWithEpoch(s string, epoch int64) (uint64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("%w: empty string", ErrInvalidSnowflakeID)
	}

	id, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse snowflake: %w", err)
	}

	if err := ValidateWithEpoch(id, epoch); err != nil {
		return 0, err
	}

	return id, nil
}
