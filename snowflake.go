package snowflake

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
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
	maxBackwardDrift = 5 * time.Millisecond

	// Sleep threshold for hybrid wait strategy
	// If we need to wait > 100μs, we sleep first, then poll
	sleepThreshold = 100 * time.Microsecond
)

// Custom epoch: Jan 1, 2025 00:00:00 UTC (in milliseconds)
// This gives us ~69 years from 2025 to ~2094
const customEpoch = int64(1735689600000)

// Custom errors
var (
	ErrInvalidWorkerID    = errors.New("worker ID must be between 0 and 1023")
	ErrClockBackwards     = errors.New("clock moved backwards beyond acceptable drift")
	ErrInvalidSnowflakeID = errors.New("invalid snowflake ID")
	ErrTimestampInFuture  = errors.New("timestamp is in the future")
	ErrTimestampOverflow  = errors.New("timestamp overflow - epoch exhausted")
)

// Node represents a Snowflake ID generator node
type Node struct {
	mu            sync.Mutex
	lastTimestamp int64 // Last timestamp used (milliseconds since custom epoch)
	workerID      int64
	sequence      int64
	timeFunc      func() int64
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
		workerID: workerID,
		timeFunc: func() int64 { return time.Now().UnixMilli() },
	}

	for _, opt := range opts {
		opt(node)
	}

	return node, nil
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
	pidBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(pidBytes, uint32(pid))
	hash.Write(pidBytes)

	// Take first 8 bytes of hash and mod by MaxWorkerID+1
	hashBytes := hash.Sum(nil)
	hashValue := binary.BigEndian.Uint64(hashBytes[:8])
	workerID := int64(hashValue % uint64(MaxWorkerID+1))

	return workerID, nil
}

// waitForNextMillisecond uses a hybrid approach: sleep most of the way, then poll
// This avoids CPU burn while ensuring we don't under-sleep due to timer granularity
func (n *Node) waitForNextMillisecond(lastTimestamp int64) int64 {
	nextTimestamp := lastTimestamp + 1
	nextTime := time.UnixMilli(nextTimestamp + customEpoch)

	timeUntilNext := time.Until(nextTime)

	// If we have enough time, sleep most of it (leaving 100μs for polling)
	if timeUntilNext > sleepThreshold {
		time.Sleep(timeUntilNext - sleepThreshold)
	}

	// Poll for the exact millisecond boundary
	now := n.timeFunc() - customEpoch
	for now <= lastTimestamp {
		now = n.timeFunc() - customEpoch
	}

	return now
}

// Generate generates a new unique Snowflake ID.
// The returned ID follows the standard Twitter Snowflake format:
// - Bit 63: Sign bit (always 0)
// - Bits 62-22: Timestamp in milliseconds since custom epoch (41 bits)
// - Bits 21-12: Worker ID (10 bits)
// - Bits 11-0: Sequence number (12 bits)
func (n *Node) Generate() (uint64, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	now := n.timeFunc() - customEpoch

	// Check for timestamp overflow (41 bits can hold ~69 years)
	maxTimestamp := int64(-1 ^ (-1 << TimeBits))
	if now > maxTimestamp {
		return 0, fmt.Errorf("%w: timestamp %d exceeds maximum %d",
			ErrTimestampOverflow, now, maxTimestamp)
	}

	// Handle clock moving backwards
	if now < n.lastTimestamp {
		drift := n.lastTimestamp - now
		driftDuration := time.Duration(drift) * time.Millisecond

		// If drift is within acceptable range, wait it out
		if driftDuration <= maxBackwardDrift {
			time.Sleep(driftDuration + time.Millisecond)
			now = n.timeFunc() - customEpoch
		} else {
			// Clock has moved backwards significantly - this is a serious issue
			return 0, fmt.Errorf("%w: drift of %dms exceeds max %dms",
				ErrClockBackwards, drift, maxBackwardDrift.Milliseconds())
		}
	}

	if now == n.lastTimestamp {
		// Increment sequence within the same millisecond
		n.sequence = (n.sequence + 1) & MaxSequence
		if n.sequence == 0 {
			// Sequence exhausted (4096 IDs in this millisecond), wait for next millisecond
			now = n.waitForNextMillisecond(n.lastTimestamp)
		}
	} else {
		// New millisecond, reset sequence
		n.sequence = 0
	}

	n.lastTimestamp = now

	// Construct the ID
	// Sign bit is implicitly 0 (positive int64)
	id := (uint64(now) << TimestampShift) |
		(uint64(n.workerID) << WorkerShift) |
		uint64(n.sequence)

	return id, nil
}

// GenerateBatch generates multiple Snowflake IDs in a single call.
// This is more efficient than calling Generate() multiple times as it
// acquires the lock only once and minimizes time advancement operations.
//
// The batch generation strategy:
// 1. Generate as many IDs as possible in the current millisecond
// 2. When sequence exhausts, advance to next millisecond once
// 3. Continue until requested count is reached
//
// Returns partial results if an error occurs mid-generation.
func (n *Node) GenerateBatch(count int) ([]uint64, error) {
	if count <= 0 {
		return nil, errors.New("count must be positive")
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	ids := make([]uint64, 0, count)
	now := n.timeFunc() - customEpoch

	// Check for timestamp overflow
	maxTimestamp := int64(-1 ^ (-1 << TimeBits))
	if now > maxTimestamp {
		return ids, fmt.Errorf("%w: timestamp %d exceeds maximum %d",
			ErrTimestampOverflow, now, maxTimestamp)
	}

	// Handle clock moving backwards (only check once at start)
	if now < n.lastTimestamp {
		drift := n.lastTimestamp - now
		driftDuration := time.Duration(drift) * time.Millisecond

		if driftDuration <= maxBackwardDrift {
			time.Sleep(driftDuration + time.Millisecond)
			now = n.timeFunc() - customEpoch
		} else {
			return ids, fmt.Errorf("%w: drift of %dms exceeds max %dms",
				ErrClockBackwards, drift, maxBackwardDrift.Milliseconds())
		}
	}

	// If we're in a new millisecond, reset sequence
	if now != n.lastTimestamp {
		n.sequence = 0
		n.lastTimestamp = now
	}

	remaining := count
	for remaining > 0 {
		// Calculate how many IDs we can generate in current millisecond
		availableInCurrentMs := MaxSequence - n.sequence + 1
		toGenerate := remaining
		if toGenerate > int(availableInCurrentMs) {
			toGenerate = int(availableInCurrentMs)
		}

		// Generate IDs for current millisecond
		for i := 0; i < toGenerate; i++ {
			id := (uint64(n.lastTimestamp) << TimestampShift) |
				(uint64(n.workerID) << WorkerShift) |
				uint64(n.sequence)

			ids = append(ids, id)
			n.sequence++
			remaining--
		}

		// If we need more IDs, advance to next millisecond
		if remaining > 0 {
			now = n.waitForNextMillisecond(n.lastTimestamp)

			// Check overflow after advancing
			if now > maxTimestamp {
				return ids, fmt.Errorf("%w: generated %d/%d IDs before overflow",
					ErrTimestampOverflow, len(ids), count)
			}

			n.lastTimestamp = now
			n.sequence = 0
		}
	}

	return ids, nil
}

// ExtractTimestamp extracts the timestamp from a Snowflake ID and returns it as a time.Time.
// Returns an error if the ID is invalid or the timestamp is in the future.
func ExtractTimestamp(id uint64) (time.Time, error) {
	if id == 0 {
		return time.Time{}, ErrInvalidSnowflakeID
	}

	timestamp := int64((id >> TimestampShift) & (-1 ^ (-1 << TimeBits)))
	t := time.UnixMilli(timestamp + customEpoch)

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
	if id == 0 {
		return ErrInvalidSnowflakeID
	}

	// Check timestamp
	_, err := ExtractTimestamp(id)
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
