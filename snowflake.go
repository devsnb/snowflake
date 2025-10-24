package snowflake

import (
	crypto_rand "crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"os"
	"runtime"
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
	maxBackwardDrift = 5 * time.Millisecond

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
	// state packs timestamp (upper 52 bits) and sequence (lower 12 bits) into a single atomic value
	// This enables lock-free Compare-And-Swap operations
	// Layout: [timestamp:52][sequence:12]
	state    atomic.Uint64
	workerID int64
	timeFunc func() int64
	epoch    int64
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
		epoch:    DefaultEpoch,
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

	return node, nil
}

// WithEpoch overrides the default epoch (milliseconds since Unix epoch).
// The provided epoch must be non-negative and fit within a 64-bit signed integer.
func WithEpoch(epoch int64) NodeOption {
	return func(n *Node) {
		n.epoch = epoch
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

// waitForNextMillisecond uses a hybrid approach: sleep most of the way, then poll
// This avoids CPU burn while ensuring we don't under-sleep due to timer granularity
// Generate generates a new unique Snowflake ID using lock-free atomic operations.
// The returned ID follows the standard Twitter Snowflake format:
// - Bit 63: Sign bit (always 0)
// - Bits 62-22: Timestamp in milliseconds since the configured epoch (41 bits)
// - Bits 21-12: Worker ID (10 bits)
// - Bits 11-0: Sequence number (12 bits)
//
// This implementation uses Compare-And-Swap (CAS) for lock-free concurrency,
// providing excellent performance under high contention.
func (n *Node) Generate() (uint64, error) {
	for {
		// Load current state atomically
		oldState := n.state.Load()
		oldSeq := oldState & uint64(MaxSequence) // Extract lower 12 bits (sequence)
		oldTs := int64(oldState >> SequenceBits) // Extract upper 52 bits (timestamp)

		nowAbsolute := n.timeFunc()
		now := nowAbsolute - n.epoch

		if now < 0 {
			return 0, fmt.Errorf("%w: now=%d epoch=%d", ErrTimeBeforeEpoch, nowAbsolute, n.epoch)
		}

		// Check for timestamp overflow
		if now > maxTimestamp {
			return 0, fmt.Errorf("%w: timestamp %d exceeds maximum %d",
				ErrTimestampOverflow, now, maxTimestamp)
		}

		// Handle clock moving backwards
		if now < oldTs {
			drift := oldTs - now
			driftDuration := time.Duration(drift) * time.Millisecond

			if driftDuration <= maxBackwardDrift {
				// Sleep and retry
				time.Sleep(driftDuration + time.Millisecond)
				continue
			}
			return 0, fmt.Errorf("%w: drift of %dms exceeds max %dms",
				ErrClockBackwards, drift, maxBackwardDrift.Milliseconds())
		}

		var newSeq uint64
		var newTs int64

		if now == oldTs {
			// Same millisecond - increment sequence
			newSeq = (oldSeq + 1) & uint64(MaxSequence)
			if newSeq == 0 {
				// Sequence exhausted - wait for the next millisecond
				for {
					nowAbsolute = n.timeFunc()
					now = nowAbsolute - n.epoch
					if now < 0 {
						return 0, fmt.Errorf("%w: now=%d epoch=%d", ErrTimeBeforeEpoch, nowAbsolute, n.epoch)
					}
					if now > oldTs {
						break
					}
					time.Sleep(50 * time.Microsecond)
					runtime.Gosched()
				}
				continue
			}
			newTs = oldTs
		} else {
			// New millisecond - reset sequence
			newSeq = 0
			newTs = now
		}

		// Pack new state: timestamp in upper 52 bits, sequence in lower 12 bits
		newState := (uint64(newTs) << SequenceBits) | newSeq

		// Try to update state atomically
		if n.state.CompareAndSwap(oldState, newState) {
			// Success! Build and return the ID
			id := (uint64(newTs) << TimestampShift) |
				(uint64(n.workerID) << WorkerShift) |
				newSeq
			return id, nil
		}
		// CAS failed - another goroutine updated state, retry
	}
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

		nowAbsolute := n.timeFunc()
		now := nowAbsolute - n.epoch

		if now < 0 {
			return nil, fmt.Errorf("%w: now=%d epoch=%d", ErrTimeBeforeEpoch, nowAbsolute, n.epoch)
		}

		// Check for timestamp overflow
		if now > maxTimestamp {
			return nil, fmt.Errorf("%w: timestamp %d exceeds maximum %d",
				ErrTimestampOverflow, now, maxTimestamp)
		}

		// Handle clock moving backwards
		if now < oldTs {
			drift := oldTs - now
			driftDuration := time.Duration(drift) * time.Millisecond

			if driftDuration <= maxBackwardDrift {
				time.Sleep(driftDuration + time.Millisecond)
				continue
			}
			return nil, fmt.Errorf("%w: drift of %dms exceeds max %dms",
				ErrClockBackwards, drift, maxBackwardDrift.Milliseconds())
		}

		// Calculate how many IDs we can reserve in this millisecond
		var available int
		var startSeq uint64
		var useTs int64

		if now == oldTs {
			// Same millisecond - reserve from current sequence
			available = int(uint64(MaxSequence) - oldSeq)
			if available == 0 {
				// Sequence exhausted - wait for the next millisecond
				for {
					nowAbsolute = n.timeFunc()
					now = nowAbsolute - n.epoch
					if now < 0 {
						return nil, fmt.Errorf("%w: now=%d epoch=%d", ErrTimeBeforeEpoch, nowAbsolute, n.epoch)
					}
					if now > oldTs {
						break
					}
					time.Sleep(50 * time.Microsecond)
					runtime.Gosched()
				}
				continue
			}
			startSeq = oldSeq + 1
			useTs = oldTs
		} else {
			// New millisecond - full range available
			available = int(MaxSequence) + 1
			startSeq = 0
			useTs = now
		}

		// Reserve as many as we need (or as many as available)
		remaining := count - len(ids)
		toReserve := remaining
		if toReserve > available {
			toReserve = available
		}

		// Calculate new state after reservation
		endSeq := startSeq + uint64(toReserve) - 1
		newState := (uint64(useTs) << SequenceBits) | (endSeq & uint64(MaxSequence))

		// Try to reserve the range atomically
		if n.state.CompareAndSwap(oldState, newState) {
			// Successfully reserved range [startSeq, endSeq] in timestamp useTs
			// Generate IDs from this range
			for i := 0; i < toReserve; i++ {
				seq := (startSeq + uint64(i)) & uint64(MaxSequence)
				id := (uint64(useTs) << TimestampShift) |
					(uint64(n.workerID) << WorkerShift) |
					seq
				ids = append(ids, id)
			}
		}
		// If CAS failed, retry (another goroutine updated state)
	}

	return ids, nil
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

	timestamp := int64((id >> TimestampShift) & (-1 ^ (-1 << TimeBits)))
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
