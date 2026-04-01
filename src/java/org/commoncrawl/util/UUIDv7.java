package org.commoncrawl.util;

import java.security.SecureRandom;
import java.time.Clock;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Thread-safe implementation of UUIDv7 (RFC 9562).
 * <p>
 * Layout (128 bits total):
 * <pre>
 * MSB (Most Significant Bits - 64 bits):
 *   [48 bits] Unix epoch timestamp (milliseconds)
 *   [ 4 bits] Version = 7 (0111)
 *   [12 bits] Sub-millisecond sequence counter (for monotonicity)
 *
 * LSB (Least Significant Bits - 64 bits):
 *   [ 2 bits] Variant = 2 (10 binary - IETF RFC variant)
 *   [62 bits] Random bits (collision resistance)
 * </pre>
 * <p>
 * Monotonicity Guarantee:
 * <ul>
 *   <li>If time advances: sequence resets to 0</li>
 *   <li>If time is same: sequence increments</li>
 *   <li>If time regresses (e.g., clock drift): preserves previous timestamp and
 *       increments sequence</li>
 *   <li>If sequence exceeds 4096/ms: spins until next millisecond</li>
 * </ul>
 *
 * <h2>Thread Safety:</h2>
 * Uses lock-free atomic operations (CAS) for high-performance concurrent UUID generation.
 * State is packed into a single AtomicLong for efficient atomic updates.
 *
 * @spec https://www.rfc-editor.org/info/rfc9562
 *      RFC 9562: Universally Unique IDentifiers (UUIDs)
 *
 * NOTE: This class was borrowed from https://github.com/belief-driven-design/blog-uuidv7
 * Copyright to the original author, and licensed as CC0.
 */
public final class UUIDv7 {

    /**
     * Singleton that ensures process-wide monotonicity for all UUIDs generated
     * via the public API.
     */
    private final static UUIDv7 SHARED = new UUIDv7(Clock.systemUTC());

    /**
     * Time source for generating timestamps.
     * Injectable for testing purposes.
     */
    private final Clock clock;

    /**
     * Packed state containing both timestamp and sequence counter.
     *
     * <p>Layout of the 64-bit long:
     * <pre>
     *   [52 bits] timestamp (only lower 48 bits used, upper 4 bits ignored)
     *   [12 bits] sequence counter
     * </pre>
     */
    private final AtomicLong state = new AtomicLong(0L);

    /**
     * Source of cryptographic randomness for the lower 62 bits of the UUID.
     * 
     * @implNote SecureRandom is used to prevent predictability, which is
     * important if UUIDs might be exposed in URLs or logs.
     * For pure collision resistance without security requirements, a faster
     * RandomGenerator could be substituted.
     */
    private final SecureRandom random = new SecureRandom();

    /**
     * Package-private constructor for dependency injection during testing.
     */
    UUIDv7(Clock clock) {
        this.clock = clock;
    }

    /**
     * Generates a new UUIDv7 with monotonic ordering guarantees.
     * <p>
     * Lock-free algorithm using Compare-And-Swap (CAS) to ensure thread-safety
     * and monotonicity even under high concurrency.
     * 
     * @implSpec
     * Algorithm Steps:
     * <ol>
     *   <li>Read current packed state (timestamp + sequence)</li>
     *   <li>Read current wall-clock time</li>
     *   <li>Calculate next state based on time progression</li>
     *   <li>Atomically update state (retry on CAS failure)</li>
     *   <li>Build and return UUID from confirmed state</li>
     * </ol>
     * Monotonicity Rules:
     * <ul>
     *   <li>Time advanced → use new timestamp, reset sequence to 0</li>
     *   <li>Same time → preserve timestamp, increment sequence</li>
     *   <li>Time regressed (clock drift) → preserve previous timestamp,
     *       increment sequence</li>
     *   <li>Sequence overflow (>4095) → spin-wait until next millisecond</li>
     * </ul>
     *
     * @return UUIDv7 that is guaranteed to be lexicographically greater than
     *         all previously generated UUIDs from this instance
     */
    UUID generate() {
        while (true) {
            // STEP 1: Read current generator state.
            //         Format: [52-bit timestamp][12-bit sequence])
            long currentState = this.state.get();

            // Unsigned right shift extracts upper 52 bits
            long prevTimestamp = currentState >>> 12; 
            // Mask extracts lower 12 bits
            int prevSequence = (int) (currentState & 0xFFFL);

            // STEP 2: Read current epoch value
            long timestamp = this.clock.millis();

            // STEP 3: Calculate next state
            long sequence;

            if (timestamp > prevTimestamp) {
                // SCENARIO 1: Clock advanced to a new millisecond.
                //             Reset sequence counter to 0.
                sequence = 0;
            }
            else {
                // SCENARIO 2: Either same millisecond OR clock moved backward (NTP adjustment).
                //             In both cases, preserve monotonicity by:
                //             1. Using the previous timestamp (ignore backward movement)
                //             2. Incrementing the sequence counter
                timestamp = prevTimestamp;
                sequence = prevSequence + 1;

                // Boundary Check: Sequence counter is only 12 bits (0-4095)
                if (sequence > 0xFFF) {
                    // If we've generated 4096 UUIDs in the same millisecond,
                    // we use Spin-wait until the clock advances.
                    // Thread.onSpinWait() hints to the CPU that we're in a busy-wait loop
                    while (this.clock.millis() <= prevTimestamp) {
                        Thread.onSpinWait();
                    }

                    // Retry with the new timestamp
                    continue;
                }
            }

            // STEP 4: Pack the new state.
            //         Format: [52-bit timestamp][12-bit sequence])
            long nextState = (timestamp << 12) | sequence;

            // STEP 5: Update State atomically.
            //         compareAndSet ensures that if another thread modified state between
            //         our read and this update, we'll retry the entire operation.
            if (this.state.compareAndSet(currentState, nextState)) {
                // STEP 5: Encode values into UUID
                return buildUUID(timestamp, sequence);
            }

            // At this point CAS failed.
            // Retry with the new state
        }
    }

    /**
     * Encodes timestamp and sequence counter into a UUIDv7.
     * <p>
     * Constructs the 128-bit UUID according to RFC 9562 layout:
     *
     * <pre>
     * MSB (64 bits):
     *   Bits  0-47: Unix timestamp (milliseconds)
     *   Bits 48-51: Version = 7 (0111 binary)
     *   Bits 52-63: Sequence counter (12 bits)
     *
     * LSB (64 bits):
     *   Bits  0- 1: Variant = 2 (10 binary, meaning bits are 10xxxxxx...)
     *   Bits  2-63: Random bits (62 bits)
     * </pre>
     *
     * @param timestamp the Unix epoch millisecond timestamp (only lower 48 bits used)
     * @param sequence the sub-millisecond sequence counter (0-4095)
     * @return a properly formatted UUIDv7
     */
    private UUID buildUUID(long timestamp, long sequence) {

        // STEP 1: HIGH BITS CONSTRUCTION (Most Significant Bits)

        // Start with timestamp in the leftmost 48 bits
        // Mask ensures we only use 48 bits: 0xFFFFFFFFFFFF = 48 set bits
        // Left shift by 16 to make room for version (4 bits) + sequence (12 bits)
        long msb = (timestamp & 0xFFFFFFFFFFFFL) << 16;

        // OR in the version field: 7 in binary is 0111
        // 0x7000 = 0111 0000 0000 0000 in binary (version 7 in correct position)
        msb |= 0x7000L;

        // OR in the sequence counter in the lowest 12 bits
        // Mask ensures sequence fits in 12 bits: 0xFFF = 0000 1111 1111 1111
        msb |= (sequence & 0xFFFL);

        // STEP 2: LOW BITS CONSTRUCTION (Least Significant Bits)

        long randomBits = this.random.nextLong();

        // Set variant bits: must be 10 (binary) per RFC 9562
        // 0x3FFFFFFFFFFFFFFF clears top 2 bits: 00111111...
        // 0x8000000000000000 sets top bit to 1:  10000000...
        // Result: 10xxxxxx... where x = random bits
        long lsb = (randomBits & 0x3FFFFFFFFFFFFFFFL) | 0x8000000000000000L;

        // STEP 3: Construct the UUID from the two 64-bit longs
        return new UUID(msb, lsb);
    }

    /**
     * Static factory to generate a new UUIDv7.
     * <p>
     * This method provides a drop-in replacement for {@link UUID#randomUUID()}
     * with the added benefits of time-ordering and monotonicity.
     * <p>
     * All UUIDs generated through this method share the same monotonic sequence,
     * ensuring process-wide ordering guarantees.
     *
     * @return a new UUIDv7
     */
    public static UUID randomUUID() {
        return SHARED.generate();
    }

}
