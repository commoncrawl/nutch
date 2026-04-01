package org.commoncrawl.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

/**
 * This class was borrowed from https://github.com/belief-driven-design/blog-uuidv7
 * Copyright to the original author, and licensed as CC0.
 */
class UUIDv7Tests {

    /**
     * Validates that generated UUIDs have correct version (7) and variant (2) fields
     * as required by RFC 9562 for proper UUID classification.
     */
    @Test
    void versionAndVariantAreCorrect() {
        UUID result = UUIDv7.randomUUID();

        assertEquals(7, result.version(), "must be UUIDv7");
        assertEquals(2, result.variant(), "must be IETF variant");
    }

    /**
     * Verifies strict monotonic ordering in a single-threaded context.
     * <p>
     * Tests both code paths:
     * <ul>
     *   <li>Same millisecond: sequence increments</li>
     *   <li>New millisecond: sequence resets, timestamp advances</li>
     * </ul>
     */
    @Test
    void idsAreStrictlyIncreasingInSingleThread() {
        MutableClock clock = new MutableClock(1_234_567_890_123L);
        UUIDv7 generator = new UUIDv7(clock);

        UUID prev = generator.generate();

        for (int i = 0; i < 10_000; i++) {
            UUID next = generator.generate();

            assertTrue(compareUnsignedLex(prev, next) < 0, "not monotonic: prev=" + prev + ", next=" + next);

            prev = next;

            // Occasionally advance time so both code paths are exercised
            // (same millisecond vs. new millisecond)
            if ((i % 257) == 0) {
                clock.addMillis(1);
            }
        }
    }

    /**
     * Validates that clock regression (e.g., NTP drift) doesn't break monotonicity.
     */
    @Test
    void clockRollbackDoesNotBreakMonotonicity() {
        MutableClock clock = new MutableClock(10_000L);
        UUIDv7 generator = new UUIDv7(clock);

        UUID a = generator.generate();
        clock.addMillis(1);
        UUID b = generator.generate();

        // Simulate wall-clock going backwards (e.g., NTP correction)
        clock.setMillis(9_000L);
        UUID c = generator.generate();

        assertTrue(compareUnsignedLex(a, b) < 0);
        assertTrue(compareUnsignedLex(b, c) < 0, "must not go backward when clock regresses");
    }

    /**
     * Validates uniqueness under high concurrency.
     * <p>Spawns multiple threads that simultaneously generate UUIDs and verifies:
     * <ul>
     *   <li>No duplicates are produced</li>
     *   <li>CAS algorithm handles contention correctly</li>
     *   <li>All generated UUIDs are unique across threads</li>
     * </ul>
     */
    @Test
    void concurrentGenerationProducesUniqueIds() throws Exception {
        int threads = Runtime.getRuntime().availableProcessors() - 2;
        int perThread = 5_000;

        Set<UUID> all = ConcurrentHashMap.<UUID>newKeySet(threads * perThread);

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);

        for (int t = 0; t < threads; t++) {
            pool.submit(() -> {
                start.await();
                for (int i = 0; i < perThread; i++) {
                    all.add(UUIDv7.randomUUID());
                }
                return null;
            });
        }

        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));

        assertEquals(threads * perThread, all.size(), "no duplicates allowed");
    }

    /**
     * Tests sequence overflow behavior when generating 4096+ UUIDs in a single millisecond.
     * <p>
     * This test verifies:
     * <ul>
     *   <li>Generator correctly handles sequence overflow</li>
     *   <li>All UUIDs remain unique and monotonic</li>
     *   <li>Generator waits for clock advancement rather than wrapping the counter</li>
     * </ul>
     */
    @Test
    void sequenceOverflowCausesClockWait() {
        MutableClock clock = new MutableClock(123_456_789L);
        UUIDv7 generator = new UUIDv7(clock);

        List<UUID> batch = new ArrayList<>();

        // Generate exactly 4096 UUIDs (fills the sequence counter: 0-4095)
        for (int i = 0; i < 4096; i++) {
            batch.add(generator.generate());
        }

        // Next generation should trigger overflow handling.
        // Start a thread that will generate the 4097th UUID.
        // We need to do this in a separate thread to not call Thread.spinWait()
        // on the main thread.

        // This is the synchronization signal between the main test thread and the overflow thread.
        AtomicInteger finalizedCount = new AtomicInteger(0);
        Thread overflowThread = new Thread(() -> {
            UUID overflow = generator.generate();
            batch.add(overflow);
            finalizedCount.incrementAndGet();
        });

        overflowThread.start();

        // Give the thread time to enter the spin-wait loop
        try {
            Thread.sleep(50);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Thread should be waiting, not completed yet
        assertEquals(0, finalizedCount.get(), "should be waiting for clock to advance");

        // Advance the clock to release the waiting thread
        clock.addMillis(1);

        // Wait for the thread to complete
        try {
            overflowThread.join(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        assertEquals(1, finalizedCount.get(), "should have completed after clock advance");
        assertEquals(4097, batch.size(), "should have all UUIDs");

        // Verify all UUIDs are unique
        assertEquals(4097, new HashSet<>(batch).size(), "all UUIDs must be unique");

        // Verify strict monotonicity
        for (int i = 1; i < batch.size(); i++) {
            assertTrue(compareUnsignedLex(batch.get(i - 1), batch.get(i)) < 0,
                    "UUIDs must be strictly increasing");
        }
    }

    /**
     * Validates the bit-level structure of generated UUIDs according to RFC 9562.
     * <p>
     * Extracts and validates individual fields:
     * <ul>
     *   <li>Timestamp: MSB 0-47</li>
     *   <li>Version (7): MSB 48-51</li>
     *   <li>Sequence: MSB 52-63</li>
     *   <li>Variant (2): LSB top 2 bits</li>
     * </ul>
     */
    @Test
    void bitLevelStructureIsCorrect() {
        long clockMs = 1_234_567_890_000L;
        MutableClock clock = new MutableClock(clockMs);
        UUIDv7 generator = new UUIDv7(clock);

        UUID first = generator.generate();
        UUID second = generator.generate();

        // Extract fields from MSB
        long msb1 = first.getMostSignificantBits();
        long timestamp1 = msb1 >>> 16; // Top 48 bits
        int version1 = (int) ((msb1 >>> 12) & 0xFL); // 4 bits after timestamp
        int sequence1 = (int) (msb1 & 0xFFFL); // Bottom 12 bits

        long msb2 = second.getMostSignificantBits();
        long timestamp2 = msb2 >>> 16;
        int version2 = (int) ((msb2 >>> 12) & 0xFL);
        int sequence2 = (int) (msb2 & 0xFFFL);

        // Validate version
        assertEquals(7, version1, "version must be 7");
        assertEquals(7, version2, "version must be 7");

        // Validate timestamp matches clock
        assertEquals(clockMs, timestamp1, "timestamp should match clock");
        assertEquals(clockMs, timestamp2, "timestamp should match clock");

        // Validate sequence increments (same millisecond)
        assertEquals(0, sequence1, "first UUID should have sequence 0");
        assertEquals(1, sequence2, "second UUID should have sequence 1");

        // Extract and validate variant from LSB
        long lsb1 = first.getLeastSignificantBits();
        long lsb2 = second.getLeastSignificantBits();

        // Variant bits should be 10 (binary), meaning the top bit is 1 and second bit is 0
        // In hex, this means the first nibble of LSB is in range [8, 9, a, b]
        assertTrue((lsb1 & 0x8000000000000000L) != 0, "variant bit 0 must be 1");
        assertTrue((lsb1 & 0x4000000000000000L) == 0, "variant bit 1 must be 0");
        assertTrue((lsb2 & 0x8000000000000000L) != 0, "variant bit 0 must be 1");
        assertTrue((lsb2 & 0x4000000000000000L) == 0, "variant bit 1 must be 0");

        // Validate random bits are different between UUIDs
        // Mask out the variant bits and compare
        long random1 = lsb1 & 0x3FFFFFFFFFFFFFFFL;
        long random2 = lsb2 & 0x3FFFFFFFFFFFFFFFL;
        assertNotEquals(random1, random2, "random bits should differ between UUIDs");
    }

    /**
     * Stress test with extreme concurrency to validate robustness.
     */
    @Test
    void stressTestWithHighConcurrency() throws Exception {
        int threads = Runtime.getRuntime().availableProcessors() * 2;
        int perThread = 50_000;

        Set<UUID> all = ConcurrentHashMap.<UUID>newKeySet(threads * perThread);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);

        for (int t = 0; t < threads; t++) {
            pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < perThread; i++) {
                        all.add(UUIDv7.randomUUID());
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }

        start.countDown();
        assertTrue(done.await(30, TimeUnit.SECONDS), "stress test should complete in reasonable time");
        pool.shutdown();

        assertEquals(threads * perThread, all.size(), "all UUIDs must be unique under stress");
    }

    /**
     * Validates that UUIDs generated at different times have increasing timestamps.
     */
    @Test
    void timestampIncreasesOverTime() {
        MutableClock clock = new MutableClock(1_000_000L);
        UUIDv7 generator = new UUIDv7(clock);

        UUID first = generator.generate();
        clock.addMillis(100);
        UUID second = generator.generate();
        clock.addMillis(200);
        UUID third = generator.generate();

        long ts1 = first.getMostSignificantBits() >>> 16;
        long ts2 = second.getMostSignificantBits() >>> 16;
        long ts3 = third.getMostSignificantBits() >>> 16;

        assertEquals(1_000_000L, ts1);
        assertEquals(1_000_100L, ts2);
        assertEquals(1_000_300L, ts3);

        assertTrue(ts1 < ts2);
        assertTrue(ts2 < ts3);
    }

    // HELPERS

    /**
     * Compares two UUIDs lexicographically using unsigned comparison.
     * <p>
     * Java's {@link UUID#compareTo(UUID)} uses signed long comparison,
     * which doesn't match the lexicographic byte ordering required by UUIDv7.
     *
     * @param a first UUID
     * @param b second UUID
     * @return negative if a < b, zero if a == b, positive if a > b (lexicographically)
     */
    private static int compareUnsignedLex(UUID a, UUID b) {
        int msb = Long.compareUnsigned(a.getMostSignificantBits(), b.getMostSignificantBits());
        if (msb != 0) {
            return msb;
        }
        return Long.compareUnsigned(a.getLeastSignificantBits(), b.getLeastSignificantBits());
    }

    /**
     * Mutable Clock implementation for testing.
     */
    private static final class MutableClock extends Clock {

        private volatile long millis;

        MutableClock(long initialMillis) {
            this.millis = initialMillis;
        }

        void setMillis(long ms) {
            this.millis = ms;
        }

        void addMillis(long delta) {
            this.millis += delta;
        }

        @Override
        public ZoneOffset getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(java.time.ZoneId zone) {
            return this;
        }

        @Override
        public long millis() {
            return this.millis;
        }

        @Override
        public Instant instant() {
            return Instant.ofEpochMilli(this.millis);
        }
    }
}