package com.satyam.trading2.infrastructure.ratelimit;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Thread-safe rate limiter using dual sliding window algorithm
 * Enforces both per-second and per-minute rate limits for Kite API:
 * - 6 requests per second (reduced from 8 for safety margin)
 * - 350 requests per minute
 * - Circuit breaker: 12 second cooldown when Kite returns "Maximum allowed order requests exceeded"
 *
 * IMPORTANT: Timestamp is added BEFORE checking limits to prevent race conditions
 */
@Slf4j
@Component
public class RateLimiter {

    // Rate limits (conservative - broker allows 10/sec, we use 6/sec for safety)
    private static final int MAX_REQUESTS_PER_SECOND = 6;  // Reduced from 8 to 6
    private static final int MAX_REQUESTS_PER_MINUTE = 350;
    private static final long SECOND_WINDOW_MS = 1000; // 1 second window
    private static final long MINUTE_WINDOW_MS = 60000; // 60 second window
    private static final long CIRCUIT_BREAKER_COOLDOWN_MS = 12000; // 12 second cooldown after rate limit error
    private static final int MAX_RETRIES = 10; // Maximum retry attempts to prevent infinite loops

    // Thread-safe queues to track request timestamps
    // Using same queue for both windows - just different cleanup thresholds
    private final ConcurrentLinkedDeque<Long> requestTimestamps = new ConcurrentLinkedDeque<>();

    // Circuit breaker - when Kite returns rate limit error
    private volatile long circuitBreakerUntil = 0; // Timestamp until which all requests are blocked

    // Counter for monitoring (optional)
    private final AtomicInteger totalRequests = new AtomicInteger(0);
    private final AtomicInteger throttledRequests = new AtomicInteger(0);
    private final AtomicInteger circuitBreakerTrips = new AtomicInteger(0);
    
    /**
     * Acquire permission to make an API call
     * Blocks until permission is granted (respects BOTH per-second AND per-minute rate limits)
     * Also enforces circuit breaker if Kite returned rate limit error
     *
     * @param apiName Name of the API being called (for logging)
     */
    public synchronized void  acquire(String apiName) {
        acquireWithRetry(apiName, 0);
    }

    /**
     * Internal method with retry counter to prevent infinite loops
     * Timestamp is added BEFORE checking to prevent race conditions
     */
    private synchronized void acquireWithRetry(String apiName, int retryCount) {
        // Prevent infinite retry loops
        if (retryCount > MAX_RETRIES) {
            log.error("❌ Rate limiter max retries ({}) exceeded for {}", MAX_RETRIES, apiName);
            throw new RuntimeException("Rate limiter max retries exceeded for " + apiName);
        }

        long now = System.currentTimeMillis();

        // ===== CIRCUIT BREAKER CHECK =====
        // If circuit breaker is active (due to Kite rate limit error), block ALL requests
        if (now < circuitBreakerUntil) {
            long remainingCooldown = circuitBreakerUntil - now;
            log.error("🚨 CIRCUIT BREAKER ACTIVE: Blocking {} call for {}ms due to Kite rate limit error",
                    apiName, remainingCooldown);

            try {
                Thread.sleep(remainingCooldown + 100); // Wait + small buffer
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Rate limiter interrupted during circuit breaker", e);
            }

            now = System.currentTimeMillis();
        }

        // Remove timestamps older than 60 seconds (need to keep 1 minute of history)
        cleanupOldTimestamps(now, MINUTE_WINDOW_MS);

        // ===== KEY FIX: ADD TIMESTAMP FIRST (reserve the slot) =====
        // This prevents race conditions where multiple threads see the same count
        requestTimestamps.addLast(now);
        totalRequests.incrementAndGet();

        // Count requests in 1-second and 1-minute windows (includes the one we just added)
        int requestsInLastSecond = countRequestsInWindow(now, SECOND_WINDOW_MS);
        int requestsInLastMinute = countRequestsInWindow(now, MINUTE_WINDOW_MS);

        // Check if we exceeded EITHER rate limit
        if (requestsInLastSecond > MAX_REQUESTS_PER_SECOND ||
            requestsInLastMinute > MAX_REQUESTS_PER_MINUTE) {

            // We exceeded the limit - remove our timestamp reservation
            requestTimestamps.removeLast();
            totalRequests.decrementAndGet();

            long waitTime = 0;
            String limitType = "";

            // Determine which limit we're hitting and calculate wait time
            if (requestsInLastSecond > MAX_REQUESTS_PER_SECOND) {
                // Per-second limit hit - wait until oldest request in 1-sec window expires
                Long oldestInSecond = findOldestInWindow(now, SECOND_WINDOW_MS);
                if (oldestInSecond != null) {
                    waitTime = (oldestInSecond + SECOND_WINDOW_MS) - now;
                    limitType = "per-second";
                }
            }

            if (requestsInLastMinute > MAX_REQUESTS_PER_MINUTE) {
                // Per-minute limit hit - wait until oldest request in 1-min window expires
                Long oldestInMinute = findOldestInWindow(now, MINUTE_WINDOW_MS);
                if (oldestInMinute != null) {
                    long minuteWaitTime = (oldestInMinute + MINUTE_WINDOW_MS) - now;
                    // Use the longer wait time
                    if (minuteWaitTime > waitTime) {
                        waitTime = minuteWaitTime;
                        limitType = "per-minute";
                    }
                }
            }

            if (waitTime > 0) {
                throttledRequests.incrementAndGet();
                log.warn("🚦 Rate limit reached ({} limit: {}/{} per sec, {}/{} per min). Waiting {}ms before {} call (retry {})",
                        limitType,
                        requestsInLastSecond, MAX_REQUESTS_PER_SECOND,
                        requestsInLastMinute, MAX_REQUESTS_PER_MINUTE,
                        waitTime, apiName, retryCount);

                try {
                    // INCREASED BUFFER: 150ms instead of 50ms to account for thread scheduling, GC pauses
                    Thread.sleep(waitTime + 150);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Rate limiter interrupted", e);
                }

                // Retry recursively
                acquireWithRetry(apiName, retryCount + 1);
            } else {
                // No wait time calculated but limit exceeded - retry immediately
                acquireWithRetry(apiName, retryCount + 1);
            }
        } else {
            // Success! We're within limits and timestamp is already added
            log.debug("✅ Rate limiter: {} call permitted ({}/{} per sec, {}/{} per min, retry {})",
                    apiName,
                    requestsInLastSecond, MAX_REQUESTS_PER_SECOND,
                    requestsInLastMinute, MAX_REQUESTS_PER_MINUTE,
                    retryCount);
        }
    }
    
    /**
     * Remove timestamps older than the specified window
     */
    private void cleanupOldTimestamps(long now, long windowSizeMs) {
        while (!requestTimestamps.isEmpty()) {
            Long oldest = requestTimestamps.peekFirst();
            if (oldest != null && (now - oldest) >= windowSizeMs) {
                requestTimestamps.pollFirst();
            } else {
                break;
            }
        }
    }

    /**
     * Count requests within a specific time window
     */
    private int countRequestsInWindow(long now, long windowSizeMs) {
        int count = 0;
        for (Long timestamp : requestTimestamps) {
            if ((now - timestamp) < windowSizeMs) {
                count++;
            }
        }
        return count;
    }

    /**
     * Find the oldest timestamp within a specific time window
     */
    private Long findOldestInWindow(long now, long windowSizeMs) {
        for (Long timestamp : requestTimestamps) {
            if ((now - timestamp) < windowSizeMs) {
                return timestamp;
            }
        }
        return null;
    }

    /**
     * Get current request count in the 1-second window
     */
    public int getCurrentWindowSize() {
        long now = System.currentTimeMillis();
        return countRequestsInWindow(now, SECOND_WINDOW_MS);
    }

    /**
     * Get current request count in the 1-minute window
     */
    public int getCurrentMinuteWindowSize() {
        long now = System.currentTimeMillis();
        return countRequestsInWindow(now, MINUTE_WINDOW_MS);
    }

    /**
     * Trigger circuit breaker when Kite returns "Maximum allowed order requests exceeded"
     * Blocks ALL API calls for 12 seconds
     *
     * IMPORTANT: Call this method immediately when you receive the error from Kite API
     */
    public void tripCircuitBreaker() {
        long now = System.currentTimeMillis();
        circuitBreakerUntil = now + CIRCUIT_BREAKER_COOLDOWN_MS;
        circuitBreakerTrips.incrementAndGet();

        log.error("🚨🚨🚨 CIRCUIT BREAKER TRIPPED! Blocking all API calls for {}ms due to Kite rate limit error",
                CIRCUIT_BREAKER_COOLDOWN_MS);
        log.error("       All pending/new requests will wait until: {}",
                new java.util.Date(circuitBreakerUntil));
    }

    /**
     * Check if circuit breaker is currently active
     */
    public boolean isCircuitBreakerActive() {
        return System.currentTimeMillis() < circuitBreakerUntil;
    }

    /**
     * Get remaining circuit breaker cooldown in milliseconds (0 if not active)
     */
    public long getCircuitBreakerRemainingMs() {
        long remaining = circuitBreakerUntil - System.currentTimeMillis();
        return Math.max(0, remaining);
    }

    /**
     * Get total number of API calls made
     */
    public int getTotalRequests() {
        return totalRequests.get();
    }

    /**
     * Get number of times we had to throttle (wait)
     */
    public int getThrottledRequests() {
        return throttledRequests.get();
    }

    /**
     * Get number of times circuit breaker was tripped
     */
    public int getCircuitBreakerTrips() {
        return circuitBreakerTrips.get();
    }

    /**
     * Reset counters (for testing or daily reset)
     */
    public void resetCounters() {
        totalRequests.set(0);
        throttledRequests.set(0);
        circuitBreakerTrips.set(0);
        circuitBreakerUntil = 0;
        log.info("📊 Rate limiter counters reset (including circuit breaker)");
    }
    
    /**
     * Get statistics for monitoring
     */
    public String getStats() {
        String cbStatus = isCircuitBreakerActive()
            ? String.format(" [🚨 CB ACTIVE: %dms remaining]", getCircuitBreakerRemainingMs())
            : "";

        return String.format("RateLimiter Stats: Total=%d, Throttled=%d, CB Trips=%d, Current: %d/%d per sec, %d/%d per min%s",
                getTotalRequests(),
                getThrottledRequests(),
                getCircuitBreakerTrips(),
                getCurrentWindowSize(), MAX_REQUESTS_PER_SECOND,
                getCurrentMinuteWindowSize(), MAX_REQUESTS_PER_MINUTE,
                cbStatus);
    }
}

