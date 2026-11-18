package com.caseware.backfill;

import java.time.Duration;
import java.util.Objects;

/**
 * @param pageSize rows read from the store per query
 * @param maxAttempts attempts per load and per write within one run (1 = no retries)
 * @param baseBackoff delay before the second attempt; doubles on each further attempt
 * @param maxBackoff upper bound for the delay between attempts
 * @param loadTimeout timeout handed to the loader for a single load
 * @param capacityPollInterval how often to re-read the capacity policy while waiting for a slot
 */
public record BackfillConfig(
        int pageSize,
        int maxAttempts,
        Duration baseBackoff,
        Duration maxBackoff,
        Duration loadTimeout,
        Duration capacityPollInterval) {

    public BackfillConfig {
        if (pageSize < 1) {
            throw new IllegalArgumentException("pageSize must be >= 1");
        }
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be >= 1");
        }
        Objects.requireNonNull(baseBackoff, "baseBackoff");
        Objects.requireNonNull(maxBackoff, "maxBackoff");
        Objects.requireNonNull(loadTimeout, "loadTimeout");
        Objects.requireNonNull(capacityPollInterval, "capacityPollInterval");
    }
}
