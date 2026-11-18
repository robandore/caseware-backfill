package com.caseware.backfill;

import java.time.Instant;
import java.util.Comparator;

/**
 * Processing order of the scheduled backfill (Stage 3):
 *
 * <ol>
 *   <li>{@code lastSeenAt DESC}, most recently used engagements first;
 *   <li>{@code NULLS LAST}, engagements never seen go last;
 *   <li>stable tie-breaker: {@code firmId ASC, engagementDocId ASC}.
 * </ol>
 *
 * The tie-breaker makes the order total, which is what allows keyset pagination.
 */
public final class BackfillOrdering {

    public static final Comparator<Candidate> ORDER =
            Comparator.comparing(
                            Candidate::lastSeenAt,
                            Comparator.nullsLast(Comparator.reverseOrder()))
                    .thenComparing(c -> c.key().firmId())
                    .thenComparing(c -> c.key().engagementDocId());

    private BackfillOrdering() {}
}
