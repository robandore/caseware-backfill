package com.caseware.backfill;

import java.util.Objects;

/**
 * Identifies an engagement safely across firms.
 *
 * <p>Global uniqueness of {@code engagementDocId} is not confirmed, so the identity is the pair
 * {@code (firmId, engagementDocId)}.
 */
public record EngagementKey(String firmId, String engagementDocId) {
    public EngagementKey {
        Objects.requireNonNull(firmId, "firmId");
        Objects.requireNonNull(engagementDocId, "engagementDocId");
    }
}
