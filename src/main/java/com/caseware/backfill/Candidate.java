package com.caseware.backfill;

import java.time.Instant;
import java.util.Objects;

/**
 * A row of {@code engagement_template_version} whose {@code templateId} and {@code versionId} are
 * both null. {@code lastSeenAt} may be null (the engagement has never been seen loaded).
 */
public record Candidate(EngagementKey key, Instant lastSeenAt) {
    public Candidate {
        Objects.requireNonNull(key, "key");
    }
}
