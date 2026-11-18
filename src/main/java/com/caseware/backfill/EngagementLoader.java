package com.caseware.backfill;

import java.time.Duration;

/**
 * Loads an Engagement File through Engagement Management and returns the template it was created
 * from. Owned by another team; one call takes ~1 minute.
 */
public interface EngagementLoader {

    /**
     * Implementations must honour {@code timeout} and be interruptible. The call must not modify
     * the engagement, so it can safely be repeated.
     */
    TemplateRef loadTemplateRef(EngagementKey key, Duration timeout) throws LoadException;
}
