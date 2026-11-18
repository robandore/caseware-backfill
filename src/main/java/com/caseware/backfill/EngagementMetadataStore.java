package com.caseware.backfill;

import java.util.List;

/** Access to {@code engagement_template_version}. */
public interface EngagementMetadataStore {

    /**
     * Returns up to {@code limit} rows whose {@code templateId} and {@code versionId} are both
     * null, in {@link BackfillOrdering#ORDER}, strictly after {@code after} ({@code null} = from
     * the beginning).
     *
     * <p>Keyset pagination: the caller passes the last row of the previous page.
     */
    List<Candidate> findUnpopulated(Candidate after, int limit);

    /**
     * Conditional update: sets {@code templateId} and {@code versionId} only if both are still
     * null.
     *
     * @return {@code true} if this call populated the row; {@code false} if it was already
     *     populated (for example by the opportunistic backfill) and was left untouched
     */
    boolean populateIfEmpty(EngagementKey key, TemplateRef ref);

    /** Rows still unknown. Used to report backfill progress. */
    long countUnpopulated();
}
