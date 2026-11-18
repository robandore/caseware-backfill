package com.caseware.backfill;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/** In-memory stand-in for {@code engagement_template_version}. */
final class InMemoryMetadataStore implements EngagementMetadataStore {

    private final Map<EngagementKey, Instant> lastSeen = new HashMap<>();
    private final Map<EngagementKey, TemplateRef> template = new HashMap<>(); // absent = both null
    final AtomicInteger writeFailuresToInject = new AtomicInteger();

    synchronized void addUnknown(String firm, String doc, Instant lastSeenAt) {
        lastSeen.put(new EngagementKey(firm, doc), lastSeenAt);
    }

    synchronized TemplateRef templateOf(String firm, String doc) {
        return template.get(new EngagementKey(firm, doc));
    }

    @Override
    public synchronized List<Candidate> findUnpopulated(Candidate after, int limit) {
        List<Candidate> rows = new ArrayList<>();
        lastSeen.forEach(
                (k, seen) -> {
                    if (!template.containsKey(k)) rows.add(new Candidate(k, seen));
                });
        rows.sort(BackfillOrdering.ORDER);
        return rows.stream()
                .filter(c -> after == null || BackfillOrdering.ORDER.compare(c, after) > 0)
                .limit(limit)
                .toList();
    }

    @Override
    public synchronized boolean populateIfEmpty(EngagementKey key, TemplateRef ref) {
        if (writeFailuresToInject.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
            throw new IllegalStateException("injected store failure");
        }
        if (template.containsKey(key)) return false;
        template.put(key, ref);
        return true;
    }

    @Override
    public synchronized long countUnpopulated() {
        return lastSeen.keySet().stream().filter(k -> !template.containsKey(k)).count();
    }
}
