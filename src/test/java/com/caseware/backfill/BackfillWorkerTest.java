package com.caseware.backfill;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class BackfillWorkerTest {

    private static final Instant T0 = Instant.parse("2026-11-01T00:00:00Z");
    private static final TemplateRef REF = new TemplateRef("tpl-1", "v1");

    private static final BackfillConfig CFG =
            new BackfillConfig(
                    5, 3, Duration.ofMillis(1), Duration.ofMillis(5),
                    Duration.ofSeconds(60), Duration.ofMillis(5));

    /** Scripted loader: records calls and concurrency, fails as configured per engagement. */
    static final class FakeLoader implements EngagementLoader {
        final List<EngagementKey> callOrder = Collections.synchronizedList(new ArrayList<>());
        final Map<EngagementKey, AtomicInteger> calls = new HashMap<>();
        final Map<EngagementKey, Integer> failFirstN = new HashMap<>(); // MAX = always
        final Map<EngagementKey, Boolean> failRetryable = new HashMap<>();
        final AtomicInteger running = new AtomicInteger();
        final AtomicInteger maxRunning = new AtomicInteger();
        volatile long loadMillis = 0;

        synchronized AtomicInteger counter(EngagementKey k) {
            return calls.computeIfAbsent(k, x -> new AtomicInteger());
        }

        int callsFor(String firm, String doc) {
            return counter(new EngagementKey(firm, doc)).get();
        }

        @Override
        public TemplateRef loadTemplateRef(EngagementKey key, Duration timeout) throws LoadException {
            int n = counter(key).incrementAndGet();
            callOrder.add(key);
            int now = running.incrementAndGet();
            maxRunning.accumulateAndGet(now, Math::max);
            try {
                if (loadMillis > 0) Thread.sleep(loadMillis);
                if (n <= failFirstN.getOrDefault(key, 0)) {
                    throw new LoadException("boom", failRetryable.getOrDefault(key, true));
                }
                return REF;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new LoadException("interrupted", false, e);
            } finally {
                running.decrementAndGet();
            }
        }
    }

    private static BackfillWorker worker(
            InMemoryMetadataStore s, FakeLoader l, CapacityPolicy p) {
        return new BackfillWorker(s, l, p, CFG);
    }

    @Test
    void processesMostRecentFirst_nullsLast_withStableTieBreaker() {
        InMemoryMetadataStore store = new InMemoryMetadataStore();
        store.addUnknown("B", "d1", T0);                 // same instant as A/d2 -> tie-break
        store.addUnknown("A", "d2", T0);
        store.addUnknown("A", "d1", T0);
        store.addUnknown("C", "old", T0.minusSeconds(3600));
        store.addUnknown("C", "never", null);            // never seen -> last
        store.addUnknown("A", "newest", T0.plusSeconds(60));
        FakeLoader loader = new FakeLoader();

        BackfillWorker.Report r = worker(store, loader, () -> 1).run(); // concurrency 1 = sequential

        assertEquals(BackfillWorker.StopReason.EXHAUSTED, r.reason());
        assertEquals(
                List.of(
                        new EngagementKey("A", "newest"),
                        new EngagementKey("A", "d1"),
                        new EngagementKey("A", "d2"),
                        new EngagementKey("B", "d1"),
                        new EngagementKey("C", "old"),
                        new EngagementKey("C", "never")),
                loader.callOrder);
        assertEquals(6, r.populated());
        assertEquals(0, r.remainingUnpopulated());
    }

    @Test
    void neverExceedsAllowedConcurrency() {
        InMemoryMetadataStore store = new InMemoryMetadataStore();
        for (int i = 0; i < 30; i++) store.addUnknown("F", "d" + i, T0.minusSeconds(i));
        FakeLoader loader = new FakeLoader();
        loader.loadMillis = 15;

        BackfillWorker.Report r = worker(store, loader, () -> 4).run();

        assertEquals(30, r.populated());
        assertTrue(loader.maxRunning.get() <= 4, "max concurrent loads " + loader.maxRunning);
        assertTrue(loader.maxRunning.get() > 1, "should actually run in parallel");
    }

    @Test
    void doesNotOverwriteARowPopulatedWhileTheLoadWasRunning() {
        InMemoryMetadataStore store = new InMemoryMetadataStore();
        store.addUnknown("F", "d1", T0);
        TemplateRef opportunistic = new TemplateRef("tpl-9", "v9");
        EngagementLoader loader =
                (key, timeout) -> {
                    // Stage 2 (a user opened the engagement) populates it during our 1-minute load.
                    store.populateIfEmpty(key, opportunistic);
                    return REF;
                };

        BackfillWorker.Report r = new BackfillWorker(store, loader, () -> 1, CFG).run();

        assertEquals(0, r.populated());
        assertEquals(1, r.alreadyPopulated());
        assertEquals(0, r.failed());
        assertEquals(opportunistic, store.templateOf("F", "d1"));
    }

    @Test
    void aFailingEngagementDoesNotBlockTheRest_andIsNotReReadInTheSameRun() {
        InMemoryMetadataStore store = new InMemoryMetadataStore();
        store.addUnknown("F", "bad", T0.plusSeconds(10)); // would be first in order
        for (int i = 0; i < 8; i++) store.addUnknown("F", "ok" + i, T0.minusSeconds(i));
        FakeLoader loader = new FakeLoader();
        loader.failFirstN.put(new EngagementKey("F", "bad"), Integer.MAX_VALUE);

        BackfillWorker.Report r = worker(store, loader, () -> 2).run();

        assertEquals(BackfillWorker.StopReason.EXHAUSTED, r.reason());
        assertEquals(8, r.populated());
        assertEquals(1, r.failed());
        assertEquals(1, r.remainingUnpopulated());
        assertEquals(3, loader.callsFor("F", "bad"), "maxAttempts, and not retried again this run");
    }

    @Test
    void transientFailureIsRetriedAndSucceeds() {
        InMemoryMetadataStore store = new InMemoryMetadataStore();
        store.addUnknown("F", "d1", T0);
        FakeLoader loader = new FakeLoader();
        loader.failFirstN.put(new EngagementKey("F", "d1"), 2);

        BackfillWorker.Report r = worker(store, loader, () -> 1).run();

        assertEquals(1, r.populated());
        assertEquals(0, r.failed());
        assertEquals(3, loader.callsFor("F", "d1"));
    }

    @Test
    void nonRetryableFailureIsAttemptedOnce() {
        InMemoryMetadataStore store = new InMemoryMetadataStore();
        store.addUnknown("F", "d1", T0);
        FakeLoader loader = new FakeLoader();
        EngagementKey k = new EngagementKey("F", "d1");
        loader.failFirstN.put(k, Integer.MAX_VALUE);
        loader.failRetryable.put(k, false);

        BackfillWorker.Report r = worker(store, loader, () -> 1).run();

        assertEquals(1, loader.callsFor("F", "d1"));
        assertEquals(1, r.failed());
        assertEquals(1, r.remainingUnpopulated());
    }

    @Test
    void aFailedWriteIsRetriedWithoutLoadingAgain() {
        InMemoryMetadataStore store = new InMemoryMetadataStore();
        store.addUnknown("F", "d1", T0);
        store.writeFailuresToInject.set(2); // fewer than maxAttempts
        FakeLoader loader = new FakeLoader();

        BackfillWorker.Report r = worker(store, loader, () -> 1).run();

        assertEquals(1, r.populated());
        assertEquals(1, loader.callsFor("F", "d1"), "the 1-minute load must not be repeated");
    }

    @Test
    void secondRunOnlyTouchesWhatIsStillUnknown() {
        InMemoryMetadataStore store = new InMemoryMetadataStore();
        store.addUnknown("F", "bad", T0);
        store.addUnknown("F", "ok", T0.minusSeconds(1));
        FakeLoader loader = new FakeLoader();
        loader.failFirstN.put(new EngagementKey("F", "bad"), 3); // fails the whole first run

        worker(store, loader, () -> 1).run();
        assertEquals(1, loader.callsFor("F", "ok"));

        BackfillWorker.Report second = worker(store, loader, () -> 1).run(); // next window

        assertEquals(1, loader.callsFor("F", "ok"), "already populated: not loaded again");
        assertEquals(4, loader.callsFor("F", "bad"));
        assertEquals(1, second.populated());
        assertEquals(0, second.remainingUnpopulated());
    }

    @Test
    void stopsWhenCapacityIsWithdrawn_andResumesInTheNextWindow() {
        InMemoryMetadataStore store = new InMemoryMetadataStore();
        for (int i = 0; i < 20; i++) store.addUnknown("F", "d" + i, T0.minusSeconds(i));
        FakeLoader loader = new FakeLoader();
        loader.loadMillis = 5;
        AtomicInteger polls = new AtomicInteger();
        CapacityPolicy closesAfterAWhile = () -> polls.incrementAndGet() <= 3 ? 1 : 0;

        BackfillWorker.Report first = worker(store, loader, closesAfterAWhile).run();

        assertEquals(BackfillWorker.StopReason.CAPACITY_WITHDRAWN, first.reason());
        assertTrue(first.remainingUnpopulated() > 0);
        assertEquals(20, first.populated() + first.remainingUnpopulated(), "in-flight work finished");

        BackfillWorker.Report second = worker(store, loader, () -> 3).run();

        assertEquals(BackfillWorker.StopReason.EXHAUSTED, second.reason());
        assertEquals(0, second.remainingUnpopulated());
        for (int i = 0; i < 20; i++) assertEquals(1, loader.callsFor("F", "d" + i));
    }

    @Test
    void doesNotStartAnythingWhenCapacityIsZero() {
        InMemoryMetadataStore store = new InMemoryMetadataStore();
        store.addUnknown("F", "d1", T0);
        FakeLoader loader = new FakeLoader();

        BackfillWorker.Report r = worker(store, loader, () -> 0).run();

        assertEquals(BackfillWorker.StopReason.CAPACITY_WITHDRAWN, r.reason());
        assertEquals(0, loader.callOrder.size());
        assertEquals(1, r.remainingUnpopulated());
    }
}
