package com.caseware.backfill;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Stage 3 of the backfill: scheduled recovery of {@code (templateId, versionId)} for existing
 * engagements, loading each one through Engagement Management.
 *
 * <p>The work queue is the data itself: the rows of {@code engagement_template_version} whose two
 * template columns are both null. There is no separate work table. A processed row leaves that
 * set; a failed row simply stays in it and is picked up by a later run.
 *
 * <p>One call to {@link #run()} is one scheduled window. It ends when there is nothing left to
 * read ({@link StopReason#EXHAUSTED}), when the {@link CapacityPolicy} says to stop
 * ({@link StopReason#CAPACITY_WITHDRAWN}), or when the thread is interrupted. Loads already in
 * flight are never abandoned: they finish and their results are written before {@code run}
 * returns. The next window resumes from whatever is still null.
 */
public final class BackfillWorker {

    private static final System.Logger LOG = System.getLogger(BackfillWorker.class.getName());

    public enum StopReason {
        EXHAUSTED,
        CAPACITY_WITHDRAWN,
        INTERRUPTED
    }

    /** Counters for this worker instance, readable while it runs. */
    public record Progress(long populated, long alreadyPopulated, long failed, int inFlight) {}

    /** Outcome of one run. {@code remainingUnpopulated} is the store's count after the run. */
    public record Report(
            StopReason reason,
            long populated,
            long alreadyPopulated,
            long failed,
            long remainingUnpopulated) {}

    private final EngagementMetadataStore store;
    private final EngagementLoader loader;
    private final CapacityPolicy capacity;
    private final BackfillConfig cfg;

    private final AtomicLong populated = new AtomicLong();
    private final AtomicLong alreadyPopulated = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition slotFreed = lock.newCondition();
    private int inFlight; // guarded by lock

    public BackfillWorker(
            EngagementMetadataStore store,
            EngagementLoader loader,
            CapacityPolicy capacity,
            BackfillConfig cfg) {
        this.store = Objects.requireNonNull(store);
        this.loader = Objects.requireNonNull(loader);
        this.capacity = Objects.requireNonNull(capacity);
        this.cfg = Objects.requireNonNull(cfg);
    }

    public Progress progress() {
        lock.lock();
        try {
            return new Progress(populated.get(), alreadyPopulated.get(), failed.get(), inFlight);
        } finally {
            lock.unlock();
        }
    }

    /** Runs one window. Blocks until it ends and all in-flight loads are finished. */
    public Report run() {
        StopReason reason = StopReason.EXHAUSTED;
        Deque<Candidate> buffer = new ArrayDeque<>();
        Candidate cursor = null;

        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            while (true) {
                if (!acquireSlot()) {
                    reason = StopReason.CAPACITY_WITHDRAWN;
                    break;
                }
                Candidate next;
                try {
                    if (buffer.isEmpty()) {
                        List<Candidate> page = store.findUnpopulated(cursor, cfg.pageSize());
                        buffer.addAll(page);
                        if (!page.isEmpty()) cursor = page.get(page.size() - 1);
                    }
                    next = buffer.poll();
                    if (next == null) {
                        releaseSlot();
                        break; // nothing left to read: EXHAUSTED
                    }
                    executor.submit(() -> process(next));
                } catch (RuntimeException e) {
                    releaseSlot();
                    throw e; // the executor's close() still waits for in-flight loads
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            reason = StopReason.INTERRUPTED;
        }

        Report report =
                new Report(
                        reason,
                        populated.get(),
                        alreadyPopulated.get(),
                        failed.get(),
                        store.countUnpopulated());
        LOG.log(System.Logger.Level.INFO, "Backfill window ended: {0}", report);
        return report;
    }

    // ---- per-engagement work ------------------------------------------------------------------

    private void process(Candidate candidate) {
        EngagementKey key = candidate.key();
        try {
            TemplateRef ref =
                    withRetries(
                            () -> loader.loadTemplateRef(key, cfg.loadTimeout()),
                            e -> e instanceof LoadException le && le.retryable());

            boolean wrote =
                    withRetries(() -> store.populateIfEmpty(key, ref), e -> e instanceof RuntimeException);

            if (wrote) {
                populated.incrementAndGet();
            } else {
                // Someone else (e.g. the opportunistic backfill) populated it first. Not an error.
                alreadyPopulated.incrementAndGet();
            }
        } catch (Exception e) {
            if (e instanceof InterruptedException) Thread.currentThread().interrupt();
            // The row stays null, so a later window retries it. It does not stop other rows.
            failed.incrementAndGet();
            LOG.log(System.Logger.Level.WARNING, "Backfill failed for " + key, e);
        } finally {
            releaseSlot();
        }
    }

    @FunctionalInterface
    private interface Operation<T> {
        T run() throws Exception;
    }

    private <T> T withRetries(Operation<T> op, java.util.function.Predicate<Exception> retryable)
            throws Exception {
        for (int attempt = 1; ; attempt++) {
            try {
                return op.run();
            } catch (InterruptedException e) {
                throw e;
            } catch (Exception e) {
                if (!retryable.test(e) || attempt >= cfg.maxAttempts()) throw e;
                Duration delay = backoff(attempt);
                LOG.log(
                        System.Logger.Level.DEBUG,
                        "Attempt {0} failed ({1}); retrying in {2}",
                        attempt,
                        e.toString(),
                        delay);
                TimeUnit.MILLISECONDS.sleep(delay.toMillis());
            }
        }
    }

    /** Incremental (exponential) backoff, capped. {@code attempt} is the one that just failed. */
    private Duration backoff(int attempt) {
        Duration d = cfg.baseBackoff().multipliedBy(1L << Math.min(attempt - 1, 20));
        return d.compareTo(cfg.maxBackoff()) > 0 ? cfg.maxBackoff() : d;
    }

    // ---- concurrency slots --------------------------------------------------------------------

    /**
     * Waits until fewer loads are in flight than the policy currently allows, and takes a slot.
     *
     * @return {@code false} if the policy says to stop
     */
    private boolean acquireSlot() throws InterruptedException {
        lock.lockInterruptibly();
        try {
            while (true) {
                int allowed = capacity.allowedConcurrency();
                if (allowed <= 0) return false;
                if (inFlight < allowed) {
                    inFlight++;
                    return true;
                }
                slotFreed.await(cfg.capacityPollInterval().toMillis(), TimeUnit.MILLISECONDS);
            }
        } finally {
            lock.unlock();
        }
    }

    private void releaseSlot() {
        lock.lock();
        try {
            inFlight--;
            slotFreed.signal();
        } finally {
            lock.unlock();
        }
    }
}
