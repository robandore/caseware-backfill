package com.caseware.backfill;

/**
 * How many engagement loads the backfill may run at the same time right now.
 *
 * <p>This is how the worker respects the capacity of Engagement Management: it is the single
 * place where the off-peak window and "reduce or stop when normal operations need capacity" are
 * decided. The safe value is not known and must be validated against Engagement Management.
 *
 * <p>Implementations must be cheap and non-blocking; the worker calls this frequently.
 */
@FunctionalInterface
public interface CapacityPolicy {

    /** {@code 0} (or less) means "stop now". Raising or lowering it takes effect on the next load. */
    int allowedConcurrency();
}
