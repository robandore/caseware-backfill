# Engagement metadata backfill worker (Part 2)

Java 21, no runtime dependencies. Tests use JUnit 5 (`mvn test`).

## Relation to the assignment

The Part 2 brief asks for "the template-publish fan-out worker", noting that a single publish
"may require processing a large number of engagements" through a downstream call that takes ~1
minute per engagement and has limited capacity.

My Part 1 design deliberately avoids that situation. Pending updates are evaluated on demand when
a user requests the engagement listing, from `engagement_template_version` and `template_version`.
Publishing a template therefore never loads or updates engagements, and a publish-triggered
fan-out would add the cost the design is built to avoid (800,000 loads at ~1 minute each).

I did not implement a publish fan-out, because it would contradict the Part 1 design. Instead I
implemented the component of my design that has the same problem the brief describes: a large
number of engagements, a ~1 minute downstream load owned by another team, and limited capacity.
That component is the **scheduled backfill worker** (Stage 3 of the backfill), which recovers
`(templateId, versionId)` for existing engagements.

The concerns the brief asks to see are the same ones handled here: concurrency, failure,
idempotency, and downstream capacity. If a publish-triggered fan-out were required, the worker
would carry over, but its work source (rows selected by publish) and its order would have to be
redefined. That would be a change to the Part 1 design, not just to this code.

## What this is

Scheduled backfill worker: a scheduled worker that recovers `(templateId, versionId)` for
existing engagements whose two columns in `engagement_template_version` are both null, by loading
each engagement through Engagement Management (~1 minute per load).

It is **not** a per-publish fan-out. In the agreed architecture, pending updates are evaluated on
demand when the listing is requested, so nothing processes engagements when a template is
published. This worker exists only because existing engagements have no metadata yet.

## How it works

- **The queue is the data.** Work = rows with both columns null. No work table. A processed row
  leaves the set; a failed row stays and is picked up by a later window.
- **Order:** `lastSeenAt DESC`, `NULLS LAST`, tie-break `firmId, engagementDocId`
  (`BackfillOrdering`). The order is total, so reading uses keyset pagination from the last row of
  the previous page.
- **One `run()` = one window.** It ends when nothing is left to read, when `CapacityPolicy` says
  stop, or on interrupt. In-flight loads are never abandoned; they finish and are written first.
  The next window resumes from whatever is still null.
- **Capacity:** `CapacityPolicy.allowedConcurrency()` is read before every load. Lower it and the
  worker slows down; return 0 and it stops. The safe value is unknown and has to be validated with
  the Engagement Management team; the worker does not guess it.
- **Idempotency:** `populateIfEmpty` is a conditional update (only if both columns are still
  null). If Stage 2 (opportunistic backfill) got there first, the result is counted as
  `alreadyPopulated` and the existing value is left untouched. Re-running is always safe.
- **Failures:** per-engagement. Retryable load errors retry with capped exponential backoff, up to
  `maxAttempts`; non-retryable ones are attempted once. A failure never stops other rows. A failed
  row is not re-read in the same run (the cursor only moves forward), so one bad engagement cannot
  starve the rest. A failed *write* is retried without repeating the 1-minute load.
- **Progress:** `progress()` while running; `Report` at the end, including the count of rows still
  unknown, which is the backfill coverage metric.

## Trade-offs

- **Backoff holds the concurrency slot.** A retrying engagement keeps its slot while it waits, so
  retries can never push load above the policy. The cost is lower throughput when many loads fail.
- **No persistent attempt counter.** Retries are per run. A permanently failing engagement is tried
  again every window; the report and logs are how it gets investigated. Adding quarantine or a
  retry budget across windows would need new state, which the design does not include.
- **Timeouts rely on the loader.** The timeout is passed to `EngagementLoader`, which must honour
  it and be interruptible. The worker does not forcibly cancel a load it cannot interrupt.
- **Single worker instance per window is assumed.** Two instances would be safe (the conditional
  update prevents wrong data) but would duplicate 1-minute loads, so the scheduler should not
  overlap windows.
- **`CapacityPolicy` is read under a lock,** so it must be cheap and non-blocking.

## Not covered

The real implementations of `EngagementMetadataStore` (SQL) and `EngagementLoader` (client for
Engagement Management), the scheduler that defines the off-peak window, and metrics export.

## Tests

`BackfillWorkerTest` covers: processing order (recency, nulls last, tie-break), concurrency never
above the policy, no overwrite of a row populated during the load, a failing engagement not
blocking others nor being re-read in the same run, retry of transient failures, single attempt for
non-retryable ones, write retry without reloading, second window touching only unknown rows, stop
on capacity withdrawal and resume, and zero capacity starting nothing.