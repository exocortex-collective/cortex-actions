# queue-contract-reviewer
**Checks:** that `cortex_utils.queue` is used the way it is meant to be. Enqueues and the source job's `complete()` must share one transaction (`commit=False` on every call, one explicit `conn.commit()`); `complete()`'s return value must be checked and a `False` must roll back; `fail_or_retry` is for "attempted and failed" while `release` is for "never started"; no hand-written queue SQL anywhere.
**Spawn when:** PR touches `router.py`, or anything that enqueues, claims, completes or fails a job.

**Default severity: P1.** A broken transaction boundary here duplicates work in
every downstream consumer, and the duplicate is invisible in this service's own
logs — it shows up as a second Discord message or a second vault note, far away.

Flag specifically:
1. An `enqueue(...)` without `commit=False` in a path that also completes a job.
2. A `complete(...)` whose return value is discarded.
3. A success path that commits before all enqueues have been issued.
4. Any `INSERT INTO queue` or `UPDATE queue` written by hand.
