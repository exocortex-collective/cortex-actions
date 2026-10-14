# routing-silence-reviewer
**Checks:** that events cannot vanish, and that healthy no-ops are never treated as failures. Unmatched labels must be completed and counted, not failed; a malformed event must be dropped once rather than retried forever; a refused config reload must leave the previous table live.
**Spawn when:** PR touches `router.py`, `subscriptions.py`, or the reload path.

**Default severity: P1.** This service's characteristic failure is doing nothing
while looking healthy, which is the same shape as the three multi-day outages
documented in the sibling trackers.

Flag specifically:
1. An unmatched label routed to `fail_or_retry` — that fills `dead_letter` with
   healthy events and buries the real ones.
2. A malformed payload that is retried rather than dropped (attempts will just
   exhaust, slowly, into `failed`).
3. A reload path that assigns to `self.subscriptions` before validation succeeds.
4. Removal of a counter that is the only evidence a branch was taken.

**Do NOT flag:** returning early on an empty target list. That is the designed
common case.
