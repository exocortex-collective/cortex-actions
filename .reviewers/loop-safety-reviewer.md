# loop-safety-reviewer
**Checks:** that the router cannot feed itself. No subscription may target `actions`; no code path may enqueue onto the queue it is currently claiming from; fan-out per event stays bounded.
**Spawn when:** PR touches `subscriptions.py` validation, `SOURCE_QUEUE`, or adds any new enqueue target.

**Default severity: P0.** A self-referential subscription is an unbounded write
loop against the shared `queue` table that every other cortex service depends on.
The blast radius is the whole pipeline, not this service.

Flag specifically:
1. Any weakening or bypass of the `queue == SOURCE_QUEUE` check in `parse()`.
2. A target queue name derived from the *payload* rather than from the config —
   that lets an email's own label choose its destination queue.
3. Fan-out proportional to anything unbounded rather than to the length of the
   routing table.
