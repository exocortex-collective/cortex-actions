# metric-cardinality-reviewer
**Checks:** that no Prometheus label dimension grows with user data.
**Spawn when:** PR adds or changes a `Counter`, `Gauge`, `Histogram`, or any `.labels(...)` call.

**Default severity: P2.** It degrades the monitoring host rather than this
service, and it degrades it slowly, which is why it needs a reviewer rather than
an alert.

Flag specifically:
1. A full label name, `gmail_id`, message-id, sender address, or subject used as
   a metric dimension. Triage rules mint labels with interpolated variables, so
   the set of full label strings is unbounded.
2. Removal or widening of `_label_prefix()`'s two-segment truncation.

**Do NOT flag:** `queue` as a dimension. The set of destination queues is
bounded by the routing table, which is a small hand-written file.

---
