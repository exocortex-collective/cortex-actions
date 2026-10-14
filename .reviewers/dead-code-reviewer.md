# dead-code-reviewer
**Checks:** Dead code in this dispatcher, chiefly config knobs parsed but never acted on, metrics defined but never incremented, and helpers orphaned by a deletion.
**Spawn when:** diff touches `src/actions/**`, `config/subscriptions.yaml`, or `*.md` docs that describe behaviour; skip for lockfile, Dockerfile and CI-only diffs.
**Contents:** What to flag: A documented `subscriptions.yaml` key with no parser support; A metric defined but never `.inc()`d; Helpers left behind by a deletion · Review approach · Do NOT flag

Review for **dead code**, with one bias specific to this service: it is a
dispatcher, so the characteristic rot is a knob that is read but never acted on.
A subscription field that `parse()` validates and `targets()` ignores looks
configured and does nothing — which is this repo's signature failure mode
(silence that reads as health) wearing a different hat.

**Default severity: P3**, except a parsed-but-unused config field, which is
**P2**: an operator sets it, sees no error, and believes routing changed.

**What to flag:**

1. **A `Subscription` field parsed and validated but never read by `matches()`
   or `targets()`.** Cross-check every key `parse()` accepts against every key
   the matching path actually consults.
2. **A documented `subscriptions.yaml` key with no parser support** — the
   inverse, and just as silent.
3. **A metric defined but never `.inc()`d**, or a label dimension never passed.
   A counter that cannot move is worse than no counter: it reads as zero.
4. Unused module-level functions, constants, imports; stale `__all__`; partial
   renames where the old name survives; `if False:` branches; commented-out code.
5. **Helpers left behind by a deletion** — if a code path went away, did its
   constants, regexes and error classes go with it?

**Review approach:**

1. For each key in `parse()`, grep for it in `matches()`/`targets()`/`router.py`.
2. For each `Counter` in `router.py`, grep for a corresponding `.labels(...).inc()`.
3. Grep the workspace for each flagged symbol and paste the result in the
   comment so the author can check the claim rather than trust it.

**Do NOT flag:**

- `SOURCE_QUEUE` and `VALID_EVENTS` — read by validation, not by the hot path.
- Anything referenced only from `config/subscriptions.yaml` or a test fixture.
- Exception classes raised but never caught inside this repo; a consumer may
  catch them.

---
