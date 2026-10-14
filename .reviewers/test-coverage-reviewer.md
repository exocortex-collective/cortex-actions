# test-coverage-reviewer
**Checks:** that the suite constrains the code, rather than agreeing with it. Added on PR #1, which existed because ten router tests passed against a `FakeJob` dataclass with `.id`/`.payload` — a shape `cortex_utils.queue.claim()` has never returned — while the service could not process a single real job.
**Spawn when:** any PR that changes `tests/**`, or that changes behaviour under test.

**Default severity: P2**, raised to **P1** where the untested path is one whose
failure is silent (a settle path, a signal handler, the reload, `run()`'s error
handling).

Flag specifically:
1. **A stub that does not match the real signature.** Check every
   `monkeypatch.setattr` against `inspect.signature` of the thing it replaces —
   argument names, order, and return type. A fake that accepts what the real
   function rejects is the original defect wearing a new face.
2. **A test that cannot fail**: assertions over the test's own constructions,
   both sides derived from the same constant, a membership test against a set
   the test just built. `assert column in ddl` passes with the column deleted
   when the name also appears in a comment above it.
3. **The direction that is never tested.** A guard is half-tested until
   something proves it does not reject the real thing.
4. **Paths nothing drives** — `main()`, signal handlers, the reload, the claim
   error path, `_drop_conn`. Survivors cluster there.

**Review approach:** mutate the production code adversarially and report the
table. A mutation whose anchor fails to apply reports as PASSING and means
nothing, so assert the anchor applied before concluding anything; clear
`__pycache__` and run with `PYTHONDONTWRITEBYTECODE=1`; and work in an isolated
copy, because a concurrent reviewer may be mutating the same tree.

**Do NOT flag:** a missing test with no concrete defect behind it. "No test for
X" without the failure X would ship is P3 at most.

---
