# Agents

The roster is deliberately short. Project-specific reviewers guard the ways this
service can quietly stop being trustworthy. The rest of the generic Python pack
(`error-handling`, `resource-leak`, `clarity`) gets added on the first PR that
brushes against it, rather than carried from day one — `test-coverage` was added
that way on PR #1.

This service sits on the critical path for every label event in the system, so
the bar for "this is fine, it only affects X" is higher here than in a leaf
consumer.

# Guidelines

## File scope

Each reviewer's `**Spawn when:**` header is authoritative for its scope.

Documentation is in scope for every reviewer, not a category of its own: a doc
describing behaviour someone else changed stays wrong indefinitely and reads as
authoritative. On PR #1, roughly half of dead-code-reviewer's findings were in
prose rather than code, and the sharpest of them (`service="actions"` against an
emitted `actions-router`) was a query that would have returned no series at all
— indistinguishable, on a dashboard, from a healthy zero.

The same review found the failure mode that makes this worth recording: a wrong
identifier in a comment gets copied. `queue_valid_status`, which exists nowhere,
travelled from a code comment into a test fixture and then into a beads ticket
before anyone read it off the database.

## Tooling assumed in CI

`black --check`, `ruff check`, `mypy src/actions`, `pytest`. A finding that one
of these would have caught is a CI gap, not a review finding — say so and move on.

## Severity convention

- **P0** — blast radius beyond this service. Blocks merge.
- **P1** — this service silently does the wrong thing. Blocks merge.
- **P2** — degrades over time or under load. Fix now or file a bead.
- **P3** — clarity and maintainability. Author's call.

## Output format

One finding per issue:

```
**[P1] queue-contract-reviewer** — `src/actions/services/router.py:174`
enqueue() defaults to commit=True here, so the job's enqueues commit before
complete() runs. A crash in between re-delivers the event and double-enqueues.
Suggested: pass commit=False and let the existing conn.commit() settle it.
```

State the failure, not the preference. If you cannot describe a concrete
sequence of events that produces a wrong outcome, it is a P3 at most.

## Deferring findings

Reasonable suggestions that are out of scope for the PR get a bead rather than a
merge block, per the parent CLAUDE.md:

```sh
bd create --title="..." --type=task --priority=3 \
  --description="Deferred from PR #N review. <context, and when to revisit>"
~/.local/bin/beads-commit "Track deferred items from PR #N review"
```
