# cryo-bridge-trust-reviewer
model: sonnet
verification: mixed
**Checks:** that the cryo bridge fetches only hosts it allowlists, trusts only provable sender identity, and never sends mail text as the user's words.
**Spawn when:** diff touches `src/actions/services/cryo_bridge.py`, its tests, `SHORTENER_HOSTS`, `is_self_note`, or any `urlopen`/`Request` call; skip for docs-only diffs.

Review `cryo_bridge.py`, the one place in this repo that fetches URLs taken from
inbound mail and posts them to another service. Everything in the email is
attacker-controlled except Gmail's own label set.

**Default severity: P1.** A fetch to an attacker-chosen host from a machine in
the house, or a forged self-note, reaches cryo's backlog and classifier.

**What to flag:**

1. **A fetch to a host not in `SHORTENER_HOSTS`**, or a widening of that set
   (suffix match instead of exact host, a wildcard, a new entry that is not a
   redirect-only shortener). Also a redirect target followed without re-checking
   it, or a non-`http(s)` scheme accepted.
2. **Self-note identity read from anything but the `SENT` label plus exact
   from/to equality.** `Reply-To`, display name, or a header substring is
   forgeable; flag any loosening of `is_self_note`.
3. **Typed text sent to cryo.** The module docstring says the text around links
   is withheld on purpose. Flag any path that puts `text`, the body or the
   subject into a payload beyond what `bridge()` sends today.
4. **Unbounded work per email**: a fetch without a timeout, or one outside the
   `EXPAND_BUDGET_S` accounting, so a claim outlives its visibility timeout.
5. **Secrets in logs or exceptions**: `CRYO_TOKEN` or the Authorization header
   reaching `logger` or a repr.

Proof: URL-handling findings are mutations or inputs run through the real
function in a scratch copy; "this trusts a header" findings are evidence-queries
(grep for the field read).

Prove each by feeding the real function an input (a host like
`share.google.evil.example`, a `Reply-To` forgery) and quoting the result.

**Do not flag:** the retry classification in `RETRYABLE_HTTP` or settle/outcome
ordering (queue-contract-reviewer); a link that stays short after a failed
expansion (designed: `queued-unexpanded`); metric label choices where `outcome`
stays a closed set.
