"""Self-notes to cryo: an email Devon sent himself with links in it becomes
cryo backlog items, after triage has labelled it a self-note.

Consumes the `cryo-backlog` queue, fed by the router (subscriptions.yaml routes
the self-note label's `added` event here). No model runs in this service:
Python decides what is a self-note, extracts the links, and calls cryod.

"From Devon" is proven, never read from headers: the message must carry
Gmail's SENT label, which only mail his own account sent has.

Text typed around the links is not sent yet. As a cryo backlog note it would
bind cryo's classifier as Devon's own words, and share sheets paste article
snippets into the body, so it waits for cryo to accept a note with the
enqueue and for a rule that separates typed text from pasted text.
"""

from __future__ import annotations

import json
import os
import re
import signal
import sys
import threading
import urllib.error
import urllib.request
from dataclasses import dataclass
from types import FrameType
from typing import Any

import psycopg2
from cortex_utils.logging import configure_logging, get_logger
from cortex_utils.queue import claim, complete, fail_or_retry, worker_identity

SERVICE = "cryo-bridge"
QUEUE = "cryo-backlog"
URL_RE = re.compile(r"https?://[^\s<>\"')\]]+")
# 401/403: a fixed token or permission makes these succeed later; 408/429: wait.
RETRYABLE_HTTP = frozenset({401, 403, 408, 429})
# Sentence punctuation that ends a pasted URL more often than it belongs to it.
URL_TRAILING = ".,;:!?"

logger = get_logger(__name__)


@dataclass(frozen=True)
class Email:
    from_addr: str
    to_addrs: list[str]
    cc_addrs: list[str]
    subject: str
    body: str
    label_ids: list[str]


def is_self_note(email: Email, self_addr: str) -> bool:
    """Sent by this account, to this account and nobody else."""
    me = self_addr.lower()
    return (
        "SENT" in email.label_ids
        and email.from_addr.lower() == me
        and [a.lower() for a in email.to_addrs] == [me]
        and not email.cc_addrs
    )


def split_note(body: str) -> tuple[list[str], str]:
    """The links in a self-note, in order and deduped, and the text around them."""
    urls: list[str] = []
    for raw in URL_RE.findall(body):
        url = raw.rstrip(URL_TRAILING)
        if url not in urls:
            urls.append(url)
    text = " ".join(URL_RE.sub(" ", body).split())
    return urls, text


class Cryo:
    """The cryod call the bridge makes."""

    def __init__(self, base_url: str, token: str, timeout: int = 30) -> None:
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.timeout = timeout

    def _call(self, method: str, path: str, body: Any = None) -> Any:
        req = urllib.request.Request(
            self.base_url + path,
            method=method,
            data=None if body is None else json.dumps(body).encode(),
            headers={
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json",
            },
        )
        with urllib.request.urlopen(req, timeout=self.timeout) as resp:
            return json.loads(resp.read() or b"null")

    def enqueue(self, urls: list[str], title: str | None) -> Any:
        items = [{"url": u, "title": title} for u in urls]
        return self._call("POST", "/api/backlog", {"items": items})


def load_email(conn: Any, gmail_id: str) -> Email | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT p.from_addr, p.to_addrs, p.cc_addrs, p.subject, p.body_text,
                   r.label_ids
            FROM emails_parsed p JOIN emails_raw r USING (gmail_id)
            WHERE p.gmail_id = %s
            """,
            (gmail_id,),
        )
        row = cur.fetchone()
    if row is None:
        return None
    f, to, cc, subject, body, labels = row
    return Email(f or "", to or [], cc or [], subject or "", body or "", labels or [])


def bridge(email: Email, self_addr: str, cryo: Cryo) -> str:
    """Hand one email to cryo. Returns the outcome; raises if cryod fails.
    Safe to repeat: cryod skips a url already in that kind's backlog."""
    if not is_self_note(email, self_addr):
        return "not-self"
    urls, text = split_note(email.body)
    if not urls:
        return "no-links"
    cryo.enqueue(urls, email.subject or None)
    return "queued-text-held" if text else "queued"


class Bridge:
    def __init__(
        self, dsn: str, self_addr: str, cryo: Cryo, poll_interval: int
    ) -> None:
        self.dsn = dsn
        self.self_addr = self_addr
        self.cryo = cryo
        self.poll_interval = poll_interval
        self._stop = threading.Event()
        self._worker = worker_identity(SERVICE)

    def stop(self) -> None:
        self._stop.set()

    def process(self, conn: Any, job: dict[str, Any]) -> str:
        raw = job.get("payload")
        payload: dict[str, Any] = raw if isinstance(raw, dict) else {}
        gmail_id = payload.get("gmail_id")

        def settle(outcome: str) -> str:
            if not complete(conn, job["id"], self._worker):
                logger.warning("claim lost before complete", gmail_id=gmail_id)
                return "lost"
            log = logger.error if outcome == "rejected" else logger.info
            log("self-note handled", gmail_id=gmail_id, outcome=outcome)
            return outcome

        def retry(exc: BaseException) -> str:
            conn.rollback()
            status = fail_or_retry(conn, job["id"], repr(exc), self._worker)
            log = logger.warning if status == "pending" else logger.error
            log("cryo bridge failed", gmail_id=gmail_id, error=repr(exc), next=status)
            return "retry"

        if not gmail_id:
            return settle("rejected")
        try:
            email = load_email(conn, gmail_id)
            if email is None:  # the parser may not have written it yet
                raise LookupError(f"no parsed email {gmail_id}")
            return settle(bridge(email, self.self_addr, self.cryo))
        except urllib.error.HTTPError as exc:
            if exc.code in RETRYABLE_HTTP or exc.code >= 500:
                return retry(exc)
            conn.rollback()
            logger.error("cryod refused the links", gmail_id=gmail_id, status=exc.code)
            return settle("rejected")
        except Exception as exc:  # noqa: BLE001 - a poison job must fail, not loop
            return retry(exc)

    def run_once(self, conn: Any) -> int:
        # 5 jobs x 30s cryod timeout stays inside the 5 minute claim.
        jobs = claim(
            conn, QUEUE, worker=self._worker, limit=5, visibility_timeout_min=5
        )
        for job in jobs:
            self.process(conn, job)
        return len(jobs)

    def run(self) -> None:
        logger.info("cryo bridge starting", queue=QUEUE, worker=self._worker)
        conn = None
        while not self._stop.is_set():
            try:
                conn = conn or psycopg2.connect(self.dsn)
                if self.run_once(conn):
                    continue
            except psycopg2.Error as exc:
                logger.error("database error", error=str(exc))
                if conn is not None:
                    conn.close()
                conn = None
            self._stop.wait(self.poll_interval)
        logger.info("cryo bridge stopped")


def main() -> None:
    configure_logging(service_name=SERVICE, level=os.environ.get("LOG_LEVEL", "INFO"))
    required = (
        "POSTGRES_HOST",
        "POSTGRES_DB",
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
        "CRYO_URL",
        "CRYO_TOKEN",
        "SELF_ADDRESS",
    )
    missing = [v for v in required if not os.environ.get(v)]
    if missing:
        sys.exit(f"missing required environment: {', '.join(missing)}")
    dsn = (
        f"host={os.environ['POSTGRES_HOST']} "
        f"port={os.environ.get('POSTGRES_PORT', '5432')} "
        f"dbname={os.environ['POSTGRES_DB']} user={os.environ['POSTGRES_USER']} "
        f"password={os.environ['POSTGRES_PASSWORD']}"
    )
    bridge_ = Bridge(
        dsn,
        os.environ["SELF_ADDRESS"],
        Cryo(os.environ["CRYO_URL"], os.environ["CRYO_TOKEN"]),
        poll_interval=int(os.environ.get("POLL_INTERVAL", "5")),
    )

    def on_term(signum: int, frame: FrameType | None) -> None:
        bridge_.stop()

    signal.signal(signal.SIGTERM, on_term)
    signal.signal(signal.SIGINT, on_term)
    if "--once" in sys.argv:
        bridge_.run_once(psycopg2.connect(dsn))
        return
    bridge_.run()


if __name__ == "__main__":
    main()
