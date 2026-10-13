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
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass
from html.parser import HTMLParser
from types import FrameType
from typing import Any

import psycopg2
from cortex_utils.logging import configure_logging, get_logger
from cortex_utils.metrics import start_metrics_server
from cortex_utils.queue import claim, complete, fail_or_retry, worker_identity
from prometheus_client import Counter

SERVICE = "cryo-bridge"
QUEUE = "cryo-backlog"
URL_RE = re.compile(r"https?://[^\s<>\"'\]]+")
# 401/403: a fixed token or permission makes these succeed later; 408/429: wait.
RETRYABLE_HTTP = frozenset({401, 403, 408, 429})
# Android's share sheet wraps links in Google short links, which hide the real
# page from cryo (and turn a YouTube share into a "page" item).
SHORTENER_HOSTS = frozenset({"share.google", "search.app", "goo.gl", "g.co"})
# Total time one email may spend expanding links; later links stay short.
EXPAND_BUDGET_S = 60
# Sentence punctuation that ends a pasted URL more often than it belongs to it.
URL_TRAILING = ".,;:!?"

logger = get_logger(__name__)

# One count per settled job. A bridge that stops recognising self-notes still
# completes every job, so "not-self" climbing while "queued" stays flat is the
# only sign of it. `outcome` is a closed set.
OUTCOMES = Counter(
    "cortex_cryo_bridge_outcomes_total", "Self-note jobs by outcome", ["outcome"]
)


@dataclass(frozen=True)
class Email:
    from_addr: str
    to_addrs: tuple[str, ...]
    cc_addrs: tuple[str, ...]
    subject: str
    body: str
    label_ids: tuple[str, ...]
    html: str = ""


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
        # A ")" closing the sentence, not a Wikipedia-style "(bar)" path.
        while url.endswith(")") and url.count(")") > url.count("("):
            url = url[:-1].rstrip(URL_TRAILING)
        if url.split("://", 1)[1] and url not in urls:
            urls.append(url)
    text = " ".join(URL_RE.sub(" ", body).split())
    return urls, text


class _Links(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.hrefs: list[str] = []
        self.text: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "a":
            self.hrefs += [v for k, v in attrs if k == "href" and v]

    def handle_data(self, data: str) -> None:
        self.text.append(data)


def html_note(body: str) -> tuple[list[str], str]:
    """split_note for an HTML-only body: links are the <a href> targets (with
    entities decoded), never <img src> or other markup, and the text is the
    visible text."""
    parser = _Links()
    parser.feed(body)
    urls, _ = split_note(" ".join(parser.hrefs))
    _, text = split_note(" ".join(parser.text))
    return urls, text


def expand(url: str, timeout: int = 10) -> str:
    """The page a Google short link points at; any other url unchanged.

    GET, not HEAD: share.google only redirects onward on GET. The body is
    never read. On any failure the short link is kept, and logged.
    """
    if _host(url) not in SHORTENER_HOSTS:
        return url

    def landed(final: str) -> str:
        if _host(final) in SHORTENER_HOSTS or not final.startswith(
            ("http://", "https://")
        ):
            logger.warning("short link did not expand", url=url, landed=final)
            return url
        return final

    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            return landed(str(resp.url))
    except urllib.error.HTTPError as exc:
        # The redirects completed and the destination refused us (403 to
        # urllib's user agent is common); its url is still the answer.
        logger.info("short link destination refused", url=url, status=exc.code)
        return landed(str(exc.url))
    except Exception as exc:  # noqa: BLE001 - one link must never fail the note
        logger.warning("could not expand short link", url=url, error=repr(exc))
        return url


def _host(url: str) -> str | None:
    try:
        return urllib.parse.urlsplit(url).hostname
    except ValueError:  # e.g. "http://[abc/x", which URL_RE lets through
        return None


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
            return json.loads(resp.read())

    def enqueue(self, urls: list[str], title: str | None) -> Any:
        items = [{"url": u, "title": title} for u in urls]
        return self._call("POST", "/api/backlog", {"items": items})


def load_email(conn: Any, gmail_id: str) -> Email | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT p.from_addr, p.to_addrs, p.cc_addrs, p.subject, p.body_text,
                   r.label_ids, p.body_html
            FROM emails_parsed p JOIN emails_raw r USING (gmail_id)
            WHERE p.gmail_id = %s
            """,
            (gmail_id,),
        )
        row = cur.fetchone()
    if row is None:
        return None
    f, to, cc, subject, body, labels, html = row
    return Email(
        f or "",
        tuple(to or ()),
        tuple(cc or ()),
        subject or "",
        body or "",
        tuple(labels or ()),
        html or "",
    )


def bridge(
    email: Email,
    self_addr: str,
    cryo: Cryo,
    expand_url: Callable[[str], str] = expand,
) -> str:
    """Hand one email to cryo. Returns the outcome; raises if cryod fails.
    Safe to repeat: cryod skips a url already in that kind's backlog."""
    if not is_self_note(email, self_addr):
        return "not-self"
    urls, text = split_note(email.body) if email.body else html_note(email.html)
    if not urls:
        return "no-links"
    deadline = time.monotonic() + EXPAND_BUDGET_S
    urls = list(
        dict.fromkeys(expand_url(u) if time.monotonic() < deadline else u for u in urls)
    )
    short = sum(_host(u) in SHORTENER_HOSTS for u in urls)
    result = cryo.enqueue(urls, email.subject or None)
    logger.info("cryod enqueue", links=len(urls), unexpanded=short, result=result)
    if short:
        return "queued-unexpanded"
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
            OUTCOMES.labels(outcome=outcome).inc()
            return outcome

        def retry(exc: BaseException) -> str:
            conn.rollback()
            status = fail_or_retry(conn, job["id"], repr(exc), self._worker)
            if status == "stale":
                logger.warning("claim lost before retry", gmail_id=gmail_id)
                return "lost"
            log = logger.warning if status == "pending" else logger.error
            log("cryo bridge failed", gmail_id=gmail_id, error=repr(exc), next=status)
            OUTCOMES.labels(outcome="retry" if status == "pending" else "failed").inc()
            return "retry"

        if not gmail_id:
            return settle("rejected")
        try:
            email = load_email(conn, gmail_id)
            if email is None:
                raise LookupError(f"no parsed email {gmail_id}")
            return settle(bridge(email, self.self_addr, self.cryo))
        except urllib.error.HTTPError as exc:
            if exc.code in RETRYABLE_HTTP or exc.code >= 500:
                return retry(exc)
            conn.rollback()
            logger.error(
                "cryod refused the links",
                gmail_id=gmail_id,
                status=exc.code,
                body=exc.read()[:300].decode(errors="replace"),
            )
            return settle("rejected")
        except Exception as exc:  # noqa: BLE001 - a poison job must fail, not loop
            return retry(exc)

    def run_once(self, conn: Any) -> int:
        # One email per claim: its expansion budget plus the 30s cryod call
        # stays well inside the 5 minute claim.
        jobs = claim(
            conn, QUEUE, worker=self._worker, limit=1, visibility_timeout_min=5
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
    start_metrics_server(port=int(os.environ.get("METRICS_PORT", "8000")))
    bridge_.run()


if __name__ == "__main__":
    main()
