import inspect
import json
import threading
import urllib.error
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import cortex_utils.queue as queue_lib
import pytest
from prometheus_client import REGISTRY

from actions.services import cryo_bridge
from actions.services.cryo_bridge import (
    RETRYABLE_HTTP,
    Cryo,
    Email,
    bridge,
    is_self_note,
    load_email,
    split_note,
)
from actions.subscriptions import load, targets

ME = "me@example.com"


def note(**over):
    base = dict(
        from_addr=ME,
        to_addrs=(ME,),
        cc_addrs=(),
        subject="Article title",
        body="https://example.com/a",
        label_ids=("SENT", "INBOX"),
    )
    base.update(over)
    return Email(**base)


class FakeCryo:
    def __init__(self):
        self.calls = []

    def enqueue(self, urls, title):
        self.calls.append((urls, title))
        return {"added": len(urls)}


class RaisingCryo:
    def __init__(self, exc):
        self.exc = exc

    def enqueue(self, urls, title):
        raise self.exc


def http(code):
    return urllib.error.HTTPError("u", code, "x", None, None)


def outcome_count(outcome):
    return (
        REGISTRY.get_sample_value(
            "cortex_cryo_bridge_outcomes_total", {"outcome": outcome}
        )
        or 0.0
    )


# --- what counts as a self-note, and what is extracted from it ---


def test_a_forged_from_without_sent_is_not_a_self_note():
    assert not is_self_note(note(label_ids=("INBOX",)), ME)


def test_other_recipients_make_it_not_a_self_note():
    assert not is_self_note(note(cc_addrs=("x@example.com",)), ME)
    assert not is_self_note(note(to_addrs=(ME, "x@example.com")), ME)
    assert not is_self_note(note(from_addr="x@example.com"), ME)


def test_address_match_ignores_case():
    assert is_self_note(
        note(from_addr="Me@Example.com", to_addrs=("ME@example.com",)), ME
    )


def test_split_note_strips_sentence_punctuation_and_dedupes():
    urls, text = split_note(
        "see https://a.example/x. and https://a.example/x, then https://b.example/y?q=1"
    )
    assert urls == ["https://a.example/x", "https://b.example/y?q=1"]
    assert text == "see and then"


def test_split_note_keeps_balanced_parens_and_drops_a_closing_one():
    urls, _ = split_note(
        "see https://en.wikipedia.org/wiki/Foo_(bar) and (https://a.com/x)."
    )
    assert urls == ["https://en.wikipedia.org/wiki/Foo_(bar)", "https://a.com/x"]


def test_split_note_drops_a_bare_scheme():
    assert split_note("https://a.com and https://!")[0] == ["https://a.com"]


def test_split_note_finds_links_in_html():
    urls, _ = split_note('<p><a href="https://a.com/x">x</a></p>')
    assert urls == ["https://a.com/x"]


def test_bridge_queues_links_with_subject_as_title():
    cryo = FakeCryo()
    assert bridge(note(), ME, cryo) == "queued"
    assert cryo.calls == [(["https://example.com/a"], "Article title")]


def test_an_empty_subject_sends_no_title():
    cryo = FakeCryo()
    bridge(note(subject=""), ME, cryo)
    assert cryo.calls == [(["https://example.com/a"], None)]


def test_bridge_holds_typed_text_back():
    cryo = FakeCryo()
    assert (
        bridge(note(body="read this https://example.com/a"), ME, cryo)
        == "queued-text-held"
    )
    assert cryo.calls == [(["https://example.com/a"], "Article title")]


def test_bridge_never_calls_cryo_for_a_non_self_note():
    cryo = FakeCryo()
    assert bridge(note(label_ids=("INBOX",)), ME, cryo) == "not-self"
    assert bridge(note(body="no links here"), ME, cryo) == "no-links"
    assert cryo.calls == []


def test_shipped_config_routes_self_notes_to_cryo_but_not_archive():
    subs = load(Path(__file__).parent.parent / "config" / "subscriptions.yaml")
    assert targets(subs, "Cortex/Self/Note", "added") == ["cryo-backlog"]
    assert targets(subs, "Cortex/Self/Todo", "added") == ["cryo-backlog"]
    assert targets(subs, "Cortex/Self/Archive", "added") == []


# --- the two boundaries: cryod over HTTP, and the email row ---


class CryodStub(BaseHTTPRequestHandler):
    seen: list = []

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        CryodStub.seen.append((self.path, self.headers["Authorization"], body))
        out = json.dumps({"added": 1, "skipped": 0}).encode()
        self.send_response(200)
        self.send_header("Content-Length", str(len(out)))
        self.end_headers()
        self.wfile.write(out)

    def log_message(self, *args):
        pass


def test_enqueue_posts_items_with_the_bearer_token():
    server = HTTPServer(("127.0.0.1", 0), CryodStub)
    threading.Thread(target=server.handle_request, daemon=True).start()
    CryodStub.seen = []
    cryo = Cryo(f"http://127.0.0.1:{server.server_port}/", "tok", timeout=5)
    assert cryo.enqueue(["https://a.com", "https://b.com"], "T") == {
        "added": 1,
        "skipped": 0,
    }
    server.server_close()
    assert CryodStub.seen == [
        (
            "/api/backlog",
            "Bearer tok",
            {
                "items": [
                    {"url": "https://a.com", "title": "T"},
                    {"url": "https://b.com", "title": "T"},
                ]
            },
        )
    ]


class RowConn:
    def __init__(self, row):
        self.row, self.executed = row, []

    def cursor(self):
        conn = self

        class Cur:
            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

            def execute(self, sql, params):
                conn.executed.append((sql, params))

            def fetchone(self):
                return conn.row

        return Cur()


def test_load_email_maps_nulls_to_empty_values():
    conn = RowConn((None, None, None, None, None, None))
    assert load_email(conn, "g1") == Email("", (), (), "", "", ())
    assert conn.executed[0][1] == ("g1",)


def test_load_email_maps_a_row_and_falls_back_to_html():
    conn = RowConn((ME, [ME], [], "S", "<a href=x>", ["SENT"]))
    assert load_email(conn, "g1") == Email(ME, (ME,), (), "S", "<a href=x>", ("SENT",))
    assert "COALESCE(NULLIF(p.body_text, ''), p.body_html)" in conn.executed[0][0]


def test_load_email_returns_none_for_no_row():
    assert load_email(RowConn(None), "g1") is None


# --- process(): every failure path settles the job; none loops or crashes ---


class FakeConn:
    def __init__(self):
        self.rollbacks = 0

    def rollback(self):
        self.rollbacks += 1


@pytest.fixture
def queue(monkeypatch):
    state = {"log": [], "complete": True, "retry_status": "pending", "claimed": []}

    def claim(conn, queue_name, worker, limit=1, visibility_timeout_min=30):
        state["claimed"].append((queue_name, limit, visibility_timeout_min))
        return [{"id": 7, "payload": {"gmail_id": "g1"}}]

    def complete(conn, job_id, worker, commit=True):
        state["log"].append(("complete", job_id))
        return state["complete"]

    def fail_or_retry(
        conn,
        job_id,
        error,
        worker,
        base_seconds=30,
        cap_seconds=900,
        jitter_ratio=0.2,
    ):
        state["log"].append(("fail_or_retry", job_id))
        return state["retry_status"]

    for real, stub in (
        (queue_lib.claim, claim),
        (queue_lib.complete, complete),
        (queue_lib.fail_or_retry, fail_or_retry),
    ):
        assert inspect.signature(real).parameters.keys() == (
            inspect.signature(stub).parameters.keys()
        )
    monkeypatch.setattr(cryo_bridge, "claim", claim)
    monkeypatch.setattr(cryo_bridge, "complete", complete)
    monkeypatch.setattr(cryo_bridge, "fail_or_retry", fail_or_retry)
    return state


def run_job(monkeypatch, email, cryo, conn=None):
    monkeypatch.setattr(cryo_bridge, "load_email", lambda conn, gid: email)
    b = cryo_bridge.Bridge("dsn", ME, cryo, poll_interval=1)
    return b.process(conn or FakeConn(), {"id": 7, "payload": {"gmail_id": "g1"}})


def test_success_completes_and_counts(monkeypatch, queue):
    before = outcome_count("queued")
    assert run_job(monkeypatch, note(), FakeCryo()) == "queued"
    assert queue["log"] == [("complete", 7)]
    assert outcome_count("queued") == before + 1


def test_a_lost_claim_is_reported_not_counted(monkeypatch, queue):
    queue["complete"] = False
    before = outcome_count("queued")
    assert run_job(monkeypatch, note(), FakeCryo()) == "lost"
    assert outcome_count("queued") == before


def test_permanent_4xx_rolls_back_and_completes_as_rejected(monkeypatch, queue):
    conn = FakeConn()
    assert run_job(monkeypatch, note(), RaisingCryo(http(422)), conn) == "rejected"
    assert queue["log"] == [("complete", 7)]
    assert conn.rollbacks == 1


@pytest.mark.parametrize("code", sorted(RETRYABLE_HTTP | {500, 503}))
def test_retryable_http_rolls_back_and_charges_an_attempt(monkeypatch, queue, code):
    conn = FakeConn()
    assert run_job(monkeypatch, note(), RaisingCryo(http(code)), conn) == "retry"
    assert queue["log"] == [("fail_or_retry", 7)]
    assert conn.rollbacks == 1


def test_retryable_set_is_exactly_auth_timeout_and_rate_limit():
    assert {401, 403, 408, 429} == RETRYABLE_HTTP


def test_unexpected_exception_fails_the_job_instead_of_escaping(monkeypatch, queue):
    conn = FakeConn()
    assert run_job(monkeypatch, note(), RaisingCryo(TypeError("boom")), conn) == "retry"
    assert queue["log"] == [("fail_or_retry", 7)]
    assert conn.rollbacks == 1


def test_exhausted_retries_count_as_failed(monkeypatch, queue):
    queue["retry_status"] = "failed"
    before = outcome_count("failed")
    run_job(monkeypatch, note(), RaisingCryo(TypeError("boom")))
    assert outcome_count("failed") == before + 1


def test_email_not_parsed_is_retried(monkeypatch, queue):
    assert run_job(monkeypatch, None, FakeCryo()) == "retry"
    assert queue["log"] == [("fail_or_retry", 7)]


def test_job_without_gmail_id_is_rejected(monkeypatch, queue):
    b = cryo_bridge.Bridge("dsn", ME, FakeCryo(), poll_interval=1)
    assert b.process(FakeConn(), {"id": 7, "payload": "garbage"}) == "rejected"
    assert queue["log"] == [("complete", 7)]


def test_run_once_claims_a_batch_that_fits_the_visibility_timeout(monkeypatch, queue):
    monkeypatch.setattr(cryo_bridge, "load_email", lambda conn, gid: note())
    b = cryo_bridge.Bridge("dsn", ME, FakeCryo(), poll_interval=1)
    assert b.run_once(FakeConn()) == 1
    assert queue["claimed"] == [("cryo-backlog", 5, 5)]
    assert queue["log"] == [("complete", 7)]
