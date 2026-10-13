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
    expand,
    html_note,
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


def test_split_note_drops_every_unbalanced_closing_paren():
    assert split_note("(see (https://a.com/x)).")[0] == ["https://a.com/x"]


def test_html_note_takes_decoded_anchor_targets_only():
    urls, text = html_note(
        '<p>read <a href="https://x.com/a?b=1&amp;c=2">this</a></p>'
        '<img src="https://x.com/t.gif"><link href="https://x.com/s.css">'
    )
    assert urls == ["https://x.com/a?b=1&c=2"]
    assert text == "read this"


def test_html_note_keeps_every_anchor_in_order_once():
    urls, _ = html_note(
        '<a href="https://a.com/1">a</a><a href="https://b.com/2">b</a>'
        '<a href="https://a.com/1">again</a>'
    )
    assert urls == ["https://a.com/1", "https://b.com/2"]


def test_html_note_ignores_urls_outside_href():
    urls, _ = html_note('<a title="https://t.com/x" href="">x</a>')
    assert urls == []


def test_an_html_only_body_is_read_from_html():
    cryo = FakeCryo()
    email = note(body="", html='<a href="https://a.com/x">x</a>')
    assert bridge(email, ME, cryo) == "queued-text-held"
    assert cryo.calls == [(["https://a.com/x"], "Article title")]


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


def test_shipped_config_routes_every_self_note_label_to_cryo():
    subs = load(Path(__file__).parent.parent / "config" / "subscriptions.yaml")
    for label in ("Cortex/Self/Note", "Cortex/Self/Todo", "Cortex/Self/Archive"):
        assert targets(subs, label, "added") == ["cryo-backlog"]
        assert targets(subs, label, "removed") == []


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


class EmptyCryod(CryodStub):
    def do_POST(self):
        self.rfile.read(int(self.headers["Content-Length"]))
        self.send_response(200)
        self.send_header("Content-Length", "0")
        self.end_headers()


def test_an_empty_200_from_cryod_is_an_error_not_a_silent_null():
    server = HTTPServer(("127.0.0.1", 0), EmptyCryod)
    threading.Thread(target=server.handle_request, daemon=True).start()
    cryo = Cryo(f"http://127.0.0.1:{server.server_port}", "tok", timeout=5)
    with pytest.raises(ValueError):
        cryo.enqueue(["https://a.com"], None)
    server.server_close()


class Redirector(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/short":
            self.send_response(302)
            self.send_header("Location", "/real/page")
        else:
            self.send_response(200)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def log_message(self, *args):
        pass


class RefusingDestination(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/short":
            self.send_response(302)
            port = self.server.server_address[1]
            self.send_header("Location", f"http://127.0.0.1:{port}/real/page")
        else:
            self.send_response(403)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def log_message(self, *args):
        pass


def test_a_destination_that_refuses_still_gives_its_url(monkeypatch):
    server = HTTPServer(("127.0.0.1", 0), RefusingDestination)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    monkeypatch.setattr(cryo_bridge, "SHORTENER_HOSTS", frozenset({"localhost"}))
    port = server.server_port
    assert expand(f"http://localhost:{port}/short", timeout=5) == (
        f"http://127.0.0.1:{port}/real/page"
    )
    server.shutdown()
    server.server_close()


def test_expand_keeps_the_short_link_when_it_cannot_be_followed(monkeypatch):
    monkeypatch.setattr(cryo_bridge, "SHORTENER_HOSTS", frozenset({"127.0.0.1"}))
    dead = HTTPServer(("127.0.0.1", 0), Redirector)
    port = dead.server_port
    dead.server_close()
    assert expand(f"http://127.0.0.1:{port}/short", timeout=2) == (
        f"http://127.0.0.1:{port}/short"
    )


def test_expand_never_touches_other_hosts(monkeypatch):
    def boom(*a, **k):
        raise AssertionError("fetched a non-shortener url")

    monkeypatch.setattr(cryo_bridge.urllib.request, "urlopen", boom)
    assert expand("https://example.com/a") == "https://example.com/a"


def test_bridge_sends_expanded_links_once():
    cryo = FakeCryo()
    body = "https://share.google/a https://share.google/b https://x.com/c"
    real = {"https://share.google/a": "https://x.com/c"}
    bridge(note(body=body), ME, cryo, expand_url=lambda u: real.get(u, u))
    assert cryo.calls == [
        (["https://x.com/c", "https://share.google/b"], "Article title")
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
    conn = RowConn((None, None, None, None, None, None, None))
    assert load_email(conn, "g1") == Email("", (), (), "", "", (), "")
    assert conn.executed[0][1] == ("g1",)


def test_load_email_maps_a_row_with_both_bodies():
    conn = RowConn((ME, [ME], [], "S", "t", ["SENT"], "<p>h</p>"))
    assert load_email(conn, "g1") == Email(
        ME, (ME,), (), "S", "t", ("SENT",), "<p>h</p>"
    )


def test_load_email_selects_the_columns_it_unpacks_in_order():
    conn = RowConn(None)
    load_email(conn, "g1")
    sql = " ".join(conn.executed[0][0].split())
    selected = sql.split("SELECT ", 1)[1].split(" FROM ", 1)[0]
    assert [c.strip() for c in selected.split(",")] == [
        "p.from_addr",
        "p.to_addrs",
        "p.cc_addrs",
        "p.subject",
        "p.body_text",
        "r.label_ids",
        "p.body_html",
    ]


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


def test_a_stale_retry_is_reported_lost_not_failed(monkeypatch, queue):
    queue["retry_status"] = "stale"
    before = outcome_count("failed")
    assert run_job(monkeypatch, note(), RaisingCryo(TypeError("boom"))) == "lost"
    assert outcome_count("failed") == before


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
