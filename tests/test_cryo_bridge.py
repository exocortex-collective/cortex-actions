from pathlib import Path

from actions.services.cryo_bridge import Email, bridge, is_self_note, split_note
from actions.subscriptions import load, targets

ME = "me@example.com"


def note(**over):
    base = dict(
        from_addr=ME,
        to_addrs=[ME],
        cc_addrs=[],
        subject="Article title",
        body="https://example.com/a",
        label_ids=["SENT", "INBOX"],
    )
    base.update(over)
    return Email(**base)


class FakeCryo:
    def __init__(self):
        self.calls = []

    def enqueue(self, urls, title):
        self.calls.append((urls, title))


def test_a_forged_from_without_sent_is_not_a_self_note():
    assert not is_self_note(note(label_ids=["INBOX"]), ME)


def test_other_recipients_make_it_not_a_self_note():
    assert not is_self_note(note(cc_addrs=["x@example.com"]), ME)
    assert not is_self_note(note(to_addrs=[ME, "x@example.com"]), ME)
    assert not is_self_note(note(from_addr="x@example.com"), ME)


def test_address_match_ignores_case():
    assert is_self_note(
        note(from_addr="Me@Example.com", to_addrs=["ME@example.com"]), ME
    )


def test_split_note_strips_sentence_punctuation_and_dedupes():
    urls, text = split_note(
        "see https://a.example/x. and https://a.example/x, then https://b.example/y?q=1"
    )
    assert urls == ["https://a.example/x", "https://b.example/y?q=1"]
    assert text == "see and then"


def test_bridge_queues_links_with_subject_as_title():
    cryo = FakeCryo()
    assert bridge(note(), ME, cryo) == "queued"
    assert cryo.calls == [(["https://example.com/a"], "Article title")]


def test_bridge_holds_typed_text_back():
    cryo = FakeCryo()
    assert (
        bridge(note(body="read this https://example.com/a"), ME, cryo)
        == "queued-text-held"
    )
    assert cryo.calls == [(["https://example.com/a"], "Article title")]


def test_bridge_never_calls_cryo_for_a_non_self_note():
    cryo = FakeCryo()
    assert bridge(note(label_ids=["INBOX"]), ME, cryo) == "not-self"
    assert bridge(note(body="no links here"), ME, cryo) == "no-links"
    assert cryo.calls == []


def test_shipped_config_routes_self_notes_to_cryo_but_not_archive():
    subs = load(Path(__file__).parent.parent / "config" / "subscriptions.yaml")
    assert targets(subs, "Cortex/Self/Note", "added") == ["cryo-backlog"]
    assert targets(subs, "Cortex/Self/Todo", "added") == ["cryo-backlog"]
    assert targets(subs, "Cortex/Self/Archive", "added") == []


# --- process(): every failure path settles the job; none loops or crashes ---

import inspect  # noqa: E402
import urllib.error  # noqa: E402

import cortex_utils.queue as queue_lib  # noqa: E402
import pytest  # noqa: E402

from actions.services import cryo_bridge  # noqa: E402


class FakeConn:
    def __init__(self):
        self.rollbacks = 0

    def rollback(self):
        self.rollbacks += 1


@pytest.fixture
def settled(monkeypatch):
    log = []

    def complete(conn, job_id, worker, commit=True):
        log.append(("complete", job_id))
        return True

    def fail_or_retry(
        conn, job_id, error, worker, base_seconds=30, cap_seconds=900, jitter_ratio=0.2
    ):
        log.append(("fail_or_retry", job_id))
        return "pending"

    for real, stub in (
        (queue_lib.complete, complete),
        (queue_lib.fail_or_retry, fail_or_retry),
    ):
        assert (
            inspect.signature(real).parameters.keys()
            == inspect.signature(stub).parameters.keys()
        )
    monkeypatch.setattr(cryo_bridge, "complete", complete)
    monkeypatch.setattr(cryo_bridge, "fail_or_retry", fail_or_retry)
    return log


def run_job(monkeypatch, email, cryo):
    monkeypatch.setattr(cryo_bridge, "load_email", lambda conn, gid: email)
    b = cryo_bridge.Bridge("dsn", ME, cryo, poll_interval=1)
    return b.process(FakeConn(), {"id": 7, "payload": {"gmail_id": "g1"}})


class RaisingCryo:
    def __init__(self, exc):
        self.exc = exc

    def enqueue(self, urls, title):
        raise self.exc


def http(code):
    return urllib.error.HTTPError("u", code, "x", None, None)


def test_success_completes(monkeypatch, settled):
    assert run_job(monkeypatch, note(), FakeCryo()) == "queued"
    assert settled == [("complete", 7)]


def test_permanent_4xx_completes_as_rejected(monkeypatch, settled):
    assert run_job(monkeypatch, note(), RaisingCryo(http(422))) == "rejected"
    assert settled == [("complete", 7)]


@pytest.mark.parametrize("code", [401, 429, 500, 503])
def test_retryable_http_charges_an_attempt(monkeypatch, settled, code):
    assert run_job(monkeypatch, note(), RaisingCryo(http(code))) == "retry"
    assert settled == [("fail_or_retry", 7)]


def test_unexpected_exception_fails_the_job_instead_of_escaping(monkeypatch, settled):
    assert run_job(monkeypatch, note(), RaisingCryo(TypeError("boom"))) == "retry"
    assert settled == [("fail_or_retry", 7)]


def test_email_not_parsed_yet_is_retried(monkeypatch, settled):
    assert run_job(monkeypatch, None, FakeCryo()) == "retry"
    assert settled == [("fail_or_retry", 7)]


def test_job_without_gmail_id_is_rejected(monkeypatch, settled):
    b = cryo_bridge.Bridge("dsn", ME, FakeCryo(), poll_interval=1)
    assert b.process(FakeConn(), {"id": 7, "payload": "garbage"}) == "rejected"
    assert settled == [("complete", 7)]
