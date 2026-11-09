"""Self-note links to cryo's drains through cryo.enqueue_mail() (cortex-wznn)."""

import os
import urllib.error

import psycopg2
import pytest
from prometheus_client import REGISTRY

from actions.services import cryo_bridge
from actions.services.cryo_bridge import Drain, bridge, drain_target, youtube_id
from tests.test_cryo_bridge import (
    ME,
    FakeConn,
    FakeCryo,
    http,
    note,
    outcome_count,
    run_job,
)

VIDEO = "dQw4w9WgXcQ"


@pytest.mark.parametrize(
    "url, video",
    [
        (f"https://www.youtube.com/watch?v={VIDEO}", VIDEO),
        (f"https://youtube.com/watch?v={VIDEO}&t=42", VIDEO),
        (f"https://m.youtube.com/watch?feature=share&v={VIDEO}", VIDEO),
        (f"https://youtu.be/{VIDEO}?si=abc", VIDEO),
        (f"https://www.youtube.com/shorts/{VIDEO}", VIDEO),
        ("https://www.youtube.com/watch?v=short", None),
        (f"https://www.youtube.com/playlist?list={VIDEO}", None),
        (f"https://youtube.com.evil.example/watch?v={VIDEO}", None),
        (f"https://example.com/watch?v={VIDEO}", None),
        ("http://[abc/x", None),
    ],
)
def test_youtube_id(url, video):
    assert youtube_id(url) == video


def test_drain_target_splits_on_the_url():
    assert drain_target(f"https://youtu.be/{VIDEO}") == (
        "drain-youtube",
        {"video_id": VIDEO},
    )
    assert drain_target("https://example.com/a") == (
        "drain-article",
        {"url": "https://example.com/a"},
    )


class FakeDrain:
    def __init__(self, results=None):
        self.results = results or {}
        self.calls = []

    def __call__(self, queue, payload):
        self.calls.append((queue, payload))
        return self.results.get(next(iter(payload.values())), "queued")


def drains(queue, result):
    return (
        REGISTRY.get_sample_value(
            "cortex_cryo_bridge_drains_total", {"queue": queue, "result": result}
        )
        or 0.0
    )


def test_each_link_goes_to_its_drain_not_the_backlog():
    cryo, drain = FakeCryo(), FakeDrain()
    body = f"https://example.com/a https://youtu.be/{VIDEO}"
    assert bridge(note(body=body), ME, cryo, lambda u: u, drain) == "drained"
    assert drain.calls == [
        ("drain-article", {"url": "https://example.com/a"}),
        ("drain-youtube", {"video_id": VIDEO}),
    ]
    assert cryo.calls == []


def test_a_capped_link_falls_back_to_the_backlog_alone():
    before = drains("drain-youtube", "capped")
    cryo, drain = FakeCryo(), FakeDrain({VIDEO: "capped"})
    body = f"https://example.com/a https://youtu.be/{VIDEO}"
    bridge(note(body=body), ME, cryo, lambda u: u, drain)
    assert cryo.calls == [([f"https://youtu.be/{VIDEO}"], "Article title")]
    assert drains("drain-youtube", "capped") == before + 1


def test_a_refused_link_is_counted_and_not_backlogged():
    before = drains("drain-article", "refused")
    cryo = FakeCryo()
    drain = FakeDrain({"https://example.com/a": "refused"})
    assert bridge(note(), ME, cryo, lambda u: u, drain) == "drained-refused"
    assert cryo.calls == []
    assert drains("drain-article", "refused") == before + 1


def test_duplicates_are_counted():
    before = drains("drain-article", "duplicate")
    drain = FakeDrain({"https://example.com/a": "duplicate"})
    assert bridge(note(), ME, FakeCryo(), lambda u: u, drain) == "drained"
    assert drains("drain-article", "duplicate") == before + 1


def test_an_unexpanded_short_link_goes_to_the_backlog_not_a_drain():
    before = drains("backlog", "unexpanded")
    cryo, drain = FakeCryo(), FakeDrain()
    bridge(note(body="https://share.google/abc"), ME, cryo, lambda u: u, drain)
    assert drain.calls == []
    assert cryo.calls == [(["https://share.google/abc"], "Article title")]
    assert drains("backlog", "unexpanded") == before + 1


def test_a_capped_link_is_backlogged_before_a_later_link_fails():
    cryo = FakeCryo()

    def drain(queue, payload):
        if queue == "drain-youtube":
            return "capped"
        raise psycopg2.OperationalError("gone")

    body = f"https://youtu.be/{VIDEO} https://example.com/a"
    with pytest.raises(psycopg2.OperationalError):
        bridge(note(body=body), ME, cryo, lambda u: u, drain)
    assert cryo.calls == [([f"https://youtu.be/{VIDEO}"], "Article title")]


class BacklogRefusing(FakeCryo):
    def __init__(self, code):
        super().__init__()
        self.code = code

    def enqueue(self, urls, title):
        raise http(self.code)


def test_a_permanent_backlog_refusal_does_not_stop_the_note():
    before = drains("backlog", "refused")
    drain = FakeDrain({VIDEO: "capped"})
    body = f"https://youtu.be/{VIDEO} https://example.com/a"
    outcome = bridge(note(body=body), ME, BacklogRefusing(400), lambda u: u, drain)
    assert outcome == "drained-refused"
    assert drain.calls[-1] == ("drain-article", {"url": "https://example.com/a"})
    assert drains("backlog", "refused") == before + 1


def test_a_retryable_backlog_failure_is_raised():
    drain = FakeDrain({VIDEO: "capped"})
    with pytest.raises(urllib.error.HTTPError):
        bridge(
            note(body=f"https://youtu.be/{VIDEO}"),
            ME,
            BacklogRefusing(503),
            lambda u: u,
            drain,
        )


def test_a_refused_note_is_logged_at_error(monkeypatch, queue):
    errors = []
    monkeypatch.setattr(
        cryo_bridge.logger, "error", lambda msg, **kw: errors.append((msg, kw))
    )
    drain = RecordingDrain()
    drain.enqueue = lambda queue, payload, message_id: "refused"
    monkeypatch.setattr(cryo_bridge, "load_email", lambda conn, gid: note())
    b = cryo_bridge.Bridge("dsn", ME, FakeCryo(), poll_interval=1, drain=drain)
    before = outcome_count("drained-refused")
    assert b.process(FakeConn(), {"id": 7, "payload": {"gmail_id": "g1"}}) == (
        "drained-refused"
    )
    assert ("self-note handled", {"gmail_id": "g1", "outcome": "drained-refused"}) in (
        errors
    )
    assert outcome_count("drained-refused") == before + 1


def test_a_link_less_note_does_not_touch_the_drain():
    drain = FakeDrain()
    assert bridge(note(body="no links"), ME, FakeCryo(), lambda u: u, drain) == (
        "no-links"
    )
    assert drain.calls == []


def test_a_non_self_note_never_drains():
    drain = FakeDrain()
    assert bridge(note(label_ids=("INBOX",)), ME, FakeCryo(), lambda u: u, drain) == (
        "not-self"
    )
    assert drain.calls == []


class RecordingDrain:
    def __init__(self, exc=None):
        self.exc = exc
        self.calls = []

    def enqueue(self, queue, payload, message_id):
        if self.exc:
            raise self.exc
        self.calls.append((queue, payload, message_id))
        return "queued"


def test_the_job_drains_under_its_gmail_id(monkeypatch, queue):
    drain = RecordingDrain()
    monkeypatch.setattr(cryo_bridge, "load_email", lambda conn, gid: note())
    b = cryo_bridge.Bridge("dsn", ME, FakeCryo(), poll_interval=1, drain=drain)
    before = outcome_count("drained")
    assert b.process(FakeConn(), {"id": 7, "payload": {"gmail_id": "g1"}}) == (
        "drained"
    )
    assert drain.calls == [("drain-article", {"url": "https://example.com/a"}, "g1")]
    assert outcome_count("drained") == before + 1


def test_a_database_failure_while_draining_retries_the_job(monkeypatch, queue):
    drain = RecordingDrain(psycopg2.OperationalError("connection refused"))
    monkeypatch.setattr(cryo_bridge, "load_email", lambda conn, gid: note())
    b = cryo_bridge.Bridge("dsn", ME, FakeCryo(), poll_interval=1, drain=drain)
    assert b.process(FakeConn(), {"id": 7, "payload": {"gmail_id": "g1"}}) == "retry"
    assert queue["log"] == [("fail_or_retry", 7)]


def test_without_a_drain_links_still_go_to_the_backlog(monkeypatch, queue):
    cryo = FakeCryo()
    assert run_job(monkeypatch, note(), cryo) == "queued"
    assert cryo.calls == [(["https://example.com/a"], "Article title")]


# --- main() wiring of CRYO_DRAIN_ENABLED ---


class BridgeRecorder:
    made: list = []

    def __init__(self, dsn, self_addr, cryo, poll_interval, drain=None):
        BridgeRecorder.made.append(drain)

    def run(self):
        pass


@pytest.fixture
def boot(monkeypatch):
    for v in (
        "POSTGRES_HOST",
        "POSTGRES_DB",
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
        "CRYO_URL",
        "CRYO_TOKEN",
        "SELF_ADDRESS",
    ):
        monkeypatch.setenv(v, "x")
    monkeypatch.setattr(cryo_bridge, "Bridge", BridgeRecorder)
    monkeypatch.setattr(cryo_bridge, "start_metrics_server", lambda port: None)
    monkeypatch.setattr(cryo_bridge.signal, "signal", lambda sig, fn: None)
    monkeypatch.setattr(cryo_bridge.sys, "argv", ["cryo_bridge"])
    BridgeRecorder.made = []
    return monkeypatch


def test_main_wires_the_drain_as_the_bridge_role(boot):
    boot.setenv("CRYO_DRAIN_ENABLED", "true")
    boot.setenv("CORTEX_CRYO_BRIDGE_PASSWORD", "pw")
    cryo_bridge.main()
    (drain,) = BridgeRecorder.made
    assert isinstance(drain, Drain)
    assert drain.dsn == (
        "host=x port=5432 dbname=x user=cortex_cryo_bridge password=pw"
    )


def test_main_leaves_the_drain_off_by_default(boot):
    boot.delenv("CRYO_DRAIN_ENABLED", raising=False)
    cryo_bridge.main()
    assert BridgeRecorder.made == [None]


def test_main_refuses_the_drain_without_its_password(boot):
    boot.setenv("CRYO_DRAIN_ENABLED", "true")
    boot.delenv("CORTEX_CRYO_BRIDGE_PASSWORD", raising=False)
    with pytest.raises(SystemExit) as e:
        cryo_bridge.main()
    assert "CORTEX_CRYO_BRIDGE_PASSWORD" in str(e.value)


# --- Drain against a stub cryo.enqueue_mail() that signals like the real one ---

DSN = os.environ.get("CORTEX_TEST_DSN")

STUB = """
CREATE TABLE cryo.mail (queue text, key text, message_id text, rule text,
                        UNIQUE (queue, key));
CREATE FUNCTION cryo.enqueue_mail(p_queue text, p_payload jsonb,
                                  p_message_id text, p_rule text)
RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE k text := coalesce(p_payload->>'url', p_payload->>'video_id');
BEGIN
    IF k LIKE '%capped%' THEN
        RAISE EXCEPTION 'over the daily cap' USING ERRCODE = 'CRY01';
    END IF;
    IF p_queue NOT IN ('drain-article', 'drain-youtube') THEN
        RAISE EXCEPTION 'queue % is off-contract', p_queue USING ERRCODE = 'CRY02';
    END IF;
    INSERT INTO cryo.mail VALUES (p_queue, k, p_message_id, p_rule)
        ON CONFLICT DO NOTHING;
    RETURN CASE WHEN FOUND THEN 1 ELSE NULL END;
END $$;
"""


@pytest.fixture
def live():
    if not DSN:
        pytest.skip("set CORTEX_TEST_DSN to a throwaway Postgres to run these")
    admin = psycopg2.connect(DSN)
    admin.autocommit = True
    with admin.cursor() as cur:
        # The stub has the real function's name, so never run against a
        # database that has cryo's real schema.
        cur.execute("SELECT to_regnamespace('cryo')")
        if cur.fetchone()[0] is not None:
            admin.close()
            pytest.fail("CORTEX_TEST_DSN has a cryo schema; use a throwaway database")
        cur.execute("CREATE SCHEMA cryo")
        cur.execute(STUB)
    drain = Drain(DSN)
    try:
        yield drain, admin
    finally:
        if drain.conn is not None:
            drain.conn.close()
        with admin.cursor() as cur:
            cur.execute("DROP SCHEMA cryo CASCADE")
        admin.close()


def test_drain_reports_queued_then_duplicate(live):
    drain, admin = live
    assert drain.enqueue("drain-article", {"url": "https://e.com/a"}, "g1") == "queued"
    assert drain.enqueue("drain-article", {"url": "https://e.com/a"}, "g1") == (
        "duplicate"
    )
    with admin.cursor() as cur:
        cur.execute("SELECT message_id, rule FROM cryo.mail")
        assert cur.fetchall() == [("g1", "self-note")]


def test_drain_sets_its_timeouts(live):
    drain, _ = live
    drain.enqueue("drain-article", {"url": "https://e.com/a"}, "g1")
    with drain.conn.cursor() as cur:
        cur.execute("SHOW statement_timeout")
        assert cur.fetchone() == ("30s",)
        cur.execute("SHOW lock_timeout")
        assert cur.fetchone() == ("20s",)
    assert drain.conn.get_dsn_parameters()["connect_timeout"] == "10"


def test_drain_maps_cry01_to_capped(live):
    drain, _ = live
    assert drain.enqueue("drain-youtube", {"video_id": "capped"}, "g1") == "capped"


def test_drain_maps_cry02_to_refused(live):
    drain, _ = live
    assert drain.enqueue("scan", {"url": "https://e.com/a"}, "g1") == "refused"


def test_drain_keeps_its_connection_after_a_refusal(live):
    drain, _ = live
    drain.enqueue("scan", {"url": "https://e.com/a"}, "g1")
    assert drain.enqueue("drain-article", {"url": "https://e.com/b"}, "g1") == "queued"


def test_any_other_database_error_raises_and_reconnects(live):
    drain, admin = live
    with admin.cursor() as cur:
        cur.execute("DROP FUNCTION cryo.enqueue_mail(text,jsonb,text,text)")
    with pytest.raises(psycopg2.Error):
        drain.enqueue("drain-article", {"url": "https://e.com/a"}, "g1")
    assert drain.conn is None
