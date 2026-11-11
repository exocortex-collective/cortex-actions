"""Fixtures shared by the cryo-bridge tests."""

import inspect

import cortex_utils.queue as queue_lib
import pytest

from actions.services import cryo_bridge


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
