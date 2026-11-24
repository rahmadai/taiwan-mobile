"""AC7 + FR1/FR2: tasks are configured via dicts and validated at submit."""

from __future__ import annotations

import datetime as dt

import pytest

from taskflow import InvalidTaskError, UnknownActionError, UnknownUserError
from taskflow.actions import BackupParams, SyncParams

from .helpers import make_engine, task


@pytest.mark.parametrize(
    ("raw", "error", "match"),
    [
        (task(time="25:00"), InvalidTaskError, r"HH:MM"),
        (task(time="noon"), InvalidTaskError, r"HH:MM"),
        (task(action="explode"), UnknownActionError, r"unknown action 'explode'"),
        (task(user="mallory"), UnknownUserError, r"unknown user 'mallory'"),
        (task(action="backup", params={"retention_days": 0}), InvalidTaskError, r"retention_days"),
        (task(params={"bogus": 1}), InvalidTaskError, r"bogus"),
        (task(target=""), InvalidTaskError, r"target"),
        ({**task(), "priority": 1}, InvalidTaskError, r"priority"),
    ],
)
def test_invalid_task_is_rejected_at_submit(
    raw: dict[str, object], error: type[Exception], match: str
) -> None:
    engine = make_engine({"alice": 3})
    with pytest.raises(error, match=match):
        engine.scheduler.submit(raw)
    assert engine.scheduler.tasks() == []


def test_params_are_parsed_into_the_actions_model_with_defaults() -> None:
    engine = make_engine({"bob": 5})
    custom = engine.scheduler.submit(
        task(user="bob", action="backup", params={"retention_days": 30, "compress": False})
    )
    default = engine.scheduler.submit(task(user="bob", action="backup"))

    assert custom.params == BackupParams(retention_days=30, compress=False)
    assert default.params == BackupParams()
    assert custom.spec.time == dt.time(12, 0)


def test_user_can_own_many_tasks_including_same_slot() -> None:
    engine = make_engine({"alice": 3})
    tasks = engine.scheduler.submit_many(
        [task(target="/a"), task(target="/b"), task(time="18:30", target="/c")]
    )

    assert [t.id for t in engine.scheduler.tasks_for("alice")] == [t.id for t in tasks]
    assert len({t.id for t in tasks}) == 3
    assert all(isinstance(t.params, SyncParams) for t in tasks)
