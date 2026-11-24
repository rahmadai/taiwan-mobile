"""End-to-end through scheduler -> queue -> workers: AC4, AC5, AC6, AC8."""

from __future__ import annotations

import asyncio
import contextlib
import datetime as dt
from collections import Counter

import pytest
from pydantic import BaseModel

from taskflow import Action, RunStatus, TaskExecutor
from taskflow.executor import ExecutionResult
from taskflow.models import Task

from .helpers import NOON, FlakyAction, make_engine, registry_with, task

LEGACY_TASKS = [
    {"user": "alice", "time": "12:00", "action": "sync", "target": "/data/x"},
    {"user": "bob", "time": "12:00", "action": "backup", "target": "/srv/y"},
    {"user": "alice", "time": "12:00", "action": "delete", "target": "/tmp/z"},
]


async def test_legacy_scenario_runs_all_tasks_once() -> None:
    engine = make_engine({"alice": 3, "bob": 5})
    engine.scheduler.submit_many(LEGACY_TASKS)

    async with engine:
        assert await engine.tick(NOON) == 3
        assert await engine.tick(NOON + dt.timedelta(seconds=59)) == 0

    statuses = {r.task_id: r.status for r in engine.run_log.all()}
    assert statuses == dict.fromkeys(["task-0000", "task-0001", "task-0002"], RunStatus.SUCCESS)


async def test_concurrent_workers_respect_quota() -> None:
    engine = make_engine({"alice": 3}, workers=8)
    engine.scheduler.submit_many(task(target=f"/t{i}") for i in range(5))

    async with engine:
        await engine.tick(NOON)

    assert len(engine.run_log.with_status(RunStatus.SUCCESS)) == 3
    assert len(engine.run_log.with_status(RunStatus.QUOTA_EXCEEDED)) == 2
    assert engine.scheduler.quotas.used("alice", NOON.date()) == 3


async def test_retries_consume_one_quota_unit_per_run() -> None:
    engine = make_engine({"alice": 3}, registry=registry_with(FlakyAction()))
    engine.scheduler.submit(task(action="flaky", params={"fail_times": 2}))

    async with engine:
        await engine.tick(NOON)

    [record] = engine.run_log.all()
    assert (record.status, record.attempts) == (RunStatus.SUCCESS, 3)
    assert engine.scheduler.quotas.used("alice", NOON.date()) == 1


async def test_failing_task_does_not_affect_others() -> None:
    engine = make_engine({"alice": 5}, registry=registry_with(FlakyAction()), workers=2)
    engine.scheduler.submit_many(
        [
            task(target="/a"),
            task(action="flaky", target="/bad", params={"fail_times": 99}),
            task(target="/b"),
        ]
    )

    async with engine:
        await engine.tick(NOON)

    by_task = {r.task_id: r.status for r in engine.run_log.all()}
    assert by_task == {
        "task-0000": RunStatus.SUCCESS,
        "task-0001": RunStatus.FAILED,
        "task-0002": RunStatus.SUCCESS,
    }


async def test_user_with_simultaneous_tasks_runs_them_all_on_each_day() -> None:
    """FR1 + AC1: one user, several tasks in the same slot, over consecutive days."""
    engine = make_engine({"alice": 3}, workers=4)
    engine.scheduler.submit_many(
        [
            task(action="sync", target="/data/x"),
            task(action="delete", target="/tmp/z"),
            task(action="backup", target="/srv/y", params={"retention_days": 30}),
        ]
    )
    days = [NOON + dt.timedelta(days=offset) for offset in range(3)]

    async with engine:
        for now in days:
            assert await engine.tick(now) == 3
            assert await engine.tick(now + dt.timedelta(minutes=1)) == 0  # no repeat same day
            await engine.drain()
            assert engine.scheduler.quotas.used("alice", now.date()) == 3

    per_day = Counter((r.run_date, r.status) for r in engine.run_log.all())
    assert per_day == {(now.date(), RunStatus.SUCCESS): 3 for now in days}
    for t in engine.scheduler.tasks_for("alice"):
        assert [r.run_date for r in engine.run_log.for_task(t.id)] == [d.date() for d in days]


async def test_simultaneous_tasks_over_quota_are_refused_each_day_then_reset() -> None:
    """Quota shortfall in one slot repeats per day; the same tasks win every day."""
    engine = make_engine({"alice": 2}, workers=4)
    engine.scheduler.submit_many(task(target=f"/t{i}") for i in range(3))
    day1, day2 = NOON, NOON + dt.timedelta(days=1)

    async with engine:
        await engine.tick(day1)
        await engine.drain()
        await engine.tick(day2)

    for day in (day1.date(), day2.date()):
        records = {r.task_id: r.status for r in engine.run_log.all() if r.run_date == day}
        assert records == {
            "task-0000": RunStatus.SUCCESS,
            "task-0001": RunStatus.SUCCESS,
            "task-0002": RunStatus.QUOTA_EXCEEDED,
        }


class _NotifyParams(BaseModel):
    channel: str


class _NotifyAction(Action[_NotifyParams]):
    """A brand-new strategy defined outside the package."""

    name = "notify"
    params_model = _NotifyParams

    def __init__(self) -> None:
        self.sent: list[tuple[str, str]] = []

    async def execute(self, target: str, params: _NotifyParams) -> None:
        self.sent.append((params.channel, target))


async def test_new_action_plugs_in_without_engine_changes() -> None:
    notify = _NotifyAction()
    engine = make_engine({"bob": 1}, registry=registry_with(notify))
    engine.scheduler.submit(task(user="bob", action="notify", params={"channel": "#ops"}))

    async with engine:
        await engine.tick(NOON)

    assert notify.sent == [("#ops", "/data/x")]


class _BrokenExecutor(TaskExecutor):
    async def execute(self, task: Task) -> ExecutionResult:
        raise RuntimeError(f"executor bug on {task.id}")


async def test_worker_survives_unexpected_executor_errors(
    caplog: pytest.LogCaptureFixture,
) -> None:
    engine = make_engine({"alice": 3}, workers=1)
    engine._executor = _BrokenExecutor(registry_with())  # pyright: ignore[reportPrivateUsage]
    engine.scheduler.submit_many([task(target="/a"), task(target="/b")])

    async with engine:
        await engine.tick(NOON)

    assert [r.status for r in engine.run_log.all()] == [RunStatus.FAILED] * 2
    assert "worker.unhandled worker=0 task_id=task-0000" in caplog.text


async def test_tick_before_start_is_a_clear_error() -> None:
    engine = make_engine({"alice": 3})
    with pytest.raises(RuntimeError, match="not started"):
        await engine.tick(NOON)


async def test_run_forever_ticks_on_the_clock() -> None:
    engine = make_engine({"alice": 3})
    engine._clock = lambda: NOON  # pyright: ignore[reportPrivateUsage]
    engine.scheduler.submit(task())

    async with engine:
        loop = asyncio.create_task(engine.run_forever(interval=0.001))
        await asyncio.sleep(0.02)
        loop.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await loop

    assert [r.status for r in engine.run_log.all()] == [RunStatus.SUCCESS]


def test_engine_rejects_zero_workers() -> None:
    with pytest.raises(ValueError, match="workers"):
        make_engine({"alice": 1}, workers=0)


class _GateParams(BaseModel):
    pass


class _GateAction(Action[_GateParams]):
    """Blocks every run until ``gate`` is set, to hold the queue full."""

    name = "gate"
    params_model = _GateParams

    def __init__(self) -> None:
        self.gate = asyncio.Event()

    async def execute(self, target: str, params: _GateParams) -> None:
        await self.gate.wait()


async def test_cancelled_tick_releases_jobs_that_never_reached_the_queue(
    caplog: pytest.LogCaptureFixture,
) -> None:
    gate = _GateAction()
    engine = make_engine({"alice": 5}, registry=registry_with(gate), workers=1, queue_maxsize=1)
    engine.scheduler.submit_many(task(action="gate", target=f"/t{i}") for i in range(5))
    await engine.start()

    ticking = asyncio.create_task(engine.tick(NOON))
    await asyncio.sleep(0.01)  # worker holds job 0, queue holds job 1, tick blocked on job 2
    ticking.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await ticking

    assert engine.scheduler.quotas.used("alice", NOON.date()) == 2  # 3 refunded
    assert "tick.cancelled enqueued=2 released=3" in caplog.text
    gate.gate.set()
    await engine.drain()
    assert await engine.tick(NOON + dt.timedelta(minutes=1)) == 3  # re-admitted, same day
    await engine.stop()

    assert len(engine.run_log.with_status(RunStatus.SUCCESS)) == 5
    assert engine.scheduler.quotas.used("alice", NOON.date()) == 5
