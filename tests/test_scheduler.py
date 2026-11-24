"""AC2, AC3, AC4 (ordering), timezone handling."""

from __future__ import annotations

import datetime as dt
from zoneinfo import ZoneInfo

import pytest

from taskflow import RunStatus

from .helpers import NOON, make_engine, task


async def test_tick_twice_in_same_minute_dispatches_once() -> None:
    engine = make_engine({"alice": 3})
    engine.scheduler.submit(task())

    first = await engine.scheduler.tick(NOON)
    second = await engine.scheduler.tick(NOON + dt.timedelta(seconds=30))

    assert len(first) == 1
    assert second == []


async def test_missed_slot_catches_up_once_and_not_before_time() -> None:
    engine = make_engine({"alice": 3})
    engine.scheduler.submit(task())

    assert await engine.scheduler.tick(NOON - dt.timedelta(minutes=1)) == []
    assert len(await engine.scheduler.tick(NOON + dt.timedelta(minutes=7))) == 1
    assert await engine.scheduler.tick(NOON + dt.timedelta(hours=3)) == []


async def test_task_runs_again_next_day() -> None:
    engine = make_engine({"alice": 3})
    engine.scheduler.submit(task())

    await engine.scheduler.tick(NOON)
    jobs = await engine.scheduler.tick(NOON + dt.timedelta(days=1))

    assert [j.run_date for j in jobs] == [dt.date(2026, 11, 25)]


async def test_quota_shortfall_admits_in_submission_order() -> None:
    engine = make_engine({"alice": 3})
    engine.scheduler.submit_many(task(target=f"/t{i}") for i in range(5))

    jobs = await engine.scheduler.tick(NOON)

    assert [j.task.target for j in jobs] == ["/t0", "/t1", "/t2"]
    exceeded = engine.run_log.with_status(RunStatus.QUOTA_EXCEEDED)
    assert [r.task_id for r in exceeded] == ["task-0003", "task-0004"]
    # a refused run is not re-evaluated (or re-logged) on later ticks that day
    await engine.scheduler.tick(NOON + dt.timedelta(minutes=1))
    assert len(engine.run_log.with_status(RunStatus.QUOTA_EXCEEDED)) == 2


async def test_quota_exceeded_is_logged_with_context(caplog: pytest.LogCaptureFixture) -> None:
    engine = make_engine({"alice": 0})
    engine.scheduler.submit(task())

    await engine.scheduler.tick(NOON)

    assert "run.quota_exceeded task_id=task-0000 user=alice" in caplog.text


async def test_due_time_is_evaluated_in_configured_timezone() -> None:
    engine = make_engine({"alice": 3}, tz=ZoneInfo("Asia/Taipei"))
    engine.scheduler.submit(task())  # 12:00 Taipei == 04:00 UTC

    assert await engine.scheduler.tick(dt.datetime(2026, 11, 24, 3, 59, tzinfo=dt.UTC)) == []
    assert len(await engine.scheduler.tick(dt.datetime(2026, 11, 24, 4, 0, tzinfo=dt.UTC))) == 1


async def test_naive_datetime_is_rejected() -> None:
    engine = make_engine({"alice": 3})
    with pytest.raises(ValueError, match="timezone-aware"):
        await engine.scheduler.tick(dt.datetime(2026, 11, 24, 12, 0))
