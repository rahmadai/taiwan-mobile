"""End-to-end cases from e2e/TEST_CASES.md. Public API / CLI only."""

from __future__ import annotations

import asyncio
import contextlib
import datetime as dt
import logging
import subprocess
import sys
import time
from collections import Counter
from collections.abc import Callable
from zoneinfo import ZoneInfo

import pytest
from pydantic import BaseModel

from taskflow import (
    Action,
    ActionError,
    InvalidTaskError,
    PermanentActionError,
    RetryPolicy,
    RunStatus,
    TaskEngine,
    UnknownActionError,
    UnknownUserError,
    build_engine,
    default_registry,
)

DAY1_NOON = dt.datetime(2026, 11, 24, 12, 0, tzinfo=dt.UTC)
# real sleeps, kept short so the suite stays fast
REAL = RetryPolicy(max_attempts=3, base_delay=0.01, max_delay=0.05, attempt_timeout=0.2)


class Clock:
    """Mutable clock passed via the public ``clock=`` parameter."""

    def __init__(self, now: dt.datetime) -> None:
        self.now = now

    def __call__(self) -> dt.datetime:
        return self.now


async def run_for(engine: TaskEngine, seconds: float, interval: float = 0.002) -> None:
    """Drive the real run_forever loop, then drain outstanding work."""
    loop = asyncio.create_task(engine.run_forever(interval=interval))
    await asyncio.sleep(seconds)
    loop.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await loop
    await engine.drain()


async def run_until(
    engine: TaskEngine, done: Callable[[], bool], limit_s: float, interval: float = 0.002
) -> None:
    """Drive run_forever until ``done()`` holds (or fail after ``limit_s`` seconds)."""
    loop = asyncio.create_task(engine.run_forever(interval=interval))
    try:
        async with asyncio.timeout(limit_s):
            while not done():  # noqa: ASYNC110 - polling a predicate is fine in a test
                await asyncio.sleep(0.005)
    finally:
        loop.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await loop


# --- test-only strategies, defined outside the package ---------------------------


class SlowParams(BaseModel):
    delay_ms: int = 10
    fail_times: int = 0
    hang: bool = False
    permanent: bool = False


class SlowAction(Action[SlowParams]):
    name = "slow"
    params_model = SlowParams

    def __init__(self) -> None:
        self.in_flight = 0
        self.max_in_flight = 0
        self.calls: Counter[str] = Counter()

    async def execute(self, target: str, params: SlowParams) -> None:
        self.calls[target] += 1
        self.in_flight += 1
        self.max_in_flight = max(self.max_in_flight, self.in_flight)
        try:
            if params.permanent:
                raise PermanentActionError(f"{target} does not exist")
            await asyncio.sleep(10 if params.hang else params.delay_ms / 1000)
            if self.calls[target] <= params.fail_times:
                raise ActionError(f"transient failure #{self.calls[target]}")
        finally:
            self.in_flight -= 1


class NotifyParams(BaseModel):
    channel: str


class NotifyAction(Action[NotifyParams]):
    name = "notify"
    params_model = NotifyParams

    def __init__(self) -> None:
        self.sent: list[tuple[str, str]] = []

    async def execute(self, target: str, params: NotifyParams) -> None:
        self.sent.append((params.channel, target))


def engine_with(
    users: dict[str, int],
    clock: Clock,
    *extra: Action[SlowParams] | Action[NotifyParams],
    workers: int = 4,
    tz: dt.tzinfo = dt.UTC,
    queue_maxsize: int = 1000,
) -> TaskEngine:
    registry = default_registry()
    for action in extra:
        registry.register(action)
    return build_engine(
        users,
        registry=registry,
        policy=REAL,
        workers=workers,
        tz=tz,
        clock=clock,
        queue_maxsize=queue_maxsize,
    )


def statuses(engine: TaskEngine) -> Counter[tuple[str, RunStatus]]:
    return Counter((r.run_date.isoformat(), r.status) for r in engine.run_log.all())


# --- cases -----------------------------------------------------------------------


def test_tc01_cli_demo_runs_legacy_scenario() -> None:
    proc = subprocess.run(
        [sys.executable, "-m", "taskflow"], capture_output=True, text=True, timeout=30, check=False
    )

    assert proc.returncode == 0, proc.stderr
    logs = proc.stderr
    assert logs.count("run.success") == 10
    assert logs.count("run.quota_exceeded") == 2
    assert "demo.summary day=2026-11-24 {'QUOTA_EXCEEDED': 1, 'SUCCESS': 5}" in logs
    assert "demo.summary day=2026-11-25 {'QUOTA_EXCEEDED': 1, 'SUCCESS': 5}" in logs
    assert "Traceback" not in logs


async def test_tc02_user_submits_multiple_dict_configured_tasks(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO)
    engine = engine_with({"bob": 5}, Clock(DAY1_NOON))
    engine.scheduler.submit_many(
        [
            {"user": "bob", "time": "12:00", "action": "backup", "target": "/srv/db",
             "params": {"retention_days": 30, "compress": False}},
            {"user": "bob", "time": "12:00", "action": "sync", "target": "/data/a",
             "params": {"delete_extraneous": True}},
            {"user": "bob", "time": "11:30", "action": "delete", "target": "/tmp/old",
             "params": {"recursive": True}},
        ]
    )  # fmt: skip

    async with engine:
        await run_for(engine, 0.05)

    assert statuses(engine) == {("2026-11-24", RunStatus.SUCCESS): 3}
    assert "action.backup target=/srv/db retention_days=30 compress=False" in caplog.text
    assert "action.sync target=/data/a delete_extraneous=True" in caplog.text
    assert "action.delete target=/tmp/old recursive=True" in caplog.text


@pytest.mark.parametrize(
    ("override", "error", "message"),
    [
        ({"time": "24:00"}, InvalidTaskError, "time must be 'HH:MM'"),
        ({"user": "eve"}, UnknownUserError, "unknown user 'eve'"),
        ({"action": "format_disk"}, UnknownActionError, "unknown action 'format_disk'"),
        (
            {"action": "backup", "params": {"retention_days": -1}},
            InvalidTaskError,
            "retention_days",
        ),
        ({"params": {"dry_run": True}}, InvalidTaskError, "dry_run"),
        ({"target": None}, InvalidTaskError, "target"),
    ],
)
async def test_tc03_invalid_tasks_rejected_with_clear_errors(
    override: dict[str, object], error: type[Exception], message: str
) -> None:
    engine = engine_with({"alice": 3}, Clock(DAY1_NOON))
    raw = {"user": "alice", "time": "12:00", "action": "sync", "target": "/x", **override}

    with pytest.raises(error) as excinfo:
        engine.scheduler.submit(raw)

    assert message in str(excinfo.value)
    assert engine.scheduler.tasks() == []


async def test_tc04_many_ticks_in_same_minute_execute_once() -> None:
    slow = SlowAction()
    engine = engine_with({"alice": 3}, Clock(DAY1_NOON), slow)
    engine.scheduler.submit({"user": "alice", "time": "12:00", "action": "slow", "target": "/x"})

    async with engine:
        await run_for(engine, 0.1, interval=0.002)  # ~50 ticks at 12:00

    assert slow.calls["/x"] == 1
    assert statuses(engine) == {("2026-11-24", RunStatus.SUCCESS): 1}


async def test_tc05_late_start_catches_up_once() -> None:
    clock = Clock(DAY1_NOON.replace(hour=15))
    engine = engine_with({"alice": 3}, clock)
    engine.scheduler.submit({"user": "alice", "time": "12:00", "action": "sync", "target": "/x"})

    async with engine:
        await run_for(engine, 0.03)
        clock.now = clock.now.replace(hour=23, minute=59)
        await run_for(engine, 0.03)

    assert statuses(engine) == {("2026-11-24", RunStatus.SUCCESS): 1}


async def test_tc06_quota_resets_across_midnight() -> None:
    clock = Clock(DAY1_NOON.replace(hour=23, minute=59))
    engine = engine_with({"alice": 1}, clock)
    engine.scheduler.submit_many(
        {"user": "alice", "time": "23:59", "action": "sync", "target": f"/t{i}"} for i in range(2)
    )

    async with engine:
        await run_for(engine, 0.03)
        clock.now += dt.timedelta(minutes=1)  # 00:00 next day: tasks not due yet
        await run_for(engine, 0.03)
        clock.now = clock.now.replace(hour=23, minute=59)
        await run_for(engine, 0.03)

    assert statuses(engine) == {
        ("2026-11-24", RunStatus.SUCCESS): 1,
        ("2026-11-24", RunStatus.QUOTA_EXCEEDED): 1,
        ("2026-11-25", RunStatus.SUCCESS): 1,
        ("2026-11-25", RunStatus.QUOTA_EXCEEDED): 1,
    }


async def test_tc07_single_user_simultaneous_tasks_on_consecutive_days() -> None:
    clock = Clock(DAY1_NOON)
    engine = engine_with({"alice": 3}, clock)
    engine.scheduler.submit_many(
        [
            {"user": "alice", "time": "12:00", "action": "sync", "target": "/data/x"},
            {"user": "alice", "time": "12:00", "action": "delete", "target": "/tmp/z"},
            {"user": "alice", "time": "12:00", "action": "backup", "target": "/srv/y"},
        ]
    )

    async with engine:
        for day in range(3):
            clock.now = DAY1_NOON + dt.timedelta(days=day)
            await run_for(engine, 0.03)
            assert engine.scheduler.quotas.used("alice", clock.now.date()) == 3

    assert statuses(engine) == {
        (d, RunStatus.SUCCESS): 3 for d in ("2026-11-24", "2026-11-25", "2026-11-26")
    }


async def test_tc08_concurrent_workers_enforce_quota_per_user() -> None:
    slow = SlowAction()
    users = {f"user{u:02d}": 5 for u in range(20)}
    engine = engine_with(users, Clock(DAY1_NOON), slow, workers=16)
    for i in range(10):
        for user in users:
            engine.scheduler.submit(
                {"user": user, "time": "12:00", "action": "slow", "target": f"/{user}/{i}",
                 "params": {"delay_ms": 20}}
            )  # fmt: skip

    async with engine:
        started = time.perf_counter()
        await run_for(engine, 0.01)
        elapsed = time.perf_counter() - started

    succeeded = {(r.user, r.task_id) for r in engine.run_log.with_status(RunStatus.SUCCESS)}
    per_user = Counter(user for user, _ in succeeded)
    assert per_user == dict.fromkeys(users, 5)
    for user in users:  # the first 5 submitted tasks of each user won
        expected = {t.id for t in engine.scheduler.tasks_for(user)[:5]}
        assert {tid for u, tid in succeeded if u == user} == expected
    assert len(engine.run_log.with_status(RunStatus.QUOTA_EXCEEDED)) == 100
    assert slow.max_in_flight > 1  # genuinely concurrent
    assert elapsed < 100 * 0.02  # far faster than 100 serial 20ms runs


async def test_tc09_flaky_action_recovers_with_real_backoff(
    caplog: pytest.LogCaptureFixture,
) -> None:
    slow = SlowAction()
    engine = engine_with({"alice": 3}, Clock(DAY1_NOON), slow)
    engine.scheduler.submit(
        {"user": "alice", "time": "12:00", "action": "slow", "target": "/flaky",
         "params": {"delay_ms": 1, "fail_times": 2}}
    )  # fmt: skip

    async with engine:
        await run_for(engine, 0.2)

    [record] = engine.run_log.all()
    assert (record.status, record.attempts) == (RunStatus.SUCCESS, 3)
    assert caplog.text.count("run.retry task_id=task-0000") == 2
    assert engine.scheduler.quotas.used("alice", DAY1_NOON.date()) == 1


async def test_tc10_hung_action_times_out_without_blocking_others() -> None:
    slow = SlowAction()
    engine = engine_with({"alice": 5}, Clock(DAY1_NOON), slow, workers=2)
    engine.scheduler.submit_many(
        [
            {"user": "alice", "time": "12:00", "action": "slow", "target": "/hang",
             "params": {"hang": True}},
            *({"user": "alice", "time": "12:00", "action": "sync", "target": f"/ok{i}"}
              for i in range(3)),
        ]
    )  # fmt: skip

    async with engine:
        loop = asyncio.create_task(engine.run_forever(interval=0.002))
        await asyncio.sleep(0.1)  # /hang needs ~0.65s (3 x 0.2s timeout + backoff)
        assert len(engine.run_log.with_status(RunStatus.SUCCESS)) == 3
        assert engine.run_log.with_status(RunStatus.FAILED) == []  # still retrying
        loop.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await loop
        await engine.drain()

    [failed] = engine.run_log.with_status(RunStatus.FAILED)
    assert (failed.attempts, failed.error) == (3, "timed out after 0.2s")
    assert engine.running is False  # stopped cleanly by the context manager


async def test_tc11_permanent_error_is_not_retried(caplog: pytest.LogCaptureFixture) -> None:
    engine = engine_with({"alice": 3}, Clock(DAY1_NOON), SlowAction())
    engine.scheduler.submit(
        {"user": "alice", "time": "12:00", "action": "slow", "target": "/gone",
         "params": {"permanent": True}}
    )  # fmt: skip

    async with engine:
        await run_for(engine, 0.03)

    [record] = engine.run_log.all()
    assert (record.status, record.attempts) == (RunStatus.FAILED, 1)
    assert "PermanentActionError: /gone does not exist" in (record.error or "")
    assert "run.retry" not in caplog.text


async def test_tc12_external_action_plugs_in() -> None:
    notify = NotifyAction()
    engine = engine_with({"bob": 1}, Clock(DAY1_NOON), notify)
    engine.scheduler.submit(
        {"user": "bob", "time": "12:00", "action": "notify", "target": "deploy-finished",
         "params": {"channel": "#ops"}}
    )  # fmt: skip

    async with engine:
        await run_for(engine, 0.03)

    assert notify.sent == [("#ops", "deploy-finished")]


async def test_tc13_schedule_follows_configured_timezone() -> None:
    clock = Clock(dt.datetime(2026, 11, 24, 3, 59, tzinfo=dt.UTC))
    engine = engine_with({"alice": 3}, clock, tz=ZoneInfo("Asia/Taipei"))
    engine.scheduler.submit({"user": "alice", "time": "12:00", "action": "sync", "target": "/x"})

    async with engine:
        await run_for(engine, 0.03)
        assert len(engine.run_log) == 0
        clock.now = clock.now.replace(hour=4, minute=0)
        await run_for(engine, 0.03)

    assert statuses(engine) == {("2026-11-24", RunStatus.SUCCESS): 1}


async def test_tc14_stop_drains_queued_jobs() -> None:
    slow = SlowAction()
    engine = engine_with({"alice": 50}, Clock(DAY1_NOON), slow, workers=2, queue_maxsize=100)
    engine.scheduler.submit_many(
        {"user": "alice", "time": "12:00", "action": "slow", "target": f"/t{i}",
         "params": {"delay_ms": 5}}
        for i in range(20)
    )  # fmt: skip

    await engine.start()
    assert await engine.tick() == 20
    await engine.stop()  # drain=True by default

    assert len(engine.run_log.with_status(RunStatus.SUCCESS)) == 20


async def test_tc15_ten_thousand_tasks_in_one_day(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.WARNING)
    users = {f"u{u:04d}": 10 for u in range(1000)}
    engine = engine_with(users, Clock(DAY1_NOON), workers=32, queue_maxsize=1000)
    engine.scheduler.submit_many(
        {"user": user, "time": f"{h:02d}:00", "action": "sync", "target": f"/{user}/{h}"}
        for user in users
        for h in range(10)
    )

    started = time.perf_counter()
    async with engine:
        await run_until(engine, lambda: len(engine.run_log) >= 10_000, limit_s=10)
    elapsed = time.perf_counter() - started

    assert statuses(engine) == {("2026-11-24", RunStatus.SUCCESS): 10_000}
    assert all(engine.scheduler.quotas.used(u, DAY1_NOON.date()) == 10 for u in users)
    assert elapsed < 10, f"10k runs took {elapsed:.2f}s"
    print(f"\nTC15: 10,000 runs in {elapsed:.2f}s")


async def test_tc16_shutdown_mid_tick_loses_no_work(caplog: pytest.LogCaptureFixture) -> None:
    """Regression: cancelling run_forever while tick is blocked on a full queue."""
    slow = SlowAction()
    engine = engine_with({"alice": 100}, Clock(DAY1_NOON), slow, workers=2, queue_maxsize=2)
    engine.scheduler.submit_many(
        {"user": "alice", "time": "12:00", "action": "slow", "target": f"/t{i}",
         "params": {"delay_ms": 5}}
        for i in range(50)
    )  # fmt: skip

    async with engine:
        await run_for(engine, 0.02)  # shutdown while most jobs are still waiting to enqueue
        first_run = len(engine.run_log)
        assert 0 < first_run < 50
        assert engine.scheduler.quotas.used("alice", DAY1_NOON.date()) == first_run
        assert "tick.cancelled" in caplog.text
        await run_until(engine, lambda: len(engine.run_log) >= 50, limit_s=5)  # restart

    assert statuses(engine) == {("2026-11-24", RunStatus.SUCCESS): 50}
    assert set(slow.calls.values()) == {1}  # every task exactly once
    assert engine.scheduler.quotas.used("alice", DAY1_NOON.date()) == 50
