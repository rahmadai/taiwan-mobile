"""Composition root: build a ready-to-start engine from plain config."""

from __future__ import annotations

import asyncio
import datetime as dt
import logging
import random
from collections.abc import Mapping

from taskflow.actions import ActionRegistry, default_registry
from taskflow.engine import Clock, TaskEngine, utc_now
from taskflow.executor import RetryPolicy, Sleep, TaskExecutor
from taskflow.quota import QuotaManager
from taskflow.run_log import RunLog
from taskflow.scheduler import Scheduler


def build_engine(
    users: Mapping[str, int],
    *,
    registry: ActionRegistry | None = None,
    policy: RetryPolicy | None = None,
    workers: int = 4,
    queue_maxsize: int = 1000,
    tz: dt.tzinfo = dt.UTC,
    clock: Clock = utc_now,
    sleep: Sleep = asyncio.sleep,
    rng: random.Random | None = None,
) -> TaskEngine:
    """``users`` maps user name -> daily quota."""
    registry = registry or default_registry()
    run_log = RunLog()
    scheduler = Scheduler(QuotaManager(users), registry, run_log, tz=tz)
    executor = TaskExecutor(registry, policy, sleep=sleep, rng=rng)
    return TaskEngine(
        scheduler, executor, run_log, workers=workers, queue_maxsize=queue_maxsize, clock=clock
    )


def configure_logging(level: int | str = logging.INFO) -> None:
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)-7s %(name)s %(message)s")
