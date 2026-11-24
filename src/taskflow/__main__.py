"""Demo: the legacy scenario on the refactored engine. Run: uv run python -m taskflow"""

from __future__ import annotations

import asyncio
import datetime as dt
import logging
from collections import Counter

from taskflow import TaskEngine, build_engine, configure_logging

logger = logging.getLogger("taskflow.demo")

USERS = {"alice": 3, "bob": 5}

TASKS = [
    # legacy tasks
    {"user": "alice", "time": "12:00", "action": "sync", "target": "/data/x"},
    {"user": "bob", "time": "12:00", "action": "backup", "target": "/srv/y"},
    {"user": "alice", "time": "12:00", "action": "delete", "target": "/tmp/z"},
    # extra: configurable params, and enough alice tasks to hit her quota of 3
    {"user": "bob", "time": "12:00", "action": "backup", "target": "/srv/db",
     "params": {"retention_days": 30, "compress": False}},
    {"user": "alice", "time": "12:00", "action": "sync", "target": "/data/w",
     "params": {"delete_extraneous": True}},
    {"user": "alice", "time": "12:00", "action": "sync", "target": "/data/v"},
]  # fmt: skip


async def main() -> None:
    configure_logging()
    engine = build_engine(USERS)
    engine.scheduler.submit_many(TASKS)
    noon = dt.datetime(2026, 11, 24, 12, 0, tzinfo=dt.UTC)

    async with engine:
        for now in (noon, noon + dt.timedelta(seconds=30), noon + dt.timedelta(days=1)):
            logger.info("demo.tick now=%s", now.isoformat())
            await engine.tick(now)
            await engine.drain()

    for day, statuses in _summary(engine).items():
        logger.info("demo.summary day=%s %s", day, dict(statuses))


def _summary(engine: TaskEngine) -> dict[str, Counter[str]]:
    out: dict[str, Counter[str]] = {}
    for record in engine.run_log.all():
        out.setdefault(record.run_date.isoformat(), Counter())[record.status.value] += 1
    return out


if __name__ == "__main__":
    asyncio.run(main())
