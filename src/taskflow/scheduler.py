"""Scheduling: task registration and deciding which runs are due.

Quota admission happens here, at dispatch, in submission order. That keeps
"who wins when quota is short" deterministic regardless of worker timing
(see steering/adr/0001-quota-admission-at-dispatch.md).
"""

from __future__ import annotations

import datetime as dt
import itertools
import logging
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from pydantic import ValidationError

from taskflow.actions import ActionRegistry
from taskflow.errors import InvalidTaskError, UnknownUserError
from taskflow.models import RunRecord, RunStatus, Task, TaskSpec
from taskflow.quota import QuotaManager
from taskflow.run_log import RunLog

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class Job:
    """One admitted run of a task, ready for a worker."""

    task: Task
    run_date: dt.date


class Scheduler:
    """Holds tasks and turns a clock reading into admitted jobs.

    A task is due when its time-of-day has passed and it has not been
    dispatched yet today, so it runs at most once per day and a missed slot
    catches up on the next tick.
    """

    def __init__(
        self,
        quotas: QuotaManager,
        registry: ActionRegistry,
        run_log: RunLog,
        *,
        tz: dt.tzinfo = dt.UTC,
    ) -> None:
        self.quotas = quotas
        self._registry = registry
        self._run_log = run_log
        self._tz = tz
        self._tasks: dict[str, Task] = {}
        self._last_dispatched: dict[str, dt.date] = {}
        self._seq = itertools.count()
        self._current_day: dt.date | None = None

    def submit(self, raw: Mapping[str, Any]) -> Task:
        """Validate a task dict and register it. Raises InvalidTaskError."""
        try:
            spec = TaskSpec.model_validate(raw)
        except ValidationError as exc:
            raise InvalidTaskError(f"invalid task: {exc}") from exc
        if not self.quotas.has_user(spec.user):
            raise UnknownUserError(f"unknown user {spec.user!r}")
        action = self._registry.get(spec.action)
        try:
            params = action.parse_params(spec.params)
        except ValidationError as exc:
            raise InvalidTaskError(f"invalid params for action {spec.action!r}: {exc}") from exc

        seq = next(self._seq)
        task = Task(id=f"task-{seq:04d}", seq=seq, spec=spec, params=params)
        self._tasks[task.id] = task
        logger.info(
            "task.submitted task_id=%s user=%s action=%s time=%s target=%s",
            task.id,
            task.user,
            task.action,
            spec.time.strftime("%H:%M"),
            task.target,
        )
        return task

    def submit_many(self, raws: Iterable[Mapping[str, Any]]) -> list[Task]:
        return [self.submit(raw) for raw in raws]

    def tasks(self) -> list[Task]:
        return list(self._tasks.values())

    def tasks_for(self, user: str) -> list[Task]:
        return [t for t in self._tasks.values() if t.user == user]

    async def tick(self, now: dt.datetime) -> list[Job]:
        """Return jobs admitted at ``now`` (timezone-aware), in submission order."""
        if now.tzinfo is None:
            raise ValueError("now must be timezone-aware (e.g. datetime.now(datetime.UTC))")
        local = now.astimezone(self._tz)
        today, time_of_day = local.date(), local.time()
        self._roll_day(today)

        jobs: list[Job] = []
        for task in self._tasks.values():  # dict order == submission order
            if self._last_dispatched.get(task.id) == today or task.spec.time > time_of_day:
                continue
            self._last_dispatched[task.id] = today
            if await self.quotas.try_consume(task.user, today):
                jobs.append(Job(task=task, run_date=today))
                continue
            logger.warning(
                "run.quota_exceeded task_id=%s user=%s action=%s day=%s used=%d quota=%d",
                task.id,
                task.user,
                task.action,
                today.isoformat(),
                self.quotas.used(task.user, today),
                self.quotas.daily_quota(task.user),
            )
            self._run_log.record(
                RunRecord(
                    task_id=task.id,
                    user=task.user,
                    action=task.action,
                    run_date=today,
                    status=RunStatus.QUOTA_EXCEEDED,
                    attempts=0,
                    error="daily quota exhausted",
                )
            )
        if jobs:
            logger.info("tick.dispatched day=%s jobs=%d", today.isoformat(), len(jobs))
        return jobs

    def _roll_day(self, today: dt.date) -> None:
        if self._current_day != today:
            self._current_day = today
            self.quotas.prune_before(today)
