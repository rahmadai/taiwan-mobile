"""Async engine: scheduler -> bounded queue -> worker pool -> run log."""

from __future__ import annotations

import asyncio
import contextlib
import datetime as dt
import logging
from collections.abc import Callable
from types import TracebackType
from typing import Self

from taskflow.executor import ExecutionResult, TaskExecutor
from taskflow.models import RunRecord, RunStatus
from taskflow.run_log import RunLog
from taskflow.scheduler import Job, Scheduler

logger = logging.getLogger(__name__)

Clock = Callable[[], dt.datetime]


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.UTC)


class TaskEngine:
    """Wires the scheduler to N concurrent workers through a bounded queue.

    The queue is the seam for scaling out: replace it with SQS/Redis and run
    workers as separate processes (see steering/architecture.md).
    """

    def __init__(
        self,
        scheduler: Scheduler,
        executor: TaskExecutor,
        run_log: RunLog,
        *,
        workers: int = 4,
        queue_maxsize: int = 1000,
        clock: Clock = utc_now,
    ) -> None:
        if workers < 1:
            raise ValueError(f"workers must be >= 1, got {workers}")
        self.scheduler = scheduler
        self.run_log = run_log
        self._executor = executor
        self._worker_count = workers
        self._clock = clock
        self._queue: asyncio.Queue[Job] = asyncio.Queue(maxsize=queue_maxsize)
        self._workers: list[asyncio.Task[None]] = []

    @property
    def running(self) -> bool:
        return bool(self._workers)

    async def start(self) -> None:
        if self._workers:
            return
        self._workers = [
            asyncio.create_task(self._worker(i), name=f"taskflow-worker-{i}")
            for i in range(self._worker_count)
        ]
        logger.info("engine.started workers=%d", self._worker_count)

    async def tick(self, now: dt.datetime | None = None) -> int:
        """Dispatch due jobs to workers; returns how many were enqueued."""
        if not self._workers:
            raise RuntimeError("engine is not started; call start() or use 'async with'")
        jobs = await self.scheduler.tick(now if now is not None else self._clock())
        for job in jobs:
            await self._queue.put(job)  # blocks when full -> backpressure
        return len(jobs)

    async def drain(self) -> None:
        """Wait until every enqueued job has been executed and recorded."""
        await self._queue.join()

    async def stop(self, *, drain: bool = True) -> None:
        if drain:
            await self.drain()
        for worker in self._workers:
            worker.cancel()
        await asyncio.gather(*self._workers, return_exceptions=True)
        self._workers = []
        logger.info("engine.stopped")

    async def run_forever(self, interval: float = 30.0) -> None:
        """Tick on the clock every ``interval`` seconds until cancelled."""
        while True:
            try:
                await self.tick()
            except Exception:
                logger.exception("engine.tick_failed")
            await asyncio.sleep(interval)

    async def __aenter__(self) -> Self:
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        with contextlib.suppress(asyncio.CancelledError):
            await self.stop(drain=exc_type is None)

    async def _worker(self, index: int) -> None:
        while True:
            job = await self._queue.get()
            try:
                try:
                    result = await self._executor.execute(job.task)
                except Exception as exc:  # guard: a bug must not kill the worker
                    logger.exception("worker.unhandled worker=%d task_id=%s", index, job.task.id)
                    result = ExecutionResult(RunStatus.FAILED, 0, f"{type(exc).__name__}: {exc}")
                self.run_log.record(
                    RunRecord(
                        task_id=job.task.id,
                        user=job.task.user,
                        action=job.task.action,
                        run_date=job.run_date,
                        status=result.status,
                        attempts=result.attempts,
                        error=result.error,
                    )
                )
            finally:
                self._queue.task_done()
