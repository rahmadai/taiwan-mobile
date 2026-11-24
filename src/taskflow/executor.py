"""Task executor: runs one task's action with timeout and retry/backoff."""

from __future__ import annotations

import asyncio
import logging
import random
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from taskflow.actions import ActionRegistry
from taskflow.errors import PermanentActionError
from taskflow.models import RunStatus, Task

logger = logging.getLogger(__name__)

Sleep = Callable[[float], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """Retry with capped exponential backoff and jitter; each attempt is time-boxed."""

    max_attempts: int = 3
    base_delay: float = 0.5
    max_delay: float = 5.0
    attempt_timeout: float = 10.0

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError(f"max_attempts must be >= 1, got {self.max_attempts}")
        if self.base_delay < 0 or self.max_delay < 0:
            raise ValueError("delays must be >= 0")
        if self.attempt_timeout <= 0:
            raise ValueError(f"attempt_timeout must be > 0, got {self.attempt_timeout}")

    def backoff(self, attempt: int, rng: random.Random) -> float:
        """Delay after failed ``attempt`` (1-based): uniform in [cap/2, cap]."""
        cap = min(self.max_delay, self.base_delay * 2 ** (attempt - 1))
        return rng.uniform(cap / 2, cap)


@dataclass(frozen=True, slots=True)
class ExecutionResult:
    status: RunStatus
    attempts: int
    error: str | None = None


class TaskExecutor:
    """Executes a task's action strategy. Never raises for action failures."""

    def __init__(
        self,
        registry: ActionRegistry,
        policy: RetryPolicy | None = None,
        *,
        sleep: Sleep = asyncio.sleep,
        rng: random.Random | None = None,
    ) -> None:
        self._registry = registry
        self._policy = policy or RetryPolicy()
        self._sleep = sleep
        self._rng = rng or random.Random()

    async def execute(self, task: Task) -> ExecutionResult:
        action = self._registry.get(task.action)
        policy = self._policy
        ctx = (task.id, task.user, task.action)

        for attempt in range(1, policy.max_attempts + 1):
            try:
                async with asyncio.timeout(policy.attempt_timeout):
                    await action.execute(task.target, task.params)
            except PermanentActionError as exc:
                error = _describe(exc)
                logger.error(
                    "run.failed task_id=%s user=%s action=%s attempts=%d retryable=false error=%r",
                    *ctx,
                    attempt,
                    error,
                )
                return ExecutionResult(RunStatus.FAILED, attempt, error)
            except Exception as exc:  # actions are plugins: isolate any failure
                error = (
                    f"timed out after {policy.attempt_timeout}s"
                    if isinstance(exc, TimeoutError)
                    else _describe(exc)
                )
                if attempt == policy.max_attempts:
                    logger.error(
                        "run.failed task_id=%s user=%s action=%s attempts=%d error=%r",
                        *ctx,
                        attempt,
                        error,
                    )
                    return ExecutionResult(RunStatus.FAILED, attempt, error)
                delay = policy.backoff(attempt, self._rng)
                logger.warning(
                    "run.retry task_id=%s user=%s action=%s attempt=%d/%d delay=%.2fs error=%r",
                    *ctx,
                    attempt,
                    policy.max_attempts,
                    delay,
                    error,
                )
                await self._sleep(delay)
            else:
                logger.info("run.success task_id=%s user=%s action=%s attempts=%d", *ctx, attempt)
                return ExecutionResult(RunStatus.SUCCESS, attempt)

        raise AssertionError("unreachable: RetryPolicy guarantees max_attempts >= 1")


def _describe(exc: BaseException) -> str:
    return f"{type(exc).__name__}: {exc}"
