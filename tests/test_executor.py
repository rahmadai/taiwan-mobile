"""AC5, AC6 at the executor level: retry, backoff, timeout, permanent errors."""

from __future__ import annotations

import asyncio
import random
from typing import Any

import pytest
from pydantic import BaseModel

from taskflow import Action, RetryPolicy, RunStatus, TaskExecutor
from taskflow.actions import ActionRegistry, default_registry
from taskflow.models import Task, TaskSpec

from .helpers import FAST, FlakyAction, FlakyParams, RecordingSleep


def _task(action: str, params: BaseModel, target: str = "/x") -> Task:
    spec = TaskSpec.model_validate(
        {"user": "alice", "time": "12:00", "action": action, "target": target}
    )
    return Task(id="task-0001", seq=1, spec=spec, params=params)


def _executor(
    *actions: Action[Any], policy: RetryPolicy = FAST
) -> tuple[TaskExecutor, RecordingSleep]:
    registry = default_registry()
    for action in actions:
        registry.register(action)
    sleep = RecordingSleep()
    return TaskExecutor(registry, policy, sleep=sleep, rng=random.Random(0)), sleep


async def test_transient_failures_are_retried_until_success() -> None:
    policy = RetryPolicy(max_attempts=3, base_delay=1.0, max_delay=10.0)
    executor, sleep = _executor(FlakyAction(), policy=policy)

    result = await executor.execute(_task("flaky", FlakyParams(fail_times=2)))

    assert (result.status, result.attempts) == (RunStatus.SUCCESS, 3)
    assert len(sleep.delays) == 2
    assert 0.5 <= sleep.delays[0] <= 1.0
    assert 1.0 <= sleep.delays[1] <= 2.0


async def test_persistent_failure_gives_up_after_max_attempts(
    caplog: pytest.LogCaptureFixture,
) -> None:
    executor, sleep = _executor(FlakyAction())

    result = await executor.execute(_task("flaky", FlakyParams(fail_times=99)))

    assert (result.status, result.attempts) == (RunStatus.FAILED, 3)
    assert result.error == "ActionError: transient failure #3 on /x"
    assert len(sleep.delays) == 2
    assert "run.failed task_id=task-0001 user=alice action=flaky attempts=3" in caplog.text


async def test_permanent_error_is_not_retried() -> None:
    executor, sleep = _executor(FlakyAction())

    result = await executor.execute(_task("flaky", FlakyParams(permanent=True)))

    assert (result.status, result.attempts) == (RunStatus.FAILED, 1)
    assert sleep.delays == []


class _Hang(Action[FlakyParams]):
    name = "hang"
    params_model = FlakyParams

    async def execute(self, target: str, params: FlakyParams) -> None:
        await asyncio.sleep(10)


async def test_hung_action_times_out_each_attempt() -> None:
    policy = RetryPolicy(max_attempts=2, base_delay=0, max_delay=0, attempt_timeout=0.01)
    executor, _ = _executor(_Hang(), policy=policy)

    result = await executor.execute(_task("hang", FlakyParams()))

    assert (result.status, result.attempts) == (RunStatus.FAILED, 2)
    assert result.error == "timed out after 0.01s"


@pytest.mark.parametrize("attempt", [1, 2, 3, 4, 5, 6])
def test_backoff_is_capped_exponential_with_jitter(attempt: int) -> None:
    policy = RetryPolicy(base_delay=1.0, max_delay=8.0)
    cap = min(8.0, 2.0 ** (attempt - 1))

    delay = policy.backoff(attempt, random.Random(attempt))

    assert cap / 2 <= delay <= cap


@pytest.mark.parametrize(
    "kwargs",
    [{"max_attempts": 0}, {"base_delay": -1}, {"attempt_timeout": 0}],
)
def test_retry_policy_rejects_bad_config(kwargs: dict[str, float]) -> None:
    with pytest.raises(ValueError):  # noqa: PT011
        RetryPolicy(**kwargs)  # type: ignore[arg-type]


def test_registry_rejects_duplicate_names() -> None:
    registry = ActionRegistry([FlakyAction()])
    with pytest.raises(ValueError, match="already registered"):
        registry.register(FlakyAction())
    registry.register(FlakyAction(), replace=True)
    assert "flaky" in registry
