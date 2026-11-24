from __future__ import annotations

import datetime as dt
import random
from collections import defaultdict
from collections.abc import Mapping
from typing import Any

from pydantic import BaseModel, ConfigDict

from taskflow import (
    Action,
    ActionError,
    ActionRegistry,
    PermanentActionError,
    RetryPolicy,
    TaskEngine,
    build_engine,
    default_registry,
)

NOON = dt.datetime(2026, 11, 24, 12, 0, tzinfo=dt.UTC)
FAST = RetryPolicy(max_attempts=3, base_delay=0.0, max_delay=0.0, attempt_timeout=1.0)


class FlakyParams(BaseModel):
    model_config = ConfigDict(extra="forbid")
    fail_times: int = 0
    permanent: bool = False


class FlakyAction(Action[FlakyParams]):
    """Test strategy: fails the first ``fail_times`` calls per target."""

    name = "flaky"
    params_model = FlakyParams

    def __init__(self) -> None:
        self.calls: defaultdict[str, int] = defaultdict(int)

    async def execute(self, target: str, params: FlakyParams) -> None:
        self.calls[target] += 1
        if params.permanent:
            raise PermanentActionError(f"{target} does not exist")
        if self.calls[target] <= params.fail_times:
            raise ActionError(f"transient failure #{self.calls[target]} on {target}")


class RecordingSleep:
    def __init__(self) -> None:
        self.delays: list[float] = []

    async def __call__(self, delay: float) -> None:
        self.delays.append(delay)


def registry_with(*actions: Action[Any]) -> ActionRegistry:
    registry = default_registry()
    for action in actions:
        registry.register(action)
    return registry


def make_engine(
    users: Mapping[str, int],
    *,
    registry: ActionRegistry | None = None,
    policy: RetryPolicy = FAST,
    workers: int = 4,
    tz: dt.tzinfo = dt.UTC,
) -> TaskEngine:
    return build_engine(
        users,
        registry=registry,
        policy=policy,
        workers=workers,
        tz=tz,
        sleep=RecordingSleep(),
        rng=random.Random(0),
    )


def task(
    user: str = "alice", time: str = "12:00", action: str = "sync", **kw: object
) -> dict[str, object]:
    return {
        "user": user,
        "time": time,
        "action": action,
        "target": kw.pop("target", "/data/x"),
        **kw,
    }
