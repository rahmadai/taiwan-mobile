"""Action strategies and their registry.

Adding an action = subclass ``Action``, set ``name`` + ``params_model``,
implement ``execute``, and register it. Nothing else changes.
"""

from __future__ import annotations

import asyncio
import logging
from abc import ABC, abstractmethod
from collections.abc import Iterable, Mapping
from typing import Any, ClassVar

from pydantic import BaseModel, ConfigDict, Field

from taskflow.errors import UnknownActionError

logger = logging.getLogger(__name__)


class Action[P: BaseModel](ABC):
    """Strategy interface for one kind of task action."""

    name: ClassVar[str]
    params_model: type[P]

    def parse_params(self, raw: Mapping[str, Any]) -> P:
        """Validate raw params; raises pydantic.ValidationError on bad input."""
        return self.params_model.model_validate(raw)

    @abstractmethod
    async def execute(self, target: str, params: P) -> None:
        """Run the action. Raise ActionError (retryable) or PermanentActionError."""


class _Params(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class SyncParams(_Params):
    delete_extraneous: bool = False


class BackupParams(_Params):
    retention_days: int = Field(default=7, ge=1)
    compress: bool = True


class DeleteParams(_Params):
    recursive: bool = False


class SyncAction(Action[SyncParams]):
    name = "sync"
    params_model = SyncParams

    async def execute(self, target: str, params: SyncParams) -> None:
        await asyncio.sleep(0)  # simulated I/O
        logger.info(
            "action.sync target=%s delete_extraneous=%s simulated=true",
            target,
            params.delete_extraneous,
        )


class BackupAction(Action[BackupParams]):
    name = "backup"
    params_model = BackupParams

    async def execute(self, target: str, params: BackupParams) -> None:
        await asyncio.sleep(0)
        logger.info(
            "action.backup target=%s retention_days=%d compress=%s simulated=true",
            target,
            params.retention_days,
            params.compress,
        )


class DeleteAction(Action[DeleteParams]):
    name = "delete"
    params_model = DeleteParams

    async def execute(self, target: str, params: DeleteParams) -> None:
        await asyncio.sleep(0)
        logger.info("action.delete target=%s recursive=%s simulated=true", target, params.recursive)


class ActionRegistry:
    """Maps action names to strategy instances."""

    def __init__(self, actions: Iterable[Action[Any]] = ()) -> None:
        self._actions: dict[str, Action[Any]] = {}
        for action in actions:
            self.register(action)

    def register(self, action: Action[Any], *, replace: bool = False) -> None:
        if action.name in self._actions and not replace:
            raise ValueError(f"action {action.name!r} is already registered")
        self._actions[action.name] = action

    def get(self, name: str) -> Action[Any]:
        try:
            return self._actions[name]
        except KeyError:
            raise UnknownActionError(
                f"unknown action {name!r}; registered: {', '.join(self.names())}"
            ) from None

    def names(self) -> list[str]:
        return sorted(self._actions)

    def __contains__(self, name: object) -> bool:
        return name in self._actions


def default_registry() -> ActionRegistry:
    return ActionRegistry([SyncAction(), BackupAction(), DeleteAction()])
