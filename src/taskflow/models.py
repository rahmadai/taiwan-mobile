"""Task data model: validated input, registered task, and run outcomes."""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


class TaskSpec(BaseModel):
    """A user-submitted task definition, validated from a plain dict.

    ``params`` stays a raw mapping here; it is validated against the chosen
    action's own params model when the task is submitted.
    """

    model_config = ConfigDict(frozen=True, extra="forbid", str_strip_whitespace=True)

    user: str = Field(min_length=1)
    time: dt.time
    action: str = Field(min_length=1)
    target: str = Field(min_length=1)
    params: dict[str, Any] = Field(default_factory=dict)

    @field_validator("time", mode="before")
    @classmethod
    def _parse_hhmm(cls, value: object) -> object:
        if isinstance(value, str):
            try:
                return dt.datetime.strptime(value, "%H:%M").time()
            except ValueError:
                raise ValueError(f"time must be 'HH:MM' (24h), got {value!r}") from None
        return value


@dataclass(frozen=True, slots=True)
class Task:
    """A validated, registered task. ``seq`` is submission order (used for fairness)."""

    id: str
    seq: int
    spec: TaskSpec
    params: BaseModel

    @property
    def user(self) -> str:
        return self.spec.user

    @property
    def action(self) -> str:
        return self.spec.action

    @property
    def target(self) -> str:
        return self.spec.target


class RunStatus(StrEnum):
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    QUOTA_EXCEEDED = "QUOTA_EXCEEDED"


@dataclass(frozen=True, slots=True)
class RunRecord:
    """Outcome of one scheduled run of a task on a given day."""

    task_id: str
    user: str
    action: str
    run_date: dt.date
    status: RunStatus
    attempts: int
    error: str | None = None
