"""In-memory history of run outcomes."""

from __future__ import annotations

from taskflow.models import RunRecord, RunStatus


class RunLog:
    def __init__(self) -> None:
        self._records: list[RunRecord] = []

    def record(self, record: RunRecord) -> None:
        self._records.append(record)

    def all(self) -> list[RunRecord]:
        return list(self._records)

    def for_task(self, task_id: str) -> list[RunRecord]:
        return [r for r in self._records if r.task_id == task_id]

    def with_status(self, status: RunStatus) -> list[RunRecord]:
        return [r for r in self._records if r.status is status]

    def __len__(self) -> int:
        return len(self._records)
