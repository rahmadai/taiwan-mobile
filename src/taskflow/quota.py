"""User management and daily quota control."""

from __future__ import annotations

import asyncio
import datetime as dt
import logging
from collections.abc import Mapping

from taskflow.errors import UnknownUserError

logger = logging.getLogger(__name__)


class QuotaManager:
    """Per-user daily run quotas.

    Usage is keyed by ``(user, day)``, so quotas reset at the day boundary
    without a reset job. ``try_consume`` is the only mutation and is atomic.
    """

    def __init__(self, quotas: Mapping[str, int] | None = None) -> None:
        self._quotas: dict[str, int] = {}
        self._usage: dict[tuple[str, dt.date], int] = {}
        self._lock = asyncio.Lock()
        for user, daily_quota in (quotas or {}).items():
            self.register(user, daily_quota)

    def register(self, user: str, daily_quota: int) -> None:
        if not user:
            raise ValueError("user name must be non-empty")
        if daily_quota < 0:
            raise ValueError(f"daily_quota must be >= 0, got {daily_quota} for user {user!r}")
        self._quotas[user] = daily_quota
        logger.info("quota.registered user=%s daily_quota=%d", user, daily_quota)

    def has_user(self, user: str) -> bool:
        return user in self._quotas

    def daily_quota(self, user: str) -> int:
        try:
            return self._quotas[user]
        except KeyError:
            raise UnknownUserError(f"unknown user {user!r}") from None

    def used(self, user: str, day: dt.date) -> int:
        return self._usage.get((user, day), 0)

    def remaining(self, user: str, day: dt.date) -> int:
        return max(0, self.daily_quota(user) - self.used(user, day))

    async def try_consume(self, user: str, day: dt.date) -> bool:
        """Atomically take one unit of ``user``'s quota for ``day``.

        Returns False (and changes nothing) if the quota is exhausted.
        """
        async with self._lock:
            quota = self.daily_quota(user)
            used = self.used(user, day)
            if used >= quota:
                return False
            self._usage[(user, day)] = used + 1
            return True

    def release(self, user: str, day: dt.date) -> None:
        """Return one unit taken by ``try_consume`` for a run that never started.

        Synchronous on purpose: it must work inside cancellation handlers. It has no
        await, so it is atomic on the event loop, and ``try_consume`` never holds the
        lock across a suspension point, so the two cannot interleave.
        """
        used = self.used(user, day)
        if used <= 0:
            raise ValueError(f"nothing to release for user {user!r} on {day.isoformat()}")
        self._usage[(user, day)] = used - 1

    def prune_before(self, day: dt.date) -> int:
        """Drop usage counters older than ``day``; returns how many were removed."""
        stale = [key for key in self._usage if key[1] < day]
        for key in stale:
            del self._usage[key]
        return len(stale)
