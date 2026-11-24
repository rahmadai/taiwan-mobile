"""AC1 + quota atomicity."""

from __future__ import annotations

import asyncio
import datetime as dt

import pytest

from taskflow import QuotaManager, UnknownUserError

DAY = dt.date(2026, 11, 24)


async def test_quota_is_per_day_and_resets_next_day() -> None:
    quotas = QuotaManager({"alice": 2})

    assert [await quotas.try_consume("alice", DAY) for _ in range(3)] == [True, True, False]
    assert quotas.remaining("alice", DAY) == 0
    assert await quotas.try_consume("alice", DAY + dt.timedelta(days=1))


async def test_concurrent_consumers_never_exceed_quota() -> None:
    quotas = QuotaManager({"alice": 3})

    results = await asyncio.gather(*(quotas.try_consume("alice", DAY) for _ in range(20)))

    assert results.count(True) == 3
    assert quotas.used("alice", DAY) == 3


async def test_unknown_user_raises() -> None:
    with pytest.raises(UnknownUserError, match="'ghost'"):
        await QuotaManager().try_consume("ghost", DAY)


@pytest.mark.parametrize(("user", "quota"), [("", 1), ("alice", -1)])
def test_register_rejects_bad_config(user: str, quota: int) -> None:
    with pytest.raises(ValueError, match=r"user|daily_quota"):
        QuotaManager().register(user, quota)


async def test_prune_drops_only_old_days() -> None:
    quotas = QuotaManager({"alice": 5})
    await quotas.try_consume("alice", DAY - dt.timedelta(days=1))
    await quotas.try_consume("alice", DAY)

    assert quotas.prune_before(DAY) == 1
    assert quotas.used("alice", DAY) == 1


async def test_release_refunds_one_unit_and_rejects_over_release() -> None:
    quotas = QuotaManager({"alice": 1})
    assert await quotas.try_consume("alice", DAY)

    quotas.release("alice", DAY)

    assert quotas.used("alice", DAY) == 0
    assert await quotas.try_consume("alice", DAY)
    quotas.release("alice", DAY)
    with pytest.raises(ValueError, match="nothing to release"):
        quotas.release("alice", DAY)
