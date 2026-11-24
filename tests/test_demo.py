"""Smoke test: the legacy-scenario demo runs end to end."""

from __future__ import annotations

import logging

import pytest

from taskflow.__main__ import main


async def test_demo_runs_and_shows_quota_and_daily_reset(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO)
    await main()

    assert "demo.summary day=2026-11-24 {'QUOTA_EXCEEDED': 1, 'SUCCESS': 5}" in caplog.text
    assert "demo.summary day=2026-11-25 {'QUOTA_EXCEEDED': 1, 'SUCCESS': 5}" in caplog.text
