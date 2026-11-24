# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

taskflow: async, strategy-based daily task scheduler with per-user quotas (Python 3.12, uv).
It is the answer to an AI-assisted refactoring coding test; the brief's decisions are in
`specs/20261124-scheduler-refactor/requirements.md`.

## Commands

```bash
make check                      # all gates: ruff format --check, ruff check, pyright, pytest
make fmt                        # ruff format + ruff check --fix
make demo                       # uv run python -m taskflow
make e2e                        # black-box suite, cases in e2e/TEST_CASES.md (not part of make check)
uv run pytest tests/test_scheduler.py::test_tick_twice_in_same_minute_dispatches_once --no-cov
uv run pytest -k quota --no-cov # subset without the coverage gate
```

Coverage gate is 90% (`--cov-fail-under` in pyproject addopts), so running a subset needs `--no-cov`.
Pyright config is in `pyrightconfig.json` on purpose (strict on `src/`) — see conventions.

## Architecture (read before changing behaviour)

`Scheduler.tick(now)` → quota admission (`QuotaManager.try_consume`) in submission order →
bounded `asyncio.Queue[Job]` → N workers in `TaskEngine` → `TaskExecutor` (timeout + retry) →
`Action` strategy from `ActionRegistry` → `RunRecord` in `RunLog`. Wiring only in `bootstrap.py`.

Invariants that tests enforce:
- Quota: 1 unit per run, taken at dispatch (not in workers); retries free; keyed `(user, day)`.
- A task runs at most once per day; missed slots catch up; `now` must be timezone-aware.
- Actions know nothing about quota/scheduling; new action = new `Action` subclass + register.
- A cancelled engine tick must release admitted-but-unqueued jobs (no silent loss) — e2e TC16.

Details: `steering/architecture.md`, `steering/conventions.md` (logging format, error types,
testing with injected clock/sleep/rng), `steering/adr/`.

## Workflow

Spec → steering → code. Non-trivial changes get `specs/YYYYMMDD-<feature>/{requirements,design,tasks}.md`;
on completion promote durable knowledge to `steering/` and add an ADR for real trade-offs.
Record AI suggestions that were rejected or corrected in `AI_USAGE.md`.
