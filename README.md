# taskflow

Refactor of a legacy single-function task runner into a modular, async, strategy-based
daily scheduler with per-user resource quotas.

```bash
make install   # uv sync
make demo      # legacy scenario: quota hit, no double run, next-day reset
make check     # ruff format/lint, pyright, pytest (+coverage gate)
```

## Usage

```python
import asyncio
from taskflow import build_engine

engine = build_engine({"alice": 3, "bob": 5})  # user -> daily quota
engine.scheduler.submit(
    {
        "user": "bob",
        "time": "12:00",
        "action": "backup",
        "target": "/srv/y",
        "params": {"retention_days": 30},
    }
)


async def main():
    async with engine:  # starts 4 workers
        await engine.run_forever(interval=30)  # or: await engine.tick(now)


asyncio.run(main())
```

Invalid input fails at `submit` with `InvalidTaskError` (bad `HH:MM`, unknown user/action,
bad or unknown params). Outcomes are in `engine.run_log` and in logs.

## Design in one paragraph

`Scheduler.tick(now)` picks tasks whose time has passed and haven't been dispatched today,
admits them against `QuotaManager` in submission order, and puts admitted `Job`s on a
bounded `asyncio.Queue`. N workers pull jobs and call `TaskExecutor`, which runs the task's
`Action` strategy under a timeout with capped exponential backoff. Every outcome
(`SUCCESS` / `FAILED` / `QUOTA_EXCEEDED`) is logged with `task_id user action` and recorded.

## What changed vs. the legacy code

| Legacy behaviour | Now |
|---|---|
| quota counter never resets | per-day quota, resets at midnight (configurable tz, default UTC) |
| `time == now` — missed minute skips, two calls in a minute double-run | at most once per day, missed slots catch up |
| one hard-coded action path, `print` | `Action` strategies with typed params, `logging` |
| no validation | pydantic validation at submit |
| sync, no failure handling | async worker pool, timeout, retry with backoff, failure isolation |

## Docs

- Architecture, scaling path, reusable modules: [steering/architecture.md](steering/architecture.md)
- Conventions: [steering/conventions.md](steering/conventions.md)
- Decisions: [steering/adr/](steering/adr/)
- Spec (requirements, design, tasks): [specs/20261124-scheduler-refactor/](specs/20261124-scheduler-refactor/)
- How AI was used and where its output was rejected: [AI_USAGE.md](AI_USAGE.md)
