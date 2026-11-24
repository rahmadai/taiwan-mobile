# Architecture

taskflow receives user-submitted task strategies, runs each at most once per day at its
time-of-day, and limits each user to a daily run quota.

## Components

```
 dict ──submit──▶ Scheduler ──tick(now)──▶ asyncio.Queue[Job] ──▶ Worker × N ──▶ TaskExecutor ──▶ Action
                    │   ▲                    (bounded:                │              (retry,       (strategy,
                    │   └── QuotaManager      backpressure)           │               timeout)      from registry)
                    │       try_consume                               ▼
                    └──── QUOTA_EXCEEDED ──────────────────────────▶ RunLog ◀── SUCCESS / FAILED
```

| Module | Responsibility | Knows about |
|---|---|---|
| `models.py` | `TaskSpec` (validated input), `Task`, `RunStatus`, `RunRecord` | pydantic only |
| `quota.py` | `QuotaManager`: users, daily quota, atomic `try_consume(user, day)` | nothing else |
| `actions.py` | `Action[P]` strategy base, built-ins, `ActionRegistry` | pydantic |
| `executor.py` | `TaskExecutor`: run one task's action with timeout + retry/backoff | registry, models |
| `scheduler.py` | `Scheduler`: register tasks, compute due runs, **admit against quota** | quota, registry, run log |
| `engine.py` | `TaskEngine`: queue + worker pool + lifecycle (`start/tick/drain/stop/run_forever`) | scheduler, executor |
| `bootstrap.py` | `build_engine(...)`: the only place concrete classes are wired | everything |

Dependencies point one way: engine → scheduler/executor → quota/actions → models.
Actions never see quota or scheduling; the scheduler never sees how actions run.

## Run lifecycle

1. `submit(dict)` → `TaskSpec` validation → user known? → action known? → action params
   validated by that action's `params_model`. Any failure raises `InvalidTaskError` and
   nothing is registered.
2. `tick(now)` (aware datetime, converted to the configured tz): for each task in
   submission order, if `time <= now.time()` and not dispatched today → mark dispatched,
   then `try_consume`. Admitted → `Job`; refused → `QUOTA_EXCEEDED` record (once per day).
3. Workers pull jobs; `TaskExecutor` runs the action under `asyncio.timeout`, retrying
   `ActionError`/any exception with capped exponential backoff + jitter, not retrying
   `PermanentActionError`. Result → `RunRecord` in `RunLog`.

## Key semantics

- **Quota**: 1 unit per scheduled run, taken at dispatch; retries inside a run are free.
  Usage is keyed by `(user, day)`, so it resets at midnight in the configured tz (default UTC).
  → [ADR 0001](adr/0001-quota-admission-at-dispatch.md)
- **Once per day, with catch-up**: a missed slot runs on the next tick that day; a task
  submitted after its time runs on the next tick.
- **State** is in-memory dicts owned by classes. → [ADR 0002](adr/0002-in-memory-state.md)

## Scaling path (10k+ tasks/day and beyond)

10k/day is ~7 runs/minute — one process handles it. What breaks first is durability and
single-instance availability, not throughput. In order:

1. **Persist tasks + dispatch marks** (Postgres). Tick becomes
   `SELECT ... WHERE next_run_at <= now FOR UPDATE SKIP LOCKED` with an indexed
   `next_run_at`, instead of scanning every task.
2. **Quota in Redis**: `INCR quota:{user}:{day}` + `EXPIRE`, compare to limit (Lua script
   for check-and-increment). Same `try_consume` contract.
3. **Queue out of process** (SQS / Redis Streams); workers become separate, autoscaled
   deployments. The `Job` dataclass is the message schema.
4. **Idempotency key** `(task_id, run_date)` on run records so at-least-once delivery
   can't double-execute.
5. **Leader election** (or a DB lock) so only one scheduler instance dispatches.
6. Metrics per `RunStatus`/action/user (counts, latency, retries) + DLQ for `FAILED`.

## Reusable modules for other teams

- `quota.py` — per-key, per-window limiter (rename to `RateLimiter[key, window]`).
- `executor.RetryPolicy` + retry loop — generic resilient-call helper.
- `actions.Action` + `ActionRegistry` — plugin/strategy registry with typed params.
