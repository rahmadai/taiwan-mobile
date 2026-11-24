# Requirements — Strategy-based task scheduler with per-user quota (refactor)

Status: **Approved 2026-11-24** · Time box: 30 min · Source: AI coding test brief

## Problem
Legacy `run()` mixes scheduling, quota, execution and output in one loop over module dicts.
Bugs/limits carried by the legacy code that this refactor fixes:
- `executed` never resets → quota is effectively lifetime, though tasks run "daily".
- Exact-minute match (`time == now`) → a missed minute skips tasks; two calls in one minute run them twice.
- `print` instead of logging; no validation; no extension point for actions; synchronous.

## Decisions (agreed)
| # | Topic | Decision |
|---|-------|----------|
| D1 | Quota window | Per calendar day; resets at midnight in a configured timezone (default UTC, injectable). |
| D2 | Quota consumption | 1 unit per scheduled **run**, consumed atomically *before* execution; retries inside the run are free. |
| D3 | Scheduling | `tick(now)` with injected clock. A task is due if `time <= now` and it has not run today → at most once/day, missed slots catch up on next tick. |
| D4 | Async model | Scheduler enqueues due runs onto `asyncio.Queue`; N workers consume (bounded concurrency). |
| D5 | Storage | Plain in-memory dicts, owned by classes; `QuotaManager.try_consume` guarded by `asyncio.Lock`. Redis/DB is a documented scale path, not built. |
| D6 | Validation | Pydantic models; invalid task dicts (bad `HH:MM`, unknown action, unknown user, bad params) rejected at submit with a clear error. Per-action params model. |
| D7 | Failures | Retry with exponential backoff + jitter, capped attempts (default 3); each attempt under a timeout. Final failure logged + recorded `FAILED`; worker continues. |
| D8 | Same-slot contention | When quota < due runs, submission order wins; the rest are logged `QUOTA_EXCEEDED`. |
| D9 | Actions | Strategy pattern: `Action` base + registry (`sync`, `backup`, `delete`), simulated (log only, no real FS). New action = new class + register, no scheduler change. |

## Functional requirements
- FR1 A user can own multiple tasks, including several in the same slot.
- FR2 Tasks are submitted as dicts: `{user, time, action, target, params?}`.
- FR3 Every run is logged with `task_id`, `user`, `action`, `attempt`, outcome (`SUCCESS` / `FAILED` / `QUOTA_EXCEEDED`).
- FR4 Run outcomes are queryable in memory (for tests and the demo).

## Acceptance criteria (each → a pytest)
- AC1 Quota resets on the next day (fake clock crosses midnight).
- AC2 `tick` twice in the same minute → task runs once.
- AC3 Tick after the slot (missed minute) → task still runs once that day.
- AC4 alice quota 3, 5 concurrent due runs → exactly 3 `SUCCESS`, 2 `QUOTA_EXCEEDED`, in submission order.
- AC5 Action fails twice then succeeds → `SUCCESS`, 3 attempts, quota used = 1.
- AC6 Action always fails → `FAILED` after max attempts, logged with `task_id`; other tasks unaffected.
- AC7 Invalid dict (time `25:00`, unknown action, unknown user) → validation error at submit.
- AC8 Registering a new action class requires no scheduler/executor change.

## Non-functional / gates
- Python 3.12, uv, `ruff format --check`, `ruff check`, `pyright`, `pytest` — all green.
- Tests use fake clock and zero/near-zero backoff; no real sleeps.
- README: architecture, decisions above, answers to scaling (10k+/day) and reuse follow-ups.
- AI_USAGE.md: prompts used, AI suggestions rejected/changed and why.

## Confirmed defaults
- Timezone: UTC, injectable.
- Max attempts per run: 3 (1 + 2 retries), exponential backoff + jitter, per-attempt timeout.
- Actions are simulated (log only).
- Code lives in the nested `taiwan-mobile/` repo (remote `rahmadai/taiwan-mobile`).

## Out of scope
Persistence, real filesystem actions, distributed workers, auth, HTTP API.
