# Design — scheduler refactor

Status: **Implemented** · Spec: [requirements.md](requirements.md)

Architecture, module responsibilities and the scaling path were promoted to
[steering/architecture.md](../../steering/architecture.md). Decisions specific to this change:
[ADR 0001](../../steering/adr/0001-quota-admission-at-dispatch.md) (admission at dispatch),
[ADR 0002](../../steering/adr/0002-in-memory-state.md) (in-memory state + lock).

## Legacy → new mapping

| Legacy | New |
|---|---|
| `users` dict, `executed` counter | `QuotaManager` — `(user, day)` keyed usage, atomic `try_consume` |
| `tasks` list of dicts | `Scheduler.submit(dict)` → validated `TaskSpec` + typed action params → `Task` |
| `if task['time'] == now` | `Scheduler.tick(now)`: `time <= now` and not dispatched today |
| `print(f"Executing ...")` | `Action` strategies + `TaskExecutor` (timeout, retry) + structured logging |
| `print(... exceeded quota)` | `run.quota_exceeded` WARNING + `QUOTA_EXCEEDED` `RunRecord` |
| `run()` loop | `TaskEngine`: bounded queue + N async workers, `run_forever(interval)` |

## Acceptance criteria → tests

| AC | Test |
|---|---|
| AC1 daily reset | `test_quota.py::test_quota_is_per_day_and_resets_next_day`, `test_scheduler.py::test_task_runs_again_next_day`, `test_engine.py::test_user_with_simultaneous_tasks_runs_them_all_on_each_day`, `test_engine.py::test_simultaneous_tasks_over_quota_are_refused_each_day_then_reset` |
| AC2 no double run | `test_scheduler.py::test_tick_twice_in_same_minute_dispatches_once` |
| AC3 catch-up | `test_scheduler.py::test_missed_slot_catches_up_once_and_not_before_time` |
| AC4 contention | `test_scheduler.py::test_quota_shortfall_admits_in_submission_order`, `test_engine.py::test_concurrent_workers_respect_quota`, `test_quota.py::test_concurrent_consumers_never_exceed_quota` |
| AC5 retry = 1 unit | `test_executor.py::test_transient_failures_are_retried_until_success`, `test_engine.py::test_retries_consume_one_quota_unit_per_run` |
| AC6 failure isolated | `test_executor.py::test_persistent_failure_gives_up_after_max_attempts`, `test_engine.py::test_failing_task_does_not_affect_others` |
| AC7 validation | `test_validation.py::test_invalid_task_is_rejected_at_submit` (8 cases) |
| AC8 extensible | `test_engine.py::test_new_action_plugs_in_without_engine_changes` |
