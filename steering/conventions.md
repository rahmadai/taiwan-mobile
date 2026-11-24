# Conventions

## Toolchain & gates
Python 3.12, uv. All must pass before merge (`make check`):
`ruff format --check` · `ruff check` · `pyright` (strict on `src/`) · `pytest` (coverage ≥ 90%).

Pyright config lives in `pyrightconfig.json`, not `pyproject.toml`: pyright 1.1.414 fails to
parse this `pyproject.toml` and silently falls back to defaults while still reporting
"0 errors". If you move it back, confirm the "could not be parsed" warning is gone.

## Logging
- `logger = logging.getLogger(__name__)` per module; configure only in entry points
  (`configure_logging`).
- Message = `event.name key=value ...`, %-style lazy args (ruff `G` rules). Every run event
  carries `task_id user action`; failures add `attempts` and `error`.
- Events: `task.submitted`, `tick.dispatched`, `run.success`, `run.retry`, `run.failed`,
  `run.quota_exceeded`, `worker.unhandled`, `engine.started/stopped`, `engine.tick_failed`.
- Log targets/user names, never action params that could carry secrets.

## Errors
- Input problems → `InvalidTaskError` (or subclass `UnknownUserError`/`UnknownActionError`)
  at `submit`, with the offending field in the message.
- Actions raise `ActionError` (retryable) or `PermanentActionError` (not retried).
- Catch `Exception` only at isolation boundaries (executor around actions, worker around
  executor, `run_forever` around tick). Never catch `BaseException` — cancellation must propagate.

## Adding an action
Subclass `Action[YourParams]`, set `name` and `params_model` (pydantic, `extra="forbid"`),
implement `async execute(target, params)`, register it in `default_registry()` or a custom
registry. No scheduler/engine/executor change. Add a test like
`test_new_action_plugs_in_without_engine_changes`.

## Testing
- Pass time in (`tick(now)`), never read the wall clock in logic. Use aware datetimes.
- Inject `sleep` (`RecordingSleep`) and `rng` so retry tests are instant and deterministic.
- Shared helpers in `tests/helpers.py`. Name tests after the behaviour, map to spec ACs.
- To see INFO logs in `caplog`, call `caplog.set_level(logging.INFO)`.
