# ADR 0002 — In-memory state in owning classes, lock around quota

Status: Accepted · 2026-11-24

## Context
Time box is 30 minutes; persistence is out of scope. The brief still asks for a design that
scales, and workers run concurrently.

## Decision
Plain dicts, each owned by exactly one class (`QuotaManager`, `Scheduler`, `RunLog`). No
repository/protocol layer. `QuotaManager.try_consume` holds an `asyncio.Lock` for the
check-and-increment.

## Notes
- Strictly, on one event loop a critical section with no `await` inside is already atomic,
  so the lock is not needed *today*. It is kept because the first scaling step (Redis)
  makes that section awaitable, and the lock documents the invariant. Cost is negligible.
- Usage is keyed `(user, day)`; `Scheduler` prunes old days on day rollover, so memory is
  bounded by users × 1 day.
- Swapping to Redis/Postgres means re-implementing `QuotaManager` / `Scheduler` storage
  behind the same public methods — see architecture.md "Scaling path".
