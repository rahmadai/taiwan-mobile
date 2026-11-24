# ADR 0001 — Quota admission at dispatch, 1 unit per run

Status: Accepted · 2026-11-24

## Context
Requirements: quota is consumed per started run (retries free); when a user has more due
runs than quota, submission order wins (spec D2, D8). Runs execute on N concurrent workers.

## Options
1. **Consume in the worker** before executing. Which task wins depends on which worker
   reaches the lock first — usually FIFO under asyncio, but not a guarantee, and not at all
   once workers are separate processes.
2. **Consume in the scheduler at dispatch** (chosen). `tick` walks tasks in submission order
   and calls `try_consume` before enqueuing. Ordering is deterministic by construction.
3. Reserve at dispatch, refund on failure. Fairer to users, more paths; rejected for scope.

## Decision
Option 2. Refused runs are recorded as `QUOTA_EXCEEDED` and marked dispatched, so they are
not re-evaluated or re-logged on every tick that day.

## Consequences
- Queue only ever holds admitted work → backpressure and quota are independent.
- A job that is admitted but lost (process crash before execution) has consumed quota.
  Acceptable in-memory; with a durable queue, the idempotency key `(task_id, run_date)`
  plus a refund on dead-letter fixes it.
