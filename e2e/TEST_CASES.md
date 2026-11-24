# E2E test cases

Black-box: only the public `taskflow` API or the CLI. Unlike `tests/`, these use the real
`run_forever` loop, real `asyncio.sleep` backoff, real timeouts, the bounded queue and
concurrent workers. Time is controlled only through the public `clock=` parameter.

Run: `make e2e` (`uv run pytest e2e --no-cov -v`).

| ID | Covers | Scenario | Expected |
|---|---|---|---|
| TC01 | demo, FR3 | `python -m taskflow` as a subprocess | exit 0; logs show 5 success + 1 quota_exceeded per day, two days |
| TC02 | FR1, FR2 | one user submits several dict-configured tasks with custom params | each runs once; params visible in action logs |
| TC03 | AC7 | invalid dicts: bad time, unknown user/action, bad/unknown params, missing field | `InvalidTaskError` subclass with field named; nothing registered |
| TC04 | AC2 | `run_forever` ticks ~50× within the same minute | task executes exactly once |
| TC05 | AC3 | engine starts at 15:00, task was due 12:00 | runs once on startup catch-up, never again that day |
| TC06 | AC1 | quota 1, two tasks at 23:59; clock moves past midnight to next day 23:59 | day 1: 1 success + 1 exceeded; day 2: same again |
| TC07 | FR1 + AC1 | one user, 3 tasks same slot, 3 consecutive days via `run_forever` | 3 successes per day, quota used = 3 per day |
| TC08 | AC4 | 20 users × 10 slow tasks (quota 5), 16 workers | exactly 5 success per user, the first 5 submitted; >1 action in flight concurrently |
| TC09 | AC5 | flaky action fails twice, real backoff | success on attempt 3; 2 `run.retry` logs; quota used = 1 |
| TC10 | AC6 | one hanging action (real timeout) alongside normal tasks | hung task FAILED "timed out" after 3 attempts; others succeed; workers alive |
| TC11 | AC6 | permanent error | FAILED after 1 attempt, no retry log |
| TC12 | AC8 | external `notify` action registered without touching taskflow | executes with its params |
| TC13 | tz | scheduler in Asia/Taipei, task 12:00 | not due at 03:59 UTC, due at 04:00 UTC |
| TC14 | lifecycle | `stop()` with jobs still queued | all queued jobs finish and are recorded before shutdown |
| TC15 | scale | 10,000 tasks (1,000 users × 10) in one day, queue max 1000 | all 10,000 SUCCESS, quota exact, completes in seconds |
| TC16 | lifecycle, regression | cancel `run_forever` while tick is blocked on a full queue, then restart | un-enqueued jobs released (quota refunded, `tick.cancelled` logged); after restart every task ran exactly once |
