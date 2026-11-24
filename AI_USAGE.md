# AI usage log

Tool: Claude Code (agentic, in-repo). Process: spec → steering → code, with gates
(ruff, pyright, pytest) deciding — not the AI's claims.

## How the AI was prompted

1. **Requirements first, no code.** "Grill me about the requirements" — the AI had to
   surface ambiguities in the brief and ask, in rounds, before writing anything. Each answer
   was recorded as a numbered decision in `specs/.../requirements.md`, which I approved.
2. **Constraints up front.** Engineering playbook (spec-driven, gates, small changes),
   Python 3.12 + uv/ruff/pyright/pytest, 30-minute time box, "async and scalable".
3. **Acceptance criteria as the contract.** Every AC maps to a named test
   (`design.md`), so "done" is checkable, not a judgement call.

## Ambiguities the AI surfaced (and what we decided)

| Brief / legacy code | Problem | Decision |
|---|---|---|
| "executing them daily" but `executed` never resets | quota is lifetime, likely a bug | per-day quota |
| `time == now` | missed minute skips; two calls in a minute double-run | once/day + catch-up, injected clock |
| retries + "every attempt counts" | a flaky action burns the whole day's quota | 1 unit per run, retries free |
| plain dicts + concurrent workers | check-then-increment race | dict inside a class + lock |
| quota short in a slot | which tasks run? | submission order |

## Suggestions rejected or changed

- **APScheduler / scheduling library** — rejected: hides the exact design being evaluated.
- **Consume quota inside the worker** (initial idea) — changed to admission at dispatch
  (ADR 0001): worker timing would make "who wins" non-deterministic.
- **Protocol/repository layer for storage** — offered, I declined in favour of plain dicts
  in owning classes; the scale path is documented instead (ADR 0002).
- **"The lock makes it atomic"** — refined: on one event loop the section has no `await`, so
  it is already atomic; the lock is kept for the Redis step, and the ADR says so honestly.
- **Real filesystem actions** — rejected: brief says *simulated*; `delete /tmp/z` must not delete.

## Where verification caught AI output

- **Type checker "0 errors" was false confidence.** Pyright printed "pyproject.toml could not
  be parsed" and ran with defaults, so strict mode wasn't active. Moved config to
  `pyrightconfig.json`, re-ran → genuinely clean under strict.
- **Generic variance.** Test helpers typed `Action[BaseModel]`; pyright flagged invariance
  (`FlakyParams` ≠ `BaseModel`). Fixed to `Action[Any]` at the registry boundary.
- **Demo smoke test failed** because `caplog` drops INFO by default — a test bug, not a code
  bug; confirmed by running the demo directly and reading the log output first.
