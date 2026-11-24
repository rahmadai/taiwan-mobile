# Tasks — scheduler refactor

- [x] T1 Scaffold: uv project, ruff, pyright (strict src), pytest + coverage gate, Makefile
- [x] T2 Data model + validation (`models.py`, `errors.py`) — AC7
- [x] T3 Quota module (`quota.py`) — AC1, AC4
- [x] T4 Action strategies + registry (`actions.py`) — AC8
- [x] T5 Executor with timeout, retry, backoff (`executor.py`) — AC5, AC6
- [x] T6 Scheduler with dispatch-time admission (`scheduler.py`) — AC2, AC3, AC4
- [x] T7 Async engine: queue + worker pool (`engine.py`, `bootstrap.py`)
- [x] T8 Demo of legacy scenario (`python -m taskflow`) + smoke test
- [x] T9 Steering (architecture, conventions, ADR 0001/0002), README, AI_USAGE.md, CLAUDE.md
- [x] T10 E2E suite from test case list (`e2e/TEST_CASES.md`, TC01–TC16)
- [x] T11 Fix: cancelled tick lost admitted jobs (found by TC15, regression TC16)
- [ ] T12 Human review → on merge mark spec Complete
