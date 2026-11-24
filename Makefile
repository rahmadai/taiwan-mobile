.PHONY: install check fmt lint type test e2e demo

install:
	uv sync

check: ## all merge gates
	uv run ruff format --check .
	uv run ruff check .
	uv run pyright
	uv run pytest

fmt:
	uv run ruff format .
	uv run ruff check --fix .

lint:
	uv run ruff check .

type:
	uv run pyright

test:
	uv run pytest

e2e: ## black-box cases from e2e/TEST_CASES.md
	uv run pytest e2e --no-cov -v -s

demo:
	uv run python -m taskflow
