.PHONY: install check fmt lint type test demo

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

demo:
	uv run python -m taskflow
