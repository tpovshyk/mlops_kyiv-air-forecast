.PHONY: setup lint hooks

setup:
	uv sync

lint:
	uv run ruff check .

hooks:
	uv run pre-commit install
