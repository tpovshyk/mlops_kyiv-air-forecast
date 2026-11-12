# Змінні з .env (паролі, адреси) доступні всім командам; у CI файлу немає — це нормально.
-include .env
export

.PHONY: setup lint hooks up down ps logs backfill counts snapshot train register reproduce

setup:
	uv sync

lint:
	uv run ruff check .

hooks:
	uv run pre-commit install

# --- Локальний стек (Docker Compose) ---
up:
	@test -f .env || (echo "Немає .env: виконай cp .env.example .env" && exit 1)
	docker compose up -d --build

down:
	docker compose down

ps:
	docker compose ps

logs:
	docker compose logs --tail=100 $(s)

# --- ETL ---
# make backfill FROM=2023-01-01 TO=2026-10-31
backfill:
	docker compose exec airflow airflow backfill create --dag-id air_etl_monthly --from-date $(FROM) --to-date $(TO)

# make counts MONTH=2025-01-01 — рядки порції в шарах raw, dwh, features
counts:
	docker compose exec -T postgres sh -c 'psql -U "$$POSTGRES_USER" -d "$$POSTGRES_DB" -v month=$(MONTH) -f -' < scripts/counts.sql

# --- Навчання і реєстр (MLflow) ---
# Знімок таблиці ознак у Parquet під DVC: на ньому вчиться модель
snapshot:
	PYTHONPATH=src uv run python -m kyiv_air.snapshot
	uv run dvc add data/features/pm25_hourly_v1.parquet
	uv run dvc push

# Навчання з логом у MLflow (без реєстрації)
train:
	PYTHONPATH=src uv run python -m kyiv_air.train

# Навчання + реєстрація як champion (лише з закоміченим кодом)
register:
	PYTHONPATH=src uv run python -m kyiv_air.train --register

# make reproduce VERSION=1 (без VERSION — версія з позначкою champion)
reproduce:
	PYTHONPATH=src uv run python -m kyiv_air.reproduce $(if $(VERSION),--version $(VERSION),--alias champion)
