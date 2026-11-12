"""Підключення до сховища PostgreSQL через psycopg2 (без SQLAlchemy: однаково працює
в образі Airflow і на ноутбуці)."""

from contextlib import contextmanager

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

from kyiv_air import settings


def connect():
    return psycopg2.connect(settings.warehouse_url().replace("postgresql+psycopg2://", "postgresql://"))


@contextmanager
def cursor():
    """Курсор в одній транзакції: commit, якщо блок завершився без помилки, інакше rollback."""
    conn = connect()
    try:
        with conn, conn.cursor() as cur:
            yield cur
    finally:
        conn.close()


def query(sql: str, params: dict | None = None) -> pd.DataFrame:
    with cursor() as cur:
        cur.execute(sql, params or {})
        cols = [d[0] for d in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)


def replace_interval(df: pd.DataFrame, table: str, delete_sql: str, params: dict) -> None:
    """Ідемпотентний запис: видалити рядки інтервалу й вставити нові в одній транзакції.
    Повторний запуск того самого інтервалу дає ту саму кількість рядків."""
    cols = ", ".join(df.columns)
    rows = list(df.astype(object).where(df.notna(), None).itertuples(index=False, name=None))
    with cursor() as cur:
        cur.execute(delete_sql, params)
        execute_values(cur, f"INSERT INTO {table} ({cols}) VALUES %s", rows, page_size=5000)
