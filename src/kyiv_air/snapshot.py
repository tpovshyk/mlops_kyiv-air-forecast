"""Знімок таблиці ознак у Parquet для навчання.

База зберігає лише поточний стан, тож навчання читає не базу, а цей файл під DVC:
його хеш однозначно визначає, на чому навчено модель, і дозволяє відтворити запуск.
Запуск: make snapshot (вивантажує файл, dvc add, dvc push).
"""

from kyiv_air import settings
from kyiv_air.db import query

SNAPSHOT = settings.ROOT / "data" / "features" / f"pm25_hourly_{settings.FEATURE_VERSION}.parquet"


def main() -> None:
    df = query(
        "SELECT * FROM features.pm25_hourly WHERE feature_version = %(v)s "
        "ORDER BY district_id, ts",
        {"v": settings.FEATURE_VERSION},
    )
    if df.empty:
        raise SystemExit("Таблиця ознак порожня: спершу запусти ETL (make backfill)")
    SNAPSHOT.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(SNAPSHOT, index=False)
    print(f"Знімок: {SNAPSHOT.relative_to(settings.ROOT)} — {len(df)} рядків, "
          f"{df['ts'].min()} … {df['ts'].max()}")


if __name__ == "__main__":
    main()
