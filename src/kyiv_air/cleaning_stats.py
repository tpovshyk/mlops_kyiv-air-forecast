"""Скільки рядків зачепило кожне правило очищення в усіх завантажених порціях.

Перераховує ті самі правила (transforms.clean), що й задача transform, над сирим шаром
raw.saveecobot_hourly, і підсумовує лічильники. Потрібна база після make backfill.
Запуск: make cleaning-stats
"""

import pandas as pd

from kyiv_air import settings
from kyiv_air.db import query
from kyiv_air.transforms import clean

LABELS = {
    "raw_rows": "сирих рядків (pm25 + aqi)",
    "duplicates_averaged": "правило 1: повтори години → середнє",
    "zero_with_aqi_nulled": "правило 2: PM2.5 = 0 при AQI > 0 → пропуск",
    "out_of_range_nulled": "правило 3: PM2.5 < 0 або > 1000 → пропуск",
    "missing_hours_added": "відсутні години додано як пропуск",
    "clean_rows": "рядків у dwh (район × година)",
    "pm25_nulls": "з них PM2.5 порожній",
}


def main() -> None:
    batches = query("SELECT DISTINCT batch_start FROM raw.saveecobot_hourly ORDER BY 1")
    if batches.empty:
        raise SystemExit("Сирий шар порожній: спершу запусти ETL (make backfill)")
    district_ids = list(settings.load_districts())
    total = dict.fromkeys(LABELS, 0)
    for start in pd.to_datetime(batches["batch_start"], utc=True):
        end = start + pd.DateOffset(months=1)
        raw = query(
            "SELECT district_id, phenomenon, value, logged_at "
            "FROM raw.saveecobot_hourly WHERE batch_start = %(s)s",
            {"s": start},
        )
        _, stats = clean(raw, start, end, district_ids)
        for key in total:
            total[key] += stats[key]

    first, last = pd.to_datetime(batches["batch_start"].iloc[[0, -1]], utc=True)
    print(f"Порцій: {len(batches)} ({first:%Y-%m} … {last:%Y-%m})")
    for key, label in LABELS.items():
        print(f"{label:45}{total[key]:>10}")
    print(f"частка порожніх PM2.5 у dwh: {total['pm25_nulls'] / total['clean_rows']:.2%}")


if __name__ == "__main__":
    main()
