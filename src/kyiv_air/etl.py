"""Кроки ETL з читанням і записом у PostgreSQL. Кожен крок працює лише з інтервалом
[start, end), який передає оркестратор; поточний час ніде не використовується.

Шари: raw.saveecobot_hourly (рядки як у файлі) → dwh.pm25_hourly (типізовані, очищені)
→ features.pm25_hourly (ознаки з feature_version).
"""

import logging
import re

import pandas as pd
import pandera as pa

from kyiv_air import settings
from kyiv_air.db import cursor, query, replace_interval
from kyiv_air.transforms import build_features as compute_features
from kyiv_air.transforms import clean, corrupt, quality_failures

log = logging.getLogger(__name__)

DDL = """
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS dwh;
CREATE SCHEMA IF NOT EXISTS features;

CREATE TABLE IF NOT EXISTS raw.saveecobot_hourly (
    batch_start  timestamptz NOT NULL,
    district_id  integer     NOT NULL,
    phenomenon   text,
    value        text,
    logged_at    text,
    source_file  text        NOT NULL
);
CREATE INDEX IF NOT EXISTS raw_batch_idx ON raw.saveecobot_hourly (batch_start);

CREATE TABLE IF NOT EXISTS dwh.pm25_hourly (
    district_id  integer     NOT NULL,
    ts           timestamptz NOT NULL,
    pm25         double precision,
    aqi          double precision,
    PRIMARY KEY (district_id, ts)
);

CREATE TABLE IF NOT EXISTS features.pm25_hourly (
    district_id      integer     NOT NULL,
    ts               timestamptz NOT NULL,
    feature_version  text        NOT NULL,
    pm25             double precision,
    pm25_lag_1       double precision,
    pm25_lag_2       double precision,
    pm25_lag_3       double precision,
    pm25_lag_24      double precision,
    pm25_lag_168     double precision,
    pm25_mean_6h     double precision,
    pm25_mean_24h    double precision,
    city_mean        double precision,
    PRIMARY KEY (district_id, ts, feature_version)
);
"""

FILE_RE = re.compile(r"saveecobot_city_district_(\d+)_hourly\.csv$")


def _ts(x) -> pd.Timestamp:
    """Межа інтервалу від Airflow (pendulum) або рядка → Timestamp в UTC."""
    t = pd.Timestamp(x)
    return t.tz_localize("UTC") if t.tzinfo is None else t.tz_convert("UTC")


def extract(start, end, inject_bad_batch: bool = False) -> int:
    start, end = _ts(start), _ts(end)
    log.info("Інтервал порції: [%s, %s)", start, end)
    with cursor() as cur:
        cur.execute(DDL)

    frames = []
    for path in sorted(settings.RAW_DIR.glob("saveecobot_city_district_*_hourly.csv")):
        df = pd.read_csv(path, dtype=str, keep_default_na=False)
        ts = pd.to_datetime(df["logged_at"], utc=True, errors="coerce")
        part = df[(ts >= start) & (ts < end)].copy()
        part["district_id"] = int(FILE_RE.search(path.name).group(1))
        part["source_file"] = path.name
        frames.append(part)
    if not frames:
        raise FileNotFoundError(f"Немає файлів у {settings.RAW_DIR}; виконай dvc pull")
    raw = pd.concat(frames, ignore_index=True)
    if inject_bad_batch:
        log.warning("inject_bad_batch=True: порцію навмисно зіпсовано (30% пропусків, 5% = -50)")
        raw = corrupt(raw)
    raw["batch_start"] = start
    raw = raw[["batch_start", "district_id", "phenomenon", "value", "logged_at", "source_file"]]
    replace_interval(raw, "raw.saveecobot_hourly",
                      "DELETE FROM raw.saveecobot_hourly WHERE batch_start = %(s)s", {"s": start})
    log.info("extract: %d сирих рядків", len(raw))
    return len(raw)


def _is_number_or_blank(s: pd.Series) -> pd.Series:
    return (s == "") | pd.to_numeric(s, errors="coerce").notna()


def _is_datetime(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce").notna()


RAW_SCHEMA = pa.DataFrameSchema(
    {
        "district_id": pa.Column(int),
        "phenomenon": pa.Column(str, pa.Check.isin(["pm25", "aqi"])),
        "value": pa.Column(str, pa.Check(_is_number_or_blank, error="value не є числом")),
        "logged_at": pa.Column(str, pa.Check(_is_datetime, error="logged_at не є датою")),
    },
    strict="filter",
)


def _read_raw(start: pd.Timestamp) -> pd.DataFrame:
    return query(
        "SELECT district_id, phenomenon, value, logged_at "
        "FROM raw.saveecobot_hourly WHERE batch_start = %(s)s",
        {"s": start},
    )


def validate(start, end) -> int:
    """Перевірка схеми сирих рядків: колонки, типи, допустимі значення phenomenon,
    числа й дати, що розбираються, відомі райони. Падає з SchemaErrors."""
    raw = _read_raw(_ts(start))
    if raw.empty:
        raise ValueError("Порожня порція: у сирих даних немає рядків за цей інтервал")
    raw = raw.astype({"district_id": "int64", "phenomenon": str, "value": str, "logged_at": str})
    RAW_SCHEMA.validate(raw, lazy=True)
    unknown = set(raw["district_id"]) - set(settings.load_districts())
    if unknown:
        raise ValueError(f"Невідомі ID районів: {sorted(unknown)}")
    log.info("validate: схема ок, %d рядків", len(raw))
    return len(raw)


def _stage_path(start: pd.Timestamp):
    settings.STAGING_DIR.mkdir(parents=True, exist_ok=True)
    return settings.STAGING_DIR / f"clean_{start:%Y%m}.csv"


def transform(start, end) -> dict:
    start, end = _ts(start), _ts(end)
    raw = _read_raw(start)
    cleaned, stats = clean(raw, start, end, list(settings.load_districts()))
    cleaned.to_csv(_stage_path(start), index=False)
    log.info("transform: %s", stats)
    return stats


def load(start, end) -> int:
    start, end = _ts(start), _ts(end)
    df = pd.read_csv(_stage_path(start))
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    replace_interval(
        df, "dwh.pm25_hourly",
        "DELETE FROM dwh.pm25_hourly WHERE ts >= %(s)s AND ts < %(e)s", {"s": start, "e": end},
    )
    log.info("load: %d рядків у dwh.pm25_hourly за [%s, %s)", len(df), start, end)
    return len(df)


def _read_dwh(start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    df = query(
        "SELECT district_id, ts, pm25 FROM dwh.pm25_hourly WHERE ts >= %(s)s AND ts < %(e)s",
        {"s": start, "e": end},
    )
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    df["pm25"] = pd.to_numeric(df["pm25"])
    return df


def build_features(start, end) -> int:
    start, end = _ts(start), _ts(end)
    history_start = start - pd.Timedelta(hours=settings.HISTORY_HOURS)
    feats = compute_features(_read_dwh(history_start, end), start, end)
    replace_interval(
        feats, "features.pm25_hourly",
        "DELETE FROM features.pm25_hourly "
        "WHERE ts >= %(s)s AND ts < %(e)s AND feature_version = %(v)s",
        {"s": start, "e": end, "v": settings.FEATURE_VERSION},
    )
    log.info("build_features: %d рядків, версія %s", len(feats), settings.FEATURE_VERSION)
    return len(feats)


def quality_check(start, end) -> dict:
    start, end = _ts(start), _ts(end)
    rows = _read_dwh(start, end)
    n_features = int(query(
        "SELECT count(*) AS n FROM features.pm25_hourly "
        "WHERE ts >= %(s)s AND ts < %(e)s AND feature_version = %(v)s",
        {"s": start, "e": end, "v": settings.FEATURE_VERSION},
    )["n"].iloc[0])
    failures = quality_failures(
        rows, n_features, start, end, len(settings.load_districts()),
        settings.MAX_NULL_RATE, settings.MAX_STALENESS_HOURS,
    )
    summary = {"dwh_rows": len(rows), "feature_rows": n_features,
               "pm25_null_rate": round(float(rows["pm25"].isna().mean()), 4)}
    if failures:
        raise ValueError("Порція не пройшла перевірку якості: " + "; ".join(failures))
    log.info("quality_check: пройдено %s", summary)
    return summary
