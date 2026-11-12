"""Чисті функції обробки без бази даних: їх легко перевірити окремо від Airflow."""

import numpy as np
import pandas as pd

from kyiv_air.settings import FEATURE_VERSION, PM25_MAX

FEATURE_COLUMNS = [
    "pm25",
    "pm25_lag_1",
    "pm25_lag_2",
    "pm25_lag_3",
    "pm25_lag_24",
    "pm25_lag_168",
    "pm25_mean_6h",
    "pm25_mean_24h",
    "city_mean",
]


def hour_grid(start: pd.Timestamp, end: pd.Timestamp) -> pd.DatetimeIndex:
    """Усі години інтервалу [start, end) в UTC."""
    return pd.date_range(start, end, freq="h", inclusive="left", tz="UTC")


def clean(
    raw: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp, district_ids: list[int]
) -> tuple[pd.DataFrame, dict[str, int]]:
    """Сирі рядки (district_id, phenomenon, value, logged_at як текст) → одна строка
    на район × годину з колонками pm25 і aqi. Повертає таблицю і лічильники правил очищення.
    """
    df = raw.copy()
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df["ts"] = pd.to_datetime(df["logged_at"], utc=True).dt.floor("h")

    # Правило 1: кілька значень для району × показника × години → середнє.
    keys = ["district_id", "ts", "phenomenon"]
    duplicates = int(df.duplicated(keys).sum())
    wide = df.groupby(keys)["value"].mean().unstack("phenomenon")
    for col in ("pm25", "aqi"):
        if col not in wide:
            wide[col] = np.nan
    wide = wide[["pm25", "aqi"]]
    wide.columns.name = None

    # Правило 2: PM2.5 = 0 при AQI > 0 — збій сенсора (AQI рахується з тих самих вимірів).
    zero_with_aqi = (wide["pm25"] == 0) & (wide["aqi"] > 0)
    wide.loc[zero_with_aqi, "pm25"] = np.nan

    # Правило 3: від'ємні або понад PM25_MAX значення — фізично неможливі.
    out_of_range = (wide["pm25"] < 0) | (wide["pm25"] > PM25_MAX)
    wide.loc[out_of_range, "pm25"] = np.nan

    # Повна сітка: кожен район × кожна година; відсутня година = пропуск, а не відсутній рядок.
    full = pd.MultiIndex.from_product(
        [sorted(district_ids), hour_grid(start, end)], names=["district_id", "ts"]
    )
    present = len(wide)
    wide = wide.reindex(full)
    stats = {
        "raw_rows": len(raw),
        "duplicates_averaged": duplicates,
        "zero_with_aqi_nulled": int(zero_with_aqi.sum()),
        "out_of_range_nulled": int(out_of_range.sum()),
        "missing_hours_added": len(full) - present,
        "clean_rows": len(wide),
        "pm25_nulls": int(wide["pm25"].isna().sum()),
    }
    return wide.reset_index(), stats


def build_features(
    dwh: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp
) -> pd.DataFrame:
    """Ознаки в момент t для кожного району × години t з [start, end).
    dwh має містити також HISTORY_HOURS годин до start, щоб лаги на початку місяця
    бралися з кінця попереднього. Жодна ознака не використовує значень після t.
    """
    df = dwh[["district_id", "ts", "pm25"]].sort_values(["district_id", "ts"]).copy()
    g = df.groupby("district_id")["pm25"]
    for lag in (1, 2, 3, 24, 168):
        df[f"pm25_lag_{lag}"] = g.shift(lag)
    df["pm25_mean_6h"] = g.transform(lambda s: s.rolling(6, min_periods=3).mean())
    df["pm25_mean_24h"] = g.transform(lambda s: s.rolling(24, min_periods=12).mean())
    df["city_mean"] = df.groupby("ts")["pm25"].transform("mean")
    out = df[(df["ts"] >= start) & (df["ts"] < end)].copy()
    out["feature_version"] = FEATURE_VERSION
    return out[["district_id", "ts", "feature_version", *FEATURE_COLUMNS]]


def quality_failures(
    clean_rows: pd.DataFrame,
    feature_rows: int,
    start: pd.Timestamp,
    end: pd.Timestamp,
    n_districts: int,
    max_null_rate: float,
    max_staleness_hours: int,
) -> list[str]:
    """Перелік порушень для порції; порожній список означає, що порція пройшла."""
    failures = []
    expected = n_districts * len(hour_grid(start, end))
    if len(clean_rows) != expected:
        failures.append(f"рядків {len(clean_rows)}, очікувалось {expected} (райони × години)")
    if clean_rows.duplicated(["district_id", "ts"]).any():
        failures.append("є повтори пари район × година")
    pm = clean_rows["pm25"]
    null_rate = float(pm.isna().mean()) if len(pm) else 1.0
    if null_rate > max_null_rate:
        failures.append(f"частка пропусків PM2.5 {null_rate:.1%} > {max_null_rate:.0%}")
    if pm.notna().any() and (pm.min() < 0 or pm.max() > PM25_MAX):
        failures.append(f"PM2.5 поза діапазоном 0–{PM25_MAX:.0f}: {pm.min()}…{pm.max()}")
    observed = clean_rows.loc[pm.notna(), "ts"]
    if observed.empty:
        failures.append("в інтервалі немає жодного виміру PM2.5")
    else:
        lag_h = (end - observed.max()) / pd.Timedelta(hours=1)
        if lag_h > max_staleness_hours:
            failures.append(
                f"останній вимір {observed.max()} — за {lag_h:.0f} год до кінця інтервалу "
                f"(дозволено {max_staleness_hours}); схоже, джерело не оновилось"
            )
    if feature_rows != len(clean_rows):
        failures.append(f"рядків ознак {feature_rows}, а очищених {len(clean_rows)}")
    return failures


def corrupt(raw: pd.DataFrame, seed: int = 42) -> pd.DataFrame:
    """Навмисно зіпсована порція для перевірки воріт якості: 30% значень PM2.5
    стерто, ще 5% замінено на -50. Використовується лише з параметром inject_bad_batch.
    """
    df = raw.copy()
    rng = np.random.default_rng(seed)
    pm_idx = df.index[df["phenomenon"] == "pm25"].to_numpy()
    shuffled = rng.permutation(pm_idx)
    n_blank = int(len(shuffled) * 0.30)
    n_negative = int(len(shuffled) * 0.05)
    df.loc[shuffled[:n_blank], "value"] = ""
    df.loc[shuffled[n_blank : n_blank + n_negative], "value"] = "-50"
    return df
