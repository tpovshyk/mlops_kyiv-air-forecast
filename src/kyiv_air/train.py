"""Навчання: базова модель («та сама година вчора») і LightGBM на знімку ознак.

Обидві оцінюються на тих самих рядках відкладеного періоду. Запуск логується в MLflow
з параметрами, метриками й двома тегами простежуваності: data_hash (DVC-хеш знімка)
і git_commit. З прапорцем --register модель реєструється з позначкою champion,
але лише якщо код закомічено.
"""

import argparse
import hashlib
import json
import subprocess
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

from kyiv_air import settings
from kyiv_air.snapshot import SNAPSHOT

warnings.filterwarnings("ignore", message="The 'generic' unit", category=DeprecationWarning)
SNAPSHOT_DVC = SNAPSHOT.with_name(SNAPSHOT.name + ".dvc")
RAW_DVC = settings.RAW_DIR.with_name(settings.RAW_DIR.name + ".dvc")
MODEL_NAME = "kyiv-air-pm25"
EXPERIMENT = "kyiv-air-pm25"
ALIAS = "champion"

# Поділ за часом. Цілі навчальних прикладів < TRAIN_END; між ними й першим прогнозом
# відкладеного періоду 7 діб, щоб тижневий лаг тестових прикладів не сягав навчальних цілей.
TRAIN_START = pd.Timestamp("2023-01-08", tz="UTC")  # перший тиждень — без тижневої історії
TRAIN_END = pd.Timestamp("2025-09-24", tz="UTC")
TEST_START = pd.Timestamp("2025-10-01", tz="UTC")
TEST_END = pd.Timestamp("2026-01-01", tz="UTC")
HORIZONS = list(range(1, 25))
TRAIN_ORIGIN_STEP = 3  # у навчанні кожна 3-тя година як момент прогнозу: менше пам'яті
EXCEEDANCE = 25.0  # мкг/м³, добовий норматив ЄС (configs/thresholds — Lab 4)

PARAMS = {
    "objective": "l1",
    "learning_rate": 0.05,
    "n_estimators": 600,
    "num_leaves": 63,
    "min_child_samples": 50,
    "subsample": 0.8,
    "subsample_freq": 1,
    "colsample_bytree": 0.9,
    "random_state": 42,
    "deterministic": True,
    "force_row_wise": True,
    "n_jobs": 4,
    "verbose": -1,
}

FEATURES = [
    "district_id", "horizon", "pm25", "pm25_lag_1", "pm25_lag_2", "pm25_lag_3",
    "pm25_lag_24", "pm25_lag_168", "pm25_mean_6h", "pm25_mean_24h", "city_mean",
    "same_hour_1d", "same_hour_7d", "hour", "dayofweek", "month", "heating_season",
]


def heating_season(ts: pd.Series) -> pd.Series:
    """15 жовтня — 15 квітня (Assumption: реальні дати щороку оголошує КМДА)."""
    m, d = ts.dt.month, ts.dt.day
    return ((m > 10) | (m < 4) | ((m == 10) & (d >= 15)) | ((m == 4) & (d < 15))).astype("int8")


def build_examples(feats: pd.DataFrame, origin_from, origin_to, target_to, step: int = 1):
    """Приклад = (район, момент прогнозу t, горизонт h). Ознаки беруться в момент t або
    раніше; ціль — PM2.5 о t+h. same_hour_1d = PM2.5 о t+h−24 (відоме, бо h ≤ 24)."""
    pm = feats.set_index(["district_id", "ts"])["pm25"]
    origins = feats[(feats["ts"] >= origin_from) & (feats["ts"] < origin_to)]
    if step > 1:
        origins = origins[origins["ts"].dt.hour % step == 0]

    def lookup(ts):
        idx = pd.MultiIndex.from_arrays([origins["district_id"].to_numpy(), ts])
        return pm.reindex(idx).to_numpy()

    parts = []
    for h in HORIZONS:
        target_ts = origins["ts"] + pd.Timedelta(hours=h)
        part = origins.copy()
        part["horizon"] = h
        part["target_ts"] = target_ts.to_numpy()
        part["target"] = lookup(target_ts)
        part["same_hour_1d"] = lookup(target_ts - pd.Timedelta(hours=24))
        part["same_hour_7d"] = lookup(target_ts - pd.Timedelta(hours=168))
        parts.append(part)
    ex = pd.concat(parts, ignore_index=True)
    ex = ex[(ex["target_ts"] < target_to) & ex["target"].notna()]
    ex["hour"] = ex["target_ts"].dt.hour.astype("int8")
    ex["dayofweek"] = ex["target_ts"].dt.dayofweek.astype("int8")
    ex["month"] = ex["target_ts"].dt.month.astype("int8")
    ex["heating_season"] = heating_season(ex["target_ts"])
    ex["district_id"] = ex["district_id"].astype("int32")
    return ex.reset_index(drop=True)


def evaluate(ex: pd.DataFrame, pred: np.ndarray, prefix: str) -> dict[str, float]:
    y = ex["target"].to_numpy()
    err = np.abs(y - pred)
    h = ex["horizon"].to_numpy()
    m = {
        f"{prefix}_mae": err.mean(),
        f"{prefix}_mae_h1_6": err[h <= 6].mean(),
        f"{prefix}_mae_h7_12": err[(h > 6) & (h <= 12)].mean(),
        f"{prefix}_mae_h13_24": err[h > 12].mean(),
        f"{prefix}_mae_above_25": err[y > EXCEEDANCE].mean() if (y > EXCEEDANCE).any() else np.nan,
    }
    # Добові попередження: прогноз о 00:00 UTC на години 1–24 → середнє за добу.
    day = ex.assign(pred=pred)[ex["ts"].dt.hour == 0]
    daily = day.groupby(["district_id", "ts"], observed=True).agg(
        actual=("target", "mean"), predicted=("pred", "mean"), n=("target", "size"))
    daily = daily[daily["n"] >= 20]  # доба з щонайменше 20 відомими годинами
    actual, warned = daily["actual"] > EXCEEDANCE, daily["predicted"] > EXCEEDANCE
    tp = int((actual & warned).sum())
    m[f"{prefix}_recall"] = tp / actual.sum() if actual.sum() else np.nan
    m[f"{prefix}_precision"] = tp / warned.sum() if warned.sum() else np.nan
    m[f"{prefix}_exceedance_days"] = float(actual.sum())
    return {k: float(v) for k, v in m.items()}


def dvc_hash(dvc_file: Path) -> str:
    with open(dvc_file, encoding="utf-8") as f:
        return yaml.safe_load(f)["outs"][0]["md5"]


def file_md5(path: Path) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def git(*args: str) -> str:
    return subprocess.run(["git", *args], cwd=settings.ROOT, check=True,
                          capture_output=True, text=True).stdout.strip()


def code_is_dirty() -> bool:
    """Незакомічені зміни в коді, що впливає на навчання (документація не рахується)."""
    paths = ["src", "dags", "scripts", "pyproject.toml", "uv.lock", "Makefile", "data"]
    return bool(git("status", "--porcelain", "--", *paths))


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--register", action="store_true", help="зареєструвати модель як champion")
    p.add_argument("--experiment", default=EXPERIMENT)
    p.add_argument("--run-name", default=f"lgbm-{settings.FEATURE_VERSION}")
    p.add_argument("--tag", action="append", default=[], help="додатковий тег key=value")
    p.add_argument("--metrics-out", type=Path, help="записати метрики в JSON (для reproduce)")
    args = p.parse_args()

    import lightgbm as lgb
    import mlflow
    from mlflow import MlflowClient

    data_hash = dvc_hash(SNAPSHOT_DVC)
    if file_md5(SNAPSHOT) != data_hash:
        raise SystemExit(f"{SNAPSHOT.name} не відповідає {SNAPSHOT_DVC.name}: "
                         "виконай make snapshot або dvc checkout")
    commit, dirty = git("rev-parse", "HEAD"), code_is_dirty()
    if args.register and dirty:
        raise SystemExit("Код має незакомічені зміни: зроби коміт, інакше git_commit "
                         "не відтворить модель. Реєстрацію скасовано.")

    feats = pd.read_parquet(SNAPSHOT)
    feats["ts"] = pd.to_datetime(feats["ts"], utc=True)
    train = build_examples(feats, TRAIN_START, TRAIN_END, TRAIN_END, step=TRAIN_ORIGIN_STEP)
    test = build_examples(feats, TEST_START, TEST_END, TEST_END)
    # Обидві моделі — на тих самих тестових рядках: де відоме значення «вчора о цій годині».
    test = test[test["same_hour_1d"].notna()].reset_index(drop=True)

    model = lgb.LGBMRegressor(**PARAMS)
    model.fit(train[FEATURES], train["target"], categorical_feature=["district_id"])
    metrics = {
        **evaluate(test, test["same_hour_1d"].to_numpy(), "baseline"),
        **evaluate(test, model.predict(test[FEATURES]), "model"),
    }
    metrics["mae_improvement_pct"] = 100 * (1 - metrics["model_mae"] / metrics["baseline_mae"])
    metrics["train_rows"], metrics["test_rows"] = float(len(train)), float(len(test))

    mlflow.set_experiment(args.experiment)
    with mlflow.start_run(run_name=args.run_name) as run:
        mlflow.log_params({**PARAMS, "feature_version": settings.FEATURE_VERSION,
                           "features": ",".join(FEATURES), "horizons": "1-24",
                           "train_origin_step": TRAIN_ORIGIN_STEP,
                           "train_period": f"{TRAIN_START.date()}..{TRAIN_END.date()}",
                           "test_period": f"{TEST_START.date()}..{TEST_END.date()}",
                           "exceedance_threshold": EXCEEDANCE})
        mlflow.log_metrics({k: v for k, v in metrics.items() if not np.isnan(v)})
        tags = {"data_hash": data_hash, "data_path": str(SNAPSHOT.relative_to(settings.ROOT)),
                "raw_data_hash": dvc_hash(RAW_DVC), "git_commit": commit,
                "git_dirty": str(dirty).lower(), "feature_version": settings.FEATURE_VERSION}
        tags.update(dict(t.split("=", 1) for t in args.tag))
        mlflow.set_tags(tags)
        mlflow.lightgbm.log_model(model.booster_, name="model",
                                  input_example=test[FEATURES].head(5))
        if args.register:
            mv = mlflow.register_model(f"runs:/{run.info.run_id}/model", MODEL_NAME)
            client = MlflowClient()
            for k in ("data_hash", "git_commit", "raw_data_hash", "feature_version"):
                client.set_model_version_tag(MODEL_NAME, mv.version, k, tags[k])
            client.set_registered_model_alias(MODEL_NAME, ALIAS, mv.version)
            print(f"Зареєстровано {MODEL_NAME} версії {mv.version} з позначкою {ALIAS}")

    print(f"run_id={run.info.run_id} data_hash={data_hash} git_commit={commit[:8]} dirty={dirty}")
    print(f"{'':16}{'baseline':>10}{'LightGBM':>10}")
    for key in ("mae", "mae_h1_6", "mae_h7_12", "mae_h13_24", "mae_above_25",
                "recall", "precision"):
        print(f"{key:16}{metrics[f'baseline_{key}']:>10.3f}{metrics[f'model_{key}']:>10.3f}")
    print(f"Покращення MAE: {metrics['mae_improvement_pct']:.1f}%")
    if args.metrics_out:
        args.metrics_out.write_text(json.dumps({"run_id": run.info.run_id, **metrics}, indent=2))


if __name__ == "__main__":
    main()
