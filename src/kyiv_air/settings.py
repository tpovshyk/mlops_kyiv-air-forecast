"""Шляхи, константи й підключення. Один модуль для Airflow-контейнера і для ноутбука."""

import os
from pathlib import Path

import yaml

# У контейнері Airflow KYIV_AIR_ROOT=/opt/airflow; на ноутбуці — корінь репозиторію.
ROOT = Path(os.environ.get("KYIV_AIR_ROOT", Path(__file__).resolve().parents[2]))
RAW_DIR = ROOT / "data" / "raw" / "districts"
CONFIG_DIR = ROOT / "configs"
STAGING_DIR = Path(os.environ.get("KYIV_AIR_STAGING", "/tmp/kyiv_air"))

FEATURE_VERSION = "v1"
PM25_MAX = 1000.0  # мкг/м³; вище — фізично неправдоподібне значення сенсора
MAX_NULL_RATE = 0.10  # частка пропущених годин у порції, вище якої порція бракується
MAX_STALENESS_HOURS = 6  # останній вимір має бути не раніше ніж за 6 год до кінця інтервалу
HISTORY_HOURS = 168  # глибина історії для ознак (найдавніший лаг — 7 діб)


def warehouse_url() -> str:
    """URL сховища. У контейнері задано WAREHOUSE_URL; на ноутбуці збирається з .env."""
    if url := os.environ.get("WAREHOUSE_URL"):
        return url
    user = os.environ["POSTGRES_USER"]
    password = os.environ["POSTGRES_PASSWORD"]
    db = os.environ["POSTGRES_DB"]
    host = os.environ.get("WAREHOUSE_HOST", "localhost")
    return f"postgresql+psycopg2://{user}:{password}@{host}:5432/{db}"


def load_districts() -> dict[int, str]:
    """ID району SaveEcoBot → slug, з configs/districts.yaml."""
    with open(CONFIG_DIR / "districts.yaml", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    return {int(d["id"]): d["slug"] for d in cfg["districts"]}
