"""ETL порції PM2.5 за один календарний місяць.

Порція = інтервал запуску [data_interval_start, data_interval_end). Інтервал задано явно
через CronDataIntervalTimetable: в Airflow 3 звичайний cron (@monthly) інтервалів не створює,
і start дорівнював би end. Поточний час у задачах не використовується, тож backfill
будь-якого минулого місяця дає ті самі рядки, що й звичайний запуск.
"""

import pendulum
from airflow.sdk import Param, dag, task
from airflow.timetables.interval import CronDataIntervalTimetable

from kyiv_air import etl


@dag(
    dag_id="air_etl_monthly",
    schedule=CronDataIntervalTimetable("@monthly", timezone="UTC"),
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,  # історію завантажуємо явно: airflow backfill create
    max_active_runs=1,  # місяці по черзі: ознаки місяця беруть історію з попереднього
    params={
        "inject_bad_batch": Param(
            False,
            type="boolean",
            description="Навмисно зіпсувати порцію, щоб перевірити, що quality_check падає",
        )
    },
    tags=["kyiv-air", "lab2"],
)
def air_etl_monthly():
    @task
    def extract(data_interval_start=None, data_interval_end=None, params=None):
        return etl.extract(data_interval_start, data_interval_end, params["inject_bad_batch"])

    @task
    def validate(data_interval_start=None, data_interval_end=None):
        return etl.validate(data_interval_start, data_interval_end)

    @task
    def transform(data_interval_start=None, data_interval_end=None):
        return etl.transform(data_interval_start, data_interval_end)

    @task
    def load(data_interval_start=None, data_interval_end=None):
        return etl.load(data_interval_start, data_interval_end)

    @task
    def build_features(data_interval_start=None, data_interval_end=None):
        return etl.build_features(data_interval_start, data_interval_end)

    @task
    def quality_check(data_interval_start=None, data_interval_end=None):
        return etl.quality_check(data_interval_start, data_interval_end)

    extract() >> validate() >> transform() >> load() >> build_features() >> quality_check()


air_etl_monthly()
