FROM apache/airflow:3.3.1
# Фіксуємо версію Airflow у тій самій команді, щоб pip не оновив її разом із залежностями.
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" "pandera==0.22.1"
