
FROM apache/airflow:2.6.3-python3.9


USER root
COPY requirements.txt /requirements.txt

# Switch to airflow user before pip install
USER airflow
COPY packages /packages
RUN pip install --no-cache-dir --find-links=/packages -r /requirements.txt
