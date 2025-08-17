FROM apache/airflow:3.0.4

RUN pip install airflow-clickhouse-plugin clickhouse-connect openpyxl apache-airflow==3.0.4
