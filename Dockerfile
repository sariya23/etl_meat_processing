FROM apache/airflow:3.0.4
COPY requirements_airflow.txt .
RUN pip install -r requirements_airflow.txt
