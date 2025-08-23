from datetime import datetime, timedelta

import pandas as pd
import requests
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from clickhouse_driver import Client

from utils.constants.const import TABLE_NAME, column_mapping
from utils.queries.queries import CREATE_MAIN_TABLE_QUERY


API_HOST = BaseHook.get_connection("file_api_url").host
API_PORT = BaseHook.get_connection("file_api_url").port

CH_HOOK = ClickHouseHook(clickhouse_conn_id="clickhouse")


def download_data_to_local_file(url: str, **context):
    response = requests.get(url)
    file_name = url.split("/")[-1]
    if not response.ok:
        print(response.content)
        print(url)
        raise requests.exceptions.RequestException()

    with open(file_name, "wb") as f:
        f.write(response.content)


def get_current_week_data(filename: str, out_filename: str, **context):
    df = pd.read_excel(filename)
    execute_date = datetime.strptime(context["ds"], "%Y-%m-%d")
    week_records = df[
        (df["Дата_заказа"] <= execute_date)
        & (df["Дата_заказа"] >= execute_date - timedelta(days=7))
    ]
    week_records.to_csv(out_filename, encoding="utf-8")


def load_data_to_clickhouse(filename: str):
    df = pd.read_csv(filename, encoding="utf-8", index_col=0)
    date_columns = [
        "Дата_заказа",
        "Ожидаемая_дата_поставки",
        "Фактическая_дата_поставки",
    ]
    for col in date_columns:
        df[col] = pd.to_datetime(df[col]).dt.date
    df = df.rename(columns=column_mapping)
    CH_HOOK.execute(
        f"insert into {TABLE_NAME} values", df.to_dict("records"), types_check=True
    )


dag = DAG(
    "main_etl",
    schedule="0 17 * * 5",
    start_date=datetime(2025, 8, 15),
    max_active_runs=1,
)


create_table_task = ClickHouseOperator(
    task_id="create_table_click",
    sql=CREATE_MAIN_TABLE_QUERY,
    clickhouse_conn_id="clickhouse",
    dag=dag,
)

download_data_task = PythonOperator(
    task_id="download_data",
    python_callable=download_data_to_local_file,
    op_args=[f"{API_HOST}:{API_PORT}/download/" + "data.xlsx"],
    dag=dag,
)

get_current_week_data_task = PythonOperator(
    task_id="get_current_week_data",
    python_callable=get_current_week_data,
    op_args=["data.xlsx", "{{ ds }}.csv"],
    dag=dag,
)

load_to_clickhouse_task = PythonOperator(
    task_id="load_to_clickhouse",
    python_callable=load_data_to_clickhouse,
    op_args=["{{ ds }}.csv"],
    dag=dag,
)

create_table_task  >> download_data_task >> get_current_week_data_task >> load_to_clickhouse_task

