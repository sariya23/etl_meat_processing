from datetime import datetime, timedelta

import pandas as pd
import requests
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from clickhouse_driver import Client
 

API_HOST = BaseHook.get_connection("file_api_url").host
API_PORT = BaseHook.get_connection("file_api_url").port

CH_HOOK = ClickHouseHook(clickhouse_conn_id="clickhouse")

TABLE_NAME = "procurement"
CREATE_MAIN_TABLE_QUERY = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    record_id UInt32,
    order_number String,
    contract_id String,
    batch_id String,
    order_date Date,
    expected_delivery_date Date,
    actual_delivery_date Date,
    delivery_delay_days Int32,
    delivery_on_time UInt8,
    supplier_id String,
    supplier_name String,
    supplier_country String,
    supplier_region String,
    warehouse String,
    manager String,
    incoterms String,
    product_category String,
    product_subcategory String,
    product_code String,
    unit String,
    quantity UInt32,
    quantity_rejected UInt32,
    quantity_accepted UInt32,
    currency String,
    unit_price Float32,
    currency_rate_rub Float32,
    total_cost_rub Float32,
    vat_rate Float32,
    vat_amount Float32,
    total_with_vat_rub Float32,
    transport_type String,
    delivery_temp_c Float32,
    quality_class String,
    defects_rate Float32,
    payment_terms String,
    payment_status String,
    approval_status String,
    order_weekday UInt8,
    order_month UInt8,
    order_year UInt16
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(order_date)
ORDER BY (order_date, supplier_id);
"""

column_mapping = {
    "ID_записи": "record_id",
    "Номер_заказа": "order_number",
    "ID_контракта": "contract_id",
    "ID_партии": "batch_id",
    "Дата_заказа": "order_date",
    "Ожидаемая_дата_поставки": "expected_delivery_date",
    "Фактическая_дата_поставки": "actual_delivery_date",
    "Задержка_поставки_дн": "delivery_delay_days",
    "Поставка_вовремя": "delivery_on_time",
    "ID_поставщика": "supplier_id",
    "Поставщик": "supplier_name",
    "Страна_поставщика": "supplier_country",
    "Регион_поставщика": "supplier_region",
    "Склад": "warehouse",
    "Менеджер": "manager",
    "Инкотермс": "incoterms",
    "Категория_товара": "product_category",
    "Подкатегория_товара": "product_subcategory",
    "Код_товара": "product_code",
    "Единица_изм": "unit",
    "Количество": "quantity",
    "Количество_отказано": "quantity_rejected",
    "Количество_принято": "quantity_accepted",
    "Валюта": "currency",
    "Цена_за_ед": "unit_price",
    "Курс_к_руб": "currency_rate_rub",
    "Сумма_руб": "total_cost_rub",
    "Ставка_НДС": "vat_rate",
    "Сумма_НДС": "vat_amount",
    "Сумма_с_НДС_руб": "total_with_vat_rub",
    "Тип_транспорта": "transport_type",
    "Темп_при_доставке_C": "delivery_temp_c",
    "Класс_качества": "quality_class",
    "Доля_брака": "defects_rate",
    "Условия_оплаты": "payment_terms",
    "Статус_оплаты": "payment_status",
    "Статус_согласования": "approval_status",
    "День_недели_заказа": "order_weekday",
    "Месяц_заказа": "order_month",
    "Год_заказа": "order_year"
}



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
    date_columns = ['Дата_заказа', 'Ожидаемая_дата_поставки', 'Фактическая_дата_поставки']
    for col in date_columns:
        df[col] = pd.to_datetime(df[col]).dt.date
    df = df.rename(columns=column_mapping)
    CH_HOOK.execute(f"insert into {TABLE_NAME} values", df.to_dict("records"), types_check=True)


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

create_table_task >> download_data_task >> get_current_week_data_task >> load_to_clickhouse_task
