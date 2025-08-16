from airflow import DAG
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook 
from datetime import datetime
import requests
import pandas as pd


API_HOST = BaseHook.get_connection("file_api_url").host
API_PORT = BaseHook.get_connection("file_api_url").port


CREATE_MAIN_TABLE_QUERY = """
CREATE TABLE IF NOT EXISTS procurement (
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

def download_data_to_local_file(url: str, **context):
    response = requests.get(url)
    file_name = url.split("/")[-1]
    if not response.ok:
        print(response.content)
        print(url)
        raise requests.exceptions.RequestException()

    with open(file_name, "wb") as f:
        f.write(response.content)
    

dag = DAG("main_etl", schedule="0 17 * * 5", start_date=datetime(2025, 8, 15), max_active_runs=1)


create_table_task = ClickHouseOperator(
    task_id="create_table_click",
    sql=CREATE_MAIN_TABLE_QUERY,
    clickhouse_conn_id="clickhouse",
    dag=dag,
)

download_data = PythonOperator(
    task_id="download_data_to_local_csv",
    python_callable=download_data_to_local_file,
    op_args=[f"{API_HOST}:{API_PORT}/download/" + "{{ ds }}.xlsx"],
    dag=dag,
)

create_table_task >> download_data