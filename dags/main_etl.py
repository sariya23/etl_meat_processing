from airflow import DAG
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator

dag = DAG("main_etl", schedule=None, max_active_runs=1)


create_table_task = ClickHouseOperator(
    task_id="create_table_click",
    sql="create table if not exists test (id String) engine=MergeTree() order by id",
    clickhouse_conn_id="clickhouse",
    dag=dag,
)