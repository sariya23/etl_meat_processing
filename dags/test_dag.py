from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator

with DAG("test_dag", schedule=None, start_date=datetime.today()) as dag:
    task1 = PythonOperator(
        task_id="task1",
        python_callable=lambda : print("HELLO WORLD")
    )
    
    task1