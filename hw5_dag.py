from airflow import DAG
from postgres import DataTransferPostgres
from datetime import datetime

DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": datetime(2021, 1, 25),
    "retries": 1,
    "email_on_failure": False,
    "email_on_retry": False,
    "depends_on_past": True,
}

with DAG(
    dag_id="pg-data-flow_hw5",
    default_args=DEFAULT_ARGS,
    schedule_interval="@daily",
    max_active_runs=1,
    tags=['data-flow'],
) as dag1:
    t1 = DataTransferPostgres(
        config={'table': 'public.customer'},
        query='select * from customer',
        task_id='customer',
        source_pg_conn_str="host='db2' port=5432 dbname='my_database' user='root' password='postgres'",
        pg_conn_str="host='db' port=5432 dbname='my_database2' user='root' password='postgres'",
        pg_meta_conn_str="host='db' port=5432 dbname='my_database2' user='root' password='postgres'",
    )