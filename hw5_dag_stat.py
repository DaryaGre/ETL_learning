from airflow import DAG
from datetime import datetime
from statistic import PostgresStatistic
from airflow.operators.sensors import ExternalTaskSensor


DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": datetime(2021, 1, 25),
    "retries": 1,
    "email_on_failure": False,
    "email_on_retry": False,
    "depends_on_past": True,
}

with DAG(
    dag_id="pg-data-flow_hw5_stat",
    default_args=DEFAULT_ARGS,
    schedule_interval="@daily",
    max_active_runs=1,
    tags=['data-flow_s'],
) as dag2:
    ts1 = ExternalTaskSensor(
        task_id='wait_for_the_first_task_to_be_completed',
        external_dag_id='pg-data-flow_hw5',
        external_task_id='customer',
        dag=dag2)
    ts2 = PostgresStatistic(
        config={'table': 'public.customer'},
        task_id='statistic',
        pg_meta_conn_str="host='db' port=5432 dbname='my_database2' user='root' password='postgres'",
    )

    ts1>>ts2