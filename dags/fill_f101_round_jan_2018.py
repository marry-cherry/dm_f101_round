from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'marina'
}

with DAG(
    dag_id='fill_f101_round_jan_2018',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    fill_f101 = PostgresOperator(
        task_id='fill_f101_for_jan_2018',
        postgres_conn_id='postgres_default',
        sql="CALL dm.fill_f101_round_f(DATE '2018-02-01');"
    )

