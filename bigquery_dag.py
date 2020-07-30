from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2015, 6, 1),
    'email': ['example@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG(
    dag_id='bigQueryPipeline', 
    default_args=default_args, 
    schedule_interval=timedelta(1)
)

t1 = BigQueryOperator(
    task_id='bigquery_test',
    bql='SELECT COUNT(userId) FROM [events:EVENTS_20160501]',
    destination_dataset_table=False,
    bigquery_conn_id='bigquery_default',
    delegate_to=False,
    udf_config=False,
    dag=dag,
)