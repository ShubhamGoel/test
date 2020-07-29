from airflow import DAG
from airflow.operators import PythonOperator, BashOperator
from airflow.hooks import PostgresHook
from datetime import datetime, timedelta

# change these args as needed
default_args = {
    'owner': 'owner',
    'depends_on_past': False,
    'start_date': datetime(2020, 3, 3),
    'email': ['owner@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG('test_sql', default_args=default_args)

pg_hook = PostgresHook(postgres_conn_id='cloudsql')

# replace 'table' with a valid table in your DB
def test_query():
    q = 'SELECT * FROM leads;'
    results = pg_hook.get_records(q)
    print(results[:5])

task1 = PythonOperator(
            task_id='extract',
            python_callable=test_query,
            dag=dag)

task2 = BashOperator(
            task_id='denote_finish',
            bash_command='echo "ALL DONE"',
            dag=dag)

task1 >> task2
