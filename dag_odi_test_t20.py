from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'SATISH MUDDE',
    'start_date': datetime(2023, 12, 18),
    'depends_on_past': False,
    'email': ['satishmudde.gcp@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'fetch_odi_test_t20_cricket_stats',
    default_args=default_args,
    description='Runs an external Python script',
    schedule_interval='@daily',
    catchup=False
)

with dag:
    # Task 1 - Start (Dummy)
    start = DummyOperator(
        task_id='start_task',
        trigger_rule='all_success'
    )

    # Task 2 - Run Python Script
    run_odi_script_task = BashOperator(
        task_id='run_extract_and_push_gcs_odi_script',
        bash_command='python /home/airflow/gcs/dags/scripts/odi_extract_and_push_gcs.py',
        trigger_rule='all_success'
    )

    # Task 3 - Run Python Script
    run_test_script_task = BashOperator(
        task_id='run_extract_and_push_gcs_test_script',
        bash_command='python /home/airflow/gcs/dags/scripts/test_extract_and_push_gcs.py',
        trigger_rule='all_success'
    )

    # Task 4 - Run Python Script
    run_t20_script_task = BashOperator(
        task_id='run_extract_and_push_gcs_t20_script',
        bash_command='python /home/airflow/gcs/dags/scripts/t20_extract_and_push_gcs.py',
        trigger_rule='all_success'
    )
    

    # Task 5 - End (Dummy)
    end = DummyOperator(
        task_id='end_task',
        trigger_rule='all_success'
    )

    # Set task order
    # start >> run_odi_script_task >> run_test_script_task >> run_t20_script_task >> end

    # start >> [run_odi_script_task,run_test_script_task,run_t20_script_task] >> end

    # start >> [run_odi_script_task,run_test_script_task] >> run_t20_script_task >> end

start >> run_odi_script_task >> [run_test_script_task,run_t20_script_task] >> end