# --------------------------------------------------------------------------------
# Load The Dependencies
# --------------------------------------------------------------------------------

import datetime
import io
import logging

from airflow import models
from airflow.operators import dummy_operator
from airflow.operators.bash_operator import BashOperator


# --------------------------------------------------------------------------------
# Set default arguments
# --------------------------------------------------------------------------------

yesterday = datetime.datetime.now() - datetime.timedelta(days=1)

default_args = {
    'owner': 'airflow',
    'start_date': yesterday,
    'depends_on_past': False,
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

logger = logging.getLogger('ecomm_ETL_batch')


# --------------------------------------------------------------------------------
# Main DAG
# --------------------------------------------------------------------------------

# Define a DAG (directed acyclic graph) of tasks.
# Any task you create within the context manager is automatically added to the
# DAG object.
with models.DAG(
        'composer_ETL_batch',
        default_args=default_args,
        schedule_interval=None) as dag:
    start = dummy_operator.DummyOperator(
        task_id='start',
        trigger_rule='all_success'
    )

    end = dummy_operator.DummyOperator(
        task_id='end',
        trigger_rule='all_success'
    )

    extract_transform_task = BashOperator(
        task_id="ExtractTransform",
        bash_command="python /home/airflow/gcs/dags/scripts/LoadStaging.py"
    )
    load_task = BashOperator(
        task_id="LoadTarget",
        bash_command="python /home/airflow/gcs/dags/scripts/LoadTarget.py"
    )
    start >> extract_transform_task >> load_task >> end
    