import sys
sys.path.append("airflow_pipeline")

from airflow.models import DAG
from datetime import datetime, timedelta
from operators.twitter_operator import TwitterOperator

from os.path import join
from airflow.utils.dates import days_ago
from pathlib import Path

TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S"
current_date = datetime.now().date().strftime(TIMESTAMP_FORMAT)

with DAG(dag_id = "TwitterDAG", start_date=days_ago(6), schedule_interval="@daily") as dag:
    
    query = "datascience"
    print(current_date)

    twitter_operator = TwitterOperator(
        file_path=join("datalake/twitter_spark/extract_date={{ data_interval_start.strftime('%Y-%m-%d') }}", "{{ dag_run.logical_date | ts }}.json"),
        start_time="{{ data_interval_start.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}",
        end_time="{{ data_interval_end.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}",
        query=query,
        
        task_id="twitter_datascience")

twitter_operator