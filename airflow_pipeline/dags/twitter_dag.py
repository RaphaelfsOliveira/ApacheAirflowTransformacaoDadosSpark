import sys
sys.path.append("airflow_pipeline")

from airflow.models import DAG
from datetime import datetime, timedelta
from operators.twitter_operator import TwitterOperator

from os.path import join
from airflow.utils.dates import days_ago
from pathlib import Path

with DAG(dag_id = "TwitterDAG", start_date=days_ago(6), schedule_interval="@daily") as dag:
    TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"
    query = "datascience"

    twitter_operator = TwitterOperator(file_path=join("curso2/datalake/twitter_datascience/extract_date={{ data_interval_start.strftime('%Y-%m-%d') }}",
                                        "datascience_{{ ds_nodash }}.json"),
                                        query=query,
                                        start_time="{{ data_interval_start.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}",
                                        end_time="{{ data_interval_end.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}",
                                        task_id="twitter_datascience")

twitter_operator