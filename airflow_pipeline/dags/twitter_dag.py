import sys
sys.path.append("airflow_pipeline")

from airflow.models import DAG
from datetime import datetime, timedelta
from operators.twitter_operator import TwitterOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from os.path import join
from airflow.utils.dates import days_ago
from pathlib import Path

# TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S"
# current_date = datetime.now().date().strftime(TIMESTAMP_FORMAT)

with DAG(dag_id = "TwitterDAG", start_date=days_ago(6), schedule_interval="@daily") as dag:
    
    QUERY = "datascience"

    twitter_operator = TwitterOperator(
        task_id="twitter_datascience",
        file_path=join(
            "datalake/twitter_spark/extract_date={{ data_interval_start.strftime('%Y-%m-%d') }}", 
            "{{ data_interval_start.strftime('%Y-%m-%d') }}.json"
        ),
        start_time="{{ data_interval_start.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}",
        end_time="{{ data_interval_end.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}",
        query=QUERY
    )

    twitter_transform = SparkSubmitOperator(
        task_id="twitter_extract",
        name="twitter_extract",
        packages="io.delta:delta-core_2.12:2.4.0",
        conf={
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        },
        application="/Volumes/KINGSTON/Projects/ApacheAirflowTransformacaoDadosSpark/spark/job_transformation.py",
        application_args=[
            "--lake_src", "/Volumes/KINGSTON/Projects/ApacheAirflowTransformacaoDadosSpark/datalake/twitter_spark",
            "--lake_target", "/Volumes/KINGSTON/Projects/ApacheAirflowTransformacaoDadosSpark/datalake/landing_new",
        ]
    )


twitter_operator >> twitter_transform