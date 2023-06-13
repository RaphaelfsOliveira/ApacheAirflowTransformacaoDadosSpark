export AIRFLOW_HOME=$(pwd)/airflow_pipeline
export SPARK_HOME=/usr/local/Cellar/apache-spark/3.4.0/libexec
export PYTHONPATH=/usr/local/Cellar/apache-spark/3.4.0/libexec/python/:$PYTHONP$

# airflow standalone
# airflow scheduler
airflow webserver