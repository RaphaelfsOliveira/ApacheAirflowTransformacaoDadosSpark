# ApacheAirflowTransformacaoDadosSpark
curso alura MOC para um projeto

Notes after install delta lake restar cluster to install new library from delta in cluster spark

#### Run Airflow by script
root folder: init_airflow.sh

#### install 
brew install apache-spark

#### run spark-submit
/usr/local/bin/spark-submit

#### Test spark-submit with Delta table
spark-submit --packages io.delta:delta-core_2.12:2.4.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" spark/job_transformation.py --lake_src datalake/twitter_spark --lake_target datalake/landing

#### ENV vars to run Airflow with Spark
run in terminal:

export AIRFLOW_HOME=$(pwd)/airflow_pipeline
export SPARK_HOME=/usr/local/Cellar/apache-spark/3.4.0/libexec
export PYTHONPATH=/usr/local/Cellar/apache-spark/3.4.0/libexec/python/:$PYTHONP$