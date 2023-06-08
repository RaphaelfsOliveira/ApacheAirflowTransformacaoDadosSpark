# ApacheAirflowTransformacaoDadosSpark
curso alura MOC para um projeto

Notes after install delta lake restar cluster to install new library from delta in cluster spark

#### install 
brew install apache-spark

#### run spark-submit
/usr/local/bin/spark-submit

#### Test spark-submit with Delta table
spark-submit --packages io.delta:delta-core_2.12:2.4.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" spark/job_transformation.py --lake_src datalake/twitter_spark --layer_target datalake/landing