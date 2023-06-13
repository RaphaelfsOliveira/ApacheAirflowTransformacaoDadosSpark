from pyspark.sql import SparkSession
from delta import * 

def spark_build():
    builder = SparkSession.builder.appName("twitter_transform") \
            .master("local[*]")\
            .config("spark.driver.host", "127.0.0.1")\
            .config("spark.jars.packages", "io.delta:delta-core_2.12:2.0.0")\
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    return spark