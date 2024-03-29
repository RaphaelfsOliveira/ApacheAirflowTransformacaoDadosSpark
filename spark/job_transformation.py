from pyspark.sql import SparkSession
from pyspark.sql import functions as f
# from delta import * TODO: deletar depois de testar mais
import argparse
from spark_builder import spark_build


def get_tweets_data(df):
    df = df.select(
        f.explode("data").alias("tweets")
    ).select(
        "tweets.author_id", 
        "tweets.conversation_id", 
        "tweets.created_at",
        "tweets.id",
        "tweets.in_reply_to_user_id",
        "tweets.lang",
        "tweets.public_metrics.*",
        "tweets.text"
    )

    return df


def get_users_data(df):
    df = df.select(
        f.explode("includes.users").alias("user")
    ).select(
        "user.*",
    )

    return df


def datalake_write_delta(df, lake_target: str, folder: str):
    path = f"{lake_target}/{folder}"
    
    df.write.format("delta").mode("overwrite")\
        .option("overwriteSchema", "true")\
        .save(path)
    
    return True


def twitter_read(spark, lake_src: str):
    df = spark.read.format("json").load(lake_src)
    return df


def twitter_extract(spark, lake_src: str, layer: str):

    try:
        df = twitter_read(spark, lake_src)
        df_tweet = get_tweets_data(df)
        df_user = get_users_data(df)

        datalake_write_delta(df_tweet, layer, "tweet")
        datalake_write_delta(df_user, layer, "user")

        return {'status': 200, 'msg': 'OK'}
    
    except Exception as err:

        return {'status': 500, 'msg': str(err)}

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Spark Twitter Load"
    )

    parser.add_argument("--lake_src", required=True)
    parser.add_argument("--lake_target", required=True)

    args = parser.parse_args()

    spark = spark_build()

    twitter_extract(spark, args.lake_src, args.lake_target)