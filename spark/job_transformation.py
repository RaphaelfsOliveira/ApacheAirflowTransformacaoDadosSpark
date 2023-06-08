from pyspark.sql import functions as f

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

def datalake_write_delta(df, layer: str, folder: str):
    path = f"../datalake/{layer}/{folder}"
    
    df.write.format("delta").mode("overwrite")\
        .option("overwriteSchema", "true")\
        .save(path)


