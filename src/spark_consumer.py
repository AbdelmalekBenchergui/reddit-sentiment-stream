from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf, when
from pyspark.sql.types import StructType, StringType
from cassandra.cluster import Cluster
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import re
import logging
import time
import uuid
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import WordNetLemmatizer

# NLTK setup 
nltk.download('punkt')
nltk.download('stopwords')
nltk.download('wordnet')
nltk.download('omw-1.4')
nltk.download('vader_lexicon')

#  Logging 
logging.basicConfig(level=logging.INFO)

# Spark
def create_spark_connection():
    try:
        spark = SparkSession.builder \
            .appName("KafkaSparkStreaming") \
            .master("local[*]") \
            .config(
                "spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,"
                "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1"
            ) \
            .config("spark.cassandra.connection.host", "127.0.0.1") \
            .config("spark.cassandra.connection.port", "9042") \
            .getOrCreate()

        spark.sparkContext.setLogLevel("WARN")
        return spark
    except Exception as e:
        logging.error(f"Could not create Spark session: {e}")
        return None

def connect_to_kafka(spark):
    try:
        df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:29092") \
            .option("subscribe", "reddit_stock_comments") \
            .option("startingOffsets", "latest") \
            .load()
        return df
    except Exception as e:
        logging.error(f"Failed to connect to Kafka: {e}")
        return None

def create_selection_df_from_kafka(df):
    schema = StructType() \
        .add("id", StringType()) \
        .add("title", StringType()) \
        .add("content", StringType())

    return df.selectExpr("CAST(value AS STRING) as json") \
             .select(from_json(col("json"), schema).alias("data")) \
             .select("data.*")

# Text cleaning 
def clean_text(text):
    if not text:
        return ""
    text = " ".join(text.lower().split())
    text = re.sub(r"http\S+|@\w+|#\w+|\d+|[^a-z\s]", "", text)
    tokens = word_tokenize(text)
    if len(tokens) < 3:
        return ""
    lemmatizer = WordNetLemmatizer()
    return " ".join([lemmatizer.lemmatize(word) for word in tokens])

clean_udf = udf(clean_text, StringType())

#Sentiment Analysis 
analyzer = SentimentIntensityAnalyzer()

def vader_sentiment(text):
    if not text:
        return "neutral"
    score = analyzer.polarity_scores(text)['compound']
    if score > 0.05:
        return "positive"
    elif score < -0.05:
        return "negative"
    return "neutral"

sentiment_udf = udf(vader_sentiment, StringType())

#UUID 
uuid_udf = udf(lambda: str(uuid.uuid4()), StringType())

#  Cassandra 
def create_cassandra_connection():
    try:
        cluster = Cluster(['127.0.0.1'])
        return cluster.connect()
    except Exception as e:
        logging.error(f"Could not connect to Cassandra: {e}")
        return None

def create_keyspace_and_table(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS reddit_stock_comments
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
    """)
    session.execute("""
        CREATE TABLE IF NOT EXISTS reddit_stock_comments.posts_sentiment (
            id TEXT PRIMARY KEY,
            title TEXT,
            content TEXT,
            title_clean TEXT,
            content_clean TEXT,
            sentiment TEXT
        )
    """)
    logging.info("Keyspace and table created successfully!")

# Main pipeline 
def main():
    # Cassandra setup
    session = create_cassandra_connection()
    if session:
        create_keyspace_and_table(session)
        time.sleep(2)
        session.shutdown()

    # Spark setup
    spark = create_spark_connection()
    if not spark:
        return

    kafka_df = connect_to_kafka(spark)
    if not kafka_df:
        return

    df_posts = create_selection_df_from_kafka(kafka_df)

    # Fill missing IDs
    df_posts = df_posts.withColumn(
        "id",
        when(col("id").isNull() | (col("id") == ""), uuid_udf()).otherwise(col("id"))
    )

    # Cleaning + sentiment
    df_processed = df_posts \
        .withColumn("title_clean", clean_udf(col("title"))) \
        .withColumn("content_clean", clean_udf(col("content"))) \
        .withColumn("sentiment", sentiment_udf(col("content_clean")))

    # Remove duplicates
    df_unique = df_processed.dropDuplicates(["id"])

    # Write to Cassandra
    query = df_unique.writeStream \
        .foreachBatch(lambda batch_df, _: (
            batch_df.write
            .format("org.apache.spark.sql.cassandra")
            .mode("append")
            .options(table="posts_sentiment", keyspace="reddit_stock_comments")
            .save()
        )) \
        .option("checkpointLocation", "/tmp/checkpoint_reddit_stock_comments") \
        .start()

    logging.info("Streaming started...")
    query.awaitTermination()

if __name__ == "__main__":
    main()
