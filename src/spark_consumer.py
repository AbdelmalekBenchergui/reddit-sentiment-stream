from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType
from cassandra.cluster import Cluster
import logging, time

# ----------------- Spark -----------------
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

# ----------------- Kafka -----------------
def connect_to_kafka(spark):
    try:
        df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:29092") \
            .option("subscribe", "users_created") \
            .option("startingOffsets", "latest") \
            .load()
        return df
    except Exception as e:
        logging.error(f"Failed to connect to Kafka: {e}")
        return None

def create_selection_df_from_kafka(df):
    schema = StructType() \
        .add("first_name", StringType()) \
        .add("last_name", StringType()) \
        .add("email", StringType()) \
        .add("username", StringType()) \
        .add("dob", StringType()) \
        .add("address", StringType())

    return df.selectExpr("CAST(value AS STRING) as json") \
             .select(from_json(col("json"), schema).alias("data")) \
             .select("data.*")

# ----------------- Cassandra -----------------
def create_cassandra_connection():
    try:
        cluster = Cluster(['127.0.0.1'])  # Cassandra accessible depuis localhost
        session = cluster.connect()
        return session
    except Exception as e:
        logging.error(f"Could not connect to Cassandra: {e}")
        return None

def create_keyspace_and_table(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
    """)
    session.execute("""
        CREATE TABLE IF NOT EXISTS spark_streams.users_created (
            username TEXT PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            email TEXT,
            dob TEXT,
            address TEXT
        )
    """)
    logging.info("Keyspace and table created successfully!")

# ----------------- Main -----------------
def main():
    session = create_cassandra_connection()
    if session:
        create_keyspace_and_table(session)
        time.sleep(2)
        session.shutdown()

    spark_conn = create_spark_connection()
    if spark_conn:
        spark_df = connect_to_kafka(spark_conn)
        if spark_df:
            selection_df = create_selection_df_from_kafka(spark_df)

            streaming_query = (selection_df.writeStream
                .foreachBatch(lambda batch_df, _: (
                    batch_df.write
                    .format("org.apache.spark.sql.cassandra")
                    .mode("append")
                    .options(table="users_created", keyspace="spark_streams")
                    .save()
                ))
                .option('checkpointLocation', '/tmp/checkpoint')
                .start())

            streaming_query.awaitTermination()

if __name__ == "__main__":
    main()
