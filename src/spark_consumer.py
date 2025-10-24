from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType

spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Lire le flux Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "users_created") \
    .option("startingOffsets", "latest") \
    .load()

# Schéma des données
schema = StructType() \
    .add("first_name", StringType()) \
    .add("last_name", StringType()) \
    .add("email", StringType()) \
    .add("username", StringType()) \
    .add("dob", StringType()) \
    .add("address", StringType())

# Parser le JSON
json_df = df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

# Afficher les données en console
query = json_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination() 



