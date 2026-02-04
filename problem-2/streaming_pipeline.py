"""
Basic Order Streaming Pipeline
- Read orders from Kafka
- Count orders per product every 1 minute
- Output to console
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, count, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# 1. Spark Session
spark = SparkSession.builder \
    .appName("OrderStreamingPipeline") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
    ) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Order Schema
order_schema = StructType([
    StructField("order_id", StringType()),
    StructField("order_timestamp", StringType()),
    StructField("user_id", StringType()),
    StructField("product_id", StringType()),
    StructField("quantity", IntegerType()),
    StructField("status", StringType())
])

# 3. Read from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9094") \
    .option("subscribe", "orders") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# 4. Parse JSON
orders_df = kafka_df \
    .selectExpr("CAST(value AS STRING) AS json_value") \
    .select(from_json(col("json_value"), order_schema).alias("data")) \
    .select("data.*") \
    .withColumn(
        "order_timestamp",
        to_timestamp(col("order_timestamp"), "yyyy-MM-dd HH:mm:ss")
    )

# 5. Window Aggregation
order_counts = orders_df \
    .withWatermark("order_timestamp", "2 minutes") \
    .groupBy(
        window(col("order_timestamp"), "1 minute"),
        col("product_id")
    ) \
    .agg(
        count("order_id").alias("order_count")
    ) \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("product_id"),
        col("order_count")
    )

# 6. Write to Console
query = order_counts.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .option("checkpointLocation", "/tmp/order-streaming-checkpoint") \
    .start()

query.awaitTermination()
