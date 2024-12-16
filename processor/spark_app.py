import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, avg, count, expr
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import redis
import logging
import json


KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
INPUT_TOPIC = os.environ.get("INPUT_TOPIC", "input_topic")
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


spark = (SparkSession.builder
         .appName("Processor")
         .getOrCreate())

spark.sparkContext.setLogLevel("WARN")


schema = StructType([
    StructField("Price", DoubleType(), True),
    StructField("District", StringType(), True),
    StructField("Type", StringType(), True),
    StructField("TotalArea", DoubleType(), True)
])


df = (spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
      .option("subscribe", INPUT_TOPIC)
      .option("startingOffsets", "latest")
      .load())


parsed = (df
          .selectExpr("CAST(value AS STRING) as json_str")
          .withColumn("data", from_json(col("json_str"), schema))
          .select("data.*")
          .filter((col("Price").isNotNull()) &
                  (col("TotalArea").isNotNull()) &
                  (col("Type").isNotNull()) &
                  (col("District").isNotNull()))
          .filter((col("Price") >= 50000) & (col("TotalArea") >= 20)))


aggregated = (parsed
              .groupBy("Type", "District")
              .agg(
                  avg("Price").alias("AveragePrice"),
                  avg("TotalArea").alias("AverageArea"),
                  count("*").alias("TotalListings"),
                  expr("avg(Price / TotalArea)").alias("AveragePricePerSquareMeter")
              ))


global_aggregated = (parsed
                     .agg(
                         avg("Price").alias("GlobalAveragePrice"),
                         avg("TotalArea").alias("GlobalAverageArea"),
                         count("*").alias("GlobalTotalListings"),
                         expr("avg(Price / TotalArea)").alias("GlobalAveragePricePerSquareMeter")
                     ))


def write_to_redis(batch_df, epoch_id):
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)

    for row in batch_df.collect():
        key = f"{row['Type']}:{row['District']}"
        value = {
            "AveragePrice": row["AveragePrice"],
            "AverageArea": row["AverageArea"],
            "TotalListings": row["TotalListings"],
            "AveragePricePerSquareMeter": row["AveragePricePerSquareMeter"]
        }
        r.set(key, json.dumps(value))

    logging.info(f"Batch {epoch_id} (Grouped) processed.")


def write_global_to_redis(global_df, epoch_id):
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)

    global_metrics = global_df.collect()[0]
    key = "global_metrics"
    value = {
        "GlobalAveragePrice": global_metrics["GlobalAveragePrice"],
        "GlobalAverageArea": global_metrics["GlobalAverageArea"],
        "GlobalTotalListings": global_metrics["GlobalTotalListings"],
        "GlobalAveragePricePerSquareMeter": global_metrics["GlobalAveragePricePerSquareMeter"]
    }
    r.set(key, json.dumps(value))

    logging.info(f"Batch {epoch_id} (Global) processed.")


grouped_query = (aggregated
                 .writeStream
                 .foreachBatch(write_to_redis)
                 .outputMode("complete")
                 .trigger(processingTime="30 seconds")
                 .start())

global_query = (global_aggregated
                .writeStream
                .foreachBatch(write_global_to_redis)
                .outputMode("complete")
                .trigger(processingTime="30 seconds")
                .start())

grouped_query.awaitTermination()
global_query.awaitTermination()
