from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

from datetime import datetime

spark = SparkSession.builder \
    .appName("SensorAlertsProcessing") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("id", IntegerType()),
    StructField("temperature", DoubleType()),
    StructField("humidity", DoubleType()),
    StructField("timestamp", TimestampType())
])

alerts_conditions = spark.read.csv("alerts_conditions.csv", header=True, inferSchema=True)

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor_data") \
    .option("startingOffsets", "earliest") \
    .load()

json_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

aggregated_df = json_df \
    .withWatermark("timestamp", "10 seconds") \
    .groupBy(
        window(col("timestamp"), "1 minute", "30 seconds")
    ) \
    .agg(
        avg("temperature").alias("t_avg"),
        avg("humidity").alias("h_avg")
    )


def process_batch(batch_df, batch_id):

    joined_df = batch_df.crossJoin(alerts_conditions)
    alerts = joined_df.filter(
        # Temperature Logic
        ((col("temperature_max") != -999) & (col("t_avg") > col("temperature_max"))) |
        ((col("temperature_min") != -999) & (col("t_avg") < col("temperature_min"))) |
        # Humidity Logic
        ((col("humidity_max") != -999) & (col("h_avg") > col("humidity_max"))) |
        ((col("humidity_min") != -999) & (col("h_avg") < col("humidity_min")))
    )

    total_qty = batch_df.count()
    crossJoin_qty = joined_df.count()
    alert_qty = alerts.count()
    skipped_qty = crossJoin_qty - alert_qty

    timestamp_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"Qty total: {total_qty}")
    print(f"Qty join: {crossJoin_qty}")
    print(f"Qty aleterd: {alert_qty}")
    print(f"Qty skipped: {skipped_qty}")

    if alert_qty > 0:
        kafka_output = alerts.select(
            struct(
                col("window.start").cast("string").alias("start"),
                col("window.end").cast("string").alias("end")
            ).alias("window"),
            "t_avg", "h_avg", "code", "message",
            current_timestamp().cast("string").alias("timestamp")
        ).selectExpr("CAST(NULL AS STRING) AS key", "to_json(struct(*)) AS value")

        kafka_output.write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("topic", "alerts_topic") \
            .save()

query = aggregated_df.writeStream \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", "./checkpoint") \
    .start()

query.awaitTermination()