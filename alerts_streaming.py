"""ДЗ6. Spark Structured Streaming.

Читає потік даних датчиків з Kafka, рахує середню температуру та вологість
у ковзному вікні (1 хв, крок 30 с, watermark 10 с), порівнює середні значення
з умовами з alerts_conditions.csv і записує алерти у вихідний Kafka-топік.
"""
import os

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (DoubleType, IntegerType, StringType, StructField,
                               StructType)

from configs import TOPIC_AVG_ALERTS, TOPIC_SENSORS, kafka_config

WINDOW_DURATION = "1 minute"
SLIDING_INTERVAL = "30 seconds"
WATERMARK_DELAY = "10 seconds"
CONDITIONS_FILE = "alerts_conditions.csv"
CHECKPOINT_DIR = "checkpoints/avg_alerts"

# Пакет для роботи Spark із Kafka (версія збігається з встановленим pyspark)
KAFKA_PACKAGE = f"org.apache.spark:spark-sql-kafka-0-10_2.13:{pyspark.__version__}"

spark = (SparkSession.builder
         .appName("IoTAlertsStreaming")
         .master("local[*]")
         .config("spark.jars.packages", KAFKA_PACKAGE)
         .config("spark.sql.shuffle.partitions", "2")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

jaas = ("org.apache.kafka.common.security.plain.PlainLoginModule required "
        f'username="{kafka_config["username"]}" password="{kafka_config["password"]}";')
kafka_options = {
    "kafka.bootstrap.servers": ",".join(kafka_config["bootstrap_servers"]),
    "kafka.security.protocol": kafka_config["security_protocol"],
    "kafka.sasl.mechanism": kafka_config["sasl_mechanism"],
    "kafka.sasl.jaas.config": jaas,
}

# ---------------------------------------------------------------
# 1. Вхідний потік з Kafka (дані з sensor_producer.py)
# ---------------------------------------------------------------
sensor_schema = StructType([
    StructField("sensor_id", IntegerType()),
    StructField("timestamp", StringType()),
    StructField("temperature", DoubleType()),
    StructField("humidity", DoubleType()),
])

sensors_df = (spark.readStream
              .format("kafka")
              .options(**kafka_options)
              .option("subscribe", TOPIC_SENSORS)
              .option("startingOffsets", "latest")
              .option("maxOffsetsPerTrigger", "500")
              .load()
              .select(F.from_json(F.col("value").cast("string"), sensor_schema).alias("data"))
              .select("data.*")
              .withColumn("timestamp", F.to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss")))

# ---------------------------------------------------------------
# 2. Агрегація: sliding window 1 хв / 30 с, watermark 10 с
# ---------------------------------------------------------------
avg_df = (sensors_df
          .withWatermark("timestamp", WATERMARK_DELAY)
          .groupBy(F.window("timestamp", WINDOW_DURATION, SLIDING_INTERVAL))
          .agg(F.avg("temperature").alias("t_avg"),
               F.avg("humidity").alias("h_avg")))

# ---------------------------------------------------------------
# 3. Умови алертів з файлу (можна змінювати без зміни коду)
# ---------------------------------------------------------------
conditions_df = spark.read.csv(CONDITIONS_FILE, header=True, inferSchema=True)

# ---------------------------------------------------------------
# 4. Визначення алертів: cross join + фільтрація
#    -999 означає, що межа для цього алерту не використовується
# ---------------------------------------------------------------
def within(value, low, high):
    return (((F.col(low) == -999) | (F.col(value) > F.col(low))) &
            ((F.col(high) == -999) | (F.col(value) < F.col(high))))

alerts_df = (avg_df
             .crossJoin(conditions_df)
             .where(within("t_avg", "temperature_min", "temperature_max") &
                    within("h_avg", "humidity_min", "humidity_max"))
             .select(
                 F.col("window"),
                 F.col("t_avg"),
                 F.col("h_avg"),
                 F.col("code").cast("string").alias("code"),
                 F.col("message"),
                 F.date_format(F.current_timestamp(), "yyyy-MM-dd HH:mm:ss.SSSSSS")
                  .alias("timestamp"),
             ))

# ---------------------------------------------------------------
# 5. Запис алертів у Kafka (+ дублюємо в консоль для наочності)
# ---------------------------------------------------------------
kafka_query = (alerts_df
               .select(F.col("code").alias("key"),
                       F.to_json(F.struct(*alerts_df.columns)).alias("value"))
               .writeStream
               .format("kafka")
               .options(**kafka_options)
               .option("topic", TOPIC_AVG_ALERTS)
               .option("checkpointLocation", CHECKPOINT_DIR)
               .outputMode("update")
               .start())

console_query = (alerts_df.writeStream
                 .format("console")
                 .option("truncate", "false")
                 .outputMode("update")
                 .start())

print(f"Streaming from '{TOPIC_SENSORS}' to '{TOPIC_AVG_ALERTS}'. Stop with Ctrl+C.")
try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    print("\nStopping streams...")
    kafka_query.stop()
    console_query.stop()
    spark.stop()
