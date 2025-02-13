# kafka_consumer.py
from kafka import KafkaConsumer
import json


def run_consumer():
    # Basic Kafka consumer for monitoring/debugging
    consumer = KafkaConsumer(
        "mogi-stream",
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='latest',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    print("Starting Kafka consumer...")
    try:
        for message in consumer:
            print(f"Received: {message.value}")
    except KeyboardInterrupt:
        print("Stopping consumer...")
        consumer.close()


if __name__ == "__main__":
    run_consumer()

# Improved spark_consumer_model.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.ml import PipelineModel


def create_spark_session():
    return SparkSession.builder \
        .appName("SparkKafkaMogiConsumer") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.shuffle.partitions", "2") \
        .master("local[*]") \
        .getOrCreate()


def process_stream(spark, model_path):
    # Schema definition (same as before)
    json_schema = StructType([...])  # Your existing schema

    # Read from Kafka with more configurations
    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "mogi-stream") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .option("maxOffsetsPerTrigger", "100") \
        .load()

    # Parse and process
    value_df = kafka_df.selectExpr("CAST(value AS STRING) as json_string")
    parsed_df = value_df \
        .select(from_json(col("json_string"), json_schema).alias("data")) \
        .select("data.*") \
        .withColumn("processing_time", current_timestamp())

    # Load and apply model
    model = PipelineModel.load(model_path)
    predictions = model.transform(parsed_df)

    # Write stream with checkpointing
    query = predictions \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .option("checkpointLocation", "/tmp/checkpoint/mogi_predictions") \
        .trigger(processingTime="5 seconds") \
        .start()

    return query


if __name__ == "__main__":
    spark = create_spark_session()
    model_path = "/Users/xuanthanhnguyen/Documents/UIT/HK2-2024/15_BigData/BCCK_Bigdata/rf_model_pipeline"
    query = process_stream(spark, model_path)

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("Stopping stream...")
        query.stop()
        spark.stop()