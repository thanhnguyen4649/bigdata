import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.ml import PipelineModel

# 1. Tạo SparkSession
spark = SparkSession.builder \
    .appName("SparkKafkaWriter") \
    .master("local[*]") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Định nghĩa schema cho dữ liệu JSON
schema = StructType([
    StructField("price (million VND)", DoubleType(), True),
    StructField("square (m2)", DoubleType(), True),
    StructField("bedroom", DoubleType(), True),
    StructField("restroom", DoubleType(), True),
    StructField("distance_to_center", DoubleType(), True),
    StructField("hospital_count", IntegerType(), True),
    StructField("hospital_avg_distance", DoubleType(), True),
    StructField("school_count", IntegerType(), True),
    StructField("school_avg_distance", DoubleType(), True),
    StructField("super_market_count", IntegerType(), True),
    StructField("super_market_avg_distance", DoubleType(), True),
    StructField("location", StringType(), True),
    StructField("date", StringType(), True),
    StructField("url", StringType(), True)
])

# 3. Đọc dữ liệu từ Kafka topic "mogi-stream"
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "127.0.0.1:9092") \
    .option("subscribe", "mogi-stream") \
    .option("startingOffsets", "earliest") \
    .load()

# Chuyển đổi cột "value" từ binary sang string
json_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str")

# 4. Parse JSON thành DataFrame theo schema đã định nghĩa
parsed_df = json_df.select(from_json(col("json_str"), schema).alias("data")).select("data.*")

# Debug: In cấu trúc schema của parsed_df
parsed_df.printSchema()

# Debug: Ghi streaming DataFrame ra console để kiểm tra dữ liệu trong 60 giây
debug_query = parsed_df.writeStream \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime="10 seconds") \
    .start()

debug_query.awaitTermination(120)

# Sau khi kiểm tra xong dữ liệu, tiến hành load mô hình và xử lý dự đoán

# 5. Load mô hình đã huấn luyện
model_path = "/Users/xuanthanhnguyen/Documents/UIT/HK2-2024/15_BigData/BCCK_Bigdata/rf_model_pipeline"
model = PipelineModel.load(model_path)

# 6. Áp dụng mô hình để tạo DataFrame dự đoán
predictions_df = model.transform(parsed_df)

# 7. Chuyển đổi DataFrame dự đoán thành định dạng phù hợp với Kafka (thêm cột key và value)
predictions_to_kafka = predictions_df.selectExpr("CAST(null AS STRING) as key") \
    .withColumn("value", to_json(struct("*")))

# 8. Ghi dữ liệu dự đoán ra Kafka topic "mogi-stream"
query = predictions_to_kafka.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "127.0.0.1:9092") \
    .option("topic", "mogi-stream") \
    .option("checkpointLocation", "/tmp/spark_checkpoint_predictions") \
    .start()

print("Streaming query started, writing predictions to Kafka topic 'mogi-stream' ...")
query.awaitTermination()
