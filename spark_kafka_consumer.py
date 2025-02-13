
#spark_kafka_consumer.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.ml import PipelineModel

# 1) Tạo SparkSession
spark = SparkSession.builder \
    .appName("SparkKafkaMogiConsumer") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("DEBUG")


# 2) Định nghĩa schema để parse JSON (khớp với dữ liệu bạn gửi từ Kafka)
#    Bạn phải khai báo đúng tên field, kiểu dữ liệu.
#    Ví dụ: data = {
#          "price (million VND)": 3.2,
#          "square (m2)": 75.0,
#          "bedroom": 2.0,
#          "restroom": 2.0,
#          "distance_to_center": 5.26,
#          ...
#    }
json_schema = StructType([
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
    StructField("url", StringType(), True),
])

# 3) Đọc dữ liệu từ Kafka (dạng streaming)
df_kafka = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "mogi-stream") \
  .option("startingOffsets", "earliest") \
  .load()


# 4) Trích cột "value" (dữ liệu JSON), ép kiểu STRING
df_value_str = df_kafka.selectExpr("CAST(value AS STRING) as json_string")

# 5) Parse json_string thành các cột tương ứng
df_parsed = df_value_str \
    .select(from_json(col("json_string"), json_schema).alias("data")) \
    .select("data.*")  # bung tất cả field ra

# 6) Load mô hình (PipelineModel) đã train và lưu
model_path = "file:///Users/xuanthanhnguyen/Documents/UIT/HK2-2024/15_BigData/BCCK_Bigdata/rf_model_pipeline"
model = PipelineModel.load(model_path)

# 7) Áp dụng mô hình để transform/predict
#    Giả sử trong pipeline có các bước VectorAssembler, v.v...
#    Lúc transform, Spark sẽ tạo thêm các cột như "prediction", "probability", ...
df_predictions = model.transform(df_parsed)

# 8) Ghi kết quả ra console (hoặc ghi vào file, DB, ...)
query = df_predictions \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
