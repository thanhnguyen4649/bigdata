#spark_consumer_model.py
from pyspark.sql import SparkSession, Row
from pyspark.ml.pipeline import PipelineModel

# 1) Khởi tạo SparkSession (nếu chưa có)
spark = SparkSession.builder \
    .appName("SparkKafkaMogiConsumer") \
    .getOrCreate()

# 2) Load mô hình Pipeline đã train/lưu
model_path = "/Users/xuanthanhnguyen/Documents/UIT/HK2-2024/15_BigData/BCCK_Bigdata/rf_model_pipeline"
model = PipelineModel.load(model_path)

# === Ví dụ data_dict nhận được từ Kafka ===
data_dict = {
    "price (million VND)": 7e-06,
    "square (m2)": 352.0,
    "bedroom": 1.0,
    "restroom": 1.0,
    "distance_to_center": 1.9189704840567312,
    "hospital_count": 14,
    "hospital_avg_distance": 3.555000982973983,
    "school_count": 71,
    "school_avg_distance": 3.401318813709558,
    "super_market_count": 147,
    "super_market_avg_distance": 3.1459648863270386,
    "location": "Phan Văn Hân, Phường 19, Quận Bình Thạnh, TPHCM",
    "date": "04/02/2025",
    "url": "https://mogi.vn/..."
}

# 3) Tạo DataFrame 1 dòng từ dict
new_df = spark.createDataFrame([Row(**data_dict)])
new_df.show()
new_df.printSchema()

# 4) Dùng pipeline model để dự đoán
predictions = model.transform(new_df)
predictions.show(truncate=False)

# 5) Nếu model là phân loại có dùng StringIndexer để chuyển label -> index
#    thì bạn giải mã nhãn (label) như sau:
indexer_model = model.stages[0]  # pipeline=[stringIndexer, vectorAssembler, rfModel]
labels = indexer_model.labels

# Lấy prediction
pred_row = predictions.collect()[0]
pred_label_idx = int(pred_row["prediction"])
pred_label = labels[pred_label_idx]

print("Dự đoán nhãn:", pred_label)
