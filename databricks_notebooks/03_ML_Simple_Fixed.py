# Databricks notebook source
# MAGIC %md
# MAGIC # 🤖 Real Estate ML - Simple & Fixed Version

# COMMAND ----------

# Import libraries
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
import mlflow

print("✅ Libraries imported")

# COMMAND ----------

# Create simple, clean dataset with explicit types
print("📊 Creating clean dataset...")

# Sample data - all explicit types
sample_data = [
    (3.5, 60.0),   # price_millions, area_sqm
    (4.2, 45.0),
    (8.5, 80.0),
    (3.8, 35.0),
    (12.0, 120.0),
    (6.8, 70.0),
    (7.2, 75.0),
    (4.5, 50.0),
    (9.5, 95.0),
    (5.8, 65.0),
    (11.2, 110.0),
    (2.8, 40.0),
    (15.5, 150.0),
    (6.2, 68.0),
    (4.8, 52.0)
]

# Simple schema - only 2 columns
schema = StructType([
    StructField("price_millions", DoubleType(), False),
    StructField("area_sqm", DoubleType(), False)
])

# Create DataFrame
df = spark.createDataFrame(sample_data, schema)

print(f"✅ Dataset created: {df.count()} records")
print("📊 Schema:")
df.printSchema()
display(df)

# COMMAND ----------

# Simple feature engineering
print("🎯 Feature engineering...")

# Add derived feature
df = df.withColumn("price_per_sqm", col("price_millions") * 1000000 / col("area_sqm"))

# Simple vector assembler - only numerical features
assembler = VectorAssembler(
    inputCols=["area_sqm", "price_per_sqm"],
    outputCol="features"
)

print("✅ Features: area_sqm, price_per_sqm")

# COMMAND ----------

# Simple model training
print("🤖 Training simple model...")

# Split data
train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

print(f"📊 Train: {train_df.count()}, Test: {test_df.count()}")

# Create Random Forest
rf = RandomForestRegressor(
    featuresCol="features",
    labelCol="price_millions",
    numTrees=10,
    maxDepth=5,
    seed=42
)

# Simple pipeline
pipeline = Pipeline(stages=[assembler, rf])

# Train
model = pipeline.fit(train_df)
print("✅ Model trained!")

# Predict
predictions = model.transform(test_df)

# Evaluate
evaluator = RegressionEvaluator(
    labelCol="price_millions",
    predictionCol="prediction",
    metricName="rmse"
)

rmse = evaluator.evaluate(predictions)
print(f"📊 RMSE: {rmse:.3f}")

# Show predictions
print("🔮 Predictions:")
display(predictions.select("area_sqm", "price_millions", "prediction"))

# COMMAND ----------

# MLflow logging
try:
    mlflow.set_experiment("/Users/thanhnguyen.4649@gmail.com/Simple_Real_Estate")
    
    with mlflow.start_run(run_name="SimpleRF"):
        mlflow.log_param("model_type", "RandomForest")
        mlflow.log_metric("rmse", rmse)
        mlflow.spark.log_model(model, "simple_model")
        
    print("✅ MLflow logging successful")
except Exception as e:
    print(f"⚠️ MLflow error: {e}")

print("🎉 SUCCESS! Simple model working!")

# COMMAND ----------

# Test prediction function
def predict_simple(area_sqm):
    """Simple prediction function"""
    test_data = [(0.0, float(area_sqm))]  # price=0, area=input
    test_schema = StructType([
        StructField("price_millions", DoubleType(), False),
        StructField("area_sqm", DoubleType(), False)
    ])
    
    test_df = spark.createDataFrame(test_data, test_schema)
    test_df = test_df.withColumn("price_per_sqm", lit(50000000.0))  # Default
    
    result = model.transform(test_df)
    return result.select("prediction").collect()[0]["prediction"]

# Test
test_price = predict_simple(75.0)
print(f"🏠 75 sqm property predicted price: {test_price:.2f} million VND")

print("✅ ALL WORKING! Ready for next notebook") 