# Databricks notebook source
# MAGIC %md
# MAGIC # 🌊 Real Estate Streaming Data Processing - Fixed Version
# MAGIC 
# MAGIC **Objective**: Process real estate data streams with Delta Lake integration

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📚 Import Libraries

# COMMAND ----------

import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.streaming import *
from delta.tables import DeltaTable
import json
import time
import uuid
from datetime import datetime, timedelta
import random

print("✅ Libraries imported successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🗂️ Setup Streaming Directories

# COMMAND ----------

# Setup streaming paths
base_path = "/FileStore/real_estate/streaming"
raw_data_path = f"{base_path}/raw"
processed_data_path = f"{base_path}/processed"
checkpoint_path = f"{base_path}/checkpoint"

# Create directories
dbutils.fs.mkdirs(raw_data_path)
dbutils.fs.mkdirs(processed_data_path)
dbutils.fs.mkdirs(checkpoint_path)

print("📁 Streaming directories created:")
print(f"  Raw data: {raw_data_path}")
print(f"  Processed: {processed_data_path}")
print(f"  Checkpoint: {checkpoint_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📋 Define Simple Streaming Schema

# COMMAND ----------

# Simple schema for real estate streaming data
streaming_schema = StructType([
    StructField("property_id", StringType(), True),
    StructField("price_millions", DoubleType(), True),
    StructField("area_sqm", DoubleType(), True),
    StructField("property_type", StringType(), True),
    StructField("location", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("listing_timestamp", StringType(), True)  # Changed to StringType for JSON
])

print("📋 Simple streaming schema defined")
print("Columns: property_id, price_millions, area_sqm, property_type, location, latitude, longitude, listing_timestamp")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏗️ Generate Sample Streaming Data - FIXED

# COMMAND ----------

def generate_sample_property():
    """Generate a realistic sample property record with fixed data types"""
    
    property_types = ["House", "Apartment", "Land"]
    locations = ["Ba Dinh", "Hoan Kiem", "Dong Da", "Hai Ba Trung", "Cau Giay", "Thanh Xuan"]
    
    # Generate realistic data based on location
    location = random.choice(locations)
    property_type = random.choice(property_types)
    
    # Area based on property type - ensure proper conversion to float
    if property_type == "Apartment":
        area = float(round(random.uniform(35, 90), 1))
        price_per_sqm = float(random.uniform(60, 120))  # Million VND per sqm
    elif property_type == "House":
        area = float(round(random.uniform(60, 150), 1))
        price_per_sqm = float(random.uniform(40, 80))
    else:  # Land
        area = float(round(random.uniform(80, 200), 1))
        price_per_sqm = float(random.uniform(20, 50))
    
    # Calculate base price
    base_price = area * price_per_sqm
    
    # Location factor
    location_multiplier = {
        "Ba Dinh": 1.5, "Hoan Kiem": 1.8, "Dong Da": 1.2,
        "Hai Ba Trung": 1.1, "Cau Giay": 1.3, "Thanh Xuan": 1.0
    }
    
    # Final price calculation
    final_price = float(round(base_price * location_multiplier[location] / 1000000, 2))
    
    # Coordinates for Hanoi districts - ensure proper float conversion
    lat_base = 21.0285
    lng_base = 105.8542
    
    latitude = float(round(lat_base + random.uniform(-0.05, 0.05), 6))
    longitude = float(round(lng_base + random.uniform(-0.05, 0.05), 6))
    
    return {
        "property_id": str(uuid.uuid4())[:8],
        "price_millions": final_price,
        "area_sqm": area,
        "property_type": property_type,
        "location": location,
        "latitude": latitude,
        "longitude": longitude,
        "listing_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

def create_streaming_batch(batch_size=5):
    """Create a batch of sample properties"""
    batch = []
    for _ in range(batch_size):
        property_data = generate_sample_property()
        batch.append(property_data)
    return batch

# Generate initial sample data
print("🏠 Generating sample streaming data...")
try:
    sample_batch = create_streaming_batch(10)
    
    # Save first batch to file
    batch_file = f"/dbfs{raw_data_path}/properties_batch_1.json"
    with open(batch_file, 'w') as f:
        for record in sample_batch:
            f.write(json.dumps(record) + "\n")
    
    print(f"✅ Created initial batch: {len(sample_batch)} properties")
    print("Sample record:", sample_batch[0])
    
except Exception as e:
    print(f"❌ Error generating data: {e}")
    print("Creating minimal sample instead...")
    
    # Fallback minimal sample
    minimal_sample = {
        "property_id": "12345678",
        "price_millions": 5.5,
        "area_sqm": 75.0,
        "property_type": "House",
        "location": "Ba Dinh",
        "latitude": 21.0285,
        "longitude": 105.8542,
        "listing_timestamp": "2024-06-03 10:00:00"
    }
    
    batch_file = f"/dbfs{raw_data_path}/properties_batch_1.json"
    with open(batch_file, 'w') as f:
        f.write(json.dumps(minimal_sample) + "\n")
    
    print("✅ Created minimal sample")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📡 Setup Structured Streaming

# COMMAND ----------

# Create streaming DataFrame using simple file-based streaming
print("📡 Setting up structured streaming...")

try:
    # Simple file-based streaming with schema
    streaming_df = spark.readStream \
        .format("json") \
        .schema(streaming_schema) \
        .option("maxFilesPerTrigger", 1) \
        .load(raw_data_path)
    
    print("✅ File-based streaming setup successful")
    print("📊 Streaming DataFrame schema:")
    streaming_df.printSchema()
    
except Exception as e:
    print(f"❌ Streaming setup error: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧹 Simple Data Processing

# COMMAND ----------

def enrich_streaming_data(df):
    """Apply simple data processing and enrichment"""
    
    processed_df = df \
        .filter(col("price_millions").isNotNull()) \
        .filter(col("area_sqm") > 0) \
        .filter(col("price_millions") > 0) \
        .withColumn("price_per_sqm", 
                   round(col("price_millions") * 1000000 / col("area_sqm"), 0)) \
        .withColumn("area_category",
                   when(col("area_sqm") < 50, "Small")
                   .when(col("area_sqm") < 100, "Medium")
                   .otherwise("Large")) \
        .withColumn("price_category",
                   when(col("price_millions") < 3, "Budget")
                   .when(col("price_millions") < 8, "Mid-range")
                   .otherwise("Premium")) \
        .withColumn("processed_timestamp", current_timestamp())
    
    return processed_df

# Apply enrichment
try:
    enriched_streaming_df = enrich_streaming_data(streaming_df)
    print("✅ Data enrichment pipeline created")
    print("Added columns: price_per_sqm, area_category, price_category")
except Exception as e:
    print(f"❌ Enrichment error: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💾 Write to Delta Lake

# COMMAND ----------

# Setup Delta Lake writing
delta_output_path = f"{processed_data_path}/properties_delta"

print("💾 Setting up Delta Lake streaming write...")

try:
    # Start streaming write to Delta Lake
    delta_stream_query = enriched_streaming_df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", f"{checkpoint_path}/delta_write") \
        .option("mergeSchema", "true") \
        .trigger(processingTime="15 seconds") \
        .start(delta_output_path)
    
    print("✅ Delta Lake streaming write started")
    print(f"🔄 Stream ID: {delta_stream_query.id}")
    print(f"📍 Writing to: {delta_output_path}")
    
except Exception as e:
    print(f"❌ Delta streaming error: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Simple Console Output

# COMMAND ----------

# Create simple console output for monitoring
try:
    console_query = enriched_streaming_df.select(
        "property_id", "location", "property_type", "price_millions", "area_sqm", "price_per_sqm"
    ).writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .trigger(processingTime="20 seconds") \
        .start()
    
    print("📊 Console monitoring started")
    
except Exception as e:
    print(f"❌ Console streaming error: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 Stream Monitoring

# COMMAND ----------

# Let the streams run for a bit
print("⏱️ Letting streams process data...")
time.sleep(25)

# Check stream status
active_streams = spark.streams.active
print(f"\n📡 ACTIVE STREAMS: {len(active_streams)}")

for i, stream in enumerate(active_streams):
    print(f"\nStream {i+1}:")
    print(f"  ID: {stream.id}")
    print(f"  Status: {stream.status}")
    
    try:
        progress = stream.lastProgress
        if progress:
            print(f"  Input Rate: {progress.get('inputRowsPerSecond', 'N/A')} rows/sec")
            print(f"  Processing Rate: {progress.get('processedRowsPerSecond', 'N/A')} rows/sec")
            print(f"  Batch ID: {progress.get('batchId', 'N/A')}")
    except:
        print("  Progress: No data available")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📋 Add More Data & Query Results

# COMMAND ----------

# Add more sample data to trigger processing
print("🏠 Adding more sample data...")

try:
    for batch_num in range(2, 4):
        new_batch = create_streaming_batch(3)
        batch_file = f"/dbfs{raw_data_path}/properties_batch_{batch_num}.json"
        
        with open(batch_file, 'w') as f:
            for record in new_batch:
                f.write(json.dumps(record) + "\n")
        
        print(f"✅ Created batch {batch_num}")
        time.sleep(8)  # Wait between batches
        
except Exception as e:
    print(f"❌ Error adding data: {e}")

# Wait for processing
print("⏱️ Waiting for processing...")
time.sleep(20)

# Query the Delta table
try:
    delta_df = spark.read.format("delta").load(delta_output_path)
    record_count = delta_df.count()
    
    print(f"\n📊 DELTA LAKE SUMMARY:")
    print(f"Total records processed: {record_count}")
    
    if record_count > 0:
        print("\n🏠 Sample processed data:")
        display(delta_df.select(
            "property_id", "location", "property_type", "area_sqm",
            "price_millions", "price_per_sqm", "area_category", "price_category"
        ).limit(10))
        
        print("\n📈 Summary by location:")
        summary_df = delta_df.groupBy("location", "property_type") \
            .agg(
                count("*").alias("count"),
                round(avg("price_millions"), 2).alias("avg_price"),
                round(avg("price_per_sqm"), 0).alias("avg_price_per_sqm")
            ).orderBy("location", "property_type")
        
        display(summary_df)
    else:
        print("⚠️ No data processed yet. Streams may still be starting...")
    
except Exception as e:
    print(f"⚠️ Error reading Delta table: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⏸️ Stream Management

# COMMAND ----------

def show_stream_info():
    """Display information about running streams"""
    streams = spark.streams.active
    print(f"📡 STREAMING STATUS ({len(streams)} active streams)")
    print("=" * 50)
    
    for i, stream in enumerate(streams):
        print(f"Stream {i+1}: {stream.id}")
        print(f"  Status: {stream.status}")
        print(f"  Name: {stream.name if stream.name else 'Unnamed'}")
        print()

def stop_all_streams():
    """Stop all active streams"""
    streams = spark.streams.active
    print(f"🛑 Stopping {len(streams)} active streams...")
    
    for stream in streams:
        stream.stop()
        print(f"✅ Stopped stream: {stream.id}")
    
    print("✅ All streams stopped")

# Show current status
show_stream_info()

# Uncomment to stop all streams
# stop_all_streams()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏆 Streaming Summary

# COMMAND ----------

print("🎉 STREAMING DATA PROCESSING COMPLETED!")
print("=" * 60)
print()
print("✅ ACHIEVEMENTS:")
print("  📡 Structured Streaming with proper error handling")
print("  💾 Delta Lake integration for reliable data storage") 
print("  🧹 Real-time data enrichment and quality checks")
print("  📊 Console monitoring for real-time visibility")
print("  🔍 Stream monitoring and status tracking")
print("  📁 Proper file-based streaming fallback")
print()
print("📊 FEATURES DEMONSTRATED:")
print("  🌊 File-based structured streaming")
print("  💽 Delta Lake ACID transactions")
print("  📈 Real-time data enrichment")
print("  🎯 Stream-to-Delta integration")
print("  📱 Simple monitoring and alerting")
print()
print("🚀 READY FOR:")
print("  📊 Dashboard and visualization (Notebook 05)")
print("  🌐 Real-time monitoring dashboards")
print("  🚨 Advanced streaming analytics")
print("=" * 60) 