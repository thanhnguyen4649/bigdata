# Databricks notebook source
# MAGIC %md
# MAGIC # 📊 Real Estate Analytics Dashboard - Fixed Version
# MAGIC 
# MAGIC **Objective**: Create interactive dashboards and visualizations for real estate analytics

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📚 Import Libraries

# COMMAND ----------

import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import random
from datetime import datetime, timedelta
import json

print("✅ Dashboard libraries imported successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Load or Create Sample Data

# COMMAND ----------

# Try to load data from various sources
print("📊 Loading data for dashboard...")

def load_or_create_data():
    """Load data from Delta Lake or create sample data"""
    
    # Try different data sources
    data_sources = [
        "/FileStore/real_estate/streaming/processed/properties_delta",
        "/FileStore/real_estate/processed_data/properties_delta", 
        "/FileStore/real_estate/models/predictions_delta"
    ]
    
    for source in data_sources:
        try:
            df = spark.read.format("delta").load(source)
            record_count = df.count()
            if record_count > 0:
                print(f"✅ Loaded {record_count} records from {source}")
                return df
        except Exception as e:
            print(f"⚠️ Could not load from {source}: {e}")
    
    # Create comprehensive sample data if no real data available
    print("📝 Creating sample data for dashboard demonstration...")
    
    def generate_property_data(num_records=50):
        """Generate realistic sample property data"""
        
        locations = ["Ba Dinh", "Hoan Kiem", "Dong Da", "Hai Ba Trung", "Cau Giay", "Thanh Xuan", "Tay Ho", "Long Bien"]
        property_types = ["House", "Apartment", "Land"]
        
        data = []
        for i in range(num_records):
            location = random.choice(locations)
            prop_type = random.choice(property_types)
            
            # Generate realistic values based on type and location
            if prop_type == "Apartment":
                area = round(random.uniform(35, 90), 1)
                base_price = random.uniform(3.5, 12.0)
            elif prop_type == "House":
                area = round(random.uniform(60, 150), 1) 
                base_price = random.uniform(5.0, 20.0)
            else:  # Land
                area = round(random.uniform(80, 200), 1)
                base_price = random.uniform(2.0, 15.0)
            
            # Location multiplier
            location_multipliers = {
                "Ba Dinh": 1.8, "Hoan Kiem": 2.0, "Tay Ho": 1.7,
                "Dong Da": 1.4, "Cau Giay": 1.5, "Hai Ba Trung": 1.3,
                "Thanh Xuan": 1.2, "Long Bien": 1.0
            }
            
            price = round(base_price * location_multipliers.get(location, 1.0), 2)
            price_per_sqm = round((price * 1000000) / area, 0)
            
            # Generate other features
            bedrooms = random.randint(1, 4) if prop_type != "Land" else 0
            bathrooms = random.randint(1, 3) if prop_type != "Land" else 0
            
            # Coordinates around Hanoi
            lat = round(21.0285 + random.uniform(-0.1, 0.1), 6)
            lng = round(105.8542 + random.uniform(-0.1, 0.1), 6)
            
            # ML prediction (simulate)
            predicted_price = round(price + random.uniform(-1.0, 1.0), 2)
            accuracy = round(100 - abs(price - predicted_price) / price * 100, 1)
            
            # Categories
            area_cat = "Small" if area < 60 else "Medium" if area < 120 else "Large"
            price_cat = "Budget" if price < 5 else "Mid-range" if price < 12 else "Premium"
            
            data.append({
                "property_id": f"PROP_{i+1:04d}",
                "price_millions": price,
                "area_sqm": area,
                "property_type": prop_type,
                "location": location,
                "bedrooms": bedrooms,
                "bathrooms": bathrooms,
                "latitude": lat,
                "longitude": lng,
                "price_per_sqm": price_per_sqm,
                "area_category": area_cat,
                "price_category": price_cat,
                "predicted_price": predicted_price,
                "prediction_accuracy": accuracy,
                "listing_date": (datetime.now() - timedelta(days=random.randint(1, 30))).strftime("%Y-%m-%d"),
                "is_luxury": price > 15.0,
                "distance_to_center": round(random.uniform(2.0, 25.0), 1)
            })
        
        return data
    
    # Generate sample data
    sample_data = generate_property_data(50)
    
    # Create DataFrame
    schema = StructType([
        StructField("property_id", StringType(), True),
        StructField("price_millions", DoubleType(), True),
        StructField("area_sqm", DoubleType(), True),
        StructField("property_type", StringType(), True),
        StructField("location", StringType(), True),
        StructField("bedrooms", IntegerType(), True),
        StructField("bathrooms", IntegerType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("price_per_sqm", DoubleType(), True),
        StructField("area_category", StringType(), True),
        StructField("price_category", StringType(), True),
        StructField("predicted_price", DoubleType(), True),
        StructField("prediction_accuracy", DoubleType(), True),
        StructField("listing_date", StringType(), True),
        StructField("is_luxury", BooleanType(), True),
        StructField("distance_to_center", DoubleType(), True)
    ])
    
    df = spark.createDataFrame(sample_data, schema)
    print(f"✅ Created sample dataset with {df.count()} records")
    return df

# Load or create data
df = load_or_create_data()

# Convert to Pandas for easy plotting
df_pandas = df.toPandas()
print(f"📊 Data ready for visualization: {len(df_pandas)} records")

# Display sample data
display(df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📈 Market Overview KPIs

# COMMAND ----------

# Calculate key performance indicators
total_properties = len(df_pandas)
avg_price = df_pandas['price_millions'].mean()
median_price = df_pandas['price_millions'].median()
avg_area = df_pandas['area_sqm'].mean()
luxury_count = df_pandas['is_luxury'].sum() if 'is_luxury' in df_pandas.columns else 0
avg_accuracy = df_pandas['prediction_accuracy'].mean() if 'prediction_accuracy' in df_pandas.columns else 0

print("🏠 REAL ESTATE MARKET DASHBOARD")
print("=" * 60)
print(f"📊 Total Properties: {total_properties:,}")
print(f"💰 Average Price: {avg_price:.2f} Million VND")
print(f"💰 Median Price: {median_price:.2f} Million VND") 
print(f"📐 Average Area: {avg_area:.1f} sqm")
print(f"💎 Luxury Properties: {luxury_count} ({luxury_count/total_properties*100:.1f}%)")
print(f"🎯 ML Model Accuracy: {avg_accuracy:.1f}%")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💰 Price Analysis Visualizations

# COMMAND ----------

# 1. Price Distribution Histogram
fig1 = px.histogram(
    df_pandas, 
    x='price_millions',
    nbins=15,
    title='📊 Property Price Distribution (Million VND)',
    labels={'price_millions': 'Price (Million VND)', 'count': 'Number of Properties'},
    color_discrete_sequence=['#1f77b4']
)
fig1.update_layout(
    showlegend=False,
    template="plotly_white",
    title_font_size=16
)
fig1.show()

# COMMAND ----------

# 2. Price by Location Box Plot
fig2 = px.box(
    df_pandas, 
    x='location', 
    y='price_millions',
    title='💰 Price Distribution by District',
    labels={'price_millions': 'Price (Million VND)', 'location': 'District'},
    color='location'
)
fig2.update_xaxis(tickangle=45)
fig2.update_layout(
    template="plotly_white",
    title_font_size=16,
    showlegend=False
)
fig2.show()

# COMMAND ----------

# 3. Price vs Area Scatter Plot
fig3 = px.scatter(
    df_pandas, 
    x='area_sqm', 
    y='price_millions',
    color='property_type',
    size='bedrooms' if 'bedrooms' in df_pandas.columns else None,
    hover_data=['location', 'price_per_sqm'],
    title='🏠 Price vs Property Area by Type',
    labels={'area_sqm': 'Area (sqm)', 'price_millions': 'Price (Million VND)'}
)
fig3.update_layout(
    template="plotly_white",
    title_font_size=16
)
fig3.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🗺️ Geographic Analysis

# COMMAND ----------

# 4. Property Count by Location
location_summary = df_pandas.groupby('location').agg({
    'price_millions': ['count', 'mean', 'median'],
    'area_sqm': 'mean',
    'price_per_sqm': 'mean'
}).round(2)

location_summary.columns = ['Property_Count', 'Avg_Price', 'Median_Price', 'Avg_Area', 'Avg_Price_Per_Sqm']
location_summary = location_summary.reset_index().sort_values('Avg_Price', ascending=False)

print("📍 LOCATION ANALYSIS SUMMARY")
display(location_summary)

# COMMAND ----------

# 5. Property Distribution by Location
fig4 = px.bar(
    location_summary,
    x='location',
    y='Property_Count',
    color='Avg_Price',
    title='🏘️ Property Count and Average Price by District',
    labels={'Property_Count': 'Number of Properties', 'location': 'District'},
    color_continuous_scale='Viridis'
)
fig4.update_xaxis(tickangle=45)
fig4.update_layout(
    template="plotly_white",
    title_font_size=16
)
fig4.show()

# COMMAND ----------

# 6. Property Type Distribution
property_type_dist = df_pandas['property_type'].value_counts()

fig5 = px.pie(
    values=property_type_dist.values,
    names=property_type_dist.index,
    title='🏠 Property Type Distribution',
    color_discrete_sequence=px.colors.qualitative.Set3
)
fig5.update_layout(
    template="plotly_white",
    title_font_size=16
)
fig5.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🤖 ML Model Performance Analysis

# COMMAND ----------

if 'prediction_accuracy' in df_pandas.columns and 'predicted_price' in df_pandas.columns:
    
    # 7. Model Accuracy Distribution
    fig6 = px.histogram(
        df_pandas,
        x='prediction_accuracy',
        nbins=15,
        title='🎯 ML Model Prediction Accuracy Distribution (%)',
        labels={'prediction_accuracy': 'Prediction Accuracy (%)', 'count': 'Number of Properties'},
        color_discrete_sequence=['#2ca02c']
    )
    fig6.update_layout(
        template="plotly_white",
        title_font_size=16,
        showlegend=False
    )
    fig6.show()
    
    # 8. Actual vs Predicted Prices
    fig7 = px.scatter(
        df_pandas,
        x='predicted_price',
        y='price_millions',
        color='location',
        title='📈 Actual vs Predicted Prices by District',
        labels={'predicted_price': 'Predicted Price (Million VND)', 'price_millions': 'Actual Price (Million VND)'}
    )
    
    # Add perfect prediction line
    min_price = min(df_pandas['predicted_price'].min(), df_pandas['price_millions'].min())
    max_price = max(df_pandas['predicted_price'].max(), df_pandas['price_millions'].max())
    fig7.add_shape(
        type="line",
        x0=min_price, y0=min_price,
        x1=max_price, y1=max_price,
        line=dict(color="red", dash="dash", width=2),
        name="Perfect Prediction"
    )
    fig7.update_layout(
        template="plotly_white",
        title_font_size=16
    )
    fig7.show()
    
    # Model performance by location
    model_perf = df_pandas.groupby('location').agg({
        'prediction_accuracy': ['mean', 'std'],
        'property_id': 'count'
    }).round(2)
    model_perf.columns = ['Avg_Accuracy', 'Accuracy_StdDev', 'Sample_Size']
    model_perf = model_perf.reset_index().sort_values('Avg_Accuracy', ascending=False)
    
    print("🤖 ML MODEL PERFORMANCE BY LOCATION")
    display(model_perf)

else:
    print("⚠️ ML prediction columns not available in data")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Advanced Analytics

# COMMAND ----------

# 9. Price per SQM Analysis
fig8 = px.box(
    df_pandas,
    x='property_type',
    y='price_per_sqm',
    color='area_category',
    title='💵 Price per SQM by Property Type and Size Category',
    labels={'price_per_sqm': 'Price per SQM (VND)', 'property_type': 'Property Type'}
)
fig8.update_layout(
    template="plotly_white",
    title_font_size=16
)
fig8.show()

# COMMAND ----------

# 10. Multi-dimensional Analysis
if 'price_category' in df_pandas.columns and 'area_category' in df_pandas.columns:
    
    category_analysis = df_pandas.groupby(['location', 'property_type', 'price_category']).size().reset_index(name='count')
    
    fig9 = px.sunburst(
        category_analysis,
        path=['location', 'property_type', 'price_category'],
        values='count',
        title='🌅 Property Distribution: Location → Type → Price Category'
    )
    fig9.update_layout(
        template="plotly_white",
        title_font_size=16
    )
    fig9.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📅 Time Series Analysis

# COMMAND ----------

if 'listing_date' in df_pandas.columns:
    
    # Convert to datetime
    df_pandas['listing_date'] = pd.to_datetime(df_pandas['listing_date'])
    
    # Daily trends
    daily_trends = df_pandas.groupby('listing_date').agg({
        'property_id': 'count',
        'price_millions': 'mean'
    }).reset_index()
    
    # 11. Time Series Chart
    fig10 = make_subplots(
        rows=2, cols=1,
        subplot_titles=('Daily New Listings', 'Daily Average Price'),
        vertical_spacing=0.1
    )
    
    fig10.add_trace(
        go.Scatter(
            x=daily_trends['listing_date'], 
            y=daily_trends['property_id'],
            mode='lines+markers',
            name='New Listings',
            line=dict(color='blue')
        ),
        row=1, col=1
    )
    
    fig10.add_trace(
        go.Scatter(
            x=daily_trends['listing_date'], 
            y=daily_trends['price_millions'],
            mode='lines+markers', 
            name='Avg Price',
            line=dict(color='green')
        ),
        row=2, col=1
    )
    
    fig10.update_layout(
        title='📅 Real Estate Market Trends Over Time',
        template="plotly_white",
        showlegend=False,
        title_font_size=16
    )
    fig10.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔗 SQL Views for Databricks SQL Dashboard

# COMMAND ----------

# Create temporary view for SQL dashboards
df.createOrReplaceTempView("real_estate_analytics")

print("📊 SQL Views created for Databricks SQL Dashboard!")
print("\nAvailable views:")
print("- real_estate_analytics: Main property dataset")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Summary statistics by location
# MAGIC SELECT 
# MAGIC   location,
# MAGIC   property_type,
# MAGIC   COUNT(*) as property_count,
# MAGIC   ROUND(AVG(price_millions), 2) as avg_price,
# MAGIC   ROUND(AVG(area_sqm), 1) as avg_area,
# MAGIC   ROUND(AVG(price_per_sqm), 0) as avg_price_per_sqm,
# MAGIC   COUNT(CASE WHEN is_luxury THEN 1 END) as luxury_count
# MAGIC FROM real_estate_analytics
# MAGIC GROUP BY location, property_type
# MAGIC ORDER BY location, avg_price DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top value properties (best price per sqm)
# MAGIC SELECT 
# MAGIC   property_id,
# MAGIC   location,
# MAGIC   property_type,
# MAGIC   price_millions,
# MAGIC   area_sqm,
# MAGIC   price_per_sqm,
# MAGIC   CASE 
# MAGIC     WHEN price_per_sqm < 50000000 THEN 'Excellent Value'
# MAGIC     WHEN price_per_sqm < 80000000 THEN 'Good Value'
# MAGIC     WHEN price_per_sqm < 120000000 THEN 'Fair Value'
# MAGIC     ELSE 'Premium'
# MAGIC   END as value_rating
# MAGIC FROM real_estate_analytics
# MAGIC WHERE area_sqm > 50
# MAGIC ORDER BY price_per_sqm ASC
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Market overview metrics
# MAGIC SELECT 
# MAGIC   COUNT(*) as total_properties,
# MAGIC   ROUND(AVG(price_millions), 2) as avg_price,
# MAGIC   ROUND(PERCENTILE_APPROX(price_millions, 0.5), 2) as median_price,
# MAGIC   ROUND(MIN(price_millions), 2) as min_price,
# MAGIC   ROUND(MAX(price_millions), 2) as max_price,
# MAGIC   ROUND(AVG(area_sqm), 1) as avg_area,
# MAGIC   COUNT(DISTINCT location) as districts_covered,
# MAGIC   COUNT(CASE WHEN is_luxury THEN 1 END) as luxury_properties,
# MAGIC   ROUND(AVG(prediction_accuracy), 1) as avg_model_accuracy
# MAGIC FROM real_estate_analytics

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💾 Save Dashboard Data

# COMMAND ----------

# Save processed data for dashboard consumption
try:
    # Save main dataset
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .save("/FileStore/real_estate/dashboard/main_data")
    
    # Create and save summary statistics
    summary_stats = df.groupBy("location", "property_type").agg(
        count("*").alias("property_count"),
        avg("price_millions").alias("avg_price"),
        avg("area_sqm").alias("avg_area"),
        avg("price_per_sqm").alias("avg_price_per_sqm")
    )
    
    summary_stats.write \
        .format("delta") \
        .mode("overwrite") \
        .save("/FileStore/real_estate/dashboard/summary_stats")
    
    print("✅ Dashboard data saved to Delta Lake")
    print("📊 Paths:")
    print("   Main data: /FileStore/real_estate/dashboard/main_data")
    print("   Summary: /FileStore/real_estate/dashboard/summary_stats")
    
except Exception as e:
    print(f"⚠️ Save error: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎯 Dashboard Setup Instructions

# COMMAND ----------

print("🎉 REAL ESTATE ANALYTICS DASHBOARD COMPLETED!")
print("=" * 70)
print()
print("✅ FEATURES COMPLETED:")
print("  📊 Interactive visualizations with Plotly")
print("  💰 Price analysis and distribution charts")
print("  🗺️ Geographic and location-based analysis")
print("  🤖 ML model performance monitoring")
print("  📅 Time series trends analysis")
print("  🔗 SQL views for Databricks SQL dashboards")
print("  💾 Data export for external consumption")
print()
print("📋 DATABRICKS SQL DASHBOARD SETUP:")
print("  1. Go to Databricks SQL workspace")
print("  2. Create new dashboard")
print("  3. Add visualizations using 'real_estate_analytics' view")
print("  4. Recommended chart types:")
print("     - Bar charts: Property count by location")
print("     - Line charts: Price trends over time")
print("     - Scatter plots: Price vs area analysis")
print("     - Maps: Geographic distribution")
print("     - KPI cards: Key metrics")
print()
print("🔄 REFRESH SETTINGS:")
print("  - Set auto-refresh: 5-10 minutes")
print("  - Enable real-time alerts for price anomalies")
print("  - Schedule daily/weekly reports")
print()
print("🚀 READY FOR PRODUCTION!")
print("=" * 70) 