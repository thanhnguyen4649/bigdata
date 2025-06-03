# Databricks notebook source
# MAGIC %md
# MAGIC # Real Estate Analytics - Environment Setup
# MAGIC 
# MAGIC This notebook sets up the environment for real estate data analytics pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install Required Libraries

# COMMAND ----------

# MAGIC %pip install requests beautifulsoup4 overpy geopy unidecode kafka-python

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Libraries

# COMMAND ----------

import requests
from bs4 import BeautifulSoup
import re
import time
import numpy as np
import overpy
from geopy.distance import geodesic
from geopy.geocoders import Nominatim
from unidecode import unidecode
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
import pandas as pd

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configure Spark Session

# COMMAND ----------

# Spark session is already configured in Databricks
print("Spark Session Info:")
print(f"App Name: {spark.sparkContext.appName}")
print(f"Spark Version: {spark.version}")
print(f"Master: {spark.sparkContext.master}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Connection and Display Sample Data

# COMMAND ----------

# Create a test DataFrame to verify Spark is working
test_data = [(1, "Ho Chi Minh City", 5.0, 50.0), (2, "Hanoi", 4.0, 45.0)]
test_schema = ["id", "location", "price", "square"]
test_df = spark.createDataFrame(test_data, test_schema)

display(test_df)

# COMMAND ----------

print("✅ Environment setup completed successfully!")
print("📊 Ready to proceed with data pipeline development") 