# Databricks notebook source
# =====================================================  
# MMM Sample Data Loader for Unity Catalog
# =====================================================

# COMMAND ----------

# MAGIC %md
# MAGIC ## Marketing Mix Model - Sample Data Generator
# MAGIC
# MAGIC This notebook generates realistic sample data for MMM analysis:
# MAGIC - **campaign_results**: Test vs Control sales data  
# MAGIC - **campaign_tactics**: Spend breakdown by marketing tactic
# MAGIC
# MAGIC The data includes realistic patterns:
# MAGIC - Seasonal effects (holidays, summer)
# MAGIC - Diminishing returns in marketing effectiveness
# MAGIC - Various marketing tactics (video, audio, display, search, etc.)
# MAGIC - Test/control experimental design

# COMMAND ----------

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Initialize Spark session
spark = SparkSession.builder.appName("MMM_Data_Generator").getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Validate Data in Unity Catalog

# COMMAND ----------

# Define your catalog and schema names
CATALOG_NAME = "tadityadb"  # Change this to your catalog
SCHEMA_NAME = "default"     # Change this to your schema

# Query the tables to validate
print("Validating campaign_results table:")
results_count = spark.sql(f"SELECT COUNT(*) as count FROM {CATALOG_NAME}.{SCHEMA_NAME}.campaign_results").collect()[0]['count']
print(f"Total campaigns: {results_count}")

print("\nValidating campaign_tactics table:")  
tactics_count = spark.sql(f"SELECT COUNT(*) as count FROM {CATALOG_NAME}.{SCHEMA_NAME}.campaign_tactics").collect()[0]['count']
print(f"Total tactic records: {tactics_count}")

print("\nTactic breakdown:")
tactic_breakdown = spark.sql(f"""
SELECT tactic, COUNT(*) as campaigns, ROUND(SUM(spend_amount), 2) as total_spend
FROM {CATALOG_NAME}.{SCHEMA_NAME}.campaign_tactics 
GROUP BY tactic 
ORDER BY total_spend DESC
""")
display(tactic_breakdown)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Create MMM Analysis View

# COMMAND ----------

# Create a view that pivots tactics for MMM analysis
mmm_view_sql = f"""
CREATE OR REPLACE TABLE {CATALOG_NAME}.{SCHEMA_NAME}.mmm_weekly_data AS
WITH weekly_tactics AS (
  SELECT 
    c.campaign_week,
    c.campaign_start_date,
    c.campaign_id,
    c.incremental_sales as response_variable,
    c.total_spend,
    c.lift_percent,
    c.iroas,
    t.tactic,
    t.spend_amount
  FROM {CATALOG_NAME}.{SCHEMA_NAME}.campaign_results c
  JOIN {CATALOG_NAME}.{SCHEMA_NAME}.campaign_tactics t ON c.campaign_id = t.campaign_id
)
SELECT 
  campaign_week,
  campaign_start_date,
  campaign_id,
  SUM(response_variable) as incremental_sales,
  SUM(CASE WHEN tactic = 'video' THEN spend_amount ELSE 0 END) as video_spend,
  SUM(CASE WHEN tactic = 'audio' THEN spend_amount ELSE 0 END) as audio_spend,
  SUM(CASE WHEN tactic = 'display' THEN spend_amount ELSE 0 END) as display_spend,
  SUM(CASE WHEN tactic = 'search' THEN spend_amount ELSE 0 END) as search_spend,
  SUM(CASE WHEN tactic = 'social' THEN spend_amount ELSE 0 END) as social_spend,
  SUM(CASE WHEN tactic = 'connected_tv' THEN spend_amount ELSE 0 END) as connected_tv_spend,
  SUM(total_spend) as total_spend
FROM weekly_tactics
GROUP BY campaign_week, campaign_start_date,campaign_id
ORDER BY campaign_start_date
"""

spark.sql(mmm_view_sql)
print("✅ MMM analysis view created!")

# Display the view
print("\nMMM Weekly Data (first 10 rows):")
display(spark.sql(f"SELECT * FROM {CATALOG_NAME}.{SCHEMA_NAME}.mmm_weekly_data LIMIT 10"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ready for MMM Analysis!
# MAGIC
# MAGIC Your data is now ready in Unity Catalog. Use this query to pull data for MMM:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT * FROM your_catalog.mmm_schema.mmm_weekly_data
# MAGIC ORDER BY campaign_start_date
# MAGIC ```
