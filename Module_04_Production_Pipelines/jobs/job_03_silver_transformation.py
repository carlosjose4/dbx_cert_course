# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Silver Layer Transformation
# MAGIC
# MAGIC This notebook transforms Bronze layer data into cleaned, enriched Silver layer tables
# MAGIC with business logic applied and data quality improvements.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

# Parameters from job orchestrator
dbutils.widgets.text("processing_date", "")
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("quality_threshold", "0.95")

processing_date = dbutils.widgets.get("processing_date")
run_id = dbutils.widgets.get("run_id")
quality_threshold = float(dbutils.widgets.get("quality_threshold"))

if not processing_date:
    from datetime import datetime
    processing_date = datetime.now().strftime("%Y-%m-%d")

if not run_id:
    import uuid
    run_id = str(uuid.uuid4())

print(f"Processing Date: {processing_date}")
print(f"Run ID: {run_id}")
print(f"Quality Threshold: {quality_threshold}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Imports

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window
from datetime import datetime
import json

# Set catalog and schema
spark.sql("USE CATALOG sm_training")
spark.sql("USE SCHEMA retail_data")

transformation_start = datetime.now()
transformation_metrics = {}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Bronze Data

# COMMAND ----------

print("📊 Loading Bronze layer data...")

# Load Bronze tables
bronze_sales_df = spark.table("retail_data.bronze_sales") \
    .filter(col("processing_date") == processing_date)

bronze_customers_df = spark.table("retail_data.bronze_customers") \
    .filter(col("processing_date") == processing_date)

bronze_inventory_df = spark.table("retail_data.bronze_inventory") \
    .filter(col("processing_date") == processing_date)

print(f"✅ Loaded Bronze data:")
print(f"   Sales: {bronze_sales_df.count():,} records")
print(f"   Customers: {bronze_customers_df.count():,} records")
print(f"   Inventory: {bronze_inventory_df.count():,} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Cleansing and Validation

# COMMAND ----------

print("\n🧹 Cleaning and validating data...")

# Clean sales data
cleaned_sales_df = bronze_sales_df \
    .filter(col("data_quality_flag") == "valid") \
    .filter(col("transaction_id").isNotNull()) \
    .filter(col("total_amount") > 0) \
    .filter(col("quantity") > 0) \
    .dropDuplicates(["transaction_id"])

# Add data quality score
sales_with_quality = cleaned_sales_df \
    .withColumn("has_customer", when(col("customer_id").isNotNull(), 1).otherwise(0)) \
    .withColumn("has_valid_amount", when(col("total_amount") > 0, 1).otherwise(0)) \
    .withColumn("has_valid_quantity", when(col("quantity") > 0, 1).otherwise(0)) \
    .withColumn("has_payment_method", when(col("payment_method").isNotNull(), 1).otherwise(0)) \
    .withColumn("data_quality_score", 
                (col("has_customer") + col("has_valid_amount") + 
                 col("has_valid_quantity") + col("has_payment_method")) / 4.0) \
    .drop("has_customer", "has_valid_amount", "has_valid_quantity", "has_payment_method")

# Filter by quality threshold
quality_sales_df = sales_with_quality \
    .filter(col("data_quality_score") >= quality_threshold)

records_cleaned = bronze_sales_df.count() - quality_sales_df.count()
print(f"✅ Removed {records_cleaned:,} low-quality records")
print(f"   Remaining: {quality_sales_df.count():,} high-quality records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enrich Sales with Customer Data

# COMMAND ----------

print("\n🔗 Enriching sales with customer dimensions...")

# Select relevant customer fields
customer_dim_df = bronze_customers_df.select(
    "customer_id",
    "customer_segment",
    "customer_region",
    "lifetime_value",
    "customer_status"
).dropDuplicates(["customer_id"])

# Enrich sales with customer data
enriched_sales_df = quality_sales_df \
    .join(customer_dim_df, "customer_id", "left") \
    .fillna({
        "customer_segment": "Unknown",
        "customer_region": "Unknown",
        "lifetime_value": 0.0,
        "customer_status": "Unknown"
    })

# Add derived metrics
silver_sales_df = enriched_sales_df \
    .withColumn("revenue_after_discount", 
                col("quantity") * col("unit_price") - col("discount_amount")) \
    .withColumn("discount_percentage", 
                when(col("unit_price") > 0, 
                     col("discount_amount") / (col("quantity") * col("unit_price")) * 100)
                .otherwise(0)) \
    .withColumn("is_high_value_transaction", 
                when(col("total_amount") > 500, True).otherwise(False)) \
    .withColumn("transaction_hour", hour("transaction_timestamp")) \
    .withColumn("transaction_day_of_week", dayofweek("transaction_timestamp")) \
    .withColumn("is_weekend", 
                when(col("transaction_day_of_week").isin(1, 7), True).otherwise(False)) \
    .withColumn("silver_processing_timestamp", current_timestamp()) \
    .select(
        "transaction_id", "store_id", "customer_id", "product_id",
        "transaction_timestamp", "quantity", "unit_price", 
        "discount_amount", "total_amount", "payment_method",
        "customer_segment", "customer_region", "lifetime_value",
        "customer_status", "revenue_after_discount", "discount_percentage",
        "is_high_value_transaction", "transaction_hour", 
        "transaction_day_of_week", "is_weekend",
        "data_quality_score", "is_valid",
        "silver_processing_timestamp", "processing_date"
    )

print(f"✅ Enriched {silver_sales_df.count():,} sales records with customer data")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Silver Inventory Table

# COMMAND ----------

print("\n📦 Creating Silver inventory table...")

# Calculate inventory metrics
inventory_window = Window.partitionBy("product_id").orderBy("store_id")

silver_inventory_df = bronze_inventory_df \
    .withColumn("total_stock_by_product", 
                sum("current_stock").over(inventory_window)) \
    .withColumn("avg_stock_by_product", 
                avg("current_stock").over(inventory_window)) \
    .withColumn("stock_coverage_days", 
                when(col("current_stock") > 0, col("current_stock") / 10).otherwise(0)) \
    .withColumn("needs_reorder", 
                when(col("current_stock") <= col("reorder_point"), True).otherwise(False)) \
    .withColumn("stock_value", 
                col("current_stock") * 50) \
    .withColumn("silver_processing_timestamp", current_timestamp())

print(f"✅ Processed {silver_inventory_df.count():,} inventory records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Silver Customer Metrics

# COMMAND ----------

print("\n👥 Creating Silver customer metrics...")

# Calculate customer transaction metrics from sales
customer_transactions = silver_sales_df \
    .groupBy("customer_id") \
    .agg(
        count("transaction_id").alias("transaction_count"),
        sum("total_amount").alias("total_spent"),
        avg("total_amount").alias("avg_transaction_value"),
        max("transaction_timestamp").alias("last_transaction_date"),
        countDistinct("product_id").alias("unique_products_purchased"),
        countDistinct("store_id").alias("unique_stores_visited")
    )

# Enrich with customer master data
silver_customers_df = bronze_customers_df \
    .join(customer_transactions, "customer_id", "left") \
    .withColumn("days_since_last_transaction", 
                datediff(current_date(), col("last_transaction_date"))) \
    .withColumn("churn_risk_score", 
                when(col("days_since_last_transaction") > 90, 0.9)
                .when(col("days_since_last_transaction") > 60, 0.7)
                .when(col("days_since_last_transaction") > 30, 0.5)
                .otherwise(0.2)) \
    .withColumn("customer_value_segment",
                when(col("total_spent") > 1000, "High Value")
                .when(col("total_spent") > 500, "Medium Value")
                .when(col("total_spent") > 100, "Low Value")
                .otherwise("New")) \
    .withColumn("silver_processing_timestamp", current_timestamp())

print(f"✅ Created metrics for {silver_customers_df.count():,} customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Silver Tables

# COMMAND ----------

print("\n💾 Writing Silver layer tables...")

try:
    # Write Silver sales table
    silver_sales_df.write \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .partitionBy("processing_date") \
        .saveAsTable("retail_data.silver_sales")
    
    sales_count = silver_sales_df.count()
    transformation_metrics["sales"] = {
        "records_written": sales_count,
        "quality_score_avg": silver_sales_df.agg(avg("data_quality_score")).collect()[0][0]
    }
    print(f"✅ Silver sales: {sales_count:,} records written")
    
    # Write Silver inventory table
    silver_inventory_df.write \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable("retail_data.silver_inventory")
    
    inventory_count = silver_inventory_df.count()
    transformation_metrics["inventory"] = {
        "records_written": inventory_count,
        "items_need_reorder": silver_inventory_df.filter(col("needs_reorder")).count()
    }
    print(f"✅ Silver inventory: {inventory_count:,} records written")
    
    # Write Silver customers table
    silver_customers_df.write \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable("retail_data.silver_customers")
    
    customers_count = silver_customers_df.count()
    transformation_metrics["customers"] = {
        "records_written": customers_count,
        "high_risk_customers": silver_customers_df.filter(col("churn_risk_score") > 0.7).count()
    }
    print(f"✅ Silver customers: {customers_count:,} records written")
    
except Exception as e:
    print(f"❌ Failed to write Silver tables: {str(e)}")
    raise e

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Validation

# COMMAND ----------

print("\n✅ Running Silver layer quality checks...")

# Check for data completeness
completeness_check = spark.sql(f"""
    SELECT 
        COUNT(*) as total_records,
        SUM(CASE WHEN customer_segment != 'Unknown' THEN 1 ELSE 0 END) as known_segments,
        SUM(CASE WHEN customer_region != 'Unknown' THEN 1 ELSE 0 END) as known_regions,
        COALESCE(AVG(data_quality_score),0) as avg_quality_score
    FROM retail_data.silver_sales
    WHERE processing_date = '{processing_date}'
""").collect()[0]

known_segments=0
if completeness_check["known_segments"] != None:
    known_segments=completeness_check["known_segments"]

known_regions=0
if completeness_check["known_regions"] != None:
    known_regions=completeness_check["known_regions"]

segment_completeness=0
try:
    segment_completeness = known_segments / completeness_check["total_records"]
except:
    segment_completeness = 1

region_completeness = 0
try:
    region_completeness = known_regions / completeness_check["total_records"]
except:
    region_completeness = 1



# Check for anomalies
anomaly_check = spark.sql(f"""
    SELECT 
        COUNT(CASE WHEN total_amount > 10000 THEN 1 END) as extreme_values,
        COUNT(CASE WHEN discount_percentage > 100 THEN 1 END) as invalid_discounts,
        COUNT(DISTINCT customer_id) as unique_customers,
        COUNT(DISTINCT product_id) as unique_products
    FROM retail_data.silver_sales
    WHERE processing_date = '{processing_date}'
""").collect()[0]

quality_results = {
    "segment_completeness": segment_completeness,
    "region_completeness": region_completeness,
    "avg_quality_score": completeness_check["avg_quality_score"],
    "extreme_values": anomaly_check["extreme_values"],
    "invalid_discounts": anomaly_check["invalid_discounts"]
}

print(f"   Segment Completeness: {segment_completeness*100:.1f}%")
print(f"   Region Completeness: {region_completeness*100:.1f}%")
print(f"   Average Quality Score: {completeness_check['avg_quality_score']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log Transformation Metrics

# COMMAND ----------

print(quality_results)

# COMMAND ----------

# Calculate overall metrics
transformation_duration = (datetime.now() - transformation_start).total_seconds()
total_records = 0
for m in transformation_metrics.values():
    total_records += m["records_written"]

# Log metrics to monitoring table
monitoring_data = []

processing_date_fix = datetime.strptime(processing_date, '%Y-%m-%d').date()

for table, metrics in transformation_metrics.items():
    for metric_name, metric_value in metrics.items():
        monitoring_data.append({
            "metric_timestamp": datetime.now(),
            "pipeline_name": "globalmart_daily_etl",
            "task_name": "silver_transformation",
            "metric_name": f"{table}_{metric_name}",
            "metric_value": float(metric_value) if metric_value else 0.0,
            "metric_unit": "count" if "records" in metric_name or "items" in metric_name or "customers" in metric_name else "score",
            "processing_date": processing_date_fix,
            "run_id": run_id
        })

# Add quality metrics
for metric_name, metric_value in quality_results.items():
    monitoring_data.append({
        "metric_timestamp": datetime.now(),
        "pipeline_name": "globalmart_daily_etl",
        "task_name": "silver_transformation",
        "metric_name": f"quality_{metric_name}",
        "metric_value": float(metric_value),
        "metric_unit": "ratio" if "completeness" in metric_name or "score" in metric_name else "count",
        "processing_date": processing_date_fix,
        "run_id": run_id
    })

# Add performance metrics
monitoring_data.extend([
    {
        "metric_timestamp": datetime.now(),
        "pipeline_name": "globalmart_daily_etl",
        "task_name": "silver_transformation",
        "metric_name": "transformation_duration",
        "metric_value": transformation_duration,
        "metric_unit": "seconds",
        "processing_date": processing_date_fix,
        "run_id": run_id
    },
    {
        "metric_timestamp": datetime.now(),
        "pipeline_name": "globalmart_daily_etl",
        "task_name": "silver_transformation",
        "metric_name": "records_per_second",
        "metric_value": total_records / transformation_duration if transformation_duration > 0 else 0,
        "metric_unit": "rate",
        "processing_date": processing_date_fix,
        "run_id": run_id
    }
])

# Write monitoring metrics
monitoring_df = spark.createDataFrame(monitoring_data)
monitoring_df.write.mode("append").saveAsTable("retail_data.pipeline_metrics")

print(f"\n{'='*50}")
print(f"SILVER TRANSFORMATION COMPLETED")
print(f"{'='*50}")
print(f"Total Records Processed: {total_records:,}")
print(f"Duration: {transformation_duration:.2f} seconds")
print(f"Throughput: {total_records/transformation_duration:.0f} records/second")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Return Results

# COMMAND ----------

# Prepare results for job orchestrator
results = {
    "status": "SUCCESS",
    "message": "Silver transformation completed successfully",
    "metrics": {
        "total_records": total_records,
        "duration_seconds": transformation_duration,
        "throughput": total_records / transformation_duration if transformation_duration > 0 else 0,
        "tables_created": list(transformation_metrics.keys()),
        "avg_quality_score": quality_results["avg_quality_score"],
        "data_completeness": {
            "segment": quality_results["segment_completeness"],
            "region": quality_results["region_completeness"]
        }
    },
    "quality_metrics": quality_results,
    "details": transformation_metrics
}

dbutils.notebook.exit(json.dumps(results))
