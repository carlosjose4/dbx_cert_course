# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Bronze Layer Ingestion
# MAGIC
# MAGIC This notebook ingests raw data into the Bronze layer with minimal transformations,
# MAGIC preserving data fidelity while adding ingestion metadata.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

# Parameters from job orchestrator
dbutils.widgets.text("processing_date", "")
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("batch_size", "10000")

processing_date = dbutils.widgets.get("processing_date")
run_id = dbutils.widgets.get("run_id")
batch_size = int(dbutils.widgets.get("batch_size"))

if not processing_date:
    from datetime import datetime
    processing_date = datetime.now().strftime("%Y-%m-%d")

if not run_id:
    import uuid
    run_id = str(uuid.uuid4())

print(f"Processing Date: {processing_date}")
print(f"Run ID: {run_id}")
print(f"Batch Size: {batch_size}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Imports

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import json

# Set catalog and schema
spark.sql("USE CATALOG sm_training")
spark.sql("USE SCHEMA retail_data")

ingestion_start = datetime.now()
ingestion_metrics = {}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest Sales Data to Bronze

# COMMAND ----------

try:
    print("📥 Ingesting sales data to Bronze layer...")
    
    # Read raw sales data
    raw_sales_df = spark.table("retail_data.raw_sales") \
        .filter(col("processing_date") == processing_date)
    
    # Add Bronze layer metadata
    bronze_sales_df = raw_sales_df \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("source_file", lit("raw_sales_table")) \
        .withColumn("bronze_run_id", lit(run_id)) \
        .withColumn("data_quality_flag", 
                   when(col("transaction_id").isNull(), "missing_id")
                   .when(col("total_amount") <= 0, "invalid_amount")
                   .when(col("customer_id").isNull(), "missing_customer")
                   .otherwise("valid"))
    
    # Count records before writing
    record_count = bronze_sales_df.count()
    
    # Write to Bronze table using merge for idempotency
    bronze_sales_df.createOrReplaceTempView("bronze_sales_staging")
    
    merge_query = f"""
    MERGE INTO retail_data.bronze_sales target
    USING bronze_sales_staging source
    ON target.transaction_id = source.transaction_id 
       AND target.processing_date = source.processing_date
    WHEN NOT MATCHED THEN INSERT *
    """
    
    spark.sql(merge_query)
    
    # Calculate ingestion metrics
    quality_metrics = bronze_sales_df.groupBy("data_quality_flag").count().collect()
    quality_dist = {row["data_quality_flag"]: row["count"] for row in quality_metrics}
    
    ingestion_metrics["sales"] = {
        "records_ingested": record_count,
        "valid_records": quality_dist.get("valid", 0),
        "invalid_records": record_count - quality_dist.get("valid", 0),
        "quality_rate": (quality_dist.get("valid", 0) / record_count) if record_count > 0 else 0
    }
    
    print(f"✅ Sales: {record_count:,} records ingested to Bronze")
    print(f"   Quality Rate: {ingestion_metrics['sales']['quality_rate']*100:.1f}%")
    
except Exception as e:
    print(f"❌ Failed to ingest sales data: {str(e)}")
    raise e

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest Inventory Data to Bronze

# COMMAND ----------

try:
    print("📥 Ingesting inventory data to Bronze layer...")
    
    # Read raw inventory data
    raw_inventory_df = spark.table("retail_data.raw_inventory") \
        .filter(col("processing_date") == processing_date)
    
    # Add Bronze layer metadata and quality checks
    bronze_inventory_df = raw_inventory_df \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("source_file", lit("raw_inventory_table")) \
        .withColumn("bronze_run_id", lit(run_id)) \
        .withColumn("stock_alert", 
                   when(col("current_stock") < col("reorder_point"), "low_stock")
                   .when(col("current_stock") <= 0, "out_of_stock")
                   .when(col("current_stock") < 0, "negative_stock")
                   .otherwise("adequate"))
    
    # Create or update Bronze inventory table
    bronze_inventory_df.write \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable("retail_data.bronze_inventory")
    
    # Calculate metrics
    inventory_count = bronze_inventory_df.count()
    stock_alerts = bronze_inventory_df.groupBy("stock_alert").count().collect()
    alert_dist = {row["stock_alert"]: row["count"] for row in stock_alerts}
    
    ingestion_metrics["inventory"] = {
        "records_ingested": inventory_count,
        "low_stock_items": alert_dist.get("low_stock", 0),
        "out_of_stock_items": alert_dist.get("out_of_stock", 0)
    }
    
    print(f"✅ Inventory: {inventory_count:,} records ingested to Bronze")
    print(f"   Low Stock Items: {alert_dist.get('low_stock', 0)}")
    print(f"   Out of Stock Items: {alert_dist.get('out_of_stock', 0)}")
    
except Exception as e:
    print(f"❌ Failed to ingest inventory data: {str(e)}")
    raise e

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest Customer Data to Bronze

# COMMAND ----------

try:
    print("📥 Ingesting customer data to Bronze layer...")
    
    # Read raw customer data
    raw_customers_df = spark.table("retail_data.raw_customers") \
        .filter(col("processing_date") == processing_date)
    
    # Add Bronze layer metadata and calculate customer metrics
    bronze_customers_df = raw_customers_df \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("source_file", lit("raw_customers_table")) \
        .withColumn("bronze_run_id", lit(run_id)) \
        .withColumn("days_since_last_purchase", 
                   datediff(current_date(), col("last_purchase_date"))) \
        .withColumn("customer_status",
                   when(col("is_active") == False, "inactive")
                   .when(col("days_since_last_purchase") > 90, "at_risk")
                   .when(col("days_since_last_purchase") > 60, "dormant")
                   .otherwise("active"))
    
    # Create or update Bronze customers table
    bronze_customers_df.write \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable("retail_data.bronze_customers")
    
    # Calculate metrics
    customer_count = bronze_customers_df.count()
    status_dist = bronze_customers_df.groupBy("customer_status").count().collect()
    status_metrics = {row["customer_status"]: row["count"] for row in status_dist}
    
    ingestion_metrics["customers"] = {
        "records_ingested": customer_count,
        "active_customers": status_metrics.get("active", 0),
        "at_risk_customers": status_metrics.get("at_risk", 0),
        "inactive_customers": status_metrics.get("inactive", 0)
    }
    
    print(f"✅ Customers: {customer_count:,} records ingested to Bronze")
    print(f"   Active: {status_metrics.get('active', 0):,}")
    print(f"   At Risk: {status_metrics.get('at_risk', 0):,}")
    
except Exception as e:
    print(f"❌ Failed to ingest customer data: {str(e)}")
    raise e

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Validation

# COMMAND ----------

# Perform Bronze layer data quality checks
print("\n📊 Running Bronze layer quality checks...")

quality_checks = []

# Check for duplicate transactions
duplicate_check = spark.sql(f"""
    SELECT COUNT(*) as total, COUNT(DISTINCT transaction_id) as unique_ids
    FROM retail_data.bronze_sales
    WHERE processing_date = '{processing_date}'
""").collect()[0]

duplicate_rate = 1 - (duplicate_check["unique_ids"] / duplicate_check["total"]) if duplicate_check["total"] > 0 else 0

quality_checks.append({
    "check_timestamp": datetime.now(),
    "table_name": "bronze_sales",
    "check_name": "duplicate_transactions",
    "check_result": "PASS" if duplicate_rate < 0.01 else "FAIL",
    "failed_records": int((duplicate_check["total"] - duplicate_check["unique_ids"])),
    "total_records": duplicate_check["total"],
    "error_percentage": duplicate_rate * 100,
    "processing_date": processing_date
})

# Check for null critical fields
#null_check = spark.sql(f"""
#    SELECT 
#        SUM(CASE WHEN transaction_id IS NULL THEN 1 ELSE 0 END) as null_ids,
#        SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) as null_customers,
#        SUM(CASE WHEN total_amount IS NULL OR total_amount <= 0 THEN 1 ELSE 0 END) as invalid_amounts,
#        COUNT(*) as total
#    FROM retail_data.bronze_sales
#    WHERE processing_date = '{processing_date}'
#""").collect()[0]

# Check for null critical fields
null_check = spark.sql(f"""
    SELECT
        COALESCE(SUM(CASE WHEN transaction_id IS NULL THEN 1 ELSE 0 END), 0) as null_ids,
        COALESCE(SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END), 0) as null_customers,
        COALESCE(SUM(CASE WHEN total_amount IS NULL OR total_amount <= 0 THEN 1 ELSE 0 END), 0) as invalid_amounts,
        COUNT(*) as total
    FROM retail_data.bronze_sales
    WHERE processing_date = '{processing_date}'
""").collect()[0]

critical_nulls = int(null_check["null_ids"]  + null_check["invalid_amounts"])

if critical_nulls is None:
    critical_nulls = int(0)

null_rate = critical_nulls / null_check["total"] if null_check["total"] > 0 else 0

quality_checks.append({
    "check_timestamp": datetime.now(),
    "table_name": "bronze_sales",
    "check_name": "critical_nulls",
    "check_result": "PASS" if null_rate < 0.05 else "FAIL",
    "failed_records": int(critical_nulls),
    "total_records": null_check["total"],
    "error_percentage": null_rate * 100,
    "processing_date": processing_date
})

# Write quality metrics
quality_df = spark.createDataFrame(quality_checks)

quality_df = quality_df.select(
    "check_timestamp",
    "table_name", 
    "check_name",
    "check_result",
    quality_df["failed_records"].cast("int").alias("failed_records"),
    quality_df["total_records"].cast("int").alias("total_records"),
    quality_df["error_percentage"].cast("double").alias("error_percentage"),
    quality_df["processing_date"].cast("date").alias("processing_date")
)

quality_df.write.mode("append").saveAsTable("retail_data.data_quality_metrics")

print(f"✅ Quality checks completed: {len([c for c in quality_checks if c['check_result'] == 'PASS'])}/{len(quality_checks)} passed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log Ingestion Metrics

# COMMAND ----------

# Calculate overall ingestion metrics
#total_records = sum(m["records_ingested"] for m in ingestion_metrics.values())

total_records = 0
for m in ingestion_metrics.values():
    total_records += m["records_ingested"]

ingestion_duration = (datetime.now() - ingestion_start).total_seconds()

# Log metrics to monitoring table
monitoring_data = []

processing_date_fix = datetime.strptime(processing_date, '%Y-%m-%d').date()

for source, metrics in ingestion_metrics.items():
    for metric_name, metric_value in metrics.items():
        monitoring_data.append({
            "metric_timestamp": datetime.now(),
            "pipeline_name": "globalmart_daily_etl",
            "task_name": "bronze_ingestion",
            "metric_name": f"{source}_{metric_name}",
            "metric_value": float(metric_value),
            "metric_unit": "count" if "records" in metric_name or "items" in metric_name or "customers" in metric_name else "ratio",
            "processing_date": processing_date_fix,
            "run_id": run_id
        })

# Add performance metrics
monitoring_data.append({
    "metric_timestamp": datetime.now(),
    "pipeline_name": "globalmart_daily_etl",
    "task_name": "bronze_ingestion",
    "metric_name": "total_records_ingested",
    "metric_value": float(total_records),
    "metric_unit": "count",
    "processing_date": processing_date_fix,
    "run_id": run_id
})

monitoring_data.append({
    "metric_timestamp": datetime.now(),
    "pipeline_name": "globalmart_daily_etl",
    "task_name": "bronze_ingestion",
    "metric_name": "ingestion_duration",
    "metric_value": ingestion_duration,
    "metric_unit": "seconds",
    "processing_date": processing_date_fix,
    "run_id": run_id
})

monitoring_data.append({
    "metric_timestamp": datetime.now(),
    "pipeline_name": "globalmart_daily_etl",
    "task_name": "bronze_ingestion",
    "metric_name": "records_per_second",
    "metric_value": total_records / ingestion_duration if ingestion_duration > 0 else 0,
    "metric_unit": "rate",
    "processing_date": processing_date_fix,
    "run_id": run_id
})

# Write monitoring metrics
monitoring_df = spark.createDataFrame(monitoring_data)
monitoring_df.write.mode("append").saveAsTable("retail_data.pipeline_metrics")

print(f"\n{'='*50}")
print(f"BRONZE INGESTION COMPLETED")
print(f"{'='*50}")
print(f"Total Records Ingested: {total_records:,}")
print(f"Duration: {ingestion_duration:.2f} seconds")
print(f"Throughput: {total_records/ingestion_duration:.0f} records/second")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Return Results

# COMMAND ----------

# Prepare results for job orchestrator
results = {
    "status": "SUCCESS",
    "message": "Bronze ingestion completed successfully",
    "metrics": {
        "total_records": total_records,
        "duration_seconds": ingestion_duration,
        "throughput": total_records / ingestion_duration if ingestion_duration > 0 else 0,
        "tables_updated": list(ingestion_metrics.keys()),
        "quality_checks_passed": len([c for c in quality_checks if c["check_result"] == "PASS"]),
        "quality_checks_total": len(quality_checks)
    },
    "details": ingestion_metrics
}

dbutils.notebook.exit(json.dumps(results))
