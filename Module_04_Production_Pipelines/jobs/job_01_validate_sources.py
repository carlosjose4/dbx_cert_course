# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Validate Data Sources
# MAGIC
# MAGIC This notebook validates that all required data sources are available and meet quality thresholds
# MAGIC before starting the main ETL pipeline.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

# Set default parameters - these will be overridden by job parameters
dbutils.widgets.text("processing_date", "")
dbutils.widgets.text("min_records_threshold", "1000")
dbutils.widgets.text("run_id", "")

processing_date = dbutils.widgets.get("processing_date")
min_records_threshold = int(dbutils.widgets.get("min_records_threshold"))
run_id = dbutils.widgets.get("run_id")

# If no processing date provided, use today
if not processing_date:
    from datetime import datetime
    processing_date = datetime.now().strftime("%Y-%m-%d")

print(f"Processing Date: {processing_date}")
print(f"Min Records Threshold: {min_records_threshold}")
print(f"Run ID: {run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Libraries and Setup

# COMMAND ----------

from pyspark.sql.functions import *
from datetime import datetime
import json
import uuid

# Set catalog and schema
spark.sql("USE CATALOG sm_training")
spark.sql("USE SCHEMA retail_data")

# Generate run_id if not provided
if not run_id:
    run_id = str(uuid.uuid4())

validation_results = []
validation_start = datetime.now()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Raw Sales Data

# COMMAND ----------

try:
    # Check if raw_sales table exists and has data for processing date
    sales_df = spark.table("retail_data.raw_sales") \
        .filter(col("processing_date") == processing_date)
        
    sales_count = sales_df.count()
        
        # Validation checks
    validation_checks = {
        "table_exists": True,
        "has_data": sales_count > 0,
        "meets_threshold": sales_count >= min_records_threshold,
        "record_count": sales_count
    }
        
        # Check for critical columns
    required_columns = ["transaction_id", "store_id", "customer_id", "product_id", 
                            "transaction_timestamp", "total_amount"]
    missing_columns = [col for col in required_columns if col not in sales_df.columns]
    validation_checks["has_required_columns"] = len(missing_columns) == 0
    validation_checks["missing_columns"] = missing_columns
        
        # Check data freshness
    if sales_count > 0:
        max_timestamp = sales_df.agg(max("transaction_timestamp")).collect()[0][0]
        hours_old = (datetime.now() - max_timestamp).total_seconds() / 3600 if max_timestamp else 999
        validation_checks["data_freshness_hours"] = hours_old
        validation_checks["is_fresh"] = hours_old < 48  # Data should be less than 48 hours old
        
    validation_results.append({
        "source": "raw_sales",
        "status": "PASS" if all([validation_checks["has_data"], 
                                    validation_checks["meets_threshold"],
                                    validation_checks["has_required_columns"]]) else "FAIL",
        "checks": validation_checks
    })
        
    print(f"✅ Raw Sales: {sales_count:,} records found")
    
except Exception as e:
    validation_results.append({
        "source": "raw_sales",
        "status": "FAIL",
        "error": str(e)
    })
    print(f"❌ Raw Sales validation failed: {str(e)}")

# COMMAND ----------

print(validation_results)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Raw Inventory Data

# COMMAND ----------

try:
    # Check raw_inventory table
    inventory_df = spark.table("retail_data.raw_inventory") \
        .filter(col("processing_date") == processing_date)
    
    inventory_count = inventory_df.count()
    
    validation_checks = {
        "table_exists": True,
        "has_data": inventory_count > 0,
        "record_count": inventory_count
    }
    
    # Check for data quality
    if inventory_count > 0:
        # Check for negative stock levels
        negative_stock = inventory_df.filter(col("current_stock") < 0).count()
        validation_checks["has_negative_stock"] = negative_stock > 0
        validation_checks["negative_stock_count"] = negative_stock
        
        # Check for orphaned products (products not in sales)
        products_in_inventory = inventory_df.select("product_id").distinct().count()
        validation_checks["unique_products"] = products_in_inventory
    
    validation_results.append({
        "source": "raw_inventory",
        "status": "PASS" if validation_checks["has_data"] else "FAIL",
        "checks": validation_checks
    })
    
    print(f"✅ Raw Inventory: {inventory_count:,} records found")
    
except Exception as e:
    validation_results.append({
        "source": "raw_inventory",
        "status": "FAIL",
        "error": str(e)
    })
    print(f"❌ Raw Inventory validation failed: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Raw Customer Data

# COMMAND ----------

try:
    # Check raw_customers table
    customers_df = spark.table("retail_data.raw_customers") \
        .filter(col("processing_date") == processing_date)
    
    customer_count = customers_df.count()
    
    validation_checks = {
        "table_exists": True,
        "has_data": customer_count > 0,
        "record_count": customer_count
    }
    
    # Additional customer data checks
    if customer_count > 0:
        # Check for active customers
        active_customers = customers_df.filter(col("is_active") == True).count()
        validation_checks["active_customers"] = active_customers
        validation_checks["active_rate"] = active_customers / customer_count
        
        # Check for data completeness
        null_segments = customers_df.filter(col("customer_segment").isNull()).count()
        validation_checks["null_segments"] = null_segments
        validation_checks["segment_completeness"] = 1 - (null_segments / customer_count)
    
    validation_results.append({
        "source": "raw_customers",
        "status": "PASS" if validation_checks["has_data"] else "FAIL",
        "checks": validation_checks
    })
    
    print(f"✅ Raw Customers: {customer_count:,} records found")
    
except Exception as e:
    validation_results.append({
        "source": "raw_customers",
        "status": "FAIL",
        "error": str(e)
    })
    print(f"❌ Raw Customers validation failed: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cross-Source Validation

# COMMAND ----------

# Perform cross-source validation checks
cross_validation = {}

try:
    # Check customer coverage in sales
    if sales_count > 0 and customer_count > 0:
        customers_in_sales = sales_df.select("customer_id").distinct().count()
        cross_validation["customer_coverage"] = customers_in_sales / customer_count
        
    # Check product coverage
    if sales_count > 0 and inventory_count > 0:
        products_in_sales = sales_df.select("product_id").distinct().count()
        products_in_inventory = inventory_df.select("product_id").distinct().count()
        cross_validation["product_coverage"] = products_in_sales / products_in_inventory
    
    # Check store consistency
    stores_in_sales = sales_df.select("store_id").distinct().count()
    stores_in_inventory = inventory_df.select("store_id").distinct().count()
    cross_validation["store_consistency"] = stores_in_sales == stores_in_inventory
    
    validation_results.append({
        "source": "cross_validation",
        "status": "PASS",
        "checks": cross_validation
    })
    
    print("✅ Cross-source validation completed")
    
except Exception as e:
    print(f"⚠️ Cross-validation skipped due to missing data: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log Validation Results

# COMMAND ----------

# Determine overall validation status
failed_sources = [r for r in validation_results if r["status"] == "FAIL"]
overall_status = "FAILED" if failed_sources else "PASSED"

# Create validation summary
validation_summary = {
    "run_id": run_id,
    "processing_date": processing_date,
    "validation_timestamp": datetime.now().isoformat(),
    "overall_status": overall_status,
    "sources_validated": len(validation_results),
    "sources_failed": len(failed_sources),
    "validation_details": validation_results,
    "duration_seconds": (datetime.now() - validation_start).total_seconds()
}

# Log to monitoring table
monitoring_data = []
processing_date_fix = datetime.strptime(processing_date, '%Y-%m-%d').date()
for result in validation_results:
    monitoring_data.append({
        "metric_timestamp": datetime.now(),
        "pipeline_name": "globalmart_daily_etl",
        "task_name": "validate_sources",
        "metric_name": f"{result['source']}_validation",
        "metric_value": 1.0 if result["status"] == "PASS" else 0.0,
        "metric_unit": "boolean",
        "processing_date": processing_date_fix,
        "run_id": run_id
    })

# Write monitoring metrics
monitoring_df = spark.createDataFrame(monitoring_data)
monitoring_df.write.mode("append").saveAsTable("retail_data.pipeline_metrics")

print(f"\n{'='*50}")
print(f"VALIDATION {overall_status}")
print(f"{'='*50}")
print(f"Sources Validated: {len(validation_results)}")
print(f"Sources Failed: {len(failed_sources)}")
if failed_sources:
    print("\nFailed Sources:")
    for source in failed_sources:
        print(f"  - {source['source']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Return Results

# COMMAND ----------

# Return validation results to the job orchestrator
if overall_status == "FAILED":
    error_msg = f"Validation failed for {len(failed_sources)} source(s): {', '.join([s['source'] for s in failed_sources])}"
    dbutils.notebook.exit(json.dumps({
        "status": "FAILED",
        "error": error_msg,
        "details": validation_summary
    }))
else:
    dbutils.notebook.exit(json.dumps({
        "status": "SUCCESS",
        "message": "All validations passed",
        "details": validation_summary
    }))
