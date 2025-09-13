# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Data Quality Validation
# MAGIC
# MAGIC This notebook performs comprehensive data quality validation across all layers
# MAGIC and generates quality reports for monitoring and alerting.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

# Parameters from job orchestrator
dbutils.widgets.text("processing_date", "")
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("alert_threshold", "0.95")

processing_date = dbutils.widgets.get("processing_date")
run_id = dbutils.widgets.get("run_id")
alert_threshold = float(dbutils.widgets.get("alert_threshold"))

if not processing_date:
    from datetime import datetime
    processing_date = datetime.now().strftime("%Y-%m-%d")

if not run_id:
    import uuid
    run_id = str(uuid.uuid4())

print(f"Processing Date: {processing_date}")
print(f"Run ID: {run_id}")
print(f"Alert Threshold: {alert_threshold}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Imports

# COMMAND ----------

from pyspark.sql.functions import *
from datetime import datetime
import json

# Set catalog and schema
spark.sql("USE CATALOG sm_training")
spark.sql("USE SCHEMA retail_data")

validation_start = datetime.now()
validation_results = []
quality_issues = []

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer Validation

# COMMAND ----------

print("🔍 Validating Bronze Layer...")

# Check Bronze sales data quality
bronze_sales_quality = spark.sql(f"""
    SELECT 
        COUNT(*) as total_records,
        SUM(CASE WHEN transaction_id IS NULL THEN 1 ELSE 0 END) as null_transaction_ids,
        SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) as null_customer_ids,
        SUM(CASE WHEN total_amount <= 0 THEN 1 ELSE 0 END) as invalid_amounts,
        SUM(CASE WHEN data_quality_flag != 'valid' THEN 1 ELSE 0 END) as flagged_records,
        COUNT(DISTINCT transaction_id) as unique_transactions,
        MIN(transaction_timestamp) as earliest_transaction,
        MAX(transaction_timestamp) as latest_transaction
    FROM retail_data.bronze_sales
    WHERE processing_date = '{processing_date}'
""").collect()[0]

# Calculate quality metrics
bronze_quality_score = 1.0
if bronze_sales_quality["total_records"] > 0:
    duplicate_rate = 1 - (bronze_sales_quality["unique_transactions"] / bronze_sales_quality["total_records"])
    null_rate = bronze_sales_quality["null_transaction_ids"] / bronze_sales_quality["total_records"]
    invalid_rate = bronze_sales_quality["invalid_amounts"] / bronze_sales_quality["total_records"]
    
    bronze_quality_score = 1 - (duplicate_rate + null_rate + invalid_rate)
    
    validation_results.append({
        "layer": "bronze",
        "table": "bronze_sales",
        "metric": "quality_score",
        "value": bronze_quality_score,
        "status": "PASS" if bronze_quality_score >= alert_threshold else "FAIL",
        "details": {
            "total_records": bronze_sales_quality["total_records"],
            "duplicate_rate": duplicate_rate,
            "null_rate": null_rate,
            "invalid_rate": invalid_rate
        }
    })
    
    if bronze_quality_score < alert_threshold:
        quality_issues.append({
            "layer": "bronze",
            "issue": "Low quality score",
            "severity": "HIGH",
            "details": f"Quality score {bronze_quality_score:.3f} below threshold {alert_threshold}"
        })

print(f"✅ Bronze Sales Quality Score: {bronze_quality_score:.3f}")

# Check Bronze inventory
bronze_inventory_issues = spark.sql(f"""
    SELECT 
        SUM(CASE WHEN current_stock < 0 THEN 1 ELSE 0 END) as negative_stock,
        SUM(CASE WHEN current_stock > 10000 THEN 1 ELSE 0 END) as excessive_stock,
        COUNT(*) as total_items
    FROM retail_data.bronze_inventory
    WHERE processing_date = '{processing_date}'
""").collect()[0]

if bronze_inventory_issues["negative_stock"] is not None:
    if bronze_inventory_issues["negative_stock"] > 0:
        quality_issues.append({
            "layer": "bronze",
            "issue": "Negative inventory",
            "severity": "CRITICAL",
            "details": f"{bronze_inventory_issues['negative_stock']} items with negative stock"
        })

print(f"✅ Bronze Inventory: {bronze_inventory_issues['total_items']} items validated")

# COMMAND ----------

print(bronze_inventory_issues)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer Validation

# COMMAND ----------

print("\n🔍 Validating Silver Layer...")

# Check Silver sales data enrichment
silver_enrichment = spark.sql(f"""
    SELECT 
        COUNT(*) as total_records,
        SUM(CASE WHEN customer_segment = 'Unknown' THEN 1 ELSE 0 END) as unknown_segments,
        SUM(CASE WHEN customer_region = 'Unknown' THEN 1 ELSE 0 END) as unknown_regions,
        AVG(data_quality_score) as avg_quality_score,
        MIN(data_quality_score) as min_quality_score,
        MAX(data_quality_score) as max_quality_score
    FROM retail_data.silver_sales
    WHERE processing_date = '{processing_date}'
""").collect()[0]

if silver_enrichment["total_records"] > 0:
    enrichment_rate = 1 - (silver_enrichment["unknown_segments"] / silver_enrichment["total_records"])
    
    validation_results.append({
        "layer": "silver",
        "table": "silver_sales",
        "metric": "enrichment_rate",
        "value": enrichment_rate,
        "status": "PASS" if enrichment_rate >= 0.90 else "WARN",
        "details": {
            "total_records": silver_enrichment["total_records"],
            "avg_quality_score": silver_enrichment["avg_quality_score"],
            "unknown_segments": silver_enrichment["unknown_segments"]
        }
    })
    
    print(f"✅ Silver Sales Enrichment Rate: {enrichment_rate:.1%}")
    print(f"   Average Quality Score: {silver_enrichment['avg_quality_score']:.3f}")

# Check Silver customer metrics
customer_validation = spark.sql(f"""
    SELECT 
        COUNT(*) as total_customers,
        SUM(CASE WHEN churn_risk_score > 0.8 THEN 1 ELSE 0 END) as high_risk_customers,
        SUM(CASE WHEN days_since_last_purchase > 180 THEN 1 ELSE 0 END) as dormant_customers,
        AVG(churn_risk_score) as avg_churn_risk
    FROM retail_data.silver_customers
    WHERE processing_date = '{processing_date}'
""").collect()[0]
if customer_validation["high_risk_customers"] is not None and customer_validation["total_customers"] is not None:
    if customer_validation["high_risk_customers"] > customer_validation["total_customers"] * 0.2:
        quality_issues.append({
            "layer": "silver",
            "issue": "High churn risk",
            "severity": "MEDIUM",
            "details": f"{customer_validation['high_risk_customers']} customers at high risk"
        })

print(f"✅ Silver Customers: {customer_validation['total_customers']} validated")
print(f"   High Risk: {customer_validation['high_risk_customers']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer Validation

# COMMAND ----------

print("\n🔍 Validating Gold Layer...")

# Check Gold aggregation completeness
gold_completeness = spark.sql(f"""
    SELECT 
        (SELECT COUNT(DISTINCT store_id) FROM retail_data.gold_daily_sales_summary 
         WHERE processing_date = '{processing_date}') as stores_in_summary,
        (SELECT COUNT(DISTINCT store_id) FROM retail_data.silver_sales 
         WHERE processing_date = '{processing_date}') as stores_in_silver,
        (SELECT COUNT(*) FROM retail_data.gold_customer_metrics 
         WHERE processing_date = '{processing_date}') as customer_metrics,
        (SELECT COUNT(DISTINCT customer_id) FROM retail_data.silver_sales 
         WHERE processing_date = '{processing_date}') as customers_in_silver
""").collect()[0]

aggregation_completeness = gold_completeness["stores_in_summary"] / gold_completeness["stores_in_silver"] \
    if gold_completeness["stores_in_silver"] > 0 else 0

validation_results.append({
    "layer": "gold",
    "table": "aggregations",
    "metric": "completeness",
    "value": aggregation_completeness,
    "status": "PASS" if aggregation_completeness >= 0.99 else "FAIL",
    "details": {
        "stores_aggregated": gold_completeness["stores_in_summary"],
        "stores_expected": gold_completeness["stores_in_silver"],
        "customer_metrics": gold_completeness["customer_metrics"]
    }
})

print(f"✅ Gold Aggregation Completeness: {aggregation_completeness:.1%}")

# Validate business metrics reasonableness
business_metrics = spark.sql(f"""
    SELECT 
        SUM(total_revenue) as total_revenue,
        AVG(avg_transaction_value) as avg_transaction,
        MAX(total_revenue) as max_store_revenue,
        MIN(total_revenue) as min_store_revenue
    FROM retail_data.gold_daily_sales_summary
    WHERE processing_date = '{processing_date}'
""").collect()[0]

# Check for anomalies in revenue
if business_metrics["max_store_revenue"] and business_metrics["min_store_revenue"]:
    revenue_ratio = business_metrics["max_store_revenue"] / business_metrics["min_store_revenue"] \
        if business_metrics["min_store_revenue"] > 0 else 999
    
    if revenue_ratio > 100:
        quality_issues.append({
            "layer": "gold",
            "issue": "Revenue anomaly",
            "severity": "LOW",
            "details": f"Store revenue variance too high: {revenue_ratio:.1f}x"
        })

print(f"✅ Business Metrics Validated")
print(f"   Total Revenue: ${business_metrics['total_revenue']}")
print(f"   Avg Transaction: ${business_metrics['avg_transaction']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cross-Layer Consistency Checks

# COMMAND ----------

print("\n🔗 Performing Cross-Layer Consistency Checks...")

# Check record count consistency
consistency_check = spark.sql(f"""
    SELECT 
        (SELECT COUNT(*) FROM retail_data.bronze_sales 
         WHERE processing_date = '{processing_date}') as bronze_count,
        (SELECT COUNT(*) FROM retail_data.silver_sales 
         WHERE processing_date = '{processing_date}') as silver_count,
        (SELECT SUM(total_transactions) FROM retail_data.gold_daily_sales_summary 
         WHERE processing_date = '{processing_date}') as gold_transactions
""").collect()[0]

# Calculate data loss between layers
bronze_to_silver_loss = 0
if consistency_check["bronze_count"] > 0:
    bronze_to_silver_loss = (consistency_check["bronze_count"] - consistency_check["silver_count"]) / \
                            consistency_check["bronze_count"]

silver_to_gold_loss = 0
if consistency_check["silver_count"] > 0 and consistency_check["gold_transactions"]:
    silver_to_gold_loss = (consistency_check["silver_count"] - consistency_check["gold_transactions"]) / \
                         consistency_check["silver_count"]

validation_results.append({
    "layer": "pipeline",
    "table": "cross_layer",
    "metric": "data_consistency",
    "value": 1 - bronze_to_silver_loss,
    "status": "PASS" if bronze_to_silver_loss < 0.05 else "WARN",
    "details": {
        "bronze_records": consistency_check["bronze_count"],
        "silver_records": consistency_check["silver_count"],
        "gold_transactions": consistency_check["gold_transactions"],
        "bronze_to_silver_loss": bronze_to_silver_loss,
        "silver_to_gold_loss": silver_to_gold_loss
    }
})

print(f"✅ Data Consistency:")
print(f"   Bronze → Silver: {(1-bronze_to_silver_loss):.1%} retention")
print(f"   Silver → Gold: {(1-silver_to_gold_loss):.1%} retention")

# Check data freshness
freshness_check = spark.sql(f"""
    SELECT 
        (SELECT MAX(ingestion_timestamp) FROM retail_data.bronze_sales 
         WHERE processing_date = '{processing_date}') as bronze_latest,
        (SELECT MAX(silver_processing_timestamp) FROM retail_data.silver_sales 
         WHERE processing_date = '{processing_date}') as silver_latest,
        (SELECT MAX(processing_timestamp) FROM retail_data.gold_daily_sales_summary 
         WHERE processing_date = '{processing_date}') as gold_latest
""").collect()[0]

if bronze_latest := freshness_check["bronze_latest"]:
    bronze_age_hours = (datetime.now() - bronze_latest).total_seconds() / 3600
    
    if bronze_age_hours > 24:
        quality_issues.append({
            "layer": "pipeline",
            "issue": "Stale data",
            "severity": "HIGH",
            "details": f"Bronze data is {bronze_age_hours:.1f} hours old"
        })

print(f"✅ Data Freshness Validated")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Quality Report

# COMMAND ----------

print("\n📊 Generating Quality Report...")

# Calculate overall quality score
quality_scores = [r["value"] for r in validation_results if "value" in r]
total_score = 0
count = 0
for score in quality_scores:
    total_score += score
    count += 1

overall_quality_score = total_score / count if count > 0 else 0

# Determine overall status
critical_issues = [i for i in quality_issues if i["severity"] == "CRITICAL"]
high_issues = [i for i in quality_issues if i["severity"] == "HIGH"]
failed_checks = [r for r in validation_results if r["status"] == "FAIL"]

if critical_issues:
    overall_status = "CRITICAL"
elif failed_checks or high_issues:
    overall_status = "FAILED"
elif quality_issues:
    overall_status = "WARNING"
else:
    overall_status = "PASSED"

# Create quality report
quality_report = {
    "run_id": run_id,
    "processing_date": processing_date,
    "validation_timestamp": datetime.now().isoformat(),
    "overall_status": overall_status,
    "overall_quality_score": overall_quality_score,
    "checks_performed": len(validation_results),
    "checks_passed": len([r for r in validation_results if r["status"] == "PASS"]),
    "checks_failed": len(failed_checks),
    "issues_found": len(quality_issues),
    "critical_issues": len(critical_issues),
    "validation_details": validation_results,
    "quality_issues": quality_issues
}

# Write quality metrics to monitoring table
quality_metrics_data = []

processing_date_fix = datetime.strptime(processing_date, '%Y-%m-%d').date()

for result in validation_results:
    quality_metrics_data.append({
        "check_timestamp": datetime.now(),
        "table_name": f"{result['layer']}_{result['table']}",
        "check_name": result["metric"],
        "check_result": result["status"],
        "failed_records": 0 if result["status"] == "PASS" else 1,
        "total_records": 1,
        "error_percentage": 0.0 if result["status"] == "PASS" else 100.0,
        "processing_date": processing_date_fix
    })

quality_df = spark.createDataFrame(quality_metrics_data)
quality_df = quality_df.select(
    "check_timestamp",
    "table_name", 
    "check_name",
    "check_result",
    quality_df["failed_records"].cast("int").alias("failed_records"),
    quality_df["total_records"].cast("int").alias("total_records"),
    "error_percentage",
    "processing_date"
)
quality_df.write.mode("append").saveAsTable("retail_data.data_quality_metrics")

print(f"\n{'='*50}")
print(f"QUALITY VALIDATION {overall_status}")
print(f"{'='*50}")
print(f"Overall Quality Score: {overall_quality_score:.1%}")
print(f"Checks Performed: {len(validation_results)}")
print(f"Checks Passed: {len([r for r in validation_results if r['status'] == 'PASS'])}")
print(f"Issues Found: {len(quality_issues)}")

if critical_issues:
    print("\n⚠️ CRITICAL ISSUES:")
    for issue in critical_issues:
        print(f"  - {issue['issue']}: {issue['details']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Alerts if Needed

# COMMAND ----------

if overall_status in ["CRITICAL", "FAILED"]:
    print("\n🚨 Generating Quality Alerts...")
    
    alert_message = f"""
    DATA QUALITY ALERT - {overall_status}
    
    Pipeline: globalmart_daily_etl
    Date: {processing_date}
    Run ID: {run_id}
    
    Quality Score: {overall_quality_score:.1%}
    Failed Checks: {len(failed_checks)}
    Critical Issues: {len(critical_issues)}
    
    Top Issues:
    """
    
    for issue in quality_issues[:3]:  # Top 3 issues
        alert_message += f"\n    - [{issue['severity']}] {issue['issue']}: {issue['details']}"
    
    # In production, this would send actual alerts
    print(alert_message)
    
    # Log alert to monitoring
    alert_data = {
        "metric_timestamp": datetime.now(),
        "pipeline_name": "globalmart_daily_etl",
        "task_name": "quality_validation",
        "metric_name": "quality_alert_triggered",
        "metric_value": 1.0,
        "metric_unit": "boolean",
        "processing_date": processing_date_fix ,
        "run_id": run_id
    }
    
    alert_df = spark.createDataFrame([alert_data])
    alert_df.write.mode("append").saveAsTable("retail_data.pipeline_metrics")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log Validation Metrics

# COMMAND ----------

validation_duration = (datetime.now() - validation_start).total_seconds()

# Log performance metrics
monitoring_data = [
    {
        "metric_timestamp": datetime.now(),
        "pipeline_name": "globalmart_daily_etl",
        "task_name": "quality_validation",
        "metric_name": "validation_duration",
        "metric_value": validation_duration,
        "metric_unit": "seconds",
        "processing_date": processing_date_fix,
        "run_id": run_id
    },
    {
        "metric_timestamp": datetime.now(),
        "pipeline_name": "globalmart_daily_etl",
        "task_name": "quality_validation",
        "metric_name": "overall_quality_score",
        "metric_value": overall_quality_score,
        "metric_unit": "score",
        "processing_date": processing_date_fix,
        "run_id": run_id
    },
    {
        "metric_timestamp": datetime.now(),
        "pipeline_name": "globalmart_daily_etl",
        "task_name": "quality_validation",
        "metric_name": "checks_performed",
        "metric_value": float(len(validation_results)),
        "metric_unit": "count",
        "processing_date": processing_date_fix,
        "run_id": run_id
    },
    {
        "metric_timestamp": datetime.now(),
        "pipeline_name": "globalmart_daily_etl",
        "task_name": "quality_validation",
        "metric_name": "issues_found",
        "metric_value": float(len(quality_issues)),
        "metric_unit": "count",
        "processing_date": processing_date_fix,
        "run_id": run_id
    }
]

monitoring_df = spark.createDataFrame(monitoring_data)
monitoring_df.write.mode("append").saveAsTable("retail_data.pipeline_metrics")

print(f"\n✅ Validation completed in {validation_duration:.2f} seconds")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Return Results

# COMMAND ----------

# Prepare results for job orchestrator
if overall_status == "CRITICAL":
    dbutils.notebook.exit(json.dumps({
        "status": "FAILED",
        "error": f"Critical quality issues detected: {critical_issues[0]['details']}",
        "report": quality_report
    }))
elif overall_status == "FAILED":
    dbutils.notebook.exit(json.dumps({
        "status": "FAILED",
        "error": f"Quality validation failed: {len(failed_checks)} checks failed",
        "report": quality_report
    }))
else:
    dbutils.notebook.exit(json.dumps({
        "status": "SUCCESS",
        "message": f"Quality validation completed: {overall_status}",
        "quality_score": overall_quality_score,
        "report": quality_report
    }))
