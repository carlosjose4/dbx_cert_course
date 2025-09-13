# Databricks notebook source
# MAGIC %md
# MAGIC # Module 4: Building Production Pipelines - Laboratory Exercises
# MAGIC
# MAGIC ## 🎯 Learning Objectives
# MAGIC
# MAGIC By the end of this module, you will be able to:
# MAGIC - Transform notebooks into production-ready jobs with proper orchestration
# MAGIC - Implement comprehensive monitoring and alerting systems
# MAGIC - Build data quality validation frameworks
# MAGIC - Create production-grade error handling and recovery mechanisms
# MAGIC - Design scalable job configurations for enterprise deployment
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📋 Prerequisites and Setup
# MAGIC
# MAGIC ### Before You Begin:
# MAGIC
# MAGIC 1. **Run the Setup Notebook First**
# MAGIC    - Navigate to `/Module_04_Production_Pipelines/setup/module4_setup_notebook.ipynb`
# MAGIC    - Run all cells to create the required catalog, tables, and sample data
# MAGIC    - This will create the `globalmart_prod.retail_data` database with all necessary tables
# MAGIC
# MAGIC 2. **Verify Your Environment**
# MAGIC    - Check that Unity Catalog is enabled in your workspace
# MAGIC    - Ensure you have permissions to create jobs and workflows
# MAGIC    - Verify access to the `globalmart_prod` catalog
# MAGIC
# MAGIC 3. **Download the Job Configuration**
# MAGIC    - The `module4-job.config.json` file contains the complete job configuration
# MAGIC    - Download it for reference when creating your Databricks job

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏗️ Architecture Overview
# MAGIC
# MAGIC Our production pipeline follows a medallion architecture with comprehensive monitoring:
# MAGIC
# MAGIC ```
# MAGIC Raw Data (Catalog Tables)
# MAGIC         ↓
# MAGIC [1. Validate Sources] → Ensure data availability
# MAGIC         ↓
# MAGIC [2. Bronze Ingestion] → Preserve raw data with metadata
# MAGIC         ↓
# MAGIC [3. Silver Transformation] → Clean and enrich data
# MAGIC         ↓
# MAGIC [4. Gold Aggregation] → Create business metrics
# MAGIC         ↓
# MAGIC [5. Quality Validation] → Validate all layers
# MAGIC         ↓
# MAGIC [Monitoring & Alerts] → Track pipeline health
# MAGIC ```

# COMMAND ----------

# Verify environment setup
from pyspark.sql import SparkSession
import sys

print("🔍 Verifying Environment Setup...")
print("=" * 50)

# Check Spark version
spark = SparkSession.builder.getOrCreate()
print(f"✓ Spark Version: {spark.version}")

# Check Python version
print(f"✓ Python Version: {sys.version.split()[0]}")

# Verify catalog access
try:
    spark.sql("USE CATALOG sm_training")
    spark.sql("USE SCHEMA retail_data")
    
    # Count tables
    tables = spark.sql("SHOW TABLES").collect()
    print(f"✓ Catalog 'retail_data' accessible")
    print(f"✓ Found {len(tables)} tables in the schema")
    
    # Check for key tables
    required_tables = ['raw_sales', 'raw_inventory', 'raw_customers']
    existing_tables = [t.tableName for t in tables]
    
    for table in required_tables:
        if table in existing_tables:
            count = spark.sql(f"SELECT COUNT(*) as cnt FROM {table}").collect()[0].cnt
            print(f"  - {table}: {count:,} records")
        else:
            print(f"  ❌ Missing table: {table}")
            print(f"     Please run the setup notebook first!")
            
except Exception as e:
    print(f"❌ Error: {str(e)}")
    print("Please ensure you've run the setup notebook first!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📁 Job Notebooks Location
# MAGIC
# MAGIC The individual job notebooks are located in:
# MAGIC - `/Module_04_Production_Pipelines/jobs/01_validate_sources.ipynb`
# MAGIC - `/Module_04_Production_Pipelines/jobs/02_bronze_ingestion.ipynb`
# MAGIC - `/Module_04_Production_Pipelines/jobs/03_silver_transformation.ipynb`
# MAGIC - `/Module_04_Production_Pipelines/jobs/04_gold_aggregation.ipynb`
# MAGIC - `/Module_04_Production_Pipelines/jobs/05_quality_validation.ipynb`
# MAGIC
# MAGIC **Important**: Upload these notebooks to your Databricks workspace before creating the job.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab 1: Testing Individual Pipeline Components (30 minutes)
# MAGIC
# MAGIC Before creating the full job, let's test each component individually to understand the data flow.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 1.1: Test Data Validation

# COMMAND ----------

# Test the validation logic locally
from datetime import datetime

processing_date = datetime.now().strftime("%Y-%m-%d")
print(f"Testing validation for date: {processing_date}")

# Check data availability
validation_results = []

# Validate sales data
try:
    sales_count = spark.sql(f"""
        SELECT COUNT(*) as cnt 
        FROM retail_data.raw_sales 
        WHERE processing_date = '{processing_date}'
    """).collect()[0].cnt
    
    validation_results.append({
        "source": "raw_sales",
        "status": "PASS" if sales_count > 0 else "FAIL",
        "records": sales_count
    })
    print(f"✓ Sales data: {sales_count:,} records")
except Exception as e:
    validation_results.append({
        "source": "raw_sales",
        "status": "FAIL",
        "error": str(e)
    })
    print(f"✗ Sales data validation failed: {e}")

# Validate inventory data
try:
    inventory_count = spark.sql(f"""
        SELECT COUNT(*) as cnt 
        FROM retail_data.raw_inventory 
        WHERE processing_date = '{processing_date}'
    """).collect()[0].cnt
    
    validation_results.append({
        "source": "raw_inventory",
        "status": "PASS" if inventory_count > 0 else "FAIL",
        "records": inventory_count
    })
    print(f"✓ Inventory data: {inventory_count:,} records")
except Exception as e:
    validation_results.append({
        "source": "raw_inventory",
        "status": "FAIL",
        "error": str(e)
    })
    print(f"✗ Inventory data validation failed: {e}")

# Overall validation status
failed_validations = [v for v in validation_results if v["status"] == "FAIL"]
if failed_validations:
    print(f"\n⚠️ Validation FAILED for {len(failed_validations)} source(s)")
else:
    print(f"\n✅ All validations PASSED")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 1.2: Test Bronze Ingestion Logic

# COMMAND ----------

# Test Bronze ingestion transformation
from pyspark.sql.functions import *

# Sample Bronze transformation
sample_sales = spark.table("retail_data.raw_sales").limit(100)

# Add Bronze layer metadata
bronze_sample = sample_sales \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_file", lit("raw_sales_table")) \
    .withColumn("data_quality_flag", 
               when(col("transaction_id").isNull(), "missing_id")
               .when(col("total_amount") <= 0, "invalid_amount")
               .otherwise("valid"))

# Show sample Bronze data
print("Sample Bronze Data with Quality Flags:")
bronze_sample.select(
    "transaction_id", 
    "total_amount", 
    "data_quality_flag",
    "ingestion_timestamp"
).show(10, truncate=False)

# Quality distribution
quality_dist = bronze_sample.groupBy("data_quality_flag").count()
print("\nData Quality Distribution:")
quality_dist.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 1.3: Test Silver Transformation Logic

# COMMAND ----------

# Test Silver enrichment logic
from pyspark.sql.window import Window

# Load sample data
sample_sales = spark.table("retail_data.raw_sales").limit(1000)
sample_customers = spark.table("retail_data.raw_customers")

# Enrich sales with customer data
enriched_sample = sample_sales \
    .join(sample_customers.select("customer_id", "customer_segment", "customer_region"), 
          "customer_id", "left") \
    .fillna({"customer_segment": "Unknown", "customer_region": "Unknown"})

# Add derived metrics
silver_sample = enriched_sample \
    .withColumn("discount_percentage", 
                when(col("unit_price") > 0, 
                     col("discount_amount") / (col("quantity") * col("unit_price")) * 100)
                .otherwise(0)) \
    .withColumn("is_high_value", 
                when(col("total_amount") > 500, True).otherwise(False)) \
    .withColumn("transaction_hour", hour("transaction_timestamp"))

# Show enriched data
print("Sample Silver Data with Enrichments:")
silver_sample.select(
    "transaction_id",
    "customer_segment",
    "customer_region", 
    "discount_percentage",
    "is_high_value",
    "transaction_hour"
).show(10)

# Segment distribution
print("\nCustomer Segment Distribution:")
silver_sample.groupBy("customer_segment").count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab 2: Creating the Production Job (45 minutes)
# MAGIC
# MAGIC ### Instructions for Creating a Databricks Job
# MAGIC
# MAGIC 1. **Navigate to Workflows**
# MAGIC    - In your Databricks workspace, click on "Workflows" in the left sidebar
# MAGIC    - Click "Create Job" button
# MAGIC
# MAGIC 2. **Configure Job Settings**
# MAGIC    - Name: `globalmart_daily_etl`
# MAGIC    - Add a description: "Daily ETL pipeline for GlobalMart retail data"
# MAGIC    - Set max concurrent runs: 1
# MAGIC
# MAGIC 3. **Add Tasks** (Add each task in order)
# MAGIC    
# MAGIC    **Task 1: Validate Sources**
# MAGIC    - Task name: `validate_sources`
# MAGIC    - Type: Notebook
# MAGIC    - Path: `/Module_04_Production_Pipelines/jobs/01_validate_sources`
# MAGIC    - Parameters:
# MAGIC      - `processing_date`: `{{job.start_time | date: '%Y-%m-%d'}}`
# MAGIC      - `min_records_threshold`: `1000`
# MAGIC    - Cluster: Create new job cluster
# MAGIC    - Timeout: 600 seconds
# MAGIC    - Retries: 2
# MAGIC    
# MAGIC    **Task 2: Bronze Ingestion**
# MAGIC    - Task name: `bronze_ingestion`
# MAGIC    - Type: Notebook
# MAGIC    - Path: `/Module_04_Production_Pipelines/jobs/02_bronze_ingestion`
# MAGIC    - Depends on: `validate_sources`
# MAGIC    - Parameters:
# MAGIC      - `processing_date`: `{{job.start_time | date: '%Y-%m-%d'}}`
# MAGIC    - Timeout: 1800 seconds
# MAGIC    - Retries: 3
# MAGIC    
# MAGIC    **Task 3: Silver Transformation**
# MAGIC    - Task name: `silver_transformation`
# MAGIC    - Type: Notebook
# MAGIC    - Path: `/Module_04_Production_Pipelines/jobs/03_silver_transformation`
# MAGIC    - Depends on: `bronze_ingestion`
# MAGIC    - Parameters:
# MAGIC      - `processing_date`: `{{job.start_time | date: '%Y-%m-%d'}}`
# MAGIC      - `quality_threshold`: `0.95`
# MAGIC    - Timeout: 2400 seconds
# MAGIC    - Retries: 2
# MAGIC    
# MAGIC    **Task 4: Gold Aggregation**
# MAGIC    - Task name: `gold_aggregation`
# MAGIC    - Type: Notebook
# MAGIC    - Path: `/Module_04_Production_Pipelines/jobs/04_gold_aggregation`
# MAGIC    - Depends on: `silver_transformation`
# MAGIC    - Parameters:
# MAGIC      - `processing_date`: `{{job.start_time | date: '%Y-%m-%d'}}`
# MAGIC      - `aggregation_window_days`: `30`
# MAGIC    - Timeout: 1800 seconds
# MAGIC    - Retries: 2
# MAGIC    
# MAGIC    **Task 5: Quality Validation**
# MAGIC    - Task name: `quality_validation`
# MAGIC    - Type: Notebook
# MAGIC    - Path: `/Module_04_Production_Pipelines/jobs/05_quality_validation`
# MAGIC    - Depends on: `gold_aggregation`
# MAGIC    - Parameters:
# MAGIC      - `processing_date`: `{{job.start_time | date: '%Y-%m-%d'}}`
# MAGIC      - `alert_threshold`: `0.95`
# MAGIC    - Timeout: 900 seconds
# MAGIC    - Retries: 1
# MAGIC
# MAGIC 4. **Configure Cluster**
# MAGIC    - Spark version: 13.3.x-scala2.12
# MAGIC    - Node type: Standard_DS3_v2
# MAGIC    - Autoscale: 2-8 workers
# MAGIC    - Add Spark configs:
# MAGIC      - `spark.sql.adaptive.enabled`: `true`
# MAGIC      - `spark.sql.adaptive.coalescePartitions.enabled`: `true`
# MAGIC
# MAGIC 5. **Set Schedule** (Optional)
# MAGIC    - Click "Add schedule"
# MAGIC    - Cron expression: `0 0 2 * * ?` (Daily at 2 AM UTC)
# MAGIC    - Timezone: UTC
# MAGIC
# MAGIC 6. **Configure Alerts**
# MAGIC    - Add email notifications for failures
# MAGIC    - Set up Slack webhook for critical alerts

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 2.1: Analyze the Job Configuration

# COMMAND ----------

import json

# Load and display the job configuration
job_config = {
    "name": "globalmart_daily_etl",
    "tasks": [
        {
            "task_key": "validate_sources",
            "notebook_path": "/Module_04_Production_Pipelines/jobs/01_validate_sources",
            "timeout_seconds": 600,
            "max_retries": 2
        },
        {
            "task_key": "bronze_ingestion",
            "depends_on": [{"task_key": "validate_sources"}],
            "notebook_path": "/Module_04_Production_Pipelines/jobs/02_bronze_ingestion",
            "timeout_seconds": 1800,
            "max_retries": 3
        },
        {
            "task_key": "silver_transformation",
            "depends_on": [{"task_key": "bronze_ingestion"}],
            "notebook_path": "/Module_04_Production_Pipelines/jobs/03_silver_transformation",
            "timeout_seconds": 2400,
            "max_retries": 2
        },
        {
            "task_key": "gold_aggregation",
            "depends_on": [{"task_key": "silver_transformation"}],
            "notebook_path": "/Module_04_Production_Pipelines/jobs/04_gold_aggregation",
            "timeout_seconds": 1800,
            "max_retries": 2
        },
        {
            "task_key": "quality_validation",
            "depends_on": [{"task_key": "gold_aggregation"}],
            "notebook_path": "/Module_04_Production_Pipelines/jobs/05_quality_validation",
            "timeout_seconds": 900,
            "max_retries": 1
        }
    ]
}

print("Job Configuration Analysis:")
print("=" * 50)
print(f"Job Name: {job_config['name']}")
print(f"Total Tasks: {len(job_config['tasks'])}")
print(f"\nTask Dependency Chain:")
for task in job_config['tasks']:
    deps = task.get('depends_on', [])
    if deps:
        print(f"  {task['task_key']} → depends on → {deps[0]['task_key']}")
    else:
        print(f"  {task['task_key']} → (entry point)")
        
print(f"\nRetry Configuration:")
for task in job_config['tasks']:
    print(f"  {task['task_key']}: {task['max_retries']} retries, {task['timeout_seconds']}s timeout")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab 3: Monitoring and Alerting (30 minutes)
# MAGIC
# MAGIC ### Exercise 3.1: Query Pipeline Metrics

# COMMAND ----------

# Query pipeline metrics
print("📊 Pipeline Metrics Dashboard")
print("=" * 50)

# Check latest pipeline run metrics
latest_metrics = spark.sql("""
    SELECT 
        task_name,
        metric_name,
        metric_value,
        metric_unit,
        metric_timestamp
    FROM retail_data.pipeline_metrics
    WHERE processing_date = current_date()
    ORDER BY metric_timestamp DESC
    LIMIT 20
""")

if latest_metrics.count() > 0:
    print("\nLatest Pipeline Metrics:")
    latest_metrics.show(truncate=False)
else:
    print("\nNo metrics found for today. Run the pipeline first!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 3.2: Create Monitoring Dashboard

# COMMAND ----------

# Create monitoring visualizations
import pandas as pd
import matplotlib.pyplot as plt

# Get aggregated metrics
metrics_summary = spark.sql("""
    SELECT 
        task_name,
        AVG(CASE WHEN metric_name LIKE '%duration%' THEN metric_value END) as avg_duration,
        SUM(CASE WHEN metric_name LIKE '%records%' THEN metric_value END) as total_records,
        MAX(metric_timestamp) as last_run
    FROM retail_data.pipeline_metrics
    WHERE metric_value IS NOT NULL
    GROUP BY task_name
""").toPandas()

if not metrics_summary.empty:
    # Create visualizations
    fig, axes = plt.subplots(1, 2, figsize=(15, 5))
    
    # Task duration chart
    if metrics_summary['avg_duration'].notna().any():
        axes[0].barh(metrics_summary['task_name'], metrics_summary['avg_duration'])
        axes[0].set_xlabel('Average Duration (seconds)')
        axes[0].set_title('Task Execution Times')
    
    # Records processed chart
    if metrics_summary['total_records'].notna().any():
        axes[1].bar(metrics_summary['task_name'], metrics_summary['total_records'])
        axes[1].set_ylabel('Records Processed')
        axes[1].set_title('Data Volume by Task')
        axes[1].tick_params(axis='x', rotation=45)
    
    plt.tight_layout()
    plt.show()
    
    print("\nMetrics Summary:")
    print(metrics_summary)
else:
    print("No metrics available for visualization. Run the pipeline first!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 3.3: Check Data Quality Metrics

# COMMAND ----------

# Query data quality metrics
quality_report = spark.sql("""
    SELECT 
        table_name,
        check_name,
        check_result,
        error_percentage,
        check_timestamp
    FROM retail_data.data_quality_metrics
    WHERE processing_date = current_date()
    ORDER BY check_timestamp DESC
""")

if quality_report.count() > 0:
    print("📋 Data Quality Report")
    print("=" * 50)
    quality_report.show(truncate=False)
    
    # Summary statistics
    failed_checks = quality_report.filter("check_result = 'FAIL'").count()
    total_checks = quality_report.count()
    
    print(f"\nQuality Summary:")
    print(f"  Total Checks: {total_checks}")
    print(f"  Failed Checks: {failed_checks}")
    print(f"  Success Rate: {(total_checks - failed_checks) / total_checks * 100:.1f}%")
else:
    print("No quality checks found. Run the pipeline first!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab 4: Alert Configuration (20 minutes)
# MAGIC
# MAGIC ### Exercise 4.1: Create Alert Rules

# COMMAND ----------

# Define alert rules
alert_rules = [
    {
        "name": "High Data Loss Alert",
        "condition": "bronze_to_silver_loss > 0.05",
        "severity": "HIGH",
        "action": "email + slack",
        "description": "More than 5% data loss between Bronze and Silver layers"
    },
    {
        "name": "Quality Score Alert",
        "condition": "overall_quality_score < 0.95",
        "severity": "MEDIUM",
        "action": "email",
        "description": "Data quality score below 95% threshold"
    },
    {
        "name": "Pipeline Duration Alert",
        "condition": "total_duration > 3600",
        "severity": "LOW",
        "action": "dashboard",
        "description": "Pipeline taking longer than 1 hour to complete"
    },
    {
        "name": "Missing Data Alert",
        "condition": "source_record_count = 0",
        "severity": "CRITICAL",
        "action": "email + slack + pagerduty",
        "description": "No source data available for processing"
    }
]

print("📢 Alert Configuration")
print("=" * 50)

for rule in alert_rules:
    print(f"\n{rule['name']} [{rule['severity']}]")
    print(f"  Condition: {rule['condition']}")
    print(f"  Action: {rule['action']}")
    print(f"  Description: {rule['description']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 4.2: Simulate Alert Triggering

# COMMAND ----------

# Simulate alert evaluation
def evaluate_alerts(metrics):
    """Evaluate alert rules against current metrics"""
    triggered_alerts = []
    
    # Simulate metrics
    current_metrics = {
        "bronze_to_silver_loss": 0.03,
        "overall_quality_score": 0.97,
        "total_duration": 2800,
        "source_record_count": 5000
    }
    
    # Check each rule
    if current_metrics["bronze_to_silver_loss"] > 0.05:
        triggered_alerts.append(("High Data Loss Alert", "HIGH"))
    
    if current_metrics["overall_quality_score"] < 0.95:
        triggered_alerts.append(("Quality Score Alert", "MEDIUM"))
    
    if current_metrics["total_duration"] > 3600:
        triggered_alerts.append(("Pipeline Duration Alert", "LOW"))
    
    if current_metrics["source_record_count"] == 0:
        triggered_alerts.append(("Missing Data Alert", "CRITICAL"))
    
    return triggered_alerts, current_metrics

# Evaluate alerts
triggered, metrics = evaluate_alerts({})

print("🔔 Alert Evaluation Results")
print("=" * 50)
print("\nCurrent Metrics:")
for metric, value in metrics.items():
    print(f"  {metric}: {value}")

print(f"\nAlerts Triggered: {len(triggered)}")
if triggered:
    for alert_name, severity in triggered:
        print(f"  ⚠️ {alert_name} [{severity}]")
else:
    print("  ✅ No alerts triggered - all metrics within thresholds")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab 5: Production Operations (25 minutes)
# MAGIC
# MAGIC ### Exercise 5.1: Implement Error Recovery

# COMMAND ----------

# Simulate error recovery scenarios
class ErrorRecoveryHandler:
    def __init__(self, task_name, max_retries=3):
        self.task_name = task_name
        self.max_retries = max_retries
        self.retry_count = 0
        
    def execute_with_retry(self, func, *args, **kwargs):
        """Execute function with exponential backoff retry"""
        import time
        
        while self.retry_count < self.max_retries:
            try:
                print(f"Attempting {self.task_name} (attempt {self.retry_count + 1}/{self.max_retries})")
                result = func(*args, **kwargs)
                print(f"✅ {self.task_name} succeeded")
                return result
            except Exception as e:
                self.retry_count += 1
                if self.retry_count >= self.max_retries:
                    print(f"❌ {self.task_name} failed after {self.max_retries} attempts")
                    raise e
                
                wait_time = 2 ** self.retry_count  # Exponential backoff
                print(f"⚠️ {self.task_name} failed: {str(e)}")
                print(f"   Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
        
        return None

# Test error recovery
def sample_task():
    """Simulated task that may fail"""
    import random
    if random.random() < 0.7:  # 70% chance of failure
        raise Exception("Simulated transient error")
    return "Success!"

# Execute with recovery
recovery_handler = ErrorRecoveryHandler("sample_task", max_retries=3)
try:
    result = recovery_handler.execute_with_retry(sample_task)
    print(f"\nFinal Result: {result}")
except Exception as e:
    print(f"\nFinal Status: Task failed permanently - {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 5.2: Performance Optimization Check

# COMMAND ----------

# Check for optimization opportunities
optimization_checks = []

# Check 1: Table statistics
print("🔧 Performance Optimization Analysis")
print("=" * 50)

# Check Delta table optimization
tables_to_check = ['bronze_sales', 'silver_sales', 'gold_daily_sales_summary']

for table in tables_to_check:
    try:
        # Get table details
        details = spark.sql(f"DESCRIBE DETAIL globalmart_prod.retail_data.{table}").collect()[0]
        
        optimization_checks.append({
            "table": table,
            "size_gb": round(details['sizeInBytes'] / (1024**3), 2) if details['sizeInBytes'] else 0,
            "num_files": details['numFiles'],
            "partitioned": 'partitionColumns' in details and len(details['partitionColumns']) > 0
        })
    except:
        optimization_checks.append({
            "table": table,
            "size_gb": 0,
            "num_files": 0,
            "partitioned": False
        })

# Display optimization recommendations
print("\nTable Optimization Status:")
for check in optimization_checks:
    print(f"\n{check['table']}:")
    print(f"  Size: {check['size_gb']} GB")
    print(f"  Files: {check['num_files']}")
    print(f"  Partitioned: {'Yes' if check['partitioned'] else 'No'}")
    
    # Recommendations
    if check['num_files'] > 1000:
        print(f"  💡 Recommendation: Consider running OPTIMIZE to reduce file count")
    if not check['partitioned'] and check['size_gb'] > 10:
        print(f"  💡 Recommendation: Consider partitioning for better performance")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📝 Summary and Best Practices
# MAGIC
# MAGIC ### Key Takeaways:
# MAGIC
# MAGIC 1. **Production Pipeline Design**
# MAGIC    - Use catalog-based tables instead of DBFS paths
# MAGIC    - Implement proper dependency management between tasks
# MAGIC    - Configure appropriate timeouts and retry logic
# MAGIC
# MAGIC 2. **Data Quality**
# MAGIC    - Validate data at each layer transition
# MAGIC    - Track quality metrics throughout the pipeline
# MAGIC    - Set up alerts for quality degradation
# MAGIC
# MAGIC 3. **Monitoring & Observability**
# MAGIC    - Log metrics at task and pipeline levels
# MAGIC    - Create dashboards for operational visibility
# MAGIC    - Implement proactive alerting
# MAGIC
# MAGIC 4. **Error Handling**
# MAGIC    - Design for idempotency - reruns should be safe
# MAGIC    - Implement exponential backoff for retries
# MAGIC    - Log detailed error information for debugging
# MAGIC
# MAGIC 5. **Performance Optimization**
# MAGIC    - Use Delta table optimization features
# MAGIC    - Partition large tables appropriately
# MAGIC    - Monitor and optimize cluster configurations
# MAGIC
# MAGIC ### Production Checklist:
# MAGIC - ✅ All notebooks tested individually
# MAGIC - ✅ Job created with proper dependencies
# MAGIC - ✅ Monitoring metrics configured
# MAGIC - ✅ Alerts set up for critical failures
# MAGIC - ✅ Documentation complete
# MAGIC - ✅ Error recovery tested
# MAGIC - ✅ Performance baseline established

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎯 Challenge Exercise
# MAGIC
# MAGIC Extend the pipeline with the following enhancements:
# MAGIC
# MAGIC 1. **Add a Data Archival Task**
# MAGIC    - Archive processed raw data after successful pipeline completion
# MAGIC    - Implement retention policies (e.g., keep 30 days of raw data)
# MAGIC
# MAGIC 2. **Implement SLA Monitoring**
# MAGIC    - Track if pipeline completes within SLA (e.g., before 6 AM)
# MAGIC    - Send alerts if SLA is at risk
# MAGIC
# MAGIC 3. **Create a Data Lineage Tracker**
# MAGIC    - Track which source files contributed to each Gold table record
# MAGIC    - Enable debugging of data issues
# MAGIC
# MAGIC 4. **Build a Cost Optimization Report**
# MAGIC    - Track cluster usage and costs
# MAGIC    - Identify optimization opportunities
# MAGIC
# MAGIC Share your solutions with the class for discussion!

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📚 Additional Resources
# MAGIC
# MAGIC ### Documentation Links:
# MAGIC - [Databricks Jobs Documentation](https://docs.databricks.com/workflows/jobs/jobs.html)
# MAGIC - [Unity Catalog Best Practices](https://docs.databricks.com/data-governance/unity-catalog/best-practices.html)
# MAGIC - [Delta Lake Optimization](https://docs.databricks.com/delta/optimize.html)
# MAGIC - [Monitoring and Alerts](https://docs.databricks.com/workflows/jobs/jobs-alerts.html)
# MAGIC
# MAGIC ### Advanced Topics:
# MAGIC - [CI/CD for Databricks](https://docs.databricks.com/dev-tools/ci-cd/index.html)
# MAGIC - [MLflow for Pipeline Tracking](https://mlflow.org/docs/latest/tracking.html)
# MAGIC - [Advanced Job Orchestration Patterns](https://docs.databricks.com/workflows/jobs/advanced.html)
# MAGIC
# MAGIC ### Community Resources:
# MAGIC - [Databricks Community Forum](https://community.databricks.com/)
# MAGIC - [Delta Lake Slack Channel](https://delta-users.slack.com/)
# MAGIC - [Apache Spark Mailing Lists](https://spark.apache.org/community.html)

# COMMAND ----------

print("🎉 Congratulations on completing Module 4!")
print("You've successfully built a production-grade data pipeline with:")
print("  ✓ Robust orchestration")
print("  ✓ Comprehensive monitoring") 
print("  ✓ Data quality validation")
print("  ✓ Error recovery mechanisms")
print("\nYou're now ready to deploy and operate production data pipelines!")
