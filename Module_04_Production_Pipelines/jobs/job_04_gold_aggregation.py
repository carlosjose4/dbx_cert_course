# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Gold Layer Aggregation
# MAGIC
# MAGIC This notebook creates business-ready aggregations and analytics tables
# MAGIC optimized for reporting and dashboard consumption.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

# Parameters from job orchestrator
dbutils.widgets.text("processing_date", "")
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("aggregation_window_days", "30")

processing_date = dbutils.widgets.get("processing_date")
run_id = dbutils.widgets.get("run_id")
aggregation_window_days = int(dbutils.widgets.get("aggregation_window_days"))

if not processing_date:
    from datetime import datetime
    processing_date = datetime.now().strftime("%Y-%m-%d")

if not run_id:
    import uuid
    run_id = str(uuid.uuid4())

print(f"Processing Date: {processing_date}")
print(f"Run ID: {run_id}")
print(f"Aggregation Window: {aggregation_window_days} days")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Imports

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window
from datetime import datetime, timedelta
import json

# Set catalog and schema
spark.sql("USE CATALOG sm_training")
spark.sql("USE SCHEMA retail_data")

aggregation_start = datetime.now()
aggregation_metrics = {}

# Calculate date range for aggregations
end_date = datetime.strptime(processing_date, "%Y-%m-%d")
start_date = end_date - timedelta(days=aggregation_window_days)

print(f"Aggregation period: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Silver Data

# COMMAND ----------

print("📊 Loading Silver layer data for aggregation...")

# Load Silver tables with date filtering
silver_sales_df = spark.table("retail_data.silver_sales") \
    .filter(col("processing_date") >= start_date.strftime("%Y-%m-%d")) \
    .filter(col("processing_date") <= processing_date)

silver_inventory_df = spark.table("retail_data.silver_inventory") \
    .filter(col("processing_date") == processing_date)

silver_customers_df = spark.table("retail_data.silver_customers") \
    .filter(col("processing_date") == processing_date)

print(f"✅ Loaded Silver data:")
print(f"   Sales: {silver_sales_df.count():,} records ({aggregation_window_days} days)")
print(f"   Inventory: {silver_inventory_df.count():,} current records")
print(f"   Customers: {silver_customers_df.count():,} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Daily Sales Summary

# COMMAND ----------

print("\n📈 Creating daily sales summary...")

daily_sales_summary = silver_sales_df \
    .groupBy("processing_date", "store_id") \
    .agg(
        count("transaction_id").alias("total_transactions"),
        sum("total_amount").alias("total_revenue"),
        avg("total_amount").alias("avg_transaction_value"),
        countDistinct("customer_id").alias("unique_customers"),
        countDistinct("product_id").alias("unique_products_sold"),
        sum("quantity").alias("total_units_sold"),
        sum("discount_amount").alias("total_discounts"),
        avg("discount_percentage").alias("avg_discount_percentage"),
        sum(when(col("is_high_value_transaction"), 1).otherwise(0)).alias("high_value_transactions"),
        sum(when(col("is_weekend"), col("total_amount")).otherwise(0)).alias("weekend_revenue"),
        sum(when(~col("is_weekend"), col("total_amount")).otherwise(0)).alias("weekday_revenue")
    ) \
    .withColumn("revenue_per_customer", 
                col("total_revenue") / col("unique_customers")) \
    .withColumn("items_per_transaction", 
                col("total_units_sold") / col("total_transactions")) \
    .withColumn("weekend_revenue_ratio", 
                col("weekend_revenue") / (col("weekend_revenue") + col("weekday_revenue"))) \
    .withColumn("summary_date", col("processing_date")) \
    .withColumn("processing_timestamp", current_timestamp()) \
    .select(
        "summary_date", "store_id", "total_transactions", "total_revenue",
        "avg_transaction_value", "unique_customers", "unique_products_sold",
        "total_units_sold", "total_discounts", "avg_discount_percentage",
        "high_value_transactions", "weekend_revenue", "weekday_revenue",
        "revenue_per_customer", "items_per_transaction", "weekend_revenue_ratio",
        "processing_timestamp", lit(processing_date).cast("date").alias("processing_date")
    )

# Find top products per store
top_products = silver_sales_df \
    .groupBy("processing_date", "store_id", "product_id") \
    .agg(sum("total_amount").alias("product_revenue")) \
    .withColumn("rank", row_number().over(
        Window.partitionBy("processing_date", "store_id")
        .orderBy(col("product_revenue").desc())
    )) \
    .filter(col("rank") == 1) \
    .select(
        col("processing_date").alias("summary_date"),
        "store_id",
        col("product_id").alias("top_product_id"),
        col("product_revenue").alias("top_product_revenue")
    )

# Join with top products
daily_sales_final = daily_sales_summary \
    .join(top_products, ["summary_date", "store_id"], "left")

aggregation_metrics["daily_sales_summary"] = daily_sales_final.count()
print(f"✅ Created {daily_sales_final.count():,} daily summary records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Customer Metrics

# COMMAND ----------

print("\n👥 Creating customer analytics...")

# Calculate customer lifetime metrics
customer_lifetime_metrics = silver_sales_df \
    .groupBy("customer_id") \
    .agg(
        count("transaction_id").alias("total_purchases"),
        sum("total_amount").alias("total_spent"),
        avg("total_amount").alias("avg_purchase_value"),
        min("transaction_timestamp").alias("first_purchase_date"),
        max("transaction_timestamp").alias("last_purchase_date"),
        countDistinct("store_id").alias("stores_visited"),
        countDistinct("product_id").alias("unique_products"),
        sum("quantity").alias("total_items_purchased"),
        avg("discount_percentage").alias("avg_discount_received"),
        stddev("total_amount").alias("purchase_value_stddev")
    ) \
    .withColumn("days_as_customer", 
                datediff(col("last_purchase_date"), col("first_purchase_date"))) \
    .withColumn("days_since_last_purchase", 
                datediff(current_date(), col("last_purchase_date"))) \
    .withColumn("purchase_frequency", 
                col("total_purchases") / greatest(col("days_as_customer"), lit(1))) \
    .withColumn("purchase_consistency", 
                when(col("purchase_value_stddev").isNull(), 1.0)
                .otherwise(1 - (col("purchase_value_stddev") / col("avg_purchase_value"))))

# Calculate customer segments and scores
customer_metrics = customer_lifetime_metrics \
    .withColumn("customer_value_score",
                when(col("total_spent") > 5000, 1.0)
                .when(col("total_spent") > 2000, 0.8)
                .when(col("total_spent") > 1000, 0.6)
                .when(col("total_spent") > 500, 0.4)
                .otherwise(0.2)) \
    .withColumn("engagement_score",
                when(col("days_since_last_purchase") < 30, 1.0)
                .when(col("days_since_last_purchase") < 60, 0.7)
                .when(col("days_since_last_purchase") < 90, 0.4)
                .otherwise(0.1)) \
    .withColumn("loyalty_score",
                (col("purchase_frequency") * 0.3 + 
                 col("purchase_consistency") * 0.3 +
                 col("stores_visited") / 20 * 0.2 +
                 col("unique_products") / 100 * 0.2)) \
    .withColumn("churn_risk_score",
                when(col("days_since_last_purchase") > 90, 0.9)
                .when(col("days_since_last_purchase") > 60, 0.7)
                .when(col("days_since_last_purchase") > 45, 0.5)
                .when(col("days_since_last_purchase") > 30, 0.3)
                .otherwise(0.1)) \
    .withColumn("customer_segment",
                when((col("customer_value_score") > 0.6) & (col("engagement_score") > 0.6), "Champions")
                .when((col("customer_value_score") > 0.6) & (col("engagement_score") <= 0.6), "At Risk Champions")
                .when((col("customer_value_score") <= 0.6) & (col("engagement_score") > 0.6), "Potential Loyalists")
                .when((col("customer_value_score") <= 0.4) & (col("engagement_score") <= 0.4), "Lost")
                .otherwise("Regular")) \
    .withColumn("processing_timestamp", current_timestamp()) \
    .select(
        "customer_id", "total_purchases", "total_spent", "avg_purchase_value",
        "first_purchase_date", "last_purchase_date", "days_as_customer",
        "days_since_last_purchase", "stores_visited", "unique_products",
        "total_items_purchased", "avg_discount_received", "purchase_frequency",
        "purchase_consistency", "customer_value_score", "engagement_score",
        "loyalty_score", "churn_risk_score", "customer_segment",
        "processing_timestamp", lit(processing_date).cast("date").alias("processing_date")
    )

aggregation_metrics["customer_metrics"] = customer_metrics.count()
print(f"✅ Created metrics for {customer_metrics.count():,} customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Product Performance Analytics

# COMMAND ----------

print("\n📦 Creating product performance analytics...")

product_performance = silver_sales_df \
    .groupBy("product_id") \
    .agg(
        count("transaction_id").alias("times_sold"),
        sum("quantity").alias("units_sold"),
        sum("total_amount").alias("total_revenue"),
        avg("unit_price").alias("avg_selling_price"),
        avg("discount_percentage").alias("avg_discount"),
        countDistinct("customer_id").alias("unique_buyers"),
        countDistinct("store_id").alias("stores_selling"),
        stddev("unit_price").alias("price_variance"),
        min("transaction_timestamp").alias("first_sale_date"),
        max("transaction_timestamp").alias("last_sale_date")
    ) \
    .withColumn("revenue_per_unit", col("total_revenue") / col("units_sold")) \
    .withColumn("popularity_score", 
                (col("times_sold") / 1000) * 0.3 + 
                (col("unique_buyers") / 100) * 0.3 +
                (col("stores_selling") / 20) * 0.4) \
    .withColumn("days_on_sale", 
                datediff(col("last_sale_date"), col("first_sale_date"))) \
    .withColumn("daily_velocity", 
                col("units_sold") / greatest(col("days_on_sale"), lit(1)))

# Join with current inventory levels
product_analytics = product_performance \
    .join(
        silver_inventory_df.groupBy("product_id")
        .agg(
            sum("current_stock").alias("total_current_stock"),
            avg("stock_coverage_days").alias("avg_stock_coverage"),
            sum(when(col("needs_reorder"), 1).otherwise(0)).alias("stores_need_reorder")
        ),
        "product_id",
        "left"
    ) \
    .withColumn("inventory_turnover", 
                col("units_sold") / greatest(col("total_current_stock"), lit(1))) \
    .withColumn("product_category",
                when(col("popularity_score") > 0.7, "Best Seller")
                .when(col("popularity_score") > 0.4, "Popular")
                .when(col("popularity_score") > 0.2, "Regular")
                .otherwise("Slow Moving")) \
    .withColumn("processing_timestamp", current_timestamp()) \
    .select(
        "product_id", "times_sold", "units_sold", "total_revenue",
        "avg_selling_price", "avg_discount", "unique_buyers", "stores_selling",
        "revenue_per_unit", "popularity_score", "daily_velocity",
        "total_current_stock", "avg_stock_coverage", "stores_need_reorder",
        "inventory_turnover", "product_category",
        "processing_timestamp", lit(processing_date).cast("date").alias("processing_date")
    )

aggregation_metrics["product_analytics"] = product_analytics.count()
print(f"✅ Created analytics for {product_analytics.count():,} products")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Store Performance Metrics

# COMMAND ----------

print("\n🏪 Creating store performance metrics...")

store_performance = silver_sales_df \
    .groupBy("store_id") \
    .agg(
        count("transaction_id").alias("total_transactions"),
        sum("total_amount").alias("total_revenue"),
        avg("total_amount").alias("avg_transaction_value"),
        countDistinct("customer_id").alias("unique_customers"),
        countDistinct("product_id").alias("unique_products"),
        sum("quantity").alias("total_units_sold"),
        avg("data_quality_score").alias("avg_data_quality"),
        
        # Time-based metrics
        count(when(col("transaction_hour").between(6, 12), 1)).alias("morning_transactions"),
        count(when(col("transaction_hour").between(12, 18), 1)).alias("afternoon_transactions"),
        count(when(col("transaction_hour").between(18, 24), 1)).alias("evening_transactions"),
        count(when(col("is_weekend"), 1)).alias("weekend_transactions"),
        
        # Payment method distribution
        count(when(col("payment_method") == "CREDIT", 1)).alias("credit_transactions"),
        count(when(col("payment_method") == "DEBIT", 1)).alias("debit_transactions"),
        count(when(col("payment_method") == "CASH", 1)).alias("cash_transactions"),
        count(when(col("payment_method") == "MOBILE", 1)).alias("mobile_transactions")
    ) \
    .withColumn("transactions_per_customer", 
                col("total_transactions") / col("unique_customers")) \
    .withColumn("revenue_per_customer", 
                col("total_revenue") / col("unique_customers")) \
    .withColumn("weekend_ratio", 
                col("weekend_transactions") / col("total_transactions")) \
    .withColumn("peak_period",
                when(col("morning_transactions") > col("afternoon_transactions"), "Morning")
                .when(col("afternoon_transactions") > col("evening_transactions"), "Afternoon")
                .otherwise("Evening")) \
    .withColumn("primary_payment_method",
                when(col("credit_transactions") > greatest(col("debit_transactions"), 
                                                          col("cash_transactions"), 
                                                          col("mobile_transactions")), "CREDIT")
                .when(col("debit_transactions") > greatest(col("cash_transactions"), 
                                                           col("mobile_transactions")), "DEBIT")
                .when(col("cash_transactions") > col("mobile_transactions"), "CASH")
                .otherwise("MOBILE")) \
    .withColumn("performance_score",
                (col("total_revenue") / 100000) * 0.4 +
                (col("unique_customers") / 1000) * 0.3 +
                (col("unique_products") / 100) * 0.3) \
    .withColumn("store_category",
                when(col("performance_score") > 0.7, "High Performance")
                .when(col("performance_score") > 0.4, "Medium Performance")
                .otherwise("Low Performance")) \
    .withColumn("processing_timestamp", current_timestamp()) \
    .select(
        "store_id", "total_transactions", "total_revenue", "avg_transaction_value",
        "unique_customers", "unique_products", "total_units_sold",
        "transactions_per_customer", "revenue_per_customer", "weekend_ratio",
        "peak_period", "primary_payment_method", "performance_score",
        "store_category", "avg_data_quality",
        "processing_timestamp", lit(processing_date).cast("date").alias("processing_date")
    )

aggregation_metrics["store_performance"] = store_performance.count()
print(f"✅ Created performance metrics for {store_performance.count():,} stores")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Gold Tables

# COMMAND ----------

daily_sales_final.printSchema()

# COMMAND ----------

print("\n💾 Writing Gold layer tables...")

try:
    # Write daily sales summary
    if daily_sales_final.count()>0:
        daily_sales_final.write \
            .mode("append") \
            .partitionBy("processing_date") \
            .saveAsTable("retail_data.gold_daily_sales_summary")
    
        print(f"✅ Daily sales summary: {daily_sales_final.count():,} records written")
    
    if customer_metrics.count()>0:
        # Write customer metrics  
        customer_metrics.write \
            .mode("overwrite") \
            .partitionBy("processing_date") \
            .saveAsTable("retail_data.gold_customer_metrics")
    
        print(f"✅ Customer metrics: {customer_metrics.count():,} records written")
    
    if product_analytics.count()>0:
        # Write product analytics
        product_analytics.write \
            .mode("overwrite") \
            .saveAsTable("retail_data.gold_product_analytics")
        
        print(f"✅ Product analytics: {product_analytics.count():,} records written")
    
    # Write store performance
    if product_analytics.count()>0:
        store_performance.write \
            .mode("overwrite") \
            .saveAsTable("retail_data.gold_store_performance")
        
        print(f"✅ Store performance: {store_performance.count():,} records written")
    
except Exception as e:
    print(f"❌ Failed to write Gold tables: {str(e)}")
    raise e

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Executive Dashboard Metrics

# COMMAND ----------

print("\n📊 Creating executive dashboard metrics...")

# Overall business metrics
exec_metrics = spark.sql(f"""
    SELECT 
        '{processing_date}' as report_date,
        SUM(total_revenue) as total_revenue,
        SUM(total_transactions) as total_transactions,
        SUM(unique_customers) as total_customers,
        AVG(avg_transaction_value) as avg_transaction_value,
        COUNT(DISTINCT store_id) as active_stores,
        MAX(total_revenue) as top_store_revenue,
        MIN(total_revenue) as bottom_store_revenue
    FROM retail_data.gold_daily_sales_summary
    WHERE processing_date = '{processing_date}'
""").collect()[0]

# Customer segment distribution
segment_dist = spark.sql(f"""
    SELECT 
        customer_segment,
        COUNT(*) as customer_count,
        AVG(total_spent) as avg_lifetime_value,
        AVG(churn_risk_score) as avg_churn_risk
    FROM retail_data.gold_customer_metrics
    WHERE processing_date = '{processing_date}'
    GROUP BY customer_segment
""").collect()

# Product category performance
product_cat_perf = spark.sql(f"""
    SELECT 
        product_category,
        COUNT(*) as product_count,
        SUM(total_revenue) as category_revenue,
        AVG(inventory_turnover) as avg_turnover
    FROM retail_data.gold_product_analytics
    WHERE processing_date = '{processing_date}'
    GROUP BY product_category
""").collect()

print("📈 Executive Metrics Summary:")
print(f"   Total Revenue: ${exec_metrics['total_revenue']:,.2f}")
print(f"   Total Transactions: {exec_metrics['total_transactions']:,}")
print(f"   Active Customers: {exec_metrics['total_customers']:,}")
print(f"   Average Transaction: ${exec_metrics['avg_transaction_value']:.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log Aggregation Metrics

# COMMAND ----------

# Calculate overall metrics
aggregation_duration = (datetime.now() - aggregation_start).total_seconds()
total_records = 0
for value in aggregation_metrics.values():
    total_records += value

# Log metrics to monitoring table
monitoring_data = []

processing_date_fix = datetime.strptime(processing_date, '%Y-%m-%d').date()

for table, count in aggregation_metrics.items():
    monitoring_data.append({
        "metric_timestamp": datetime.now(),
        "pipeline_name": "globalmart_daily_etl",
        "task_name": "gold_aggregation",
        "metric_name": f"{table}_records",
        "metric_value": float(count),
        "metric_unit": "count",
        "processing_date": processing_date_fix,
        "run_id": run_id
    })

# Add executive metrics
monitoring_data.extend([
    {
        "metric_timestamp": datetime.now(),
        "pipeline_name": "globalmart_daily_etl",
        "task_name": "gold_aggregation",
        "metric_name": "total_revenue",
        "metric_value": float(exec_metrics["total_revenue"]) if exec_metrics["total_revenue"] else 0.0,
        "metric_unit": "currency",
        "processing_date": processing_date_fix,
        "run_id": run_id
    },
    {
        "metric_timestamp": datetime.now(),
        "pipeline_name": "globalmart_daily_etl",
        "task_name": "gold_aggregation",
        "metric_name": "aggregation_duration",
        "metric_value": aggregation_duration,
        "metric_unit": "seconds",
        "processing_date": processing_date_fix,
        "run_id": run_id
    }
])

# Write monitoring metrics
monitoring_df = spark.createDataFrame(monitoring_data)
monitoring_df.write.mode("append").saveAsTable("retail_data.pipeline_metrics")

print(f"\n{'='*50}")
print(f"GOLD AGGREGATION COMPLETED")
print(f"{'='*50}")
print(f"Tables Created: {len(aggregation_metrics)}")
print(f"Total Records: {total_records:,}")
print(f"Duration: {aggregation_duration:.2f} seconds")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Return Results

# COMMAND ----------

# Prepare results for job orchestrator
results = {
    "status": "SUCCESS",
    "message": "Gold aggregation completed successfully",
    "metrics": {
        "tables_created": len(aggregation_metrics),
        "total_records": total_records,
        "duration_seconds": aggregation_duration,
        "business_metrics": {
            "total_revenue": float(exec_metrics["total_revenue"]) if exec_metrics["total_revenue"] else 0.0,
            "total_transactions": exec_metrics["total_transactions"],
            "total_customers": exec_metrics["total_customers"]
        }
    },
    "segment_distribution": [
        {"segment": row["customer_segment"], "count": row["customer_count"]} 
        for row in segment_dist
    ],
    "details": aggregation_metrics
}

dbutils.notebook.exit(json.dumps(results))
