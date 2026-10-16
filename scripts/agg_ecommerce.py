from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, broadcast

# Spark Session Setup
spark = SparkSession.builder \
.appName("Ecommerce_Data_Agg_Pipeline") \
.config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
.config("spark.sql.adaptive.enabled", "false") \
.config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
.config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
.config("spark.network.timeout", "300s") \
.config("spark.speculation", "false") \
.enableHiveSupport() \
.getOrCreate()

# Read Tables from HDFS
path = "hdfs://namenode:9000/user/hive/warehouse/ecommerce_db.db"
table_names =["fact_sales","dim_customer","dim_product","dim_date"]

dfs = {}
for table_name in table_names:
    dfs[table_name] = spark.read.parquet(f"{path}/{table_name}")

fact_sales = dfs["fact_sales"].sample(withReplacement=False, fraction=0.1, seed=42)
fact_sales.count()

dim_customer = dfs["dim_customer"]
dim_product = dfs["dim_product"]
dim_date = dfs["dim_date"]

base = fact_sales.join(
    broadcast(dim_customer.select("customerid", "country")), 
    on="customerid", 
    how="inner"
).cache()

# MART 1: COUNTRY PERFORMANCE SUMMARY
mart_country_summary = (
    dim_customer
    .join(fact_sales, on="customerid", how="left")
    .groupBy("country")
    .agg(
        F.countDistinct("customerid").alias("total_customers"),
        F.countDistinct("invoiceno").alias("total_orders"),
        F.round(F.coalesce(F.sum("TotalAmount"), F.lit(0)), 2).alias("total_revenue")
    )
)

# MART 2: TOP CUSTOMERS
window_country = Window.partitionBy("country").orderBy(F.col("total_spent").desc())
mart_top_customers_per_country = (
    base
    .groupBy("customerid", "country")
    .agg(
        F.round(F.sum("TotalAmount"), 2).alias("total_spent"),
        F.countDistinct("invoiceno").alias("total_orders"),
    )
    .withColumn("rank_in_country", rank().over(window_country))
    .filter(F.col("rank_in_country") <= 5)
)

# MART 3: TOP PRODUCTS PER COUNTRY
window_product = Window.partitionBy("country").orderBy(F.col("total_quantity").desc())
mart_top_products_per_country = (
    base
    .join(broadcast(dim_product), on="StockCode", how="inner")
    .groupBy("country", "StockCode", "description")
    .agg(
        F.sum("quantity").alias("total_quantity")
    )
    .withColumn("product_rank", rank().over(window_product))
    .filter(F.col("product_rank") == 1)
)


# MART 4: TIME ANALYTICS PER COUNTRY
mart_sales_by_time = (
    base
    .join(broadcast(dim_date), on="DateKey", how="inner")
    .groupBy("country", "Year", "MonthName")
    .agg(
        F.round(F.sum("TotalAmount"), 2).alias("total_revenue"),
        F.countDistinct("invoiceno").alias("total_orders")
    )
    .orderBy(
        F.col("Year").asc(), 
        F.col("country").asc(), 
        F.col("total_revenue").desc()
    )
)

# SAVE ALL MARTS TO HDFS & HIVE METASTORE
spark.sql("CREATE DATABASE IF NOT EXISTS ecommerce_datamart")

save_path = "hdfs://namenode:9000/user/hive/warehouse/ecommerce_datamart.db"
marts = {
    "mart_country_summary": mart_country_summary,
    "mart_top_customers_per_country": mart_top_customers_per_country,
    "mart_top_products_per_country": mart_top_products_per_country,
    "mart_sales_by_time": mart_sales_by_time
}

for mart_name, mart_df in marts.items():
    mart_df.write \
    .mode("overwrite") \
    .format("parquet") \
    .option("path", f"{save_path}/{mart_name}") \
    .saveAsTable(f"ecommerce_datamart.{mart_name}")

print("--- ALL DATA MARTS CREATED & SAVED SUCCESSFULLY ---")
spark.stop()