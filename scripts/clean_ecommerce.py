from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# SPARK SESSION
spark = ( 
	SparkSession.builder
	.appName("Ecommerce_Data_Cleaning_Pipeline")
	.config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
	.config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
	.config("spark.sql.adaptive.enabled", "false") \
	.enableHiveSupport()
	.getOrCreate()
)
# DATA LOADING (STAGING ZONE)
df = spark.read.parquet("hdfs://namenode:9000/user/root/stating_zone/raw_ecommerce")

# DATA TYPE CASTING
df = (
		df.withColumn("quantity", F.col("quantity").cast("int"))
		.withColumn("unitprice", F.col("unitprice").cast("double"))
		.withColumn("customerid", F.col("customerid").cast("int"))
		.withColumn("country", F.col("country").cast("string"))
		.withColumn("invoicedate", (F.col("invoicedate") / 1000).cast("timestamp"))
)

# DATA CLEANING & STANDARDIZATION
df = df.dropDuplicates()
df = (
		df.withColumn("description", F.trim(F.col("description")))
		.withColumn("country", F.trim(F.col("country")))
		.withColumn("invoiceno", F.trim(F.col("invoiceno")))
		.withColumn("StockCode", F.trim(F.col("StockCode")))
)

df = df.filter((F.col("quantity") > 0) & (F.col("unitprice") > 0))

df = df.filter(
	(F.col("customerid").isNotNull()) &
	(F.col("invoiceno").isNotNull()) &
	(F.col("description").isNotNull()) & 
	(F.col("quantity").isNotNull()) & 
    (F.col("unitprice").isNotNull()) &
    (F.col("description") != "")
)

df = (
		df.withColumn("description", F.upper(F.col("description")))
		.withColumn("country", F.initcap(F.col("country"))) 
)

# FEATURE ENGINEERING & DATE DIMENSION ATTRIBUTES
df = (
		df.withColumn("DateKey", F.date_format(F.col("invoicedate"), "yyyyMMdd").cast("int"))
		.withColumn("FullDate", F.to_date(F.col("invoicedate")))
		.withColumn("Year", F.year(F.col("invoicedate")))
		.withColumn("Month", F.month(F.col("invoicedate")))
		.withColumn("MonthName", F.date_format(F.col("invoicedate"), "MMMM"))
		.withColumn("Day", F.dayofmonth(F.col("invoicedate")))
		.withColumn("Quarter", F.concat(F.lit("Q"), F.quarter(F.col("invoicedate"))))
		.withColumn("IsWeekend", F.when(F.dayofweek(F.col("invoicedate")).isin([1, 7]), True).otherwise(False))
)

# DIMENSIONAL MODELING (STAR SCHEMA CREATION)
dim_customer = df.select("customerid", "country").distinct()
dim_product = df.select("StockCode", "description").dropDuplicates(["StockCode"])
dim_date = df.select("DateKey", "FullDate", "Year", "Month", "MonthName", "Day", "Quarter", "IsWeekend").distinct()

fact_sales =( 
	df.withColumn("TotalAmount", F.round(F.col("quantity") * F.col("unitprice"), 2))
	    .select("invoiceno", "customerid", "StockCode", "DateKey", "quantity", "unitprice", "TotalAmount")
)

# WRITE TO HIVE METASTORE & HDFS STORAGE
spark.sql("CREATE DATABASE IF NOT EXISTS ecommerce_db")
base_hdfs_path = "hdfs://namenode:9000/user/hive/warehouse/ecommerce_db.db"

tables = {
	"dim_customer": dim_customer,
	"dim_product": dim_product,
    "dim_date": dim_date,
    "fact_sales": fact_sales
}

for table_name, df in tables.items():
	df.write \
		.mode("overwrite") \
		.option("path", f"{base_hdfs_path}/{table_name}") \
		.format("parquet") \
		.saveAsTable(f"ecommerce_db.{table_name}")

print("--- Pipeline Execution & Hive Storage Completed Successfully ---")
spark.stop()