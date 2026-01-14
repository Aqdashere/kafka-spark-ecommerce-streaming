from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, desc

spark = SparkSession.builder \
    .appName("BatchRevenueAnalysis") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet("/app/output/revenue_by_category")

df.printSchema()

total_revenue = df.groupBy("category") \
    .agg(sum("total_revenue").alias("total_revenue")) \
    .orderBy(desc("total_revenue"))

print("\n=== TOTAL REVENUE PER CATEGORY ===")
total_revenue.show(truncate=False)

spark.stop()
