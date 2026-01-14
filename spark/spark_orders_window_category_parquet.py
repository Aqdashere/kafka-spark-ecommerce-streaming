from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_csv, window, sum as _sum

spark = SparkSession.builder \
    .appName("OrdersWindowCategoryParquet") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

order_schema_ddl = """
    order_id STRING,
    user_id STRING,
    product_id STRING,
    category STRING,
    price DOUBLE,
    quantity INT,
    timestamp TIMESTAMP
"""

orders = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "orders") \
    .option("startingOffsets", "latest") \
    .load()

orders_parsed = orders.select(
    from_csv(col("value").cast("string"), order_schema_ddl).alias("data")
).select("data.*")

orders_with_value = orders_parsed.withColumn(
    "order_value", col("price") * col("quantity")
)

revenue_by_category = orders_with_value \
    .withWatermark("timestamp", "1 minute") \
    .groupBy(
        window(col("timestamp"), "1 minute"),
        col("category")
    ) \
    .agg(
        _sum("order_value").alias("total_revenue")
    )

query = revenue_by_category.writeStream \
    .format("parquet") \
    .option("path", "/app/output/revenue_by_category") \
    .option("checkpointLocation", "/app/output/checkpoints/revenue_by_category") \
    .outputMode("append") \
    .start()

query.awaitTermination()
