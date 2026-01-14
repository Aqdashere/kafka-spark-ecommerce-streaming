from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_csv, to_json, struct

spark = SparkSession.builder \
    .appName("OrderValueAlerts") \
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
    .option("failOnDataLoss", "false") \
    .load()

orders_parsed = orders.select(
    from_csv(col("value").cast("string"), order_schema_ddl).alias("data")
).select("data.*")

alerts = orders_parsed \
    .withColumn("order_value", col("price") * col("quantity")) \
    .filter(col("order_value") >= 5000)

alerts_json = alerts.select(
    to_json(
        struct(
            col("order_id"),
            col("category"),
            col("order_value"),
            col("timestamp")
        )
    ).alias("value")
)

query = alerts_json.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("topic", "alerts") \
    .option("checkpointLocation", "/app/output/checkpoints/alerts") \
    .outputMode("append") \
    .start()

query.awaitTermination()
