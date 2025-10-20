from pyspark.sql import functions as F

# ---- Read your source ----
df = (
    spark.read.option("header", True)
        .option("inferSchema", True)            # Prefer explicit schema in prod
        .csv("abfss://landing/mydata/*.csv")    # or .parquet(...)/.format("delta")
)

# ---- Normalize datetimes ----
# Example fields from the migration notes:
#  - provEntAdrStrtDt: "YYYY-MM-DD"  (date-only)
#  - odsUpdtDttm: "YYYY-MM-DDTHH:mm:ss.SSSSSS" (microseconds)
#  - If local time, convert to UTC.

df_clean = (
    df
    # Date-only -> timestamp (midnight) in UTC
    .withColumn(
        "provEntAdrStrtDt",
        F.to_utc_timestamp(F.to_timestamp("provEntAdrStrtDt", "yyyy-MM-dd"), "UTC")
    )
    # Microseconds -> trim to milliseconds, then parse and convert to UTC
    .withColumn(
        "odsUpdtDttm",
        F.to_utc_timestamp(
            F.to_timestamp(F.expr("substring(odsUpdtDttm, 1, 23)"), "yyyy-MM-dd'T'HH:mm:ss.SSS"),
            "America/Chicago"     # set to your source timezone; use "UTC" if already UTC
        )
    )
    # Example: ensure numeric types
    .withColumn("amount", F.col("amount").cast("decimal(18,2)"))
)

# ---- Ensure stable _id / upsert key ----
# Use your HBase row key or another natural key to make loads idempotent.
df_final = df_clean.withColumn("_id", F.col("rowKey"))

# ---- Repartition by shard key (recommended when writing to a sharded collection) ----
df_final = df_final.repartition(96, "_id")  # pick a partition count that matches your cluster

# ---- Write to Atlas (append/upsert) ----
(
    df_final.write
    .format("mongodb")                                    # MongoDB Spark Connector v10+
    .mode("append")
    .option("replaceDocument", "false")                   # partial updates (faster)
    .option("operationType", "update")                    # upsert behavior
    .option("upsert", "true")
    .option("spark.mongodb.write.connection.uri", "mongodb+srv://<user>:***@<cluster>/?retryWrites=true&w=majority")
    .option("spark.mongodb.write.database", "gaps_care")
    .option("spark.mongodb.write.collection", "members")
    .option("spark.mongodb.write.maxBatchSize", "1000")   # tune by doc size / latency
    .save()
)
