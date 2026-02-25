from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("Iceberg CDC Silver Demo").getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Assume you have some JSON CDC files already in bronze (from Kafka Connect)
df_cdc = spark.table("bronze.inventory.customers")
df_cdc.show(10, truncate=False)

# Basic cleaning & selection (adjust according to your Debezium schema)
df_changes = (
    df_cdc.select(
        F.coalesce(
            F.get_json_object("payload", "$.after.id"),
            F.get_json_object("payload", "$.before.id"),  # <- important for delete
            F.get_json_object("payload", "$.id"),
        )
        .cast("bigint")
        .alias("id"),
        F.coalesce(
            F.get_json_object("payload", "$.after.first_name"),
            F.get_json_object("payload", "$.first_name"),
        ).alias("first_name"),
        F.coalesce(
            F.get_json_object("payload", "$.after.last_name"),
            F.get_json_object("payload", "$.last_name"),
        ).alias("last_name"),
        F.coalesce(
            F.get_json_object("payload", "$.after.email"),
            F.get_json_object("payload", "$.email"),
        ).alias("email"),
        F.lower(F.coalesce(F.get_json_object("payload", "$.op"), F.lit("u"))).alias(
            "op"
        ),
        F.coalesce(
            F.get_json_object("payload", "$.ts_ms"),
            F.get_json_object("payload", "$.source.ts_ms"),
        )
        .cast("bigint")
        .alias("source_ts_ms"),
    )
    .filter(F.col("id").isNotNull())
    .filter(F.col("op").isin("c", "r", "u", "d"))  # ignore tombstone {"value":null}
)
df_changes.show(10, truncate=False)

# SCD Type 1:
# - keep only latest event per id
# - apply c/r/u as upserts
# - apply d as hard delete
latest_window = Window.partitionBy("id").orderBy(
    F.col("source_ts_ms").desc_nulls_last()
)
df_latest = df_changes.withColumn("rn", F.row_number().over(latest_window)).filter(
    F.col("rn") == 1
)
df_merge = df_latest.select("id", "first_name", "last_name", "email", "op").withColumn(
    "processed_at", F.current_timestamp()
)
df_merge.show(10, truncate=False)

# Target table
table_name = "event_streaming.customers_scd1"
target_table = f"silver.{table_name}"
target_namespace = "silver.event_streaming"

# Create table if not exists (format-version 3 to support unknown/null structs)
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {target_namespace}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {target_table} (
    id          BIGINT,
    first_name  STRING,
    last_name   STRING,
    email       STRING,
    -- add all your actual columns here...
    processed_at TIMESTAMP
)
USING iceberg
TBLPROPERTIES (
    'format-version'='3',
    'write.format.default'='parquet',
    'write.parquet.compression-codec'='zstd'
)
PARTITIONED BY (days(processed_at))
""")

# Create merge source view from dataframe
df_merge.createOrReplaceTempView("customers_merge_source")

# SCD Type 1 MERGE (latest event wins by business key):
# - d => delete current row
# - c/r/u => insert or overwrite current row
spark.sql(f"""
MERGE INTO {target_table} t
USING customers_merge_source s
ON t.id = s.id
WHEN MATCHED AND s.op = 'd' THEN DELETE
WHEN MATCHED AND s.op IN ('c', 'r', 'u') THEN UPDATE SET
    t.first_name = s.first_name,
    t.last_name = s.last_name,
    t.email = s.email,
    t.processed_at = s.processed_at
WHEN NOT MATCHED AND s.op IN ('c', 'r', 'u') THEN INSERT (
    id, first_name, last_name, email, processed_at
) VALUES (
    s.id, s.first_name, s.last_name, s.email, s.processed_at
)
""")

print("SCD1 table updated.")
spark.sql(f"SELECT * FROM {target_table} LIMIT 10").show()

spark.stop()
