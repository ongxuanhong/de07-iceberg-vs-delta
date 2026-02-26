from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Polaris Smoke Test").getOrCreate()
spark.sparkContext.setLogLevel("WARN")


def run(label: str, sql: str):
    print(f"\n=== {label} ===")
    print(sql)
    spark.sql(sql).show(truncate=False)


def table_exists(fully_qualified_name: str) -> bool:
    try:
        spark.table(fully_qualified_name).limit(1).collect()
        return True
    except Exception:
        return False


try:
    run("SHOW CATALOGS", "SHOW CATALOGS")
    run("Bronze namespaces", "SHOW NAMESPACES IN bronze")
    run("Silver namespaces", "SHOW NAMESPACES IN silver")

    run("Ensure bronze namespace", "CREATE NAMESPACE IF NOT EXISTS bronze.inventory")
    run("Ensure silver namespace", "CREATE NAMESPACE IF NOT EXISTS silver.event_streaming")

    # If bronze source table doesn't exist, create dummy source data.
    if not table_exists("bronze.inventory.customers"):
        run(
            "Create bronze customers table",
            """
            CREATE TABLE IF NOT EXISTS bronze.inventory.customers (
                id BIGINT,
                first_name STRING,
                last_name STRING,
                email STRING
            )
            USING iceberg
            """,
        )
        run(
            "Insert dummy bronze rows",
            """
            INSERT INTO bronze.inventory.customers VALUES
              (1001, 'Sally',  'Thomas', 'sally.thomas@acme.com'),
              (1002, 'George', 'Bailey', 'gbailey@foobar.com'),
              (1003, 'Edward', 'Walker', 'ed@walker.com')
            """,
        )

    run("Bronze tables", "SHOW TABLES IN bronze.inventory")
    run("Bronze sample", "SELECT * FROM bronze.inventory.customers ORDER BY id")

    # Write into silver
    run(
        "Create silver table",
        """
        CREATE TABLE IF NOT EXISTS silver.event_streaming.polaris_smoke_test (
            id BIGINT,
            first_name STRING,
            last_name STRING,
            email STRING,
            processed_at TIMESTAMP,
            processed_date DATE
        )
        USING iceberg
        PARTITIONED BY (processed_date)
        """,
    )

    run(
        "Upsert from bronze to silver",
        """
        MERGE INTO silver.event_streaming.polaris_smoke_test t
        USING (
          SELECT
            id,
            first_name,
            last_name,
            email,
            current_timestamp() AS processed_at,
            current_date() AS processed_date
          FROM bronze.inventory.customers
        ) s
        ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET
          t.first_name = s.first_name,
          t.last_name = s.last_name,
          t.email = s.email,
          t.processed_at = s.processed_at,
          t.processed_date = s.processed_date
        WHEN NOT MATCHED THEN INSERT (
          id, first_name, last_name, email, processed_at, processed_date
        ) VALUES (
          s.id, s.first_name, s.last_name, s.email, s.processed_at, s.processed_date
        )
        """,
    )

    run(
        "Silver sample",
        "SELECT id, first_name, last_name, email, processed_at FROM silver.event_streaming.polaris_smoke_test ORDER BY id",
    )

    print("\nPolaris smoke test PASSED.")
finally:
    spark.stop()