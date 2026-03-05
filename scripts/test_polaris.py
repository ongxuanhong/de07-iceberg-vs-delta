import os
from pyspark.sql import SparkSession

CATALOG = "dev_gridlake_us_us"
NAMESPACE = "inventory"
TABLE = "customers"
FULL_TABLE = f"{CATALOG}.{NAMESPACE}.{TABLE}"

spark = (
    SparkSession.builder
    .appName("Polaris Smoke Test")

    .config("spark.driver.extraJavaOptions", "-Daws.region=us-west-2")
    .config("spark.executor.extraJavaOptions", "-Daws.region=us-west-2")

    .config("spark.hadoop.aws.region", "us-west-2")

    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


def header(label: str):
    print("\n" + "=" * 70)
    print(label)
    print("=" * 70)


def run(label: str, sql: str):
    header(label)
    print(sql.strip())
    spark.sql(sql).show(truncate=False)


def table_exists(table_name: str) -> bool:
    try:
        spark.table(table_name).limit(1).collect()
        return True
    except Exception:
        return False


run("SHOW CATALOGS", "SHOW CATALOGS")

run(
    f"Namespaces in {CATALOG}",
    f"SHOW NAMESPACES IN {CATALOG}",
)

run(
    f"Ensure namespace {CATALOG}.{NAMESPACE}",
    f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{NAMESPACE}",
)

if not table_exists(FULL_TABLE):

    run(
        "Create Iceberg customers table",
        f"""
        CREATE TABLE IF NOT EXISTS {FULL_TABLE} (
            id BIGINT,
            first_name STRING,
            last_name STRING,
            email STRING
        )
        USING iceberg
        """,
    )

    run(
        "Insert sample rows",
        f"""
        INSERT INTO {FULL_TABLE} VALUES
        (1001,'Sally','Thomas','sally.thomas@acme.com'),
        (1002,'George','Bailey','gbailey@foobar.com'),
        (1003,'Edward','Walker','ed@walker.com')
        """,
    )

run("List tables", f"SHOW TABLES IN {CATALOG}.{NAMESPACE}")

run("Sample data", f"SELECT * FROM {FULL_TABLE} ORDER BY id")

header("Smoke test completed successfully")