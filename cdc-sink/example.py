from pyiceberg.catalog import load_catalog
import pyarrow as pa
from datetime import datetime

from pyiceberg.schema import Schema as IcebergSchema
from pyiceberg.types import (
    NestedField,
    TimestampType,
    DoubleType,
    StringType,
    IntegerType,
)

# 1. Load catalog (removed unnecessary "scope" — it's for token request, not config)
catalog = load_catalog(
    "bronze",
    **{
        "type": "rest",
        "uri": "http://iceberg-rest:8181",
        "warehouse": "bronze",
        "s3.endpoint": "http://minio:9000",
        "s3.region": "us-east-1",
        "s3.access-key-id": "minioadmin",
        "s3.secret-access-key": "minioadmin",
        "s3.path-style-access": "true",
    }
)

# Quick connectivity test
print("Namespaces in catalog:", catalog.list_namespaces())

# 2. Sample data → Arrow Table
data = [
    {
        "id": 1001,
        "event_time": datetime(2026, 2, 24, 14, 30),
        "user_id": "u78912",
        "action": "click",
        "page": "/products/iceberg-book",
        "value": 29.99
    },
    {
        "id": 1002,
        "event_time": datetime(2026, 2, 24, 14, 32),
        "user_id": "u12345",
        "action": "purchase",
        "page": None,
        "value": 149.00
    },
]

# Use explicit Arrow schema for consistency
arrow_schema = pa.schema([
    ("id",         pa.int32()),
    ("event_time", pa.timestamp("us")),   # no tz
    ("user_id",    pa.string()),
    ("action",     pa.string()),
    ("page",       pa.string()),
    ("value",      pa.float64()),
])

arrow_table = pa.Table.from_pylist(data, schema=arrow_schema)

# 3. Iceberg table setup
namespace = "event_streaming"
table_id = "click_events"
table_identifier = f"{namespace}.{table_id}"

# Ensure namespace exists (idempotent)
catalog.create_namespace_if_not_exists(namespace)
print(f"Namespace '{namespace}' ready")

# Iceberg schema (matches Arrow schema)
iceberg_schema = IcebergSchema(
    NestedField(1, "id",        IntegerType(), required=False),
    NestedField(2, "event_time", TimestamptzType(), required=False),
    NestedField(3, "user_id",   StringType(), required=False),
    NestedField(4, "action",    StringType(), required=False),
    NestedField(5, "page",      StringType(), required=False),
    NestedField(6, "value",     DoubleType(), required=False),
)

# Create or load table
try:
    table = catalog.load_table(table_identifier)
    print(f"Table {table_identifier} already exists → will append")
except Exception:
    print(f"Creating new table {table_identifier}")
    table = catalog.create_table(
        identifier=table_identifier,
        schema=iceberg_schema
    )
    print(f"Table created")

# Quick validation
print("Tables in namespace:", catalog.list_tables(namespace))

# 4. Append data
table.append(arrow_table)
print(f"Successfully appended {arrow_table.num_rows} rows to {table_identifier}")

# Optional: read back to verify
df = table.scan().to_arrow().to_pandas()
print(df.head())