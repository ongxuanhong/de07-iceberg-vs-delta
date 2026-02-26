# Initialize services
```bash
docker compose up -d
```

# Check services
```bash
docker compose ps

# check postgres
docker compose exec postgres psql -U postgres -c "SELECT * FROM inventory.customers;"

# check kafka topics
docker compose exec kafka kafka-topics --bootstrap-server kafka:9092 --list
```

# Create connector for PostgreSQL debezium_demo database
```bash
curl -i -X POST \
  -H "Accept: application/json" \
  -H "Content-Type: application/json" \
  http://localhost:8083/connectors \
  -d '{
    "name": "debezium-demo-connector",
    "config": {
      "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
      "tasks.max": "1",
      "database.hostname": "postgres",
      "database.port": "5432",
      "database.user": "postgres",
      "database.password": "postgres",
      "database.dbname": "postgres",

      "topic.prefix": "postgres1",

      "plugin.name": "pgoutput",
      "slot.name": "debezium_demo_slot",
      "publication.name": "debezium_demo_pub",
      "publication.autocreate.mode": "filtered",

      "schema.include.list": "inventory",
      "table.include.list": "inventory.customers,inventory.orders",
      "snapshot.mode": "initial",

      "key.converter": "org.apache.kafka.connect.json.JsonConverter",
      "value.converter": "org.apache.kafka.connect.json.JsonConverter",
      "key.converter.schemas.enable": "false",
      "value.converter.schemas.enable": "false"
    }
  }'

# Check connectors
curl -XGET http://localhost:8083/connectors

# Retrieves additional state information for each connector and its tasks
curl -XGET http://localhost:8083/connectors?expand=status | jq

# Returns metadata for each connector (config, tasks, type)
curl -XGET http://localhost:8083/connectors?expand=info | jq

# delete
curl -i -X DELETE http://localhost:8083/connectors/debezium-demo-connector


# inspecting
curl -s http://localhost:8083/connectors/debezium-demo-connector/status | jq
curl -s http://localhost:8083/connectors/debezium-demo-connector | jq '.config'  

# Observing CDC with CRUD
docker exec kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic postgres1.inventory.customers --from-beginning | jq

# Create
docker exec postgres psql -U postgres -d postgres -c "
INSERT INTO inventory.customers(first_name,last_name,email)
VALUES ('CDC','Create','cdc.create@example.com');
"

# Update
docker exec postgres psql -U postgres -d postgres -c "
UPDATE inventory.customers
SET first_name='CDC-Updated', email='cdc.updated@example.com'
WHERE email='cdc.create@example.com';
"

# Delete
docker exec postgres psql -U postgres -d postgres -c "
DELETE FROM inventory.customers
WHERE email='cdc.updated@example.com';
"
```

# Working with Polaris
```bash
# Get a valid access token
TOKEN=$(curl -s -X POST http://localhost:8181/api/catalog/v1/oauth/tokens \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  -d "grant_type=client_credentials&client_id=root&client_secret=s3cr3t&scope=PRINCIPAL_ROLE:ALL" \
  | jq -r '.access_token')

# Create the bronze catalog
curl -sS -X POST "http://localhost:8181/api/management/v1/catalogs" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Polaris-Realm: POLARIS" \
  -H "Content-Type: application/json" \
  -d '{
    "catalog": {
        "name": "bronze",
        "type": "INTERNAL",
        "readOnly": false,
        "properties": {
            "default-base-location": "s3://bronze"
        },
        "storageConfigInfo": {
            "storageType": "S3",
            "allowedLocations": ["s3://bronze"],
            "endpoint": "http://localhost:9000",
            "endpointInternal": "http://minio:9000",
            "pathStyleAccess": true
        }
    }
  }' | jq

# Create the silver catalog
curl -sS -X POST "http://localhost:8181/api/management/v1/catalogs" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Polaris-Realm: POLARIS" \
  -H "Content-Type: application/json" \
  -d '{
    "catalog": {
        "name": "silver",
        "type": "INTERNAL",
        "readOnly": false,
        "properties": {
            "default-base-location": "s3://silver"
        },
        "storageConfigInfo": {
            "storageType": "S3",
            "allowedLocations": ["s3://silver"],
            "endpoint": "http://localhost:9000",
            "endpointInternal": "http://minio:9000",
            "pathStyleAccess": true
        }
    }
  }' | jq  

# List catalogs
curl -s http://localhost:8181/api/management/v1/catalogs \
  -H "Authorization: Bearer $TOKEN" \
  -H "Polaris-Realm: POLARIS" | jq

# Delete catalog
curl -sS -X DELETE "http://localhost:8181/api/management/v1/catalogs/bronze" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Polaris-Realm: POLARIS" \
  -H "Content-Type: application/json" \
  | jq  
```

# Spark submit
```bash
# Using REST catalog
AWS_ACCESS_KEY_ID=minioadmin \
AWS_SECRET_ACCESS_KEY=minioadmin \
AWS_REGION=us-east-1 \
SPARK_LOCAL_IP=127.0.0.1 SPARK_LOCAL_HOSTNAME=localhost \
spark-submit --master "local[2]" \
  --packages org.apache.polaris:polaris-spark-3.5_2.12:1.3.0-incubating,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0,org.apache.iceberg:iceberg-aws-bundle:1.10.0 \
  --conf spark.driver.bindAddress=127.0.0.1 \
  --conf spark.driver.host=localhost \
  --conf spark.ui.host=127.0.0.1 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.bronze=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.bronze.type=rest \
  --conf spark.sql.catalog.bronze.warehouse=s3a://bronze/ \
  --conf spark.sql.catalog.bronze.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
  --conf spark.sql.catalog.bronze.s3.endpoint=http://localhost:9000 \
  --conf spark.sql.catalog.bronze.s3.path-style-access=true \
  --conf spark.sql.catalog.bronze.s3.access-key-id=minioadmin \
  --conf spark.sql.catalog.bronze.s3.secret-access-key=minioadmin \
  --conf spark.sql.catalog.bronze.s3.region=us-east-1 \
  --conf spark.sql.catalog.bronze.uri=http://localhost:8183 \
scripts/demo_iceberg_scd1_local.py

# Using Polaris catalog
AWS_ACCESS_KEY_ID=minioadmin \
AWS_SECRET_ACCESS_KEY=minioadmin \
AWS_REGION=us-east-1 \
SPARK_LOCAL_IP=127.0.0.1 \
SPARK_LOCAL_HOSTNAME=localhost \
spark-submit --master "local[2]" \
  --packages org.apache.polaris:polaris-spark-3.5_2.12:1.3.0-incubating,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0,org.apache.iceberg:iceberg-aws-bundle:1.10.0 \
  --conf spark.driver.bindAddress=127.0.0.1 \
  --conf spark.driver.host=localhost \
  --conf spark.ui.host=127.0.0.1 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.bronze=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.bronze.type=rest \
  --conf spark.sql.catalog.bronze.uri=http://localhost:8181/api/catalog \
  --conf spark.sql.catalog.bronze.warehouse=bronze \
  --conf spark.sql.catalog.bronze.credential=root:s3cr3t \
  --conf spark.sql.catalog.bronze.scope=PRINCIPAL_ROLE:ALL \
  --conf spark.sql.catalog.bronze.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
  --conf spark.sql.catalog.bronze.s3.endpoint=http://localhost:9000 \
  --conf spark.sql.catalog.bronze.s3.path-style-access=true \
  --conf spark.sql.catalog.bronze.s3.access-key-id=minioadmin \
  --conf spark.sql.catalog.bronze.s3.secret-access-key=minioadmin \
  --conf spark.sql.catalog.bronze.client.region=us-east-1 \
  --conf spark.sql.catalog.silver=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.silver.type=rest \
  --conf spark.sql.catalog.silver.uri=http://localhost:8181/api/catalog \
  --conf spark.sql.catalog.silver.warehouse=silver \
  --conf spark.sql.catalog.silver.credential=root:s3cr3t \
  --conf spark.sql.catalog.silver.scope=PRINCIPAL_ROLE:ALL \
  --conf spark.sql.catalog.silver.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
  --conf spark.sql.catalog.silver.s3.endpoint=http://localhost:9000 \
  --conf spark.sql.catalog.silver.s3.path-style-access=true \
  --conf spark.sql.catalog.silver.s3.access-key-id=minioadmin \
  --conf spark.sql.catalog.silver.s3.secret-access-key=minioadmin \
  --conf spark.sql.catalog.silver.client.region=us-east-1 \
scripts/test_polaris_smoke.py
```  

Perfect timing to switch to Polaris RBAC.  
Key point first: with your current Trino config, both catalogs use `root:s3cr3t`, so Polaris sees **one identity** for all queries. To make Polaris RBAC meaningful, Trino must use **team-specific Polaris principals**.

## What to change (architecture)

For one shared Trino, use this pattern:

- `bronze_de` catalog -> Polaris credential for DE principal
- `silver_de` catalog -> Polaris credential for DE principal
- `silver_ds` catalog -> Polaris credential for DS principal (read-only in Polaris)

Then DBeaver:
- DE team uses `bronze_de`, `silver_de`
- DS team uses `silver_ds`

Trino `rules.json` should only hide catalog names per user (not enforce write/read semantics). Polaris enforces read/write.

---

## Polaris RBAC setup

### 1) Get Polaris admin token (root)
```bash
export TOKEN=$(curl -s -X POST http://localhost:8181/api/catalog/v1/oauth/tokens \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  -d 'grant_type=client_credentials' \
  -d 'client_id=root' \
  -d 'client_secret=s3cr3t' \
  -d 'scope=PRINCIPAL_ROLE:ALL' | jq -r '.access_token')
```

### 2) Create 2 Polaris principals (DE, DS)
```bash
curl -s -X POST http://localhost:8181/api/management/v1/principals \
  -H "Authorization: Bearer $TOKEN" -H "Polaris-Realm: POLARIS" \
  -H "Content-Type: application/json" \
  -d '{"principal":{"name":"trino_de","properties":{}}}' | jq

curl -s -X POST http://localhost:8181/api/management/v1/principals \
  -H "Authorization: Bearer $TOKEN" -H "Polaris-Realm: POLARIS" \
  -H "Content-Type: application/json" \
  -d '{"principal":{"name":"trino_ds","properties":{}}}' | jq

# delete principal if needed
curl -s -X DELETE "http://localhost:8181/api/management/v1/principals/trino_de" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Polaris-Realm: POLARIS"

# list principals
curl -s "http://localhost:8181/api/management/v1/principals" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Polaris-Realm: POLARIS"  | jq
```

Save returned `clientId`/`clientSecret` for each.

  

### 3) Create principal roles
- `de_role`
```bash
curl --fail-with-body -s -X POST http://localhost:8181/api/management/v1/principal-roles \
  -H "Authorization: Bearer $TOKEN" \
  -H "Polaris-Realm: POLARIS" \
  -H "Content-Type: application/json" \
  -d '{
    "principalRole": {
      "name": "de_role",
      "properties": {}
    }
  }' | jq
```

- `ds_role`
```bash
curl --fail-with-body -s -X POST http://localhost:8181/api/management/v1/principal-roles \
  -H "Authorization: Bearer $TOKEN" \
  -H "Polaris-Realm: POLARIS" \
  -H "Content-Type: application/json" \
  -d '{
    "principalRole": {
      "name": "ds_role",
      "properties": {}
    }
  }' | jq

curl --fail-with-body -s http://localhost:8181/api/management/v1/principal-roles \
  -H "Authorization: Bearer $TOKEN" \
  -H "Polaris-Realm: POLARIS" | jq  
```  

### 4) Create catalog roles
In catalog `bronze`:
- `bronze_contributor` (DE only)
```bash
curl --fail-with-body -s -X POST \
  http://localhost:8181/api/management/v1/catalogs/bronze/catalog-roles \
  -H "Authorization: Bearer $TOKEN" \
  -H "Polaris-Realm: POLARIS" \
  -H "Content-Type: application/json" \
  -d '{
    "catalogRole": {
      "name": "bronze_contributor",
      "properties": {}
    }
  }' | jq
```

In catalog `silver`:
- `silver_contributor` (DE)
```bash
curl --fail-with-body -s -X POST \
  http://localhost:8181/api/management/v1/catalogs/silver/catalog-roles \
  -H "Authorization: Bearer $TOKEN" \
  -H "Polaris-Realm: POLARIS" \
  -H "Content-Type: application/json" \
  -d '{
    "catalogRole": {
      "name": "silver_contributor",
      "properties": {}
    }
  }' | jq
```

- `silver_reader` (DS)
```bash
curl --fail-with-body -s -X POST \
  http://localhost:8181/api/management/v1/catalogs/silver/catalog-roles \
  -H "Authorization: Bearer $TOKEN" \
  -H "Polaris-Realm: POLARIS" \
  -H "Content-Type: application/json" \
  -d '{
    "catalogRole": {
      "name": "silver_reader",
      "properties": {}
    }
  }' | jq
```

- Verify catalog roles:
```bash
echo "bronze roles:"
curl --fail-with-body -s \
  http://localhost:8181/api/management/v1/catalogs/bronze/catalog-roles \
  -H "Authorization: Bearer $TOKEN" \
  -H "Polaris-Realm: POLARIS" | jq

echo "silver roles:"
curl --fail-with-body -s \
  http://localhost:8181/api/management/v1/catalogs/silver/catalog-roles \
  -H "Authorization: Bearer $TOKEN" \
  -H "Polaris-Realm: POLARIS" | jq
```

### 5) Grant privileges to catalog roles
Use Polaris privileges from docs:

- **DE contributor roles** (`bronze_contributor`, `silver_contributor`):
  - `CATALOG_MANAGE_CONTENT` (simple full read/write content path)
```bash
curl --fail-with-body -s -X PUT \
  http://localhost:8181/api/management/v1/catalogs/bronze/catalog-roles/bronze_contributor/grants \
  -H "Authorization: Bearer $TOKEN" \
  -H "Polaris-Realm: POLARIS" \
  -H "Content-Type: application/json" \
  -d '{"type":"catalog","privilege":"CATALOG_MANAGE_CONTENT"}'

curl --fail-with-body -s -X PUT \
  http://localhost:8181/api/management/v1/catalogs/silver/catalog-roles/silver_contributor/grants \
  -H "Authorization: Bearer $TOKEN" \
  -H "Polaris-Realm: POLARIS" \
  -H "Content-Type: application/json" \
  -d '{"type":"catalog","privilege":"CATALOG_MANAGE_CONTENT"}'
```  

- **DS reader role** (`silver_reader`):
  - `CATALOG_READ_PROPERTIES`
  - `NAMESPACE_LIST`
  - `NAMESPACE_READ_PROPERTIES`
  - `TABLE_LIST`
  - `TABLE_READ_PROPERTIES`
  - `TABLE_READ_DATA`

(Do **not** grant `TABLE_WRITE_DATA`, `TABLE_CREATE`, `TABLE_DROP`, etc. to DS)
```bash
for PRIV in \
  CATALOG_READ_PROPERTIES \
  NAMESPACE_LIST \
  NAMESPACE_READ_PROPERTIES \
  TABLE_LIST \
  TABLE_READ_PROPERTIES \
  TABLE_READ_DATA
do
  curl --fail-with-body -s -X PUT \
    "http://localhost:8181/api/management/v1/catalogs/silver/catalog-roles/silver_reader/grants" \
    -H "Authorization: Bearer $TOKEN" \
    -H "Polaris-Realm: POLARIS" \
    -H "Content-Type: application/json" \
    -d "{\"type\":\"catalog\",\"privilege\":\"$PRIV\"}"
done
```

### 6) Bind roles
- assign `de_role` to principal `trino_de`
- assign `ds_role` to principal `trino_ds`
- grant `bronze_contributor` + `silver_contributor` to `de_role`
- grant `silver_reader` to `ds_role`

```bash
curl --fail-with-body -s -X PUT \
  http://localhost:8181/api/management/v1/principals/trino_de/principal-roles \
  -H "Authorization: Bearer $TOKEN" \
  -H "Polaris-Realm: POLARIS" \
  -H "Content-Type: application/json" \
  -d '{"principalRole":{"name":"de_role"}}'

curl --fail-with-body -s -X PUT \
  http://localhost:8181/api/management/v1/principals/trino_ds/principal-roles \
  -H "Authorization: Bearer $TOKEN" \
  -H "Polaris-Realm: POLARIS" \
  -H "Content-Type: application/json" \
  -d '{"principalRole":{"name":"ds_role"}}'

# 6.2 grant catalog roles to principal roles
curl --fail-with-body -s -X PUT \
  http://localhost:8181/api/management/v1/principal-roles/de_role/catalog-roles/bronze \
  -H "Authorization: Bearer $TOKEN" \
  -H "Polaris-Realm: POLARIS" \
  -H "Content-Type: application/json" \
  -d '{"catalogRole":{"name":"bronze_contributor"}}'

curl --fail-with-body -s -X PUT \
  http://localhost:8181/api/management/v1/principal-roles/de_role/catalog-roles/silver \
  -H "Authorization: Bearer $TOKEN" \
  -H "Polaris-Realm: POLARIS" \
  -H "Content-Type: application/json" \
  -d '{"catalogRole":{"name":"silver_contributor"}}'

curl --fail-with-body -s -X PUT \
  http://localhost:8181/api/management/v1/principal-roles/ds_role/catalog-roles/silver \
  -H "Authorization: Bearer $TOKEN" \
  -H "Polaris-Realm: POLARIS" \
  -H "Content-Type: application/json" \
  -d '{"catalogRole":{"name":"silver_reader"}}'
```

### Verify role bindings:
```bash
# principal-role binding
curl -s http://localhost:8181/api/management/v1/principals/trino_de/principal-roles \
  -H "Authorization: Bearer $TOKEN" -H "Polaris-Realm: POLARIS" | jq
curl -s http://localhost:8181/api/management/v1/principals/trino_ds/principal-roles \
  -H "Authorization: Bearer $TOKEN" -H "Polaris-Realm: POLARIS" | jq

# catalog-role grants
curl -s http://localhost:8181/api/management/v1/catalogs/bronze/catalog-roles/bronze_contributor/grants \
  -H "Authorization: Bearer $TOKEN" -H "Polaris-Realm: POLARIS" | jq
curl -s http://localhost:8181/api/management/v1/catalogs/silver/catalog-roles/silver_contributor/grants \
  -H "Authorization: Bearer $TOKEN" -H "Polaris-Realm: POLARIS" | jq
curl -s http://localhost:8181/api/management/v1/catalogs/silver/catalog-roles/silver_reader/grants \
  -H "Authorization: Bearer $TOKEN" -H "Polaris-Realm: POLARIS" | jq
```
---

## Step 7) Update Trino catalog property files

Create/update in `iceberge-project/trino/catalog`:

- `bronze_de.properties` with:
  - `iceberg.rest-catalog.warehouse=bronze`
  - `iceberg.rest-catalog.oauth2.credential=<trino_de_clientId>:<trino_de_clientSecret>`

- `silver_de.properties` with:
  - warehouse `silver`
  - DE credential

- `silver_ds.properties` with:
  - warehouse `silver`
  - DS credential

Keep other S3/endpoint settings same as your current files.

Then restart Trino:
```bash
docker compose restart trino
```

---

## Step 8) DBeaver connections

- `polaris-DE` (username `de_alice`) -> use catalogs `bronze_de`, `silver_de`
- `polaris-DS` (username `ds_bob`) -> use catalog `silver_ds`

(Username here is mostly for Trino-side catalog visibility; Polaris identity is from catalog credential.)

---

## Verification checklist

### A) Polaris-side verification
- List principal roles assigned to `trino_de` / `trino_ds`
- List catalog-role grants and privileges for `bronze` and `silver`
- Confirm DS role has no `TABLE_WRITE_DATA`

### B) SQL verification in Trino/DBeaver

With DE connection:
- `SELECT * FROM bronze_de.inventory.customers` ✅
- `INSERT INTO silver_de.event_streaming.polaris_smoke_test (...) VALUES (...)` ✅

With DS connection:
- `SELECT * FROM silver_ds.event_streaming.polaris_smoke_test` ✅
- `INSERT INTO silver_ds.event_streaming.polaris_smoke_test (...) VALUES (...)` ❌ (expected denied)
- `SELECT * FROM bronze_de.inventory.customers` ❌ (if hidden by Trino rules)