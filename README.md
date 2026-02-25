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