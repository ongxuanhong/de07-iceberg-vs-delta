#!/usr/bin/env bash
set -euo pipefail

# =======================================================
# CONFIG
# =======================================================

export AUTH0_CLIENT_ID=""
export AUTH0_CLIENT_SECRET=""
export AUTH0_DOMAIN="abc.us.auth0.com"
export AUTH0_AUDIENCE="unity-catalog"

export POLARIS_URL="https://polaris.abc.tech"
export REALM="POLARIS"
export CATALOG="dev_data_us_us"
export S3_LOCATION="s3://dev-data-us-us-bronze/"
export AWS_ACCESS_KEY_ID=""
export AWS_SECRET_ACCESS_KEY=""
export AWS_REGION="us-west-2"

# AWS credentials required for S3FileIO (table/data storage).
# Set before running, e.g.:
#   export AWS_ACCESS_KEY_ID=AKIA...
#   export AWS_SECRET_ACCESS_KEY=...
if [[ -z "${AWS_ACCESS_KEY_ID:-}" || -z "${AWS_SECRET_ACCESS_KEY:-}" ]]; then
  echo "❌ AWS credentials required for S3. Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY"
  exit 1
fi

# =======================================================
# TOKEN
# =======================================================

echo "Getting Auth0 token..."

TOKEN=$(curl -s -X POST "https://${AUTH0_DOMAIN}/oauth/token" \
  -H "Content-Type: application/json" \
  -d "{
    \"grant_type\":\"client_credentials\",
    \"client_id\":\"${AUTH0_CLIENT_ID}\",
    \"client_secret\":\"${AUTH0_CLIENT_SECRET}\",
    \"audience\":\"${AUTH0_AUDIENCE}\"
  }" | jq -r '.access_token')

if [[ "$TOKEN" == "null" || -z "$TOKEN" ]]; then
  echo "❌ Failed to obtain token"
  exit 1
fi

echo "✅ Token received"

# =======================================================
# DELETE CATALOG IF EXISTS (SAFE RESET)
# =======================================================

echo "Deleting catalog if exists (best-effort; may fail if catalog not empty)..."

DELETE_OUT=$(curl -s -w "\n%{http_code}" -X DELETE \
  -H "Authorization: Bearer $TOKEN" \
  -H "Polaris-Realm: ${REALM}" \
  "${POLARIS_URL}/api/management/v1/catalogs/${CATALOG}" 2>/dev/null) || true
DELETE_HTTP=$(echo "$DELETE_OUT" | tail -n1)
if [[ "$DELETE_HTTP" == "204" ]]; then
  echo "Catalog deleted."
elif [[ "$DELETE_HTTP" == "400" ]]; then
  echo "Catalog not empty, skipping delete (will use existing catalog)."
else
  echo "Delete returned HTTP ${DELETE_HTTP} (continuing anyway)."
fi

echo "Creating catalog (STATIC AWS KEYS MODE)..."

CREATE_OUT=$(curl -s -w "\n%{http_code}" -X POST "${POLARIS_URL}/api/management/v1/catalogs" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Polaris-Realm: ${REALM}" \
  -H "Content-Type: application/json" \
  -d "{
    \"catalog\": {
      \"name\": \"${CATALOG}\",
      \"type\": \"INTERNAL\",
      \"readOnly\": false,
      \"properties\": {
        \"default-base-location\": \"${S3_LOCATION}\"
      },
      \"storageConfigInfo\": {
        \"storageType\": \"S3\",
        \"allowedLocations\": [
          \"${S3_LOCATION}\"
        ]
      }
    }
  }" 2>/dev/null)
CREATE_HTTP=$(echo "$CREATE_OUT" | tail -n1)
CREATE_BODY=$(echo "$CREATE_OUT" | sed '$d')
echo "$CREATE_BODY" | jq . 2>/dev/null || echo "$CREATE_BODY"
if [[ "$CREATE_HTTP" == "201" ]]; then
  echo "Catalog created."
elif [[ "$CREATE_HTTP" == "409" ]]; then
  echo "Catalog already exists (continuing with smoke test)."
else
  echo "Create returned HTTP ${CREATE_HTTP}."
fi

echo ""
echo "-------------------------------------------------------"
echo "Running Spark smoke test..."
echo "-------------------------------------------------------"

SPARK_LOCAL_IP=127.0.0.1 \
SPARK_LOCAL_HOSTNAME=localhost \
spark-submit \
  --master local[2] \
  --name "Polaris Smoke Test" \
  \
  --packages \
org.apache.polaris:polaris-spark-3.5_2.12:1.3.0-incubating,\
org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0,\
org.apache.iceberg:iceberg-aws-bundle:1.10.0 \
  \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  \
  --conf spark.sql.catalog.${CATALOG}=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.${CATALOG}.type=rest \
  --conf spark.sql.catalog.${CATALOG}.uri=${POLARIS_URL}/api/catalog \
  --conf spark.sql.catalog.${CATALOG}.warehouse=${CATALOG} \
  \
  --conf spark.sql.catalog.${CATALOG}.rest.auth.type=oauth2 \
  --conf spark.sql.catalog.${CATALOG}.token=${TOKEN} \
  --conf spark.sql.catalog.${CATALOG}.oauth2-server-uri=${POLARIS_URL}/api/catalog/v1/oauth/tokens \
  --conf spark.sql.catalog.${CATALOG}.header.Polaris-Realm=${REALM} \
  \
  --conf spark.sql.catalog.${CATALOG}.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
  --conf spark.sql.catalog.${CATALOG}.s3.region=${AWS_REGION} \
  --conf "spark.sql.catalog.${CATALOG}.s3.access-key-id=${AWS_ACCESS_KEY_ID}" \
  --conf "spark.sql.catalog.${CATALOG}.s3.secret-access-key=${AWS_SECRET_ACCESS_KEY}" \
  \
  scripts/test_polaris.py