#!/usr/bin/env bash
set -euo pipefail

POLARIS_URL="${POLARIS_URL:-http://localhost:8181}"
REALM="${POLARIS_REALM:-POLARIS}"
ROOT_CLIENT_ID="${ROOT_CLIENT_ID:-root}"
ROOT_CLIENT_SECRET="${ROOT_CLIENT_SECRET:-s3cr3t}"

# Catalog names (match your Trino files)
BRONZE_CATALOG="${BRONZE_CATALOG:-bronze_catalog}"
SILVER_CATALOG="${SILVER_CATALOG:-silver_catalog}"
GOLD_CATALOG="${GOLD_CATALOG:-gold_catalog}"
BRONZE_BUCKET="${BRONZE_BUCKET:-bronze}"
SILVER_BUCKET="${SILVER_BUCKET:-silver}"
GOLD_BUCKET="${GOLD_BUCKET:-gold}"
RECREATE_TEAM_PRINCIPALS="${RECREATE_TEAM_PRINCIPALS:-false}"

echo "Getting root token..."
TOKEN=$(curl -sS -X POST "${POLARIS_URL}/api/catalog/v1/oauth/tokens" \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  -d "grant_type=client_credentials&client_id=${ROOT_CLIENT_ID}&client_secret=${ROOT_CLIENT_SECRET}&scope=PRINCIPAL_ROLE:ALL" \
  | jq -r '.access_token')

auth_hdr=(-H "Authorization: Bearer ${TOKEN}" -H "Polaris-Realm: ${REALM}" -H "Content-Type: application/json")

api_json() {
  local method="$1"
  local path="$2"
  local payload="${3:-}"
  local allow_409="${4:-false}"
  local allow_404="${5:-false}"
  local body_file
  body_file="$(mktemp)"

  local code
  if [[ -n "${payload}" ]]; then
    code=$(curl -sS -o "${body_file}" -w "%{http_code}" -X "${method}" "${POLARIS_URL}${path}" \
      "${auth_hdr[@]}" \
      -d "${payload}")
  else
    code=$(curl -sS -o "${body_file}" -w "%{http_code}" -X "${method}" "${POLARIS_URL}${path}" \
      "${auth_hdr[@]}")
  fi

  if [[ "${code}" =~ ^2[0-9][0-9]$ ]]; then
    cat "${body_file}"
    rm -f "${body_file}"
    return 0
  fi

  if [[ "${allow_409}" == "true" && "${code}" == "409" ]]; then
    rm -f "${body_file}"
    return 0
  fi

  if [[ "${allow_404}" == "true" && "${code}" == "404" ]]; then
    rm -f "${body_file}"
    return 0
  fi

  echo "Request failed: ${method} ${path} (HTTP ${code})" >&2
  cat "${body_file}" >&2
  rm -f "${body_file}"
  return 1
}

create_principal() {
  local principal_name="$1"
  echo "Creating principal: ${principal_name}"
  local response
  if response=$(api_json "POST" "/api/management/v1/principals" \
    "{\"principal\":{\"name\":\"${principal_name}\",\"properties\":{}}}" "true"); then
    if [[ -z "${response}" ]]; then
      echo "${principal_name} already exists, skipping create."
      return 0
    fi
    echo "${response}" > "/tmp/${principal_name}.json"
  fi

  local cid csec
  cid=$(jq -r '.credentials.clientId' "/tmp/${principal_name}.json")
  csec=$(jq -r '.credentials.clientSecret' "/tmp/${principal_name}.json")
  echo "${principal_name} CLIENT_ID=${cid}"
  echo "${principal_name} CLIENT_SECRET=${csec}"
  echo
}

create_principal_role() {
  local role_name="$1"
  api_json "POST" "/api/management/v1/principal-roles" \
    "{\"principalRole\":{\"name\":\"${role_name}\",\"properties\":{}}}" "true" >/dev/null
}

assign_principal_role() {
  local principal_name="$1"
  local role_name="$2"
  api_json "PUT" "/api/management/v1/principals/${principal_name}/principal-roles" \
    "{\"principalRole\":{\"name\":\"${role_name}\"}}" "true" >/dev/null
}

delete_principal() {
  local principal_name="$1"
  api_json "DELETE" "/api/management/v1/principals/${principal_name}" "" "false" "true" >/dev/null
}

create_catalog() {
  local catalog="$1"
  local bucket="$2"
  api_json "POST" "/api/management/v1/catalogs" "{
    \"catalog\": {
      \"name\": \"${catalog}\",
      \"type\": \"INTERNAL\",
      \"readOnly\": false,
      \"properties\": {
        \"default-base-location\": \"s3://${bucket}\"
      },
      \"storageConfigInfo\": {
        \"storageType\": \"S3\",
        \"allowedLocations\": [\"s3://${bucket}\"],
        \"endpoint\": \"http://localhost:9000\",
        \"endpointInternal\": \"http://minio:9000\",
        \"pathStyleAccess\": true
      }
    }
  }" "true" >/dev/null
}

create_catalog_role() {
  local catalog="$1"
  local role_name="$2"
  api_json "POST" "/api/management/v1/catalogs/${catalog}/catalog-roles" \
    "{\"catalogRole\":{\"name\":\"${role_name}\",\"properties\":{}}}" "true" >/dev/null
}

assign_catalog_role_to_principal_role() {
  local principal_role="$1"
  local catalog="$2"
  local catalog_role="$3"
  api_json "PUT" "/api/management/v1/principal-roles/${principal_role}/catalog-roles/${catalog}" \
    "{\"catalogRole\":{\"name\":\"${catalog_role}\"}}" "true" >/dev/null
}

grant_catalog_privilege() {
  local catalog="$1"
  local catalog_role="$2"
  local privilege="$3"
  api_json "PUT" "/api/management/v1/catalogs/${catalog}/catalog-roles/${catalog_role}/grants" \
    "{\"type\":\"catalog\",\"privilege\":\"${privilege}\"}" "true" >/dev/null
}

# 0) Ensure catalogs exist (avoids 404 on catalog-role endpoints)
echo "Ensuring catalogs exist..."
create_catalog "${BRONZE_CATALOG}" "${BRONZE_BUCKET}"
create_catalog "${SILVER_CATALOG}" "${SILVER_BUCKET}"
create_catalog "${GOLD_CATALOG}" "${GOLD_BUCKET}"

# 1) Create principals
if [[ "${RECREATE_TEAM_PRINCIPALS}" == "true" ]]; then
  echo "Recreating team principals to rotate credentials..."
  delete_principal "de_team"
  delete_principal "ds_team"
fi

create_principal "de_team"
create_principal "ds_team"

# 2) Create principal roles
create_principal_role "de_team_role"
create_principal_role "ds_team_role"

# 3) Attach principal roles to principals
assign_principal_role "de_team" "de_team_role"
assign_principal_role "ds_team" "ds_team_role"

# 4) Create catalog roles
create_catalog_role "${BRONZE_CATALOG}" "de_bronze_role"
create_catalog_role "${SILVER_CATALOG}" "de_silver_role"
create_catalog_role "${SILVER_CATALOG}" "ds_silver_read_role"
create_catalog_role "${GOLD_CATALOG}"   "ds_gold_write_role"

# 5) Map catalog roles to principal roles
assign_catalog_role_to_principal_role "de_team_role" "${BRONZE_CATALOG}" "de_bronze_role"
assign_catalog_role_to_principal_role "de_team_role" "${SILVER_CATALOG}" "de_silver_role"
assign_catalog_role_to_principal_role "ds_team_role" "${SILVER_CATALOG}" "ds_silver_read_role"
assign_catalog_role_to_principal_role "ds_team_role" "${GOLD_CATALOG}"   "ds_gold_write_role"

# 6) Grants
# DE: manage bronze + silver
grant_catalog_privilege "${BRONZE_CATALOG}" "de_bronze_role" "CATALOG_MANAGE_CONTENT"
grant_catalog_privilege "${SILVER_CATALOG}" "de_silver_role" "CATALOG_MANAGE_CONTENT"

# DS: read silver, write gold
grant_catalog_privilege "${SILVER_CATALOG}" "ds_silver_read_role" "CATALOG_READ_PROPERTIES"
grant_catalog_privilege "${GOLD_CATALOG}"   "ds_gold_write_role"  "CATALOG_MANAGE_CONTENT"

echo "Done."
echo "Tip: set RECREATE_TEAM_PRINCIPALS=true to rotate de_team/ds_team credentials."