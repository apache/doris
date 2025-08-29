#!/bin/sh
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -eu

HOST=${POLARIS_HOST:-polaris-s3}
PORT=${POLARIS_PORT:-8181}
USER=${POLARIS_BOOTSTRAP_USER:-root}
PASS=${POLARIS_BOOTSTRAP_PASSWORD:-secret123}
CATALOG=${POLARIS_CATALOG_NAME:-minio}
BASE_LOCATION=${CATALOG_BASE_LOCATION:-s3://warehouse/wh/}

echo "[polaris-init] Waiting for Polaris health check at http://$HOST:$PORT/q/health ..."
for i in $(seq 1 120); do
  if curl -sSf "http://$HOST:8182/q/health" >/dev/null; then
    break
  fi
  sleep 2
done

echo "[polaris-init] Fetching OAuth token via client_credentials ..."
# Try to obtain token using correct OAuth endpoint
TOKEN_JSON=$(curl -sS \
  -X POST "http://$HOST:$PORT/api/catalog/v1/oauth/tokens" \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  -d "grant_type=client_credentials&client_id=$USER&client_secret=$PASS&scope=PRINCIPAL_ROLE:ALL")

# Extract access_token field
TOKEN=$(printf "%s" "$TOKEN_JSON" | sed -n 's/.*"access_token"\s*:\s*"\([^"]*\)".*/\1/p')

if [ -z "$TOKEN" ]; then
  echo "[polaris-init] ERROR: Failed to obtain OAuth token. Response: $TOKEN_JSON" >&2
  exit 1
fi

echo "[polaris-init] Creating catalog '$CATALOG' with base '$BASE_LOCATION' ..."
CREATE_PAYLOAD=$(cat <<JSON
{
  "name": "$CATALOG",
  "type": "INTERNAL",
  "properties": {
    "default-base-location": "$BASE_LOCATION",
    "s3.endpoint": "http://minio:9000",
    "s3.path-style-access": "true",
    "s3.access-key-id": "admin",
    "s3.secret-access-key": "password",
    "s3.region": "${AWSRegion:-us-east-1}"
  },
  "storageConfigInfo": {
    "roleArn": "arn:aws:iam::000000000000:role/minio-polaris-role",
    "storageType": "S3",
    "allowedLocations": ["$BASE_LOCATION"]
  }
}
JSON
)

# Try create; on 409 Conflict, treat as success
HTTP_CODE=$(curl -sS -o /tmp/resp.json -w "%{http_code}" \
  -X POST "http://$HOST:$PORT/api/management/v1/catalogs" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d "$CREATE_PAYLOAD")

if [ "$HTTP_CODE" = "201" ]; then
  echo "[polaris-init] Catalog created."
elif [ "$HTTP_CODE" = "409" ]; then
  echo "[polaris-init] Catalog already exists. Skipping."
else
  echo "[polaris-init] Create catalog failed (HTTP $HTTP_CODE):"
  cat /tmp/resp.json || true
  exit 1
fi

echo "[polaris-init] Setting up permissions for catalog '$CATALOG' ..."

# Create a catalog admin role grants
echo "[polaris-init] Creating catalog admin role grants ..."
HTTP_CODE=$(curl -sS -o /tmp/resp.json -w "%{http_code}" \
  -X PUT "http://$HOST:$PORT/api/management/v1/catalogs/$CATALOG/catalog-roles/catalog_admin/grants" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"grant":{"type":"catalog", "privilege":"CATALOG_MANAGE_CONTENT"}}')

if [ "$HTTP_CODE" != "200" ] && [ "$HTTP_CODE" != "201" ]; then
  echo "[polaris-init] Warning: Failed to create catalog admin grants (HTTP $HTTP_CODE)"
  cat /tmp/resp.json || true
fi

# Create a data engineer role
echo "[polaris-init] Creating data engineer role ..."
HTTP_CODE=$(curl -sS -o /tmp/resp.json -w "%{http_code}" \
  -X POST "http://$HOST:$PORT/api/management/v1/principal-roles" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"principalRole":{"name":"data_engineer"}}')

if [ "$HTTP_CODE" != "200" ] && [ "$HTTP_CODE" != "201" ] && [ "$HTTP_CODE" != "409" ]; then
  echo "[polaris-init] Warning: Failed to create data engineer role (HTTP $HTTP_CODE)"
  cat /tmp/resp.json || true
fi

# Connect the roles
echo "[polaris-init] Connecting roles ..."
HTTP_CODE=$(curl -sS -o /tmp/resp.json -w "%{http_code}" \
  -X PUT "http://$HOST:$PORT/api/management/v1/principal-roles/data_engineer/catalog-roles/$CATALOG" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"catalogRole":{"name":"catalog_admin"}}')

if [ "$HTTP_CODE" != "200" ] && [ "$HTTP_CODE" != "201" ]; then
  echo "[polaris-init] Warning: Failed to connect roles (HTTP $HTTP_CODE)"
  cat /tmp/resp.json || true
fi

# Give root the data engineer role
echo "[polaris-init] Assigning data engineer role to root ..."
HTTP_CODE=$(curl -sS -o /tmp/resp.json -w "%{http_code}" \
  -X PUT "http://$HOST:$PORT/api/management/v1/principals/root/principal-roles" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"principalRole": {"name":"data_engineer"}}')

if [ "$HTTP_CODE" != "200" ] && [ "$HTTP_CODE" != "201" ]; then
  echo "[polaris-init] Warning: Failed to assign data engineer role to root (HTTP $HTTP_CODE)"
  cat /tmp/resp.json || true
fi

echo "[polaris-init] Permissions setup completed."
echo "[polaris-init] Done."

