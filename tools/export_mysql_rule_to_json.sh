#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -e

# Configuration
DB_HOST="localhost"
DB_USER="root"
DB_NAME="file_cache_admission_control"
DB_PASS=""
TABLE_NAME="admission_policy"
OUTPUT_FILE="rule_$(date +%Y%m%d_%H%M%S).json"

echo "=== Database Export Configuration ==="
echo "Database Host: $DB_HOST"
echo "Database User: $DB_USER"
echo "Database Name: $DB_NAME"
echo "Password: $(if [ -n "$DB_PASS" ]; then echo "Set"; else echo "Not set"; fi)"
echo "Table Name: $TABLE_NAME"
echo "Output File: $OUTPUT_FILE"
echo "====================================="
echo ""

# Query and convert to JSON (including long type timestamps)
QUERY=$(cat <<SQL
SELECT
    JSON_ARRAYAGG(
        JSON_OBJECT(
            'id', id,
            'user_identity', user_identity,
            'catalog_name', IFNULL(catalog_name, ''),
            'database_name', IFNULL(database_name, ''),
            'table_name', IFNULL(table_name, ''),
            'partition_pattern', IFNULL(partition_pattern, ''),
            'rule_type', rule_type,
            'enabled', CASE WHEN enabled = 1 THEN true ELSE false END,
            'created_time', UNIX_TIMESTAMP(created_time),
            'updated_time', UNIX_TIMESTAMP(updated_time)
        )
    ) AS json_data
FROM ${TABLE_NAME}
SQL
)

# Execute query
if [ -n "$DB_PASS" ]; then
    JSON_DATA=$(echo "$QUERY" | mysql -h $DB_HOST -u $DB_USER -p$DB_PASS $DB_NAME -N 2>/dev/null)
else
    JSON_DATA=$(echo "$QUERY" | mysql -h $DB_HOST -u $DB_USER $DB_NAME -N)
fi

# Handle NULL
if [ "$JSON_DATA" = "NULL" ] || [ -z "$JSON_DATA" ]; then
    JSON_DATA="[]"
fi

# Save to file
echo "$JSON_DATA" > "$OUTPUT_FILE"

# Format
if command -v jq &> /dev/null; then
    jq '.' "$OUTPUT_FILE" | awk '
        /^    {/ && NR > 3 {print ""}
        {print}
    ' > "${OUTPUT_FILE}.tmp" && mv "${OUTPUT_FILE}.tmp" "$OUTPUT_FILE"
fi

echo "Export completed: $OUTPUT_FILE"
