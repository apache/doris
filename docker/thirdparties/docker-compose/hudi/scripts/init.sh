#!/bin/bash
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

set -euo pipefail

# Remove SUCCESS file from previous run to ensure fresh initialization
SUCCESS_FILE="/opt/hudi-scripts/SUCCESS"
if [[ -f "${SUCCESS_FILE}" ]]; then
  echo "Removing previous SUCCESS file to ensure fresh initialization..."
  rm -f "${SUCCESS_FILE}"
fi

SPARK_HOME=/opt/spark
CONF_DIR="${SPARK_HOME}/conf"
JARS_DIR="${SPARK_HOME}/jars"
CACHE_DIR=/opt/hudi-cache

mkdir -p "${CONF_DIR}" "${CACHE_DIR}"

# Function to download a JAR file if it doesn't exist
download_jar() {
  local jar_name="$1"
  local version="$2"
  local url="$3"
  local jar_file="${CACHE_DIR}/${jar_name}-${version}.jar"
  
  if [[ ! -f "${jar_file}" ]]; then
    echo "Downloading ${jar_name} JAR ${version} from ${url} ..." >&2
    local download_success=false
    if command -v curl >/dev/null 2>&1; then
      if curl -sSfL "${url}" -o "${jar_file}"; then
        download_success=true
      else
        echo "Error: Failed to download ${jar_name} from ${url}" >&2
      fi
    elif command -v wget >/dev/null 2>&1; then
      if wget -qO "${jar_file}" "${url}"; then
        download_success=true
      else
        echo "Error: Failed to download ${jar_name} from ${url}" >&2
      fi
    else
      echo "Error: Neither curl nor wget is available in hudi-spark container." >&2
      exit 1
    fi
    
    if [[ "${download_success}" == "false" ]]; then
      echo "Error: Failed to download ${jar_name} JAR. Please check the URL: ${url}" >&2
      exit 1
    fi
    
    if [[ ! -f "${jar_file}" ]]; then
      echo "Error: Downloaded file ${jar_file} does not exist" >&2
      exit 1
    fi
  fi
  echo "${jar_file}"
}

# Function to link a JAR file to Spark jars directory
link_jar() {
  local jar_file="$1"
  local jar_name="$2"
  local version="$3"
  ln -sf "${jar_file}" "${JARS_DIR}/${jar_name}-${version}.jar"
}

# Wait for Hive Metastore to be ready
echo "Waiting for Hive Metastore to be ready..."
METASTORE_HOST=$(echo "${HIVE_METASTORE_URIS}" | sed 's|thrift://||' | cut -d: -f1)
METASTORE_PORT=$(echo "${HIVE_METASTORE_URIS}" | sed 's|thrift://||' | cut -d: -f2)
MAX_RETRIES=120
RETRY_COUNT=0

while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
  if command -v nc >/dev/null 2>&1; then
    if nc -z "${METASTORE_HOST}" "${METASTORE_PORT}" 2>/dev/null; then
      echo "Hive Metastore is ready at ${METASTORE_HOST}:${METASTORE_PORT}"
      break
    fi
  elif command -v timeout >/dev/null 2>&1; then
    if timeout 1 bash -c "cat < /dev/null > /dev/tcp/${METASTORE_HOST}/${METASTORE_PORT}" 2>/dev/null; then
      echo "Hive Metastore is ready at ${METASTORE_HOST}:${METASTORE_PORT}"
      break
    fi
  else
    # Fallback: just wait a bit and assume it's ready
    if [ $RETRY_COUNT -eq 0 ]; then
      echo "Warning: nc or timeout command not available, skipping metastore readiness check"
      sleep 10
      break
    fi
  fi
  
  RETRY_COUNT=$((RETRY_COUNT + 1))
  if [ $((RETRY_COUNT % 10)) -eq 0 ]; then
    echo "Waiting for Hive Metastore... (${RETRY_COUNT}/${MAX_RETRIES})"
  fi
  sleep 2
done

if [ $RETRY_COUNT -ge $MAX_RETRIES ]; then
  echo "Error: Hive Metastore did not become ready within $((MAX_RETRIES * 2)) seconds"
  exit 1
fi

# Write core-site for MinIO (S3A)
cat >"${CONF_DIR}/core-site.xml" <<EOF
<configuration>
  <property>
    <name>fs.s3a.endpoint</name>
    <value>${S3_ENDPOINT}</value>
  </property>
  <property>
    <name>fs.s3a.access.key</name>
    <value>${MINIO_ROOT_USER}</value>
  </property>
  <property>
    <name>fs.s3a.secret.key</name>
    <value>${MINIO_ROOT_PASSWORD}</value>
  </property>
  <property>
    <name>fs.s3a.path.style.access</name>
    <value>true</value>
  </property>
  <property>
    <name>fs.s3a.impl</name>
    <value>org.apache.hadoop.fs.s3a.S3AFileSystem</value>
  </property>
</configuration>
EOF

# hive-site to point Spark to the external metastore
cat >"${CONF_DIR}/hive-site.xml" <<EOF
<configuration>
  <property>
    <name>hive.metastore.uris</name>
    <value>${HIVE_METASTORE_URIS}</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionDriverName</name>
    <value>org.postgresql.Driver</value>
  </property>
  <property>
    <name>datanucleus.schema.autoCreateAll</name>
    <value>true</value>
  </property>
  <property>
    <name>hive.metastore.warehouse.dir</name>
    <value>s3a://${HUDI_BUCKET}/warehouse</value>
  </property>
</configuration>
EOF

# Download Hudi bundle
HUDI_BUNDLE_JAR_FILE=$(download_jar "hudi-spark3.5-bundle_2.12" "${HUDI_BUNDLE_VERSION}" "${HUDI_BUNDLE_URL}")
link_jar "${HUDI_BUNDLE_JAR_FILE}" "hudi-spark3.5-bundle_2.12" "${HUDI_BUNDLE_VERSION}"

# Download Hadoop AWS S3A filesystem JAR (required for S3A support)
# Note: hadoop-common is already included in Spark's built-in Hadoop, no need to download separately
HADOOP_AWS_JAR=$(download_jar "hadoop-aws" "${HADOOP_AWS_VERSION}" "${HADOOP_AWS_URL}")
link_jar "${HADOOP_AWS_JAR}" "hadoop-aws" "${HADOOP_AWS_VERSION}"

# Download AWS Java SDK Bundle v1 (required for Hadoop 3.3.6 S3A support)
# Note: Hadoop 3.3.x uses AWS SDK v1, version 1.12.262 is recommended
AWS_SDK_BUNDLE_JAR=$(download_jar "aws-java-sdk-bundle" "${AWS_SDK_BUNDLE_VERSION}" "${AWS_SDK_BUNDLE_URL}")
link_jar "${AWS_SDK_BUNDLE_JAR}" "aws-java-sdk-bundle" "${AWS_SDK_BUNDLE_VERSION}"

# Download PostgreSQL JDBC driver (required for Hive Metastore connection)
POSTGRESQL_JDBC_JAR=$(download_jar "postgresql" "${POSTGRESQL_JDBC_VERSION}" "${POSTGRESQL_JDBC_URL}")
link_jar "${POSTGRESQL_JDBC_JAR}" "postgresql" "${POSTGRESQL_JDBC_VERSION}"

# Process SQL files with environment variable substitution and execute them
# Similar to iceberg's approach: group SQL files together to reduce client creation overhead
SCRIPTS_DIR="/opt/hudi-scripts/create_preinstalled_scripts/hudi"
TEMP_SQL_DIR="/tmp/hudi_sql"

if [[ -d "${SCRIPTS_DIR}" ]]; then
  mkdir -p "${TEMP_SQL_DIR}"
  
  # Process each SQL file: substitute environment variables and combine them
  echo "Processing Hudi SQL scripts..."
  for sql_file in $(find "${SCRIPTS_DIR}" -name '*.sql' | sort); do
    echo "Processing ${sql_file}..."
    # Use sed to replace environment variables in SQL files
    # Replace ${HIVE_METASTORE_URIS} and ${HUDI_BUCKET} with actual values
    sed "s|\${HIVE_METASTORE_URIS}|${HIVE_METASTORE_URIS}|g; s|\${HUDI_BUCKET}|${HUDI_BUCKET}|g" "${sql_file}" >> "${TEMP_SQL_DIR}/hudi_total.sql"
    echo "" >> "${TEMP_SQL_DIR}/hudi_total.sql"
  done
  
  # Run Spark SQL to execute all SQL scripts
  echo "Executing Hudi SQL scripts..."
  START_TIME=$(date +%s)
  ${SPARK_HOME}/bin/spark-sql \
    --master local[*] \
    --name hudi-init \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.sql.catalogImplementation=hive \
    --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog \
    -f "${TEMP_SQL_DIR}/hudi_total.sql"
  END_TIME=$(date +%s)
  EXECUTION_TIME=$((END_TIME - START_TIME))
  echo "Hudi SQL scripts executed in ${EXECUTION_TIME} seconds"
  
  # Clean up temporary SQL file
  rm -f "${TEMP_SQL_DIR}/hudi_total.sql"
else
  echo "Warning: SQL scripts directory ${SCRIPTS_DIR} not found, skipping table initialization."
fi

# Create success marker file to indicate initialization is complete
# This file is used by docker healthcheck to verify container is ready
touch ${SUCCESS_FILE}

echo "Hudi demo data initialized."
echo "Initialization completed successfully."

# Keep container running for healthcheck and potential future use
# Similar to iceberg's approach: tail -f /dev/null
tail -f /dev/null
