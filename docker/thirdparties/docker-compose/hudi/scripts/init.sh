#!/usr/bin/env bash
set -euo pipefail

SPARK_HOME=/opt/bitnami/spark
CONF_DIR="${SPARK_HOME}/conf"
JARS_DIR="${SPARK_HOME}/jars"
CACHE_DIR=/opt/hudi-cache
HUDI_BUNDLE_JAR="${CACHE_DIR}/hudi-spark3.4-bundle_2.12-${HUDI_BUNDLE_VERSION}.jar"

mkdir -p "${CONF_DIR}" "${CACHE_DIR}"

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

# Download Hudi bundle if missing
if [[ ! -f "${HUDI_BUNDLE_JAR}" ]]; then
  echo "Downloading Hudi bundle ${HUDI_BUNDLE_VERSION} ..."
  if command -v curl >/dev/null 2>&1; then
    curl -sSfL "${HUDI_BUNDLE_URL}" -o "${HUDI_BUNDLE_JAR}"
  elif command -v wget >/dev/null 2>&1; then
    wget -qO "${HUDI_BUNDLE_JAR}" "${HUDI_BUNDLE_URL}"
  else
    echo "Neither curl nor wget is available in hudi-spark container." >&2
    exit 1
  fi
fi
ln -sf "${HUDI_BUNDLE_JAR}" "${JARS_DIR}/hudi-spark3.4-bundle_2.12-${HUDI_BUNDLE_VERSION}.jar"

# Prepare Spark SQL to create demo tables
cat >/tmp/hudi_init.sql <<SQL
SET hoodie.datasource.write.hive_style_partitioning = true;
SET hoodie.upsert.shuffle.parallelism = 2;
SET hoodie.insert.shuffle.parallelism = 2;

CREATE DATABASE IF NOT EXISTS regression_hudi;
USE regression_hudi;

CREATE TABLE IF NOT EXISTS user_activity_log_cow_partition (
  user_id BIGINT,
  event_time BIGINT,
  action STRING,
  dt STRING
) USING hudi
TBLPROPERTIES (
  type = 'cow',
  primaryKey = 'user_id',
  preCombineField = 'event_time',
  hoodie.datasource.hive_sync.enable = 'true',
  hoodie.datasource.hive_sync.metastore.uris = '${HIVE_METASTORE_URIS}',
  hoodie.datasource.hive_sync.mode = 'hms',
  hoodie.datasource.hive_sync.support_timestamp = 'true'
)
PARTITIONED BY (dt)
LOCATION 's3a://${HUDI_BUCKET}/warehouse/regression_hudi/user_activity_log_cow_partition';

CREATE TABLE IF NOT EXISTS user_activity_log_cow_non_partition (
  user_id BIGINT,
  event_time BIGINT,
  action STRING
) USING hudi
TBLPROPERTIES (
  type = 'cow',
  primaryKey = 'user_id',
  preCombineField = 'event_time',
  hoodie.datasource.hive_sync.enable = 'true',
  hoodie.datasource.hive_sync.metastore.uris = '${HIVE_METASTORE_URIS}',
  hoodie.datasource.hive_sync.mode = 'hms',
  hoodie.datasource.hive_sync.support_timestamp = 'true'
)
LOCATION 's3a://${HUDI_BUCKET}/warehouse/regression_hudi/user_activity_log_cow_non_partition';

CREATE TABLE IF NOT EXISTS user_activity_log_mor_partition (
  user_id BIGINT,
  event_time BIGINT,
  action STRING,
  dt STRING
) USING hudi
TBLPROPERTIES (
  type = 'mor',
  primaryKey = 'user_id',
  preCombineField = 'event_time',
  hoodie.compact.inline = 'true',
  hoodie.compact.inline.max.delta.commits = '1',
  hoodie.datasource.hive_sync.enable = 'true',
  hoodie.datasource.hive_sync.metastore.uris = '${HIVE_METASTORE_URIS}',
  hoodie.datasource.hive_sync.mode = 'hms',
  hoodie.datasource.hive_sync.support_timestamp = 'true'
)
PARTITIONED BY (dt)
LOCATION 's3a://${HUDI_BUCKET}/warehouse/regression_hudi/user_activity_log_mor_partition';

CREATE TABLE IF NOT EXISTS user_activity_log_mor_non_partition (
  user_id BIGINT,
  event_time BIGINT,
  action STRING
) USING hudi
TBLPROPERTIES (
  type = 'mor',
  primaryKey = 'user_id',
  preCombineField = 'event_time',
  hoodie.compact.inline = 'true',
  hoodie.compact.inline.max.delta.commits = '1',
  hoodie.datasource.hive_sync.enable = 'true',
  hoodie.datasource.hive_sync.metastore.uris = '${HIVE_METASTORE_URIS}',
  hoodie.datasource.hive_sync.mode = 'hms',
  hoodie.datasource.hive_sync.support_timestamp = 'true'
)
LOCATION 's3a://${HUDI_BUCKET}/warehouse/regression_hudi/user_activity_log_mor_non_partition';

INSERT OVERWRITE TABLE user_activity_log_cow_partition VALUES
  (1, 1710000000000, 'login', '2024-03-01'),
  (2, 1710000001000, 'click', '2024-03-01'),
  (3, 1710000002000, 'logout', '2024-03-02');

INSERT OVERWRITE TABLE user_activity_log_cow_non_partition VALUES
  (1, 1710000000000, 'login'),
  (2, 1710000001000, 'click'),
  (3, 1710000002000, 'logout');

INSERT OVERWRITE TABLE user_activity_log_mor_partition VALUES
  (1, 1710000000000, 'login', '2024-03-01'),
  (2, 1710000001000, 'click', '2024-03-01'),
  (3, 1710000002000, 'logout', '2024-03-02');

INSERT OVERWRITE TABLE user_activity_log_mor_non_partition VALUES
  (1, 1710000000000, 'login'),
  (2, 1710000001000, 'click'),
  (3, 1710000002000, 'logout');
SQL

# Run Spark SQL to build tables
${SPARK_HOME}/bin/spark-sql \
  --master local[*] \
  --name hudi-init \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.sql.catalogImplementation=hive \
  --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog \
  -f /tmp/hudi_init.sql

echo "Hudi demo data initialized."
