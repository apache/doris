CONTAINER_UID=doris--
HUDI_NETWORK=${CONTAINER_UID}hudi-network

# Ports exposed to host
HIVE_METASTORE_PORT=19083
MINIO_API_PORT=19100
MINIO_CONSOLE_PORT=19101
SPARK_UI_PORT=18080

# MinIO credentials/buckets
MINIO_ROOT_USER=minio
MINIO_ROOT_PASSWORD=minio123
HUDI_BUCKET=datalake

# Hudi bundle
HUDI_BUNDLE_VERSION=0.15.2
HUDI_BUNDLE_URL=https://repo1.maven.org/maven2/org/apache/hudi/hudi-spark3.4-bundle_2.12/0.15.2/hudi-spark3.4-bundle_2.12-0.15.2.jar
