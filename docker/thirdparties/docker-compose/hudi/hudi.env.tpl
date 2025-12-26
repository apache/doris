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
HUDI_BUNDLE_VERSION=1.1.1
HUDI_BUNDLE_URL=https://repo1.maven.org/maven2/org/apache/hudi/hudi-spark3.5-bundle_2.12/1.1.1/hudi-spark3.5-bundle_2.12-1.1.1.jar

# Hadoop AWS S3A filesystem (required for S3A support)
# Note: Version must match Spark's built-in Hadoop version (3.3.4 for Spark 3.5.7)
HADOOP_AWS_VERSION=3.3.4
HADOOP_AWS_URL=https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar

# Hadoop Common (required dependency for hadoop-aws)
# Note: Version must match Spark's built-in Hadoop version (3.3.4 for Spark 3.5.7)
HADOOP_COMMON_VERSION=3.3.4
HADOOP_COMMON_URL=https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.3.4/hadoop-common-3.3.4.jar

# AWS Java SDK Bundle (required for S3A support)
AWS_SDK_BUNDLE_VERSION=1.12.262
AWS_SDK_BUNDLE_URL=https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# PostgreSQL JDBC driver (required for Hive Metastore connection)
POSTGRESQL_JDBC_VERSION=42.7.1
POSTGRESQL_JDBC_URL=https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.1/postgresql-42.7.1.jar
