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
#

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
# Hudi 1.0.2 supports Spark 3.5.x (default), 3.4.x, and 3.3.x
# Using Spark 3.5 bundle to match Spark 3.5.7 image (default build)
HUDI_BUNDLE_VERSION=1.0.2
HUDI_BUNDLE_URL=${HUDI_BUNDLE_URL}

# Hadoop AWS S3A filesystem (required for S3A support)
# Note: Version must match Spark's built-in Hadoop version (3.3.4 for Spark 3.5.7)
HADOOP_AWS_VERSION=3.3.4
HADOOP_AWS_URL=${HADOOP_AWS_URL}

# AWS Java SDK Bundle v1 (required for Hadoop 3.3.4 S3A support)
# Note: Hadoop 3.3.x uses AWS SDK v1, version 1.12.x is recommended
AWS_SDK_BUNDLE_VERSION=1.12.262
AWS_SDK_BUNDLE_URL=${AWS_SDK_BUNDLE_URL}

# PostgreSQL JDBC driver (required for Hive Metastore connection)
POSTGRESQL_JDBC_VERSION=42.7.1
POSTGRESQL_JDBC_URL=${POSTGRESQL_JDBC_URL}
