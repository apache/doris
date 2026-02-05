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

networks:
  ${HUDI_NETWORK}:
    name: ${HUDI_NETWORK}
    ipam:
      driver: default
      config:
        - subnet: 168.3.0.0/24

services:
  ${CONTAINER_UID}hudi-minio:
    image: minio/minio:RELEASE.2025-01-20T14-49-07Z
    container_name: ${CONTAINER_UID}hudi-minio
    command: server /data --console-address ":${MINIO_CONSOLE_PORT}"
    environment:
      MINIO_ROOT_USER: ${MINIO_ROOT_USER}
      MINIO_ROOT_PASSWORD: ${MINIO_ROOT_PASSWORD}
    ports:
      - "${MINIO_API_PORT}:9000"
      - "${MINIO_CONSOLE_PORT}:9001"
    networks:
      - ${HUDI_NETWORK}

  ${CONTAINER_UID}hudi-minio-mc:
    image: minio/mc:RELEASE.2025-01-17T23-25-50Z
    container_name: ${CONTAINER_UID}hudi-minio-mc
    entrypoint: |
      /bin/bash -c "
      set -euo pipefail
      sleep 5
      mc alias set myminio http://${CONTAINER_UID}hudi-minio:9000 ${MINIO_ROOT_USER} ${MINIO_ROOT_PASSWORD}
      mc mb --quiet myminio/${HUDI_BUCKET} || true
      mc mb --quiet myminio/${HUDI_BUCKET}-tmp || true
      "
    depends_on:
      - ${CONTAINER_UID}hudi-minio
    networks:
      - ${HUDI_NETWORK}

  ${CONTAINER_UID}hudi-metastore-db:
    image: postgres:14
    container_name: ${CONTAINER_UID}hudi-metastore-db
    environment:
      POSTGRES_USER: hive
      POSTGRES_PASSWORD: hive
      POSTGRES_DB: metastore
    networks:
      - ${HUDI_NETWORK}

  ${CONTAINER_UID}hudi-metastore:
    image: starburstdata/hive:3.1.2-e.18
    container_name: ${CONTAINER_UID}hudi-metastore
    hostname: ${CONTAINER_UID}hudi-metastore
    environment:
      HIVE_METASTORE_DRIVER: org.postgresql.Driver
      HIVE_METASTORE_JDBC_URL: jdbc:postgresql://${CONTAINER_UID}hudi-metastore-db:5432/metastore
      HIVE_METASTORE_USER: hive
      HIVE_METASTORE_PASSWORD: hive
      HIVE_METASTORE_WAREHOUSE_DIR: s3a://${HUDI_BUCKET}/warehouse
      S3_ENDPOINT: http://${CONTAINER_UID}hudi-minio:9000
      S3_ACCESS_KEY: ${MINIO_ROOT_USER}
      S3_SECRET_KEY: ${MINIO_ROOT_PASSWORD}
      S3_PATH_STYLE_ACCESS: "true"
      REGION: "us-east-1"
      GOOGLE_CLOUD_KEY_FILE_PATH: ""
      AZURE_ADL_CLIENT_ID: ""
      AZURE_ADL_CREDENTIAL: ""
      AZURE_ADL_REFRESH_URL: ""
      AZURE_ABFS_STORAGE_ACCOUNT: ""
      AZURE_ABFS_ACCESS_KEY: ""
      AZURE_WASB_STORAGE_ACCOUNT: ""
      AZURE_ABFS_OAUTH: ""
      AZURE_ABFS_OAUTH_TOKEN_PROVIDER: ""
      AZURE_ABFS_OAUTH_CLIENT_ID: ""
      AZURE_ABFS_OAUTH_SECRET: ""
      AZURE_ABFS_OAUTH_ENDPOINT: ""
      AZURE_WASB_ACCESS_KEY: ""
      HIVE_METASTORE_USERS_IN_ADMIN_ROLE: "hive"
    depends_on:
      - ${CONTAINER_UID}hudi-metastore-db
      - ${CONTAINER_UID}hudi-minio
    ports:
      - "${HIVE_METASTORE_PORT}:9083"
    networks:
      - ${HUDI_NETWORK}

  ${CONTAINER_UID}hudi-spark:
    image: spark:3.5.7-scala2.12-java17-ubuntu
    container_name: ${CONTAINER_UID}hudi-spark
    hostname: ${CONTAINER_UID}hudi-spark
    user: root
    environment:
      HUDI_BUNDLE_VERSION: ${HUDI_BUNDLE_VERSION}
      HUDI_BUNDLE_URL: ${HUDI_BUNDLE_URL}
      HADOOP_AWS_VERSION: ${HADOOP_AWS_VERSION}
      HADOOP_AWS_URL: ${HADOOP_AWS_URL}
      AWS_SDK_BUNDLE_VERSION: ${AWS_SDK_BUNDLE_VERSION}
      AWS_SDK_BUNDLE_URL: ${AWS_SDK_BUNDLE_URL}
      POSTGRESQL_JDBC_VERSION: ${POSTGRESQL_JDBC_VERSION}
      POSTGRESQL_JDBC_URL: ${POSTGRESQL_JDBC_URL}
      MINIO_ROOT_USER: ${MINIO_ROOT_USER}
      MINIO_ROOT_PASSWORD: ${MINIO_ROOT_PASSWORD}
      HUDI_BUCKET: ${HUDI_BUCKET}
      HIVE_METASTORE_URIS: thrift://${CONTAINER_UID}hudi-metastore:9083
      S3_ENDPOINT: http://${CONTAINER_UID}hudi-minio:9000
    volumes:
      - ./scripts:/opt/hudi-scripts
      - ./cache:/opt/hudi-cache
    depends_on:
      - ${CONTAINER_UID}hudi-minio
      - ${CONTAINER_UID}hudi-minio-mc
      - ${CONTAINER_UID}hudi-metastore
    command: ["/opt/hudi-scripts/init.sh"]
    ports:
      - "${SPARK_UI_PORT}:8080"
    healthcheck:
      test: ["CMD", "test", "-f", "/opt/hudi-scripts/SUCCESS"]
      interval: 5s
      timeout: 10s
      retries: 120
      start_period: 30s
    networks:
      - ${HUDI_NETWORK}
