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

version: "3.8"

services:
  # S3 Catalog
  iceberg-rest-s3:
    image: apache/iceberg-rest-fixture:1.9.2
    container_name: ${CONTAINER_UID}iceberg-rest-s3
    ports:
      - "${ICEBERG_REST_S3_PORT}:8181"
    environment:
      - AWS_ACCESS_KEY_ID=${AWSAk}
      - AWS_SECRET_ACCESS_KEY=${AWSSk}
      - AWS_REGION=${AWSRegion}
      - CATALOG_CATALOG__IMPL=org.apache.iceberg.jdbc.JdbcCatalog
      - CATALOG_URI=jdbc:sqlite:/tmp/s3_catalog.db
      - CATALOG_JDBC_USER=user
      - CATALOG_JDBC_PASSWORD=password
      - CATALOG_WAREHOUSE=s3://selectdb-qa-datalake-test-hk/iceberg_rest_warehouse/
      - CATALOG_IO__IMPL=org.apache.iceberg.aws.s3.S3FileIO
      - CATALOG_S3_ENDPOINT=https://${AWSEndpoint}
      - CATALOG_S3_REGION=${AWSRegion}
      - CATALOG_S3_PATH__STYLE__ACCESS=false
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8181/v1/config"]
      interval: 10s
      timeout: 5s
      retries: 5
    networks:
      - ${CONTAINER_UID}iceberg-rest

  # OSS Catalog
  iceberg-rest-oss:
    image: apache/iceberg-rest-fixture:1.9.2
    container_name: ${CONTAINER_UID}iceberg-rest-oss
    ports:
      - "${ICEBERG_REST_OSS_PORT}:8181"
    environment:
      - AWS_ACCESS_KEY_ID=${OSSAk}
      - AWS_SECRET_ACCESS_KEY=${OSSSk}
      - AWS_REGION=${OSSRegion}
      - CATALOG_CATALOG__IMPL=org.apache.iceberg.jdbc.JdbcCatalog
      - CATALOG_URI=jdbc:sqlite:/tmp/oss_catalog.db
      - CATALOG_JDBC_USER=user
      - CATALOG_JDBC_PASSWORD=password
      - CATALOG_WAREHOUSE=s3://doris-regression-bj/iceberg_rest_warehouse/
      - CATALOG_IO__IMPL=org.apache.iceberg.aws.s3.S3FileIO
      - CATALOG_S3_ENDPOINT=https://${OSSEndpoint}
      - CATALOG_S3_REGION=${OSSRegion}
      - CATALOG_S3_PATH__STYLE__ACCESS=false
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8181/v1/config"]
      interval: 10s
      timeout: 5s
      retries: 5
    networks:
      - ${CONTAINER_UID}iceberg-rest

  # COS Catalog  
  iceberg-rest-cos:
    image: apache/iceberg-rest-fixture:1.9.2
    container_name: ${CONTAINER_UID}iceberg-rest-cos
    ports:
      - "${ICEBERG_REST_COS_PORT}:8181"
    environment:
      - AWS_ACCESS_KEY_ID=${COSAk}
      - AWS_SECRET_ACCESS_KEY=${COSSk}
      - AWS_REGION=${COSRegion}
      - CATALOG_CATALOG__IMPL=org.apache.iceberg.jdbc.JdbcCatalog
      - CATALOG_URI=jdbc:sqlite:/tmp/cos_catalog.db
      - CATALOG_JDBC_USER=user
      - CATALOG_JDBC_PASSWORD=password
      - CATALOG_WAREHOUSE=s3://sdb-qa-datalake-test-1308700295/iceberg_rest_warehouse/
      - CATALOG_IO__IMPL=org.apache.iceberg.aws.s3.S3FileIO
      - CATALOG_S3_ENDPOINT=https://${COSEndpoint}
      - CATALOG_S3_REGION=${COSRegion}
      - CATALOG_S3_PATH__STYLE__ACCESS=false
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8181/v1/config"]
      interval: 10s
      timeout: 5s
      retries: 5
    networks:
      - ${CONTAINER_UID}iceberg-rest

networks:
  ${CONTAINER_UID}iceberg-rest:
    driver: bridge