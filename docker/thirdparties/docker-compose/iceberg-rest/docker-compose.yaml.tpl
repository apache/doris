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

  # OBS Catalog
  iceberg-rest-obs:
    image: apache/iceberg-rest-fixture:1.9.2
    container_name: ${CONTAINER_UID}iceberg-rest-obs
    ports:
      - "${ICEBERG_REST_OBS_PORT}:8181"
    environment:
      - AWS_ACCESS_KEY_ID=${OBSAk}
      - AWS_SECRET_ACCESS_KEY=${OBSSk}
      - AWS_REGION=${OBSRegion}
      - CATALOG_CATALOG__IMPL=org.apache.iceberg.jdbc.JdbcCatalog
      - CATALOG_URI=jdbc:sqlite:/tmp/obs_catalog.db
      - CATALOG_JDBC_USER=user
      - CATALOG_JDBC_PASSWORD=password
      - CATALOG_WAREHOUSE=s3://doris-build/iceberg_rest_warehouse/
      - CATALOG_IO__IMPL=org.apache.iceberg.aws.s3.S3FileIO
      - CATALOG_S3_ENDPOINT=https://${OBSEndpoint}
      - CATALOG_S3_REGION=${OBSRegion}
      - CATALOG_S3_PATH__STYLE__ACCESS=false
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8181/v1/config"]
      interval: 10s
      timeout: 5s
      retries: 5
    networks:
      - ${CONTAINER_UID}iceberg-rest

  # GCS Catalog
  iceberg-rest-gcs:
    image: apache/iceberg-rest-fixture:1.9.2
    container_name: ${CONTAINER_UID}iceberg-rest-gcs
    ports:
      - "${ICEBERG_REST_GCS_PORT}:8181"
    environment:
      - AWS_ACCESS_KEY_ID=${GCSAk}
      - AWS_SECRET_ACCESS_KEY=${GCSSk}
      - AWS_REGION=auto
      - CATALOG_CATALOG__IMPL=org.apache.iceberg.jdbc.JdbcCatalog
      - CATALOG_URI=jdbc:sqlite:/tmp/gcs_catalog.db
      - CATALOG_JDBC_USER=user
      - CATALOG_JDBC_PASSWORD=password
      - CATALOG_WAREHOUSE=s3://selectdb-qa-datalake-test/iceberg_rest_warehouse/
      - CATALOG_IO__IMPL=org.apache.iceberg.aws.s3.S3FileIO
      - CATALOG_S3_ENDPOINT=https://${GCSEndpoint}
      - CATALOG_S3_REGION=auto
      - CATALOG_S3_PATH__STYLE__ACCESS=true
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8181/v1/config"]
      interval: 10s
      timeout: 5s
      retries: 5
    networks:
      - ${CONTAINER_UID}iceberg-rest

  # Built-in HDFS NameNode
  hdfs-namenode:
    image: bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8
    container_name: ${CONTAINER_UID}iceberg-hdfs-namenode
    restart: always
    ports:
      - "${HDFS_NAMENODE_WEBUI_PORT}:9870"
      - "${HDFS_NAMENODE_RPC_PORT}:8020"
    environment:
      - CLUSTER_NAME=iceberg-hdfs
      - CORE_CONF_fs_defaultFS=hdfs://hdfs-namenode:8020
      - CORE_CONF_hadoop_http_staticuser_user=root
      - CORE_CONF_hadoop_proxyuser_hue_hosts=*
      - CORE_CONF_hadoop_proxyuser_hue_groups=*
      - CORE_CONF_io_compression_codecs=org.apache.hadoop.io.compress.SnappyCodec
      - HDFS_CONF_dfs_webhdfs_enabled=true
      - HDFS_CONF_dfs_permissions_enabled=false
      - HDFS_CONF_dfs_namenode_rpc_address=hdfs-namenode:8020
      - HDFS_CONF_dfs_namenode_http_address=0.0.0.0:9870
      - HDFS_CONF_dfs_replication=1
      - HDFS_CONF_dfs_namenode_name_dir=/hadoop/dfs/name
      - HDFS_CONF_dfs_namenode_datanode_registration_ip___hostname___check=false
    networks:
      - ${CONTAINER_UID}iceberg-rest
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9870/"]
      interval: 30s
      timeout: 10s
      retries: 5

  # Built-in HDFS DataNode
  hdfs-datanode:
    image: bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8
    container_name: ${CONTAINER_UID}iceberg-hdfs-datanode
    restart: always
    ports:
      - "${HDFS_DATANODE_WEBUI_PORT}:9864"
    environment:
      - CORE_CONF_fs_defaultFS=hdfs://hdfs-namenode:8020
      - CORE_CONF_hadoop_http_staticuser_user=root
      - CORE_CONF_hadoop_proxyuser_hue_hosts=*
      - CORE_CONF_hadoop_proxyuser_hue_groups=*
      - CORE_CONF_io_compression_codecs=org.apache.hadoop.io.compress.SnappyCodec
      - HDFS_CONF_dfs_webhdfs_enabled=true
      - HDFS_CONF_dfs_permissions_enabled=false
      - HDFS_CONF_dfs_datanode_data_dir=/hadoop/dfs/data
      - HDFS_CONF_dfs_replication=1
      - SERVICE_PRECONDITION=hdfs-namenode:9870
    networks:
      - ${CONTAINER_UID}iceberg-rest
    depends_on:
      hdfs-namenode:
        condition: service_healthy
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9864/"]
      interval: 30s
      timeout: 10s
      retries: 5

  # HDFS Catalog (using built-in HDFS)
  iceberg-rest-hdfs:
    image: tabulario/iceberg-rest:1.6.0
    container_name: ${CONTAINER_UID}iceberg-rest-hdfs
    ports:
      - "${ICEBERG_REST_HDFS_PORT}:8181"
    environment:
      - CATALOG_WAREHOUSE=hdfs://${LOCAL_IP}:${HDFS_NAMENODE_RPC_PORT}/user/hive/iceberg_rest_warehouse/
      - CATALOG_IO__IMPL=org.apache.iceberg.hadoop.HadoopFileIO
    networks:
      - ${CONTAINER_UID}iceberg-rest
    depends_on:
      - hdfs-namenode
      - hdfs-datanode

networks:
  ${CONTAINER_UID}iceberg-rest:
    driver: bridge
