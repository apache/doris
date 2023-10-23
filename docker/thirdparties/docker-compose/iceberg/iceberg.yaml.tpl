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

version: "3"

services:
  doris--spark-iceberg:
    image: tabulario/spark-iceberg
    container_name: doris--spark-iceberg
    hostname: doris--spark-iceberg
    build: spark/
    depends_on:
      - doris--rest
      - doris--minio
    volumes:
      - ./warehouse:/home/iceberg/warehouse
      - ./notebooks:/home/iceberg/notebooks/notebooks
      - ./entrypoint.sh:/opt/spark/entrypoint.sh
      - ./spark-defaults.conf:/opt/spark/conf/spark-defaults.conf
    environment:
      - AWS_ACCESS_KEY_ID=admin
      - AWS_SECRET_ACCESS_KEY=password
      - AWS_REGION=us-east-1
    ports:
      - ${NOTEBOOK_SERVER_PORT}:8888
      - ${SPARK_DRIVER_UI_PORT}:8080
      - ${SPARK_HISTORY_UI_PORT}:10000
    links:
      - doris--rest:rest
      - doris--minio:minio
    networks:
      - doris--iceberg
    entrypoint:
      - /opt/spark/entrypoint.sh

  doris--rest:
    image: tabulario/iceberg-rest:0.2.0
    ports:
      - ${REST_CATALOG_PORT}:8181
    environment:
      - AWS_ACCESS_KEY_ID=admin
      - AWS_SECRET_ACCESS_KEY=password
      - AWS_REGION=us-east-1
      - CATALOG_WAREHOUSE=s3a://warehouse/wh/
      - CATALOG_IO__IMPL=org.apache.iceberg.aws.s3.S3FileIO
      - CATALOG_S3_ENDPOINT=http://doris--minio:9000
    networks:
      - doris--iceberg
  doris--minio:
    image: minio/minio
    container_name: doris--minio
    hostname: doris--minio
    environment:
      - MINIO_ROOT_USER=admin
      - MINIO_ROOT_PASSWORD=password
    ports:
      - ${MINIO_UI_PORT}:9001
      - ${MINIO_API_PORT}:9000
    networks:
      - doris--iceberg
    command: ["server", "/data", "--console-address", ":9001"]
  doris--mc:
    depends_on:
      - doris--minio
    image: minio/mc
    container_name: doris--mc
    environment:
      - AWS_ACCESS_KEY_ID=admin
      - AWS_SECRET_ACCESS_KEY=password
      - AWS_REGION=us-east-1
    networks:
      - doris--iceberg
    entrypoint: >
      /bin/sh -c "
      until (/usr/bin/mc config host add minio http://doris--minio:9000 admin password) do echo '...waiting...' && sleep 1; done;
      /usr/bin/mc rm -r --force minio/warehouse;
      /usr/bin/mc mb minio/warehouse;
      /usr/bin/mc policy set public minio/warehouse;
      tail -f /dev/null
      "
networks:
  doris--iceberg:
