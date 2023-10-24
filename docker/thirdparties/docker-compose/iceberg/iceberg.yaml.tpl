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
  spark-iceberg:
    image: tabulario/spark-iceberg
    container_name: doris--spark-iceberg
    hostname: doris--spark-iceberg
    build: spark/
    depends_on:
      - rest
      - minio
    volumes:
      - ./data/output/spark-warehouse:/home/iceberg/warehouse
      - ./data/output/spark-notebooks:/home/iceberg/notebooks/notebooks
      - ./data:/mnt/data
    environment:
      - AWS_ACCESS_KEY_ID=admin
      - AWS_SECRET_ACCESS_KEY=password
      - AWS_REGION=us-east-1
    networks:
      - doris--iceberg

  rest:
    image: tabulario/iceberg-rest
    container_name: doris--iceberg-rest
    ports:
      - ${REST_CATALOG_PORT}:8181
    volumes:
      - ./data:/mnt/data
    environment:
      - AWS_ACCESS_KEY_ID=admin
      - AWS_SECRET_ACCESS_KEY=password
      - AWS_REGION=us-east-1
      - CATALOG_WAREHOUSE=s3a://warehouse/wh/
      - CATALOG_IO__IMPL=org.apache.iceberg.aws.s3.S3FileIO
      - CATALOG_S3_ENDPOINT=http://minio:9000
    networks:
      - doris--iceberg
    entrypoint: /bin/bash /mnt/data/input/script/rest_init.sh

  minio:
    image: minio/minio
    container_name: doris--minio
    environment:
      - MINIO_ROOT_USER=admin
      - MINIO_ROOT_PASSWORD=password
      - MINIO_DOMAIN=minio
    networks:
      doris--iceberg:
        aliases:
          - warehouse.minio
    command: ["server", "/data", "--console-address", ":9001"]

  mc:
    depends_on:
      - minio
    image: minio/mc
    container_name: doris--mc
    environment:
      - AWS_ACCESS_KEY_ID=admin
      - AWS_SECRET_ACCESS_KEY=password
      - AWS_REGION=us-east-1
    networks:
      - doris--iceberg
    volumes:
      - ./data:/mnt/data
    entrypoint: >
      /bin/sh -c "
      until (/usr/bin/mc config host add minio http://minio:9000 admin password) do echo '...waiting...' && sleep 1; done;
      /usr/bin/mc rm -r --force minio/warehouse;
      /usr/bin/mc mb minio/warehouse;
      /usr/bin/mc policy set public minio/warehouse;
      echo 'copy data';
      mc cp -r /mnt/data/input/minio/warehouse/* minio/warehouse/;
      tail -f /dev/null
      "
networks:
  doris--iceberg:
