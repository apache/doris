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
      rest:
        condition: service_started
      mc:
        condition: service_completed_successfully
    volumes:
      - ./data/output/spark-warehouse:/home/iceberg/warehouse
      - ./data/output/spark-notebooks:/home/iceberg/notebooks/notebooks
      - ./data:/mnt/data
      - ./scripts:/mnt/scripts
      - ./spark-defaults.conf:/opt/spark/conf/spark-defaults.conf
      - ./data/input/jars/paimon-spark-3.5-1.0.1.jar:/opt/spark/jars/paimon-spark-3.5-1.0.1.jar
      - ./data/input/jars/paimon-s3-1.0.1.jar:/opt/spark/jars/paimon-s3-1.0.1.jar
    environment:
      - AWS_ACCESS_KEY_ID=admin
      - AWS_SECRET_ACCESS_KEY=password
      - AWS_REGION=us-east-1
    entrypoint: /bin/sh /mnt/scripts/entrypoint.sh
    networks:
      - doris--iceberg
    healthcheck:
      test: ls /mnt/SUCCESS
      interval: 5s
      timeout: 120s
      retries: 120

  postgres:
    image: postgis/postgis:14-3.3
    container_name: doris--postgres
    environment:
      POSTGRES_PASSWORD: 123456
      POSTGRES_USER: root
      POSTGRES_DB: iceberg
    healthcheck:
      test: [ "CMD-SHELL", "pg_isready -U root" ]
      interval: 5s
      timeout: 60s
      retries: 120
    volumes:
      - ./data/input/pgdata:/var/lib/postgresql/data
    networks:
      - doris--iceberg

  rest:
    image: tabulario/iceberg-rest:1.6.0
    container_name: doris--iceberg-rest
    ports:
      - ${REST_CATALOG_PORT}:8181
    volumes:
      - ./data:/mnt/data
    depends_on:
      postgres:
        condition: service_healthy
      minio:
        condition: service_healthy
    environment:
      - AWS_ACCESS_KEY_ID=admin
      - AWS_SECRET_ACCESS_KEY=password
      - AWS_REGION=us-east-1
      - CATALOG_WAREHOUSE=s3a://warehouse/wh/
      - CATALOG_IO__IMPL=org.apache.iceberg.aws.s3.S3FileIO
      - CATALOG_S3_ENDPOINT=http://minio:9000
      - CATALOG_URI=jdbc:postgresql://postgres:5432/iceberg
      - CATALOG_JDBC_USER=root
      - CATALOG_JDBC_PASSWORD=123456
    networks:
      - doris--iceberg
    entrypoint: /bin/bash /mnt/data/input/script/rest_init.sh

  minio:
    image: minio/minio:RELEASE.2025-01-20T14-49-07Z
    container_name: doris--minio
    ports:
      - ${MINIO_API_PORT}:9000
    healthcheck:
      test: [ "CMD", "mc", "ready", "local" ]
      interval: 10s
      timeout: 60s
      retries: 120
    environment:
      - MINIO_ROOT_USER=admin
      - MINIO_ROOT_PASSWORD=password
      - MINIO_DOMAIN=minio
    volumes:
      - ./data/input/minio_data:/data
    networks:
      doris--iceberg:
        aliases:
          - warehouse.minio
    command: ["server", "/data", "--console-address", ":9001"]

  mc:
    depends_on:
      minio:
        condition: service_healthy
    image: minio/mc:RELEASE.2025-01-17T23-25-50Z
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
      if /usr/bin/mc ls minio/warehouse > /dev/null 2>&1; then
        echo 'minio/warehouse already exists, skipping creation and copy.';
      else
        echo 'Creating minio/warehouse and copying data...';
        /usr/bin/mc mb minio/warehouse;
        /usr/bin/mc policy set public minio/warehouse;
        /usr/bin/mc cp -r /mnt/data/input/minio/warehouse/* minio/warehouse/;
      fi
      "

networks:
  doris--iceberg:
    ipam:
      driver: default
      config:
        - subnet: 168.38.0.0/24
