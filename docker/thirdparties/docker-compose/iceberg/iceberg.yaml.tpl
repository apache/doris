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
      - mc
    volumes:
      - ./data/output/spark-warehouse:/home/iceberg/warehouse
      - ./data/output/spark-notebooks:/home/iceberg/notebooks/notebooks
      - ./data:/mnt/data
      - ./scripts:/mnt/scripts
      - ./spark-defaults.conf:/opt/spark/conf/spark-defaults.conf
      - ./data/input/jars/paimon-spark-3.5-0.8.0.jar:/opt/spark/jars/paimon-spark-3.5-0.8.0.jar
      - ./data/input/jars/paimon-s3-0.8.0.jar:/opt/spark/jars/paimon-s3-0.8.0.jar
    environment:
      - AWS_ACCESS_KEY_ID=admin
      - AWS_SECRET_ACCESS_KEY=password
      - AWS_REGION=us-east-1
    entrypoint: /bin/sh /mnt/scripts/entrypoint.sh 
    networks:
      - doris--iceberg
    healthcheck:
      test: ["CMD", "ls", "/mnt/SUCCESS"]
      interval: 5s
      timeout: 120s
      retries: 120
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
    ports:
      - ${MINIO_API_PORT}:9000
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
  
  iceberg-hello-world:
    image: hello-world
    container_name: doris--iceberg-hello-world
    depends_on:
      spark-iceberg:
        condition: service_healthy
    network_mode: "host"

networks:
  doris--iceberg:
    ipam:
      driver: default
      config:
        - subnet: 168.38.0.0/24