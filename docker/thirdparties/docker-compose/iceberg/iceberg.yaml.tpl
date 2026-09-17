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
    image: apache/spark:4.0.0
    container_name: doris--spark-iceberg
    hostname: doris--spark-iceberg
    depends_on:
      rest:
        condition: service_started
      mc:
        condition: service_completed_successfully
      spark-worker-1:
        condition: service_healthy
      spark-worker-2:
        condition: service_healthy
    volumes:
      - ./data/output/spark-warehouse:/opt/spark/warehouse
      - ./data:/mnt/data
      - ./scripts:/mnt/scripts
      - ./spark-defaults.conf:/opt/spark/conf/spark-defaults.conf
      - ./data/input/jars/iceberg-aws-bundle-1.10.1.jar:/opt/spark/jars/iceberg-aws-bundle-1.10.1.jar
      - ./data/input/jars/iceberg-spark-runtime-4.0_2.13-1.10.1.jar:/opt/spark/jars/iceberg-spark-runtime-4.0_2.13-1.10.1.jar
      - ./data/input/jars/paimon-s3-1.3.1.jar:/opt/spark/jars/paimon-s3-1.3.1.jar
      - ./data/input/jars/paimon-spark-4.0-1.3.1.jar:/opt/spark/jars/paimon-spark-4.0-1.3.1.jar
    environment:
      - AWS_ACCESS_KEY_ID=admin
      - AWS_SECRET_ACCESS_KEY=password
      - AWS_REGION=us-east-1
    ports:
      - ${SPARK_THRIFT_PORT}:10000
    entrypoint: /bin/sh /mnt/scripts/entrypoint.sh
    user: root
    networks:
      - doris--iceberg
    healthcheck:
      test: ls /mnt/SUCCESS
      interval: 5s
      timeout: 120s
      retries: 120

  spark-worker-1:
    image: apache/spark:4.0.0
    container_name: doris--spark-worker-1
    hostname: doris--spark-worker-1
    environment:
      - SPARK_WORKER_CORES=4
      - AWS_ACCESS_KEY_ID=admin
      - AWS_SECRET_ACCESS_KEY=password
      - AWS_REGION=us-east-1
    volumes:
      - ./spark-defaults.conf:/opt/spark/conf/spark-defaults.conf
      - ./data/input/jars/iceberg-aws-bundle-1.10.1.jar:/opt/spark/jars/iceberg-aws-bundle-1.10.1.jar
      - ./data/input/jars/iceberg-spark-runtime-4.0_2.13-1.10.1.jar:/opt/spark/jars/iceberg-spark-runtime-4.0_2.13-1.10.1.jar
      - ./data/input/jars/paimon-s3-1.3.1.jar:/opt/spark/jars/paimon-s3-1.3.1.jar
      - ./data/input/jars/paimon-spark-4.0-1.3.1.jar:/opt/spark/jars/paimon-spark-4.0-1.3.1.jar
    entrypoint:
      - /bin/sh
      - -c
      - /opt/spark/sbin/start-worker.sh -h doris--spark-worker-1 spark://doris--spark-iceberg:7077 && tail -f /dev/null
    user: root
    networks:
      - doris--iceberg
    healthcheck:
      test: ["CMD-SHELL", "curl -fsS http://localhost:8081 >/dev/null"]
      interval: 5s
      timeout: 10s
      retries: 60

  spark-worker-2:
    image: apache/spark:4.0.0
    container_name: doris--spark-worker-2
    hostname: doris--spark-worker-2
    environment:
      - SPARK_WORKER_CORES=4
      - AWS_ACCESS_KEY_ID=admin
      - AWS_SECRET_ACCESS_KEY=password
      - AWS_REGION=us-east-1
    volumes:
      - ./spark-defaults.conf:/opt/spark/conf/spark-defaults.conf
      - ./data/input/jars/iceberg-aws-bundle-1.10.1.jar:/opt/spark/jars/iceberg-aws-bundle-1.10.1.jar
      - ./data/input/jars/iceberg-spark-runtime-4.0_2.13-1.10.1.jar:/opt/spark/jars/iceberg-spark-runtime-4.0_2.13-1.10.1.jar
      - ./data/input/jars/paimon-s3-1.3.1.jar:/opt/spark/jars/paimon-s3-1.3.1.jar
      - ./data/input/jars/paimon-spark-4.0-1.3.1.jar:/opt/spark/jars/paimon-spark-4.0-1.3.1.jar
    entrypoint:
      - /bin/sh
      - -c
      - /opt/spark/sbin/start-worker.sh -h doris--spark-worker-2 spark://doris--spark-iceberg:7077 && tail -f /dev/null
    user: root
    networks:
      - doris--iceberg
    healthcheck:
      test: ["CMD-SHELL", "curl -fsS http://localhost:8081 >/dev/null"]
      interval: 5s
      timeout: 10s
      retries: 60

  postgres:
    image: ${ICEBERG_POSTGRES_IMAGE:-postgis/postgis:14-3.3}
    container_name: doris--iceberg-postgres
    environment:
      POSTGRES_PASSWORD: 123456
      POSTGRES_USER: root
      POSTGRES_DB: iceberg
    healthcheck:
      test: [ "CMD-SHELL", "pg_isready -U root -d iceberg" ]
      interval: 5s
      timeout: 60s
      retries: 120
    volumes:
      - ./data/input/pgdata:/var/lib/postgresql/data
    networks:
      - doris--iceberg

  rest:
    image: apache/iceberg-rest-fixture:1.10.0
    container_name: doris--iceberg-rest
    ports:
      - ${REST_CATALOG_PORT}:8181
    volumes:
      - ./data:/mnt/data
      - ./data/input/jars/postgresql-42.7.4.jar:/opt/jdbc/postgresql.jar
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
    command: 
      - java
      - -cp
      - /usr/lib/iceberg-rest/iceberg-rest-adapter.jar:/opt/jdbc/postgresql.jar
      - org.apache.iceberg.rest.RESTCatalogServer

  # Dedicated, mandatory fixture for the server-planning regression suite. Keep the
  # existing Spark/Postgres baseline on its released REST fixture.
  rest-scan-planning:
    # Content pinned by digest: upstream main b0df3ca01d61b2f7ae7143ac660c6b16e33b6e46,
    # built 2026-04-29 before the 1.11.0 release; same pin as apache/iceberg-go.
    # Replace after CI maintainers publish a verified 1.11.0 build to doristhirdpartydocker.
    image: apache/iceberg-rest-fixture:latest@sha256:db8de90b5b7693d4ac334c336f91d9bbe320d7b19f4f514d26de84cdfbcbfe8d
    container_name: doris--iceberg-rest-scan-planning
    ports:
      - ${ICEBERG_SCAN_PLANNING_REST_PORT:-18182}:8181
    depends_on:
      scan-planning-init:
        condition: service_completed_successfully
    environment:
      AWS_ACCESS_KEY_ID: admin
      AWS_SECRET_ACCESS_KEY: password
      AWS_REGION: us-east-1
      CATALOG_WAREHOUSE: s3://scan-planning/wh/
      CATALOG_IO__IMPL: org.apache.iceberg.aws.s3.S3FileIO
      CATALOG_S3_ENDPOINT: http://minio:9000
      CATALOG_S3_PATH__STYLE__ACCESS: "true"
    networks:
      - doris--iceberg

  scan-planning-init:
    image: doristhirdpartydocker/mc:RELEASE.2025-01-17T23-25-50Z
    container_name: doris--iceberg-scan-planning-init
    depends_on:
      minio:
        condition: service_healthy
    volumes:
      - ./scripts/scan-planning:/fixtures:ro
    entrypoint: ["/bin/sh", "/fixtures/init-storage.sh"]
    networks:
      - doris--iceberg

  trino:
    image: trinodb/trino:482
    container_name: doris--iceberg-trino
    depends_on:
      rest:
        condition: service_started
      mc:
        condition: service_completed_successfully
    user: root
    networks:
      - doris--iceberg
    healthcheck:
      test: ["CMD-SHELL", "test -d /etc/trino/catalog && command -v trino >/dev/null"]
      interval: 5s
      timeout: 60s
      retries: 120

  minio:
    image: doristhirdpartydocker/minio:RELEASE.2025-01-20T14-49-07Z
    container_name: doris--iceberg-minio
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
      - ./scripts/preinstalled_data/:/mnt/preinstalled_data
    networks:
      doris--iceberg:
        aliases:
          - warehouse.minio
    command: ["server", "/data", "--console-address", ":9001"]

  mc:
    depends_on:
      minio:
        condition: service_healthy
    image: doristhirdpartydocker/mc:RELEASE.2025-01-17T23-25-50Z
    container_name: doris--iceberg-mc
    environment:
      - AWS_ACCESS_KEY_ID=admin
      - AWS_SECRET_ACCESS_KEY=password
      - AWS_REGION=us-east-1
    networks:
      - doris--iceberg
    volumes:
      - ./data:/mnt/data
      - ./scripts/preinstalled_data/:/mnt/preinstalled_data
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
      fi;
      /usr/bin/mc cp -r /mnt/preinstalled_data/iceberg/ minio/warehouse/wh/multi_catalog/;
      "

networks:
  doris--iceberg:
    ipam:
      driver: default
      config:
        - subnet: 168.38.0.0/24
