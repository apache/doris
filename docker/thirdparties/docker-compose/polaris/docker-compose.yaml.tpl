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
  # MinIO: S3 compatible object storage for local dev
  minio:
    image: minio/minio:RELEASE.2025-01-20T14-49-07Z
    container_name: ${CONTAINER_UID}polaris-minio
    ports:
      - "${MINIO_API_PORT}:9000"
      - "${MINIO_CONSOLE_PORT}:9001"
    environment:
      - MINIO_ROOT_USER=${MINIO_ACCESS_KEY}
      - MINIO_ROOT_PASSWORD=${MINIO_SECRET_KEY}
      - MINIO_DOMAIN=minio
    command: ["server", "/data", "--console-address", ":9001"]
    healthcheck:
      test: [ "CMD", "curl", "-f", "http://localhost:9000/minio/health/ready" ]
      interval: 10s
      timeout: 60s
      retries: 30
    networks:
      ${CONTAINER_UID}polaris:
        aliases:
          - warehouse.minio

  # MinIO client to bootstrap bucket and path
  minio-client:
    image: minio/mc:RELEASE.2025-01-17T23-25-50Z
    container_name: ${CONTAINER_UID}polaris-mc
    depends_on:
      minio:
        condition: service_healthy
    entrypoint: >
      /bin/sh -c "
      until (mc alias set minio http://minio:9000 ${MINIO_ACCESS_KEY} ${MINIO_SECRET_KEY}) do echo '...waiting...' && sleep 1; done;
      mc rm -r --force minio/${MINIO_BUCKET} || echo 'warehouse bucket does not exist yet, continuing...';
      mc mb minio/${MINIO_BUCKET} || echo 'warehouse bucket already exists, skipping...';
      mc anonymous set public minio/${MINIO_BUCKET};
      echo 'MinIO setup completed successfully';
      "
    networks:
      - ${CONTAINER_UID}polaris

  # S3 Catalog with AWS S3
  polaris:
    image: apache/polaris:1.0.1-incubating
    container_name: ${CONTAINER_UID}polaris
    depends_on:
      minio:
        condition: service_healthy
      minio-client:
        condition: service_completed_successfully
    ports:
      - "${POLARIS_S3_PORT}:8181"
      - "${POLARIS_S3_ADMIN_PORT}:8182"
    environment:
      # Basic configuration
      POLARIS_BOOTSTRAP_CREDENTIALS: default-realm,root,${POLARIS_BOOTSTRAP_PASSWORD}
      polaris.features.DROP_WITH_PURGE_ENABLED: "true"
      polaris.realm-context.realms: default-realm
      
      # MinIO credentials and endpoints (S3-compatible)
      AWS_REGION: ${AWSRegion}
      AWS_ACCESS_KEY_ID: ${MINIO_ACCESS_KEY}
      AWS_SECRET_ACCESS_KEY: ${MINIO_SECRET_KEY}
      AWS_ENDPOINT_URL_S3: http://minio:9000
      AWS_ENDPOINT_URL_STS: http://minio:9000
      
      # Logging configuration
      QUARKUS_LOG_LEVEL: INFO
      QUARKUS_LOG_CATEGORY_"org.apache.polaris".LEVEL: DEBUG
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8182/q/health"]
      interval: 15s
      timeout: 10s
      retries: 5
      start_period: 30s
    networks:
      - ${CONTAINER_UID}polaris

  # Initialize a Polaris INTERNAL catalog pointing to MinIO
  polaris-init:
    image: curlimages/curl:8.11.1
    container_name: ${CONTAINER_UID}polaris-init
    depends_on:
      polaris:
        condition: service_healthy
    environment:
      POLARIS_HOST: polaris
      POLARIS_PORT: 8181
      POLARIS_BOOTSTRAP_USER: root
      POLARIS_BOOTSTRAP_PASSWORD: ${POLARIS_BOOTSTRAP_PASSWORD}
      POLARIS_CATALOG_NAME: ${POLARIS_CATALOG_NAME}
      CATALOG_BASE_LOCATION: ${CATALOG_BASE_LOCATION}
      AWSRegion: ${AWSRegion}
    volumes:
      - ./init-catalog.sh:/init-catalog.sh:ro
    entrypoint: ["/bin/sh","-c","/init-catalog.sh"]
    networks:
      - ${CONTAINER_UID}polaris

networks:
  ${CONTAINER_UID}polaris:
    name: ${CONTAINER_UID}polaris
