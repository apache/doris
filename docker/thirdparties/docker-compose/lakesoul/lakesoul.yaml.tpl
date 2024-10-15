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

version: '3'

services:
  lakesoul-meta-db:
    image: postgres:14.5
    container_name: lakesoul-test-pg
    hostname: lakesoul-docker-compose-env-lakesoul-meta-db-1
    ports:
      - "5432:5432"
    restart: always
    environment:
      POSTGRES_PASSWORD: lakesoul_test
      POSTGRES_USER: lakesoul_test
      POSTGRES_DB: lakesoul_test
    command:
      - "postgres"
      - "-c"
      - "max_connections=4096"
      - "-c"
      - "default_transaction_isolation=serializable"
    volumes:
      - ./meta_init.sql:/docker-entrypoint-initdb.d/meta_init.sql
      - ./meta_cleanup.sql:/meta_cleanup.sql

#  minio:
#    image: bitnami/minio:latest
#    ports:
#      - "9000:9000"
#      - "9001:9001"
#    environment:
#      MINIO_DEFAULT_BUCKETS: lakesoul-test-bucket:public
#      MINIO_ROOT_USER: minioadmin1
#      MINIO_ROOT_PASSWORD: minioadmin1
#    healthcheck:
#      test: [ "CMD", "curl", "-f", "http://localhost:9000/minio/health/live" ]
#      interval: 3s
#      timeout: 5s
#      retries: 3
#    hostname: minio
#    profiles: ["s3"]


networks:
  default:
    driver: bridge
    ipam:
      driver: default
