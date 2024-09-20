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

version: "2.1"

services:
  doris--clickhouse:
    image: "clickhouse/clickhouse-server:23.8"
    restart: always
    environment:
      CLICKHOUSE_PASSWORD: 123456
      CLICKHOUSE_ALWAYS_RUN_INITDB_SCRIPTS: "true" # Add this line to always run init scripts
    ulimits:
      nofile:
        soft: 262144
        hard: 262144
    ports:
      - ${DOCKER_CLICKHOUSE_EXTERNAL_HTTP_PORT}:8123
    healthcheck:
      test: ["CMD-SHELL", "clickhouse-client --password=123456 --query 'SELECT 1 FROM doris_test.deadline'"]
      interval: 30s
      timeout: 10s
      retries: 5
    volumes:
      - ./init:/docker-entrypoint-initdb.d
    networks:
      - doris--clickhouse
  doris--clickhouse-hello-world:
    image: hello-world
    depends_on:
      doris--clickhouse:
        condition: service_healthy
    networks:
      - doris--clickhouse

networks:
  doris--clickhouse:
    ipam:
      driver: default
      config:
        - subnet: 168.35.0.0/24
