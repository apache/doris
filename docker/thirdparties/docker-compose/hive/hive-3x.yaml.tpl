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
  namenode:
    image: bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8
    environment:
      - CLUSTER_NAME=test
    env_file:
      - ./hadoop-hive.env
    container_name: ${CONTAINER_UID}hadoop3-namenode
    expose:
      - "9870"
      - "${FS_PORT}"
    healthcheck:
      test: [ "CMD", "curl", "http://localhost:9870/" ]
      interval: 5s
      timeout: 120s
      retries: 120
    network_mode: "host"

  datanode:
    image: bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8
    env_file:
      - ./hadoop-hive.env
    environment:
      SERVICE_PRECONDITION: "${IP_HOST}:9870"
    container_name: ${CONTAINER_UID}hadoop3-datanode
    expose:
      - "9864"
    healthcheck:
      test: [ "CMD", "curl", "http://localhost:9864" ]
      interval: 5s
      timeout: 60s
      retries: 120
    network_mode: "host"

  hive-server:
    image: doristhirdpartydocker/hive:3.1.2-postgresql-metastore
    env_file:
      - ./hadoop-hive.env
    environment:
      HIVE_CORE_CONF_javax_jdo_option_ConnectionURL: "jdbc:postgresql://${IP_HOST}:${PG_PORT}/metastore"
      SERVICE_PRECONDITION: "${IP_HOST}:${HMS_PORT}"
    container_name: ${CONTAINER_UID}hive3-server
    expose:
      - "${HS_PORT}"
    depends_on:
      - datanode
      - namenode
    healthcheck:
      test: beeline -u "jdbc:hive2://127.0.0.1:${HS_PORT}/default" -n health_check -e "show databases;"
      interval: 10s
      timeout: 120s
      retries: 120
    network_mode: "host"


  hive-metastore:
    image: doristhirdpartydocker/hive:3.1.2-postgresql-metastore
    env_file:
      - ./hadoop-hive.env
    command: /bin/bash /mnt/scripts/hive-metastore.sh
    environment:
      SERVICE_PRECONDITION: "${IP_HOST}:9870 ${IP_HOST}:9864 ${IP_HOST}:${PG_PORT}"
    container_name: ${CONTAINER_UID}hive3-metastore
    expose:
      - "${HMS_PORT}"
    volumes:
      - ./scripts:/mnt/scripts
    depends_on:
      - hive-metastore-postgresql
    healthcheck:
      test: ["CMD", "sh", "-c", "/mnt/scripts/healthy_check.sh"]
      interval: 20s
      timeout: 60s
      retries: 120
    network_mode: "host"

  hive-metastore-postgresql:
    image: bde2020/hive-metastore-postgresql:3.1.0
    container_name: ${CONTAINER_UID}hive3-metastore-postgresql
    ports:
      - "${PG_PORT}:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres"]
      interval: 5s
      timeout: 60s
      retries: 120

  hive-hello-world:
    image: hello-world
    container_name: ${CONTAINER_UID}hive3-hello-world
    depends_on:
      hive-metastore:
        condition: service_healthy
    network_mode: "host"
