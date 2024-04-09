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
  ${CONTAINER_UID}hadoop3-namenode:
    image: bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8
    environment:
      - CLUSTER_NAME=test
    env_file:
      - ./hadoop-hive.env
    container_name: ${CONTAINER_UID}hadoop3-namenode
    expose:
      - "${NAMENODE_HTTP_PORT}"
      - "${FS_PORT}"
    healthcheck:
      test: [ "CMD", "curl", "http://localhost:${NAMENODE_HTTP_PORT}/" ]
      interval: 5s
      timeout: 120s
      retries: 120
    network_mode: "host"

  ${CONTAINER_UID}hadoop3-datanode:
    image: bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8
    env_file:
      - ./hadoop-hive.env
    environment:
      SERVICE_PRECONDITION: "${externalEnvIp}:${NAMENODE_HTTP_PORT}"
    container_name: ${CONTAINER_UID}hadoop3-datanode
    expose:
      - "${DATANODE_HTTP_PORT}"
    healthcheck:
      test: [ "CMD", "curl", "http://localhost:${DATANODE_HTTP_PORT}" ]
      interval: 5s
      timeout: 60s
      retries: 120
    network_mode: "host"

  ${CONTAINER_UID}hive-server:
    image: lishizhen/hive:3.1.2-postgresql-metastore
    env_file:
      - ./hadoop-hive.env
    environment:
      HIVE_CORE_CONF_javax_jdo_option_ConnectionURL: "jdbc:postgresql://${externalEnvIp}:${PG_PORT}/metastore"
      SERVICE_PRECONDITION: "${externalEnvIp}:${HMS_PORT}"
    container_name: ${CONTAINER_UID}hive3-server
    expose:
      - "${HS_PORT}"
    depends_on:
      - ${CONTAINER_UID}hadoop3-datanode
      - ${CONTAINER_UID}hadoop3-namenode
    healthcheck:
      test: beeline -u "jdbc:hive2://127.0.0.1:${HS_PORT}/default" -n health_check -e "show databases;"
      interval: 10s
      timeout: 120s
      retries: 120
    network_mode: "host"


  ${CONTAINER_UID}hive3-metastore:
    image: lishizhen/hive:3.1.2-postgresql-metastore
    env_file:
      - ./hadoop-hive.env
    command: /bin/bash /mnt/scripts/hive-metastore.sh
    # command: /opt/hive/bin/hive --service metastore
    environment:
      SERVICE_PRECONDITION: "${externalEnvIp}:${NAMENODE_HTTP_PORT} ${externalEnvIp}:${DATANODE_HTTP_PORT} ${externalEnvIp}:${PG_PORT}"
    container_name: ${CONTAINER_UID}hive3-metastore
    expose:
      - "${HMS_PORT}"
    volumes:
      - ./scripts:/mnt/scripts
    depends_on:
      - ${CONTAINER_UID}hive3-metastore-postgresql
    network_mode: "host"

  ${CONTAINER_UID}hive3-metastore-postgresql:
    image: bde2020/hive-metastore-postgresql:3.1.0
    restart: always
    container_name: ${CONTAINER_UID}hive3-metastore-postgresql
    ports:
      - ${PG_PORT}:5432
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres"]
      interval: 5s
      timeout: 60s
      retries: 120
    network_mode: "bridge"
