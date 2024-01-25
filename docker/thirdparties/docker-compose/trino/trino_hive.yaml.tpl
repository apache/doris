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

networks:
  doris--network:
    ipam:
      driver: default
      config:
        - subnet: 168.43.0.0/24

services:

  doris--trino:
    image: trinodb/trino:418
    hostname: doris--trino
    container_name: doris--trino
    env_file:
      - ./trino_hive.env
    ports:
      - "${DOCKER_TRINO_EXTERNAL_PORT}:8080"
    volumes:
          - ./scripts:/scripts
    healthcheck:
      test: [ "CMD", "curl", "-f", "http://localhost:8080/" ]
      interval: 5s
      timeout: 120s
      retries: 120
    networks:
      - doris--network

  doris--namenode:
    image: bde2020/hadoop-namenode:2.0.0-hadoop2.7.4-java8
    environment:
      - CLUSTER_NAME=test
    env_file:
      - ./trino_hive.env
    hostname: doris--namenode
    container_name: doris--namenode
    expose:
      - "50070"
      - "8020"
      - "9000"
      - "${FS_PORT}"
    ports:
      - "${FS_PORT}:${FS_PORT}"
    healthcheck:
      test: [ "CMD", "curl", "http://localhost:50070/" ]
      interval: 5s
      timeout: 120s
      retries: 120
    networks:
      - doris--network

  doris--datanode:
    image: bde2020/hadoop-datanode:2.0.0-hadoop2.7.4-java8
    env_file:
      - ./trino_hive.env
    environment:
      SERVICE_PRECONDITION: "doris--namenode:50070"
    hostname: doris--datanode
    container_name: doris--datanode
    expose:
      - "50075"
    healthcheck:
      test: [ "CMD", "curl", "http://localhost:50075" ]
      interval: 5s
      timeout: 60s
      retries: 120
    networks:
      - doris--network

  doris--hive-server:
    image: bde2020/hive:2.3.2-postgresql-metastore
    env_file:
      - ./trino_hive.env
    environment:
      HIVE_CORE_CONF_javax_jdo_option_ConnectionURL: "jdbc:postgresql://doris--hive-metastore-postgresql:5432/metastore"
      SERVICE_PRECONDITION: "doris--hive-metastore:9083"
    hostname: doris--hive-server
    container_name: doris--hive-server
    expose:
      - "10000"
    depends_on:
      - doris--datanode
      - doris--namenode
    healthcheck:
      test: beeline -u "jdbc:hive2://127.0.0.1:10000/default" -n health_check -e "show databases;"
      interval: 10s
      timeout: 120s
      retries: 120
    networks:
      - doris--network


  doris--hive-metastore:
    image: bde2020/hive:2.3.2-postgresql-metastore
    env_file:
      - ./trino_hive.env
    command: /opt/hive/bin/hive --service metastore
    environment:
      SERVICE_PRECONDITION: "doris--namenode:50070 doris--datanode:50075 doris--hive-metastore-postgresql:5432"
    hostname: doris--hive-metastore
    container_name: doris--hive-metastore
    expose:
      - "9083"
    ports:
      - "${HMS_PORT}:9083"
    volumes:
      - ./scripts:/mnt/scripts
    depends_on:
      - doris--hive-metastore-postgresql
    networks:
      - doris--network

  doris--hive-metastore-postgresql:
    image: bde2020/hive-metastore-postgresql:2.3.0
    restart: always
    hostname: doris--hive-metastore-postgresql
    container_name: doris--hive-metastore-postgresql
    expose:
      - "5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres"]
      interval: 5s
      timeout: 60s
      retries: 120
    networks:
      - doris--network
