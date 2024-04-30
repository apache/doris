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
    ports:
      - "${FS_PORT}:8020"
    healthcheck:
      test: [ "CMD", "curl", "http://localhost:9870/" ]
      interval: 5s
      timeout: 120s
      retries: 120

  datanode:
    image: bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8
    env_file:
      - ./hadoop-hive.env
    environment:
      SERVICE_PRECONDITION: "namenode:9870"
    container_name: ${CONTAINER_UID}hadoop3-datanode
    healthcheck:
      test: [ "CMD", "curl", "http://localhost:9864" ]
      interval: 5s
      timeout: 60s
      retries: 120

  hive-server:
    image: lishizhen/hive:3.1.2-postgresql-metastore
    env_file:
      - ./hadoop-hive-metastore.env
    environment:
      HIVE_CORE_CONF_javax_jdo_option_ConnectionURL: "jdbc:postgresql://hive-metastore/metastore"
      SERVICE_PRECONDITION: "hive-metastore:9083"
    container_name: ${CONTAINER_UID}hive3-server
    ports:
      - "${HS_PORT}:10000"
    depends_on:
      - datanode
      - namenode
    healthcheck:
      test: beeline -u "jdbc:hive2://127.0.0.1:10000/default" -n health_check -e "show databases;"
      interval: 10s
      timeout: 120s
      retries: 120


  hive-metastore:
    image: lishizhen/hive:3.1.2-postgresql-metastore
    env_file:
      - ./hadoop-hive-metastore.env
    command: /bin/bash /mnt/scripts/hive-metastore.sh
    # command: /opt/hive/bin/hive --service metastore
    environment:
      SERVICE_PRECONDITION: "namenode:9870 datanode:9864 hive-metastore-postgresql:5432"
    container_name: ${CONTAINER_UID}hive3-metastore
    ports:
      - "${HMS_PORT}:9083"
    volumes:
      - ./scripts:/mnt/scripts
    depends_on:
      - hive-metastore-postgresql

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

# solve HiveServer2 connect error:
# java.net.URISyntaxException Illegal character in hostname :thrift://${CONTAINER_UID}hive3_default:9083

networks:
  default:
    name: ${CONTAINER_UID}hive3-default
    ipam:
      driver: default
      config: 
        - subnet: 168.59.0.0/24
