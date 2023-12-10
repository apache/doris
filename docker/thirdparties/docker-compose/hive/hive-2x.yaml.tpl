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
  doris--namenode:
    image: bde2020/hadoop-namenode:2.0.0-hadoop2.7.4-java8
    environment:
      - CLUSTER_NAME=test
    env_file:
      - ./hadoop-hive.env
    container_name: doris--namenode
    expose:
      - "50070"
      - "8020"
      - "9000"
    healthcheck:
      test: [ "CMD", "curl", "http://localhost:50070/" ]
      interval: 5s
      timeout: 120s
      retries: 120
    network_mode: "host"

  doris--datanode:
    image: bde2020/hadoop-datanode:2.0.0-hadoop2.7.4-java8
    env_file:
      - ./hadoop-hive.env
    environment:
      SERVICE_PRECONDITION: "externalEnvIp:50070"
    container_name: doris--datanode
    expose:
      - "50075"
    healthcheck:
      test: [ "CMD", "curl", "http://localhost:50075" ]
      interval: 5s
      timeout: 60s
      retries: 120
    network_mode: "host"

  doris--hive-server:
    image: bde2020/hive:2.3.2-postgresql-metastore
    env_file:
      - ./hadoop-hive.env
    environment:
      HIVE_CORE_CONF_javax_jdo_option_ConnectionURL: "jdbc:postgresql://externalEnvIp:5432/metastore"
      SERVICE_PRECONDITION: "externalEnvIp:9083"
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
    network_mode: "host"


  doris--hive-metastore:
    image: bde2020/hive:2.3.2-postgresql-metastore
    env_file:
      - ./hadoop-hive.env
    command: /bin/bash /mnt/scripts/hive-metastore.sh
    # command: /opt/hive/bin/hive --service metastore
    environment:
      SERVICE_PRECONDITION: "externalEnvIp:50070 externalEnvIp:50075 externalEnvIp:5432"
    container_name: doris--hive-metastore
    expose:
      - "9083"
    volumes:
      - ./scripts:/mnt/scripts
    depends_on:
      - doris--hive-metastore-postgresql
    network_mode: "host"

  doris--hive-metastore-postgresql:
    image: bde2020/hive-metastore-postgresql:2.3.0
    restart: always
    container_name: doris--hive-metastore-postgresql
    expose:
      - "5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres"]
      interval: 5s
      timeout: 60s
      retries: 120
    network_mode: "host"
