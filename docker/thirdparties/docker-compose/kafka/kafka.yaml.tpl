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

networks:
  doris--kafka--network:
    ipam:
      driver: default
      config:
        - subnet: 192.168.0.0/24
services:
    doris--zookeeper:
        image: bitnami/zookeeper
        restart: always
        container_name: doris--zookeeper
        ports:
            - ${DOCKER_ZOOKEEPER_EXTERNAL_PORT}:2181
        environment:
            - ZOO_CFG_LISTEN_PORT=2181
            - ALLOW_ANONYMOUS_LOGIN=yes
        networks:
            - doris--kafka--network
    doris--kafka:
        image: bitnami/kafka:2
        restart: always
        container_name: doris--kafka
        depends_on:
            - doris--zookeeper
        ports:
            - ${DOCKER_KAFKA_EXTERNAL_PORT}:19193
        environment:
            - KAFKA_BROKER_ID=1
            - KAFKA_LISTENERS=PLAINTEXT://:19193
            - KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:19193
            - KAFKA_ZOOKEEPER_CONNECT=doris--zookeeper:2181
            - ALLOW_PLAINTEXT_LISTENER=yes
        volumes:
            - /var/run/docker.sock:/var/run/docker.sock
        networks:
            - doris--kafka--network
