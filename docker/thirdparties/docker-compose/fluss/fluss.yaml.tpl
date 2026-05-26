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
  doris--fluss--network:
    ipam:
      driver: default
      config:
        - subnet: 168.52.0.0/24

services:
    doris--fluss-zookeeper:
        image: zookeeper:3.9.2
        restart: always
        container_name: doris--fluss-zookeeper
        ports:
            - ${DOCKER_FLUSS_ZOOKEEPER_EXTERNAL_PORT}:2181
        environment:
            - ALLOW_ANONYMOUS_LOGIN=yes
            - ZOO_4LW_COMMANDS_WHITELIST=ruok,stat
        healthcheck:
            test: ["CMD-SHELL", "echo ruok | nc -w 2 localhost 2181 | grep -q imok"]
            interval: 5s
            timeout: 10s
            retries: 30
            start_period: 10s
        networks:
            - doris--fluss--network

    doris--fluss-coordinator:
        image: apache/fluss:${FLUSS_IMAGE_VERSION}
        restart: always
        container_name: doris--fluss-coordinator
        command: coordinatorServer
        depends_on:
            doris--fluss-zookeeper:
                condition: service_healthy
        environment:
            - |
                FLUSS_PROPERTIES=
                zookeeper.address: doris--fluss-zookeeper:2181
                bind.listeners: FLUSS://doris--fluss-coordinator:9123
                remote.data.dir: /tmp/fluss/remote-data
        healthcheck:
            test: ["CMD-SHELL", "bash -c '</dev/tcp/localhost/9123' 2>/dev/null"]
            interval: 5s
            timeout: 10s
            retries: 60
            start_period: 15s
        networks:
            - doris--fluss--network

    doris--fluss-tablet-server:
        image: apache/fluss:${FLUSS_IMAGE_VERSION}
        restart: always
        container_name: doris--fluss-tablet-server
        command: tabletServer
        depends_on:
            doris--fluss-coordinator:
                condition: service_healthy
        ports:
            - ${DOCKER_FLUSS_EXTERNAL_PORT}:9123
        environment:
            - |
                FLUSS_PROPERTIES=
                zookeeper.address: doris--fluss-zookeeper:2181
                bind.listeners: FLUSS://doris--fluss-tablet-server:9123
                internal.listener.name: FLUSS
                advertised.listeners: FLUSS://localhost:${DOCKER_FLUSS_EXTERNAL_PORT}
                data.dir: /tmp/fluss/data
                remote.data.dir: /tmp/fluss/remote-data
        healthcheck:
            test: ["CMD-SHELL", "bash -c '</dev/tcp/localhost/9123' 2>/dev/null"]
            interval: 5s
            timeout: 10s
            retries: 60
            start_period: 15s
        networks:
            - doris--fluss--network
