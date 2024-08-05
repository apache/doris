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
  doris--db2_11:
    image: icr.io/db2_community/db2:11.5.9.0
    ports:
      - ${DOCKER_DB2_EXTERNAL_PORT}:50000
    privileged: true
    healthcheck:
      test: ["CMD-SHELL", "su - db2inst1 -c \"source ~/.bash_profile; db2 connect to doris && db2 'select 1 from sysibm.sysdummy1'\""]
      interval: 20s
      timeout: 60s
      retries: 10
    volumes:
      - ./init:/docker-entrypoint-initdb.d
    environment:
      - LICENSE=accept
      - DBNAME=doris
      - DB2INSTANCE=db2inst1
      - DB2INST1_PASSWORD=123456
      - TZ=Asia/Shanghai
    restart: always
    networks:
      - doris--db2_network
  db2-hello-world:
    image: hello-world
    depends_on:
      doris--db2_11:
        condition: service_healthy
    networks:
      - doris--db2_network
networks:
  doris--db2_network:
    ipam:
      driver: default
      config:
        - subnet: 168.50.0.0/24
