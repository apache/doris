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
  doris--oracle_11:
    image: oracleinanutshell/oracle-xe-11g:latest
    restart: always
    ports:
      - ${DOCKER_ORACLE_EXTERNAL_PORT}:1521
    privileged: true
    healthcheck:
      test: [ "CMD", "bash", "-c", "echo 'SELECT 1 FROM doris_test.deadline;' | ORACLE_HOME=/u01/app/oracle/product/11.2.0/xe /u01/app/oracle/product/11.2.0/xe/bin/sqlplus -s DORIS_TEST/123456@localhost" ]
      interval: 20s
      timeout: 60s
      retries: 120
    volumes:
      - ./init:/docker-entrypoint-initdb.d
    environment:
      - ORACLE_ALLOW_REMOTE=true
      - ORACLE_ENABLE_XDB=true
      - DBCA_TOTAL_MEMORY=2048
      - IMPORT_FROM_VOLUME=true
      - TZ=Asia/Shanghai
    networks:
      - doris--oracle_11
  doris--oracle-hello-world:
    image: hello-world
    depends_on:
      doris--oracle_11:
        condition: service_healthy 
    networks:
      - doris--oracle_11

networks:
  doris--oracle_11:
    ipam:
      driver: default
      config:
        - subnet: 168.40.0.0/24
