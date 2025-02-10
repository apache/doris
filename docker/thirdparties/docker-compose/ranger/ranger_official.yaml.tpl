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
  ranger-zk:
    image: apache/ranger-zk:${RANGER_VERSION}
    container_name: ${CONTAINER_UID}-ranger-zk
    hostname: doris--ranger-zk
    restart: always
    ports:
      - ${RANGER_ZK_PORT}:2181
    networks:
      - doris--ranger
  ranger-solr:
    image: apache/ranger-solr:${RANGER_VERSION}
    container_name: ${CONTAINER_UID}-ranger-solr
    hostname: doris--ranger-solr
    restart: always
    ports:
      - ${RANGER_SOLAR_PORT}:8983
    command: solr-precreate ranger_audits /opt/solr/server/solr/configsets/ranger_audits/
    networks:
      - doris--ranger
  ranger-db:
    image: apache/ranger-db:${RANGER_VERSION}
    container_name: ${CONTAINER_UID}-ranger-db
    hostname: doris--ranger-db
    restart: always
    healthcheck:
      test: ["CMD-SHELL", "su -c 'pg_isready -q' postgres"]
      interval: 10s
      timeout: 2s
      retries: 30
    networks:
      - doris--ranger
  ranger:
    image: apache/ranger:${RANGER_VERSION}
    container_name: ${CONTAINER_UID}-ranger
    hostname: doris--ranger
    restart: always
    ports:
      - ${RANGER_PORT}:6080
    command: /home/ranger/scripts/ranger.sh
    networks:
      - doris--ranger

networks:
  doris--ranger:
    ipam:
      driver: default
      config:
        - subnet: 168.45.0.0/24
