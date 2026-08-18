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

version: '3.7'

services:

  ranger-admin:
    image: ghcr.io/takezoe/ranger-docker/ranger-admin:v2.4.0
    # build:
    #   context: ./ranger-admin
    #   dockerfile: Dockerfile
    container_name: ${CONTAINER_UID}-ranger-admin
    ports:
      - ${RANGER_PORT}:6080
    networks:
      - doris--ranger
    depends_on:
      ranger-mysql:
        condition: service_healthy
      ranger-solr:
        condition: service_started
    healthcheck:
      # The marker, not the admin port. `compose up --wait` returns as soon as this
      # passes, and what a caller needs is a Ranger that knows about Doris - probing
      # 6080 is probing the very URL install_doris_service_def.sh waits for, so it
      # could hand back a "healthy" stack with neither the Doris service definition
      # nor its service instance registered, and a suite starting then fails in a way
      # that looks like a policy bug. install_doris_service_def.sh writes the marker
      # once both are in place; ranger-entrypoint.sh removes it at boot so a
      # restarted container is not healthy on the previous run's marker.
      test: ["CMD-SHELL", "test -f /tmp/doris-ranger-ready"]
      interval: 10s
      timeout: 10s
      retries: 30
      # setup.sh rebuilds the whole admin install on every boot; on a cold
      # machine that runs well past 10 * 30s.
      start_period: 600s
    volumes:
      - ./ranger-admin/ranger-entrypoint.sh:/opt/ranger-entrypoint.sh
      - ./script/install_doris_ranger_plugins.sh:/opt/install_doris_ranger_plugins.sh
      - ./script/install_doris_service_def.sh:/opt/install_doris_service_def.sh
      # Doris plugin jars + service definition, fetched on the host so that a
      # flaky download cannot kill the container's `set -e` entrypoint.
      - ./cache:/opt/doris-ranger-artifacts:ro

    entrypoint : ["bash", "-c", "bash /opt/ranger-entrypoint.sh"]

  ranger-mysql:
    image: mysql:8.0.33
    container_name: ranger-mysql
    ports:
      - ${RANGER_MYSQL_PORT}:3306
    healthcheck:
      test: mysqladmin ping -h 127.0.0.1 -u root --password=root && mysql -h 127.0.0.1 -u root --password=root -e "SELECT 1 FROM mysql.innodb_table_stats;"
      interval: 5s
      timeout: 60s
      retries: 120
    networks:
      - doris--ranger
    volumes:
      - ./ranger-mysql:/etc/mysql/conf.d
    environment:
      MYSQL_ROOT_PASSWORD: root
      MYSQL_USER: rangeradmin
      MYSQL_PASSWORD: rangeradmin
      MYSQL_DATABASE: ranger

  ranger-solr:
    image: solr:8.11.2
    container_name: ranger-solr
    ports:
      - ${RANGER_SOLR_PORT}:8983
    networks:
      - doris--ranger
    volumes:
      - ./ranger-solr:/opt/solr/server/solr/configsets/ranger_audits/conf
    entrypoint:
      - solr-precreate
      - ranger_audits
      - /opt/solr/server/solr/configsets/ranger_audits

networks:
  doris--ranger:
    ipam:
      driver: default
      config:
        - subnet: 168.45.0.0/24
