#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

################################################################
# This script will restart all thirdparty containers
################################################################

set -eo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

# If you want to start multi group of these containers on same host,
# Change this to a specific string.
# Do not use "_" or other sepcial characters, only number and alphabeta.
# NOTICE: change this uid will modify the file in docker-compose.
CONTAINER_UID="doris--"

# elasticsearch
# docker compose -f "${ROOT}"/docker-compose/elasticsearch/elasticsearch.yaml down
# docker compose -f "${ROOT}"/docker-compose/elasticsearch/elasticsearch.yaml up -d

# mysql 5.7
sed -i "s/doris--/${CONTAINER_UID}/g" "${ROOT}"/docker-compose/mysql/mysql-5.7.yaml
sudo docker compose -f "${ROOT}"/docker-compose/mysql/mysql-5.7.yaml --env-file "${ROOT}"/docker-compose/mysql/mysql-5.7.env down
sudo mkdir -p "${ROOT}"/docker-compose/mysql/data/
sudo rm "${ROOT}"/docker-compose/mysql/data/* -rf
sudo docker compose -f "${ROOT}"/docker-compose/mysql/mysql-5.7.yaml --env-file "${ROOT}"/docker-compose/mysql/mysql-5.7.env up -d

# pg 14
sed -i "s/doris--/${CONTAINER_UID}/g" "${ROOT}"/docker-compose/postgresql/postgresql-14.yaml
sudo docker compose -f "${ROOT}"/docker-compose/postgresql/postgresql-14.yaml --env-file "${ROOT}"/docker-compose/postgresql/postgresql-14.env down
sudo mkdir -p "${ROOT}"/docker-compose/postgresql/data/data
sudo rm "${ROOT}"/docker-compose/postgresql/data/data/* -rf
sudo docker compose -f "${ROOT}"/docker-compose/postgresql/postgresql-14.yaml --env-file "${ROOT}"/docker-compose/postgresql/postgresql-14.env up -d

# hive
# before start it, you need to download parquet file package, see "README" in "docker-compose/hive/scripts/"
sed -i "s/doris--/${CONTAINER_UID}/g" "${ROOT}"/docker-compose/hive/hive-2x.yaml
sed -i "s/doris--/${CONTAINER_UID}/g" "${ROOT}"/docker-compose/hive/hadoop-hive.env.tpl
sudo "${ROOT}"/docker-compose/hive/gen_env.sh
sudo docker compose -f "${ROOT}"/docker-compose/hive/hive-2x.yaml --env-file "${ROOT}"/docker-compose/hive/hadoop-hive.env down
sudo docker compose -f "${ROOT}"/docker-compose/hive/hive-2x.yaml --env-file "${ROOT}"/docker-compose/hive/hadoop-hive.env up -d
