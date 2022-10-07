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

# elasticsearch
docker compose -f "${ROOT}"/docker-compose/elasticsearch/elasticsearch.yaml down
docker compose -f "${ROOT}"/docker-compose/elasticsearch/elasticsearch.yaml up -d

# mysql 5.7
mkdir -p "${ROOT}"/docker-compose/mysql/data/
rm "${ROOT}"/docker-compose/mysql/data/* -rf
docker compose -f "${ROOT}"/docker-compose/mysql/mysql-5.7.yaml --env-file "${ROOT}"/docker-compose/mysql/mysql-5.7.env down
docker compose -f "${ROOT}"/docker-compose/mysql/mysql-5.7.yaml --env-file "${ROOT}"/docker-compose/mysql/mysql-5.7.env up -d

# pg 14
mkdir -p "${ROOT}"/docker-compose/postgresql/data/data
rm "${ROOT}"/docker-compose/postgresql/data/data/* -rf
docker compose -f "${ROOT}"/docker-compose/postgresql/postgresql-14.yaml --env-file "${ROOT}"/docker-compose/postgresql/postgresql-14.env down
docker compose -f "${ROOT}"/docker-compose/postgresql/postgresql-14.yaml --env-file "${ROOT}"/docker-compose/postgresql/postgresql-14.env up -d
