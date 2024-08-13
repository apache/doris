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

version: "2.1"

services:
  doris--postgres:
    image: postgis/postgis:14-3.3
    restart: always
    environment:
      POSTGRES_PASSWORD: 123456
    ports:
      - ${DOCKER_PG_14_EXTERNAL_PORT}:5432
    healthcheck:
      test: [ "CMD-SHELL", "pg_isready -U postgres && psql -U postgres -c 'SELECT 1 FROM doris_test.deadline;'" ]
      interval: 5s
      timeout: 60s
      retries: 120
    volumes:
      - ./data/data/:/var/lib/postgresql/data/
      - ./init:/docker-entrypoint-initdb.d
    networks:
      - doris--postgres
  doris--postgres--hello-world:
    image: hello-world
    depends_on:
      doris--postgres:
        condition: service_healthy
    networks:
      - doris--postgres

networks:
  doris--postgres:
    ipam:
      driver: default
      config:
        - subnet: 168.41.0.0/24
