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

version: "3.1"

services:
  doris--mariadb:
    image: mariadb:10.4
    command: --default-authentication-plugin=mysql_native_password
    restart: always
    environment:
      MARIADB_ROOT_PASSWORD: 123456
      MARIADBL_DATABASE: init_db
      # set terminal charset
      LANG: C.UTF-8
    ports:
      - ${DOCKER_MARIADB_EXTERNAL_PORT}:3306
    healthcheck:
      test: mysqladmin ping -h 127.0.0.1 -u root --password=$$MARIADB_ROOT_PASSWORD && mysql -h 127.0.0.1 -u root --password=$$MARIADB_ROOT_PASSWORD -e "SELECT 1 FROM doris_test.deadline;"
      interval: 5s
      timeout: 60s
      retries: 120
    volumes:
      - ./data/:/var/lib/mysql
      - ./init:/docker-entrypoint-initdb.d
    networks:
      - doris--mariadb
  doris--mariadb-hello-world:
    image: hello-world
    depends_on:
      doris--mariadb:
        condition: service_healthy 
    networks:
      - doris--mariadb

networks:
  doris--mariadb:
    ipam:
      driver: default
      config:
        - subnet: 168.39.0.0/24
