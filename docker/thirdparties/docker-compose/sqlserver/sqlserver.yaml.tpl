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
  doris--sqlserver_2022:
    image: "doristhirdpartydocker/mssql-server:2022-latest"
    container_name: "doris--sqlserver_2022"
    ports:
      - ${DOCKER_SQLSERVER_EXTERNAL_PORT}:1433
    healthcheck:
      test: ["CMD-SHELL", "test -f /tmp/doris_sqlserver_initialized && /opt/mssql-tools/bin/sqlcmd -Usa -PDoris123456 -b -Q 'SELECT 1 FROM doris_test.dbo.student'"]
      interval: 5s
      timeout: 30s
      retries: 120
    volumes:
        - ./init:/docker-entrypoint-initdb.d
    command:
      - /bin/bash
      - -c
      - |
        set -e
        rm -f /tmp/doris_sqlserver_initialized
        # Launch MSSQL and send to background
        /opt/mssql/bin/sqlservr &
        sqlserver_pid=$$!
        trap 'kill -TERM "$$sqlserver_pid"; wait "$$sqlserver_pid"' TERM INT
        # Wait for it to be available
        echo "Waiting for MS SQL to be available ⏳"
        until /opt/mssql-tools/bin/sqlcmd -l 30 -S localhost -h-1 -b -U sa -P Doris123456 -Q "SET NOCOUNT ON SELECT \"YAY WE ARE UP\" , @@servername"; do
          kill -0 "$$sqlserver_pid"
          sleep 5
        done
        # Run every script in /scripts
        # TODO set a flag so that this is only done once on creation,
        #      and not every time the container runs
        for foo in /docker-entrypoint-initdb.d/*.sql
          do /opt/mssql-tools/bin/sqlcmd -U sa -P Doris123456 -b -l 30 -e -i "$$foo"
        done
        touch /tmp/doris_sqlserver_initialized
        wait "$$sqlserver_pid"

    restart: always
    environment:
      # Accept the end user license Agreement
      - ACCEPT_EULA=Y
      # password of SA
      - SA_PASSWORD=Doris123456
    networks:
      - doris--sqlserver_2022

networks:
  doris--sqlserver_2022:
    ipam:
      driver: default
      config:
        - subnet: 168.42.0.0/24
