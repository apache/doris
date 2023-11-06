#
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
#
version: "3"

volumes:
  metadata_data: {}
  middle_var: {}
  historical_var: {}
  broker_var: {}
  coordinator_var: {}
  router_var: {}
  druid_shared: {}
  zookeeper_data: {}

services:
  postgres-druid:
    image: postgres:14
    container_name: postgres-druid
    ports:
      - "11432:5432"
    volumes:
      - ./data/druid_pg_data:/var/lib/postgresql/data
    environment:
      - POSTGRES_PASSWORD=FoolishPassword
      - POSTGRES_USER=druid
      - POSTGRES_DB=druid
        # networks:
        #   - doris--druid
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "druid"]
      interval: 10s
      timeout: 5s
      retries: 3

  # Need 3.5 or later for container nodes
  zookeeper-druid:
    image: zookeeper:3.7.0
    container_name: zookeeper-druid
    volumes:
      - ./data/druid_zk_data:/data
    ports:
      - "10181:2181"
    environment:
      - ZOO_MY_ID=1
        # networks:
        #   - doris--druid
    healthcheck:
      test: nc -z localhost 2181 || exit -1
      start_period: 15s
      interval: 30s
      timeout: 10s
      retries: 3

  coordinator-druid:
    image: apache/druid:27.0.0
    container_name: coordinator-druid
    volumes:
      - ./data/druid_shared:/opt/shared
      - ./data/druid_coordinator_var:/opt/druid/var
    depends_on:
      - zookeeper-druid
      - postgres-druid
    ports:
      - "11881:8081"
        # networks:
        #   - doris--druid
    healthcheck:
      test: nc -z localhost 8081 || exit -1
      interval: 30s
      timeout: 10s
      retries: 3
    command:
      - coordinator
    env_file:
      - ./druid.env

  broker-druid:
    image: apache/druid:27.0.0
    container_name: broker-druid
    volumes:
      - ./data/druid_broker_var:/opt/druid/var
    depends_on:
      - zookeeper-druid
      - postgres-druid
      - coordinator-druid
        # networks:
        #   - doris--druid
    ports:
      - "11882:8082"
    healthcheck:
      test: nc -z localhost 8082 || exit -1
      interval: 30s
      timeout: 10s
      retries: 3
    command:
      - broker
    env_file:
      - ./druid.env

  historical-druid:
    image: apache/druid:27.0.0
    container_name: historical-druid
    volumes:
      - ./data/druid_shared:/opt/shared
      - ./data/druid_historical_var:/opt/druid/var
    depends_on:
      - zookeeper-druid
      - postgres-druid
      - coordinator-druid
    ports:
      - "11883:8083"
        # networks:
        #   - doris--druid
    healthcheck:
      test: nc -z localhost 8083 || exit -1
      interval: 30s
      timeout: 10s
      retries: 3
    command:
      - historical
    env_file:
      - ./druid.env

  middlemanager-druid:
    image: apache/druid:27.0.0
    container_name: middlemanager-druid
    volumes:
      - ./data/druid_shared:/opt/shared
      - ./data/druid_middle_var:/opt/druid/var
    depends_on:
      - zookeeper-druid
      - postgres-druid
      - coordinator-druid
    ports:
      - "11891:8091"
      - "21100-21105:8100-8105"
        # networks:
        #   - doris--druid
    healthcheck:
      test: nc -z localhost 8091 || exit -1
      interval: 30s
      timeout: 10s
      retries: 3
    command:
      - middleManager
    env_file:
      - ./druid.env

  router-druid:
    image: apache/druid:27.0.0
    container_name: router-druid
    volumes:
      - ./data/druid_router_var:/opt/druid/var
    depends_on:
      - zookeeper-druid
      - postgres-druid
      - coordinator-druid
        # networks:
        #   - doris--druid
    ports:
      - "21888:8888"
    healthcheck:
      test: nc -z localhost 8888 || exit -1
      interval: 30s
      timeout: 10s
      retries: 3
    command:
      - router
    env_file:
      - ./druid.env

  toolbox-druid:
    image: webdevops/toolbox
    container_name: toolbox-druid
    volumes:
      - ./scripts:/mnt/scripts
    environment:
      DRUID_COORDINATOR_HOST: "coordinator-druid"
    command: [ "sh","-c","/mnt/scripts/druid_init.sh" ]
    depends_on:
      - postgres-druid
      - zookeeper-druid
      - middlemanager-druid
      - broker-druid
      - historical-druid
      - router-druid
      - coordinator-druid
    # networks:
    #   - doris--druid

    #networks:
    #    doris--druid: