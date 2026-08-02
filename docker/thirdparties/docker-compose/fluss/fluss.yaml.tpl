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

version: "3"

networks:
  doris--fluss--network:
    ipam:
      driver: default
      config:
        - subnet: 168.52.0.0/24

services:
  doris--fluss-zookeeper:
    image: zookeeper:3.9.2
    container_name: doris--fluss-zookeeper
    hostname: doris--fluss-zookeeper
    restart: always
    ports:
      - ${DOCKER_FLUSS_ZOOKEEPER_EXTERNAL_PORT}:2181
    healthcheck:
      test: ["CMD-SHELL", "bash -c 'exec 3<>/dev/tcp/127.0.0.1/2181' >/dev/null 2>&1"]
      interval: 5s
      timeout: 10s
      retries: 60
    networks:
      - doris--fluss--network

  doris--fluss-coordinator:
    image: ${FLUSS_SERVER_IMAGE}
    container_name: doris--fluss-coordinator
    hostname: doris--fluss-coordinator
    command: coordinatorServer
    depends_on:
      doris--fluss-zookeeper:
        condition: service_healthy
    ports:
      - ${DOCKER_FLUSS_COORDINATOR_EXTERNAL_PORT}:9123
    environment:
      # Doris FE/BE run on the host, so the servers must advertise the host
      # address plus the published port: everything (host clients, the flink
      # containers and the servers among themselves) then talks over it.
      - |
        FLUSS_PROPERTIES=
        zookeeper.address: doris--fluss-zookeeper:2181
        bind.listeners: FLUSS://0.0.0.0:9123
        advertised.listeners: FLUSS://${FLUSS_HOST_IP}:${DOCKER_FLUSS_COORDINATOR_EXTERNAL_PORT}
        remote.data.dir: ${FLUSS_REMOTE_DATA_DIR}
        default.bucket.number: 3
        default.replication.factor: 1
    volumes:
      - ${FLUSS_REMOTE_DATA_DIR}:${FLUSS_REMOTE_DATA_DIR}
    healthcheck:
      test: ["CMD-SHELL", "bash -c 'exec 3<>/dev/tcp/127.0.0.1/9123' >/dev/null 2>&1"]
      interval: 5s
      timeout: 10s
      retries: 60
    networks:
      - doris--fluss--network

  doris--fluss-tablet-server:
    image: ${FLUSS_SERVER_IMAGE}
    container_name: doris--fluss-tablet-server
    hostname: doris--fluss-tablet-server
    command: tabletServer
    depends_on:
      doris--fluss-coordinator:
        condition: service_healthy
    ports:
      - ${DOCKER_FLUSS_TABLET_EXTERNAL_PORT}:9123
    environment:
      - |
        FLUSS_PROPERTIES=
        zookeeper.address: doris--fluss-zookeeper:2181
        bind.listeners: FLUSS://0.0.0.0:9123
        advertised.listeners: FLUSS://${FLUSS_HOST_IP}:${DOCKER_FLUSS_TABLET_EXTERNAL_PORT}
        tablet-server.id: 0
        data.dir: /tmp/fluss/data
        remote.data.dir: ${FLUSS_REMOTE_DATA_DIR}
        default.bucket.number: 3
        default.replication.factor: 1
        # The fixtures write kilobytes, but the guard measures the whole host
        # disk: on a build machine that happens to sit above the threshold it
        # rejects every write, and the client retries ~2^31 times instead of
        # failing, so the environment hangs instead of reporting anything.
        server.data-disk.write-limit-ratio: 1.0
    volumes:
      - ${FLUSS_REMOTE_DATA_DIR}:${FLUSS_REMOTE_DATA_DIR}
    healthcheck:
      test: ["CMD-SHELL", "bash -c 'exec 3<>/dev/tcp/127.0.0.1/9123' >/dev/null 2>&1"]
      interval: 5s
      timeout: 10s
      retries: 60
    networks:
      - doris--fluss--network

  doris--fluss-jobmanager:
    image: ${FLUSS_FLINK_IMAGE}
    container_name: doris--fluss-jobmanager
    hostname: doris--fluss-jobmanager
    command: jobmanager
    ports:
      - ${DOCKER_FLUSS_FLINK_JOBMANAGER_EXTERNAL_PORT}:8081
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: doris--fluss-jobmanager
        rest.address: doris--fluss-jobmanager
        rest.bind-address: 0.0.0.0
    healthcheck:
      test: ["CMD-SHELL", "curl -sf http://127.0.0.1:8081/overview >/dev/null"]
      interval: 5s
      timeout: 10s
      retries: 60
    networks:
      - doris--fluss--network

  doris--fluss-taskmanager:
    image: ${FLUSS_FLINK_IMAGE}
    container_name: doris--fluss-taskmanager
    hostname: doris--fluss-taskmanager
    command: taskmanager
    depends_on:
      doris--fluss-jobmanager:
        condition: service_healthy
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: doris--fluss-jobmanager
        taskmanager.numberOfTaskSlots: 4
        taskmanager.memory.process.size: 2048m
        taskmanager.memory.task.off-heap.size: 128m
    # The taskmanager RPC port is ephemeral, so health means "registered with
    # the jobmanager": that is also exactly what submitting a job needs.
    healthcheck:
      test: ["CMD-SHELL", "curl -sf http://doris--fluss-jobmanager:8081/taskmanagers | grep -q '\"id\"'"]
      interval: 5s
      timeout: 10s
      retries: 60
    networks:
      - doris--fluss--network

  # One-shot data preparation: runs sql/init.sql through the Flink SQL client,
  # then keeps running so that `compose up --wait` has a healthy service to
  # gate on. The marker file only appears when every statement succeeded.
  doris--fluss-sql-client:
    image: ${FLUSS_FLINK_IMAGE}
    container_name: doris--fluss-sql-client
    hostname: doris--fluss-sql-client
    depends_on:
      doris--fluss-tablet-server:
        condition: service_healthy
      doris--fluss-taskmanager:
        condition: service_healthy
    # Runs as a command, not as an entrypoint override: the image entrypoint is
    # what turns FLINK_PROPERTIES into the config the SQL client submits with.
    command: ["/opt/fluss-scripts/run-init-sql.sh"]
    environment:
      - FLUSS_BOOTSTRAP_SERVERS=doris--fluss-coordinator:9123
      - FLUSS_JOBMANAGER_HOST=doris--fluss-jobmanager
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: doris--fluss-jobmanager
        rest.address: doris--fluss-jobmanager
    volumes:
      - ./sql:/opt/fluss-sql:ro
      - ./scripts:/opt/fluss-scripts:ro
    healthcheck:
      test: ["CMD-SHELL", "test -f /tmp/fluss-init/SUCCESS"]
      interval: 5s
      timeout: 10s
      retries: 120
    networks:
      - doris--fluss--network
