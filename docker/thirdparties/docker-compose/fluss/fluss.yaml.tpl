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

  # The object store the lake lives in. Part of this environment rather than
  # borrowed from another component's: the fluss servers, the tiering job and
  # Doris all have to name one endpoint, and an environment that only works when
  # something else was started first is a failure mode nobody reads a compose
  # file to discover.
  doris--fluss-minio:
    # The release the other object-store environments in this directory use, and
    # the one whose server image still carries mc -- which is what creates the
    # bucket below without a second container to do it.
    image: minio/minio:RELEASE.2025-01-20T14-49-07Z
    container_name: doris--fluss-minio
    hostname: doris--fluss-minio
    restart: always
    ports:
      - ${DOCKER_FLUSS_MINIO_EXTERNAL_PORT}:9000
    environment:
      - MINIO_ROOT_USER=${FLUSS_LAKE_S3_ACCESS_KEY}
      - MINIO_ROOT_PASSWORD=${FLUSS_LAKE_S3_SECRET_KEY}
      # Paimon's S3 client signs with a region whether or not one was configured,
      # and minio rejects a signature for a region it does not answer to.
      - MINIO_REGION_NAME=us-east-1
      - FLUSS_LAKE_S3_BUCKET=${FLUSS_LAKE_S3_BUCKET}
    volumes:
      - ./scripts:/opt/fluss-scripts:ro
    # The server is started through a script that creates the lake bucket beside
    # it, because the condition this service has to reach is "the bucket exists",
    # not "the server answers" -- the coordinator writes into it as soon as the
    # first lake table is created.
    entrypoint: ["bash", "/opt/fluss-scripts/run-minio.sh"]
    healthcheck:
      test: ["CMD-SHELL", "test -f /tmp/fluss-minio/READY"]
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
    # The image chowns all of /opt/fluss to uid 9999 but never declares USER, so
    # it runs as root unless told otherwise -- and then the paimon table
    # directories it creates in the shared warehouse belong to root, while the
    # tiering job writing into them is the flink image's uid 9999. Same uid on
    # both sides, and the two can share the warehouse.
    user: "9999:9999"
    depends_on:
      doris--fluss-zookeeper:
        condition: service_healthy
      # It creates the paimon table -- and therefore writes the warehouse -- the
      # moment a datalake-enabled fluss table is created.
      doris--fluss-minio:
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
        # Lakehouse storage. The coordinator creates the paimon table when a
        # datalake-enabled fluss table is created, so it needs the warehouse and
        # the credentials to reach it, not just the tiering job. The plugin jars
        # are in the image: fluss-dist ships plugins/paimon (fluss-lake-paimon +
        # paimon-bundle + shaded hadoop) and build-images.sh adds paimon-s3
        # beside them, which is where the S3 FileIO comes from.
        #
        # This block is also most of what makes the tables READABLE by Doris: the
        # coordinator copies its datalake.paimon.* config into every lake table's
        # properties under a table. prefix, and that copy is the only place the
        # fluss connector learns where the warehouse is.
        #
        # MOST of it, not all: fluss deletes every option whose name contains
        # key, secret or password from that copy before answering a client
        # (MetadataManager.removeSensitiveTableOptions), so the two lines below
        # that carry credentials reach the lake table but never reach Doris.
        # Doris has to be given them as catalog properties of its own -- which is
        # exactly the path this environment exists to exercise.
        datalake.enabled: true
        datalake.format: paimon
        datalake.paimon.metastore: filesystem
        datalake.paimon.warehouse: ${FLUSS_PAIMON_WAREHOUSE}
        datalake.paimon.s3.endpoint: ${FLUSS_LAKE_S3_ENDPOINT}
        datalake.paimon.s3.path.style.access: true
        datalake.paimon.s3.access-key: ${FLUSS_LAKE_S3_ACCESS_KEY}
        datalake.paimon.s3.secret-key: ${FLUSS_LAKE_S3_SECRET_KEY}
    volumes:
      - ${FLUSS_REMOTE_DATA_DIR}:${FLUSS_REMOTE_DATA_DIR}
      # Only used when the warehouse is switched back to a directory for
      # debugging; see FLUSS_PAIMON_WAREHOUSE in fluss.env.tpl. Left in place so
      # that the switch is one line and not four mounts.
      - ${FLUSS_PAIMON_WAREHOUSE_DIR}:${FLUSS_PAIMON_WAREHOUSE_DIR}
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
    # Same uid as the coordinator and the flink containers; see there.
    user: "9999:9999"
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
        # Ten minutes by default, which would leave the primary-key fixtures
        # with no kv snapshot for as long as the suites take to run: they would
        # then be read by replaying the change log, and the path where Doris BE
        # reads a snapshot FILE this container wrote -- the one thing only an
        # end-to-end run can check -- would never be exercised. Short enough
        # that init can wait for it; this does not pile up files, because a
        # tablet whose log has not advanced since its last snapshot is skipped
        # (KvTabletSnapshotTarget), and the fixtures stop writing after init.
        kv.snapshot.interval: 10s
        # Same lakehouse settings as the coordinator: a tablet server reads them
        # to decide the key encoding and bucketing a datalake table uses.
        datalake.enabled: true
        datalake.format: paimon
        datalake.paimon.metastore: filesystem
        datalake.paimon.warehouse: ${FLUSS_PAIMON_WAREHOUSE}
        datalake.paimon.s3.endpoint: ${FLUSS_LAKE_S3_ENDPOINT}
        datalake.paimon.s3.path.style.access: true
        datalake.paimon.s3.access-key: ${FLUSS_LAKE_S3_ACCESS_KEY}
        datalake.paimon.s3.secret-key: ${FLUSS_LAKE_S3_SECRET_KEY}
    volumes:
      - ${FLUSS_REMOTE_DATA_DIR}:${FLUSS_REMOTE_DATA_DIR}
      # See the coordinator: kept for the directory warehouse, unused otherwise.
      - ${FLUSS_PAIMON_WAREHOUSE_DIR}:${FLUSS_PAIMON_WAREHOUSE_DIR}
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
    # The tiering job runs on this cluster and writes the paimon warehouse; the
    # jobmanager builds the job graph, which opens the lake catalog. Reaching the
    # warehouse itself needs no mount now that it is in the object store -- this
    # one is kept for the directory warehouse; see the coordinator.
    volumes:
      - ${FLUSS_PAIMON_WAREHOUSE_DIR}:${FLUSS_PAIMON_WAREHOUSE_DIR}
      # Tiering a primary-key table reads the kv snapshot FILES rather than the
      # change log, straight out of remote.data.dir -- the same way Doris BE reads
      # them. Without this mount a log table tiers and a primary-key one fails
      # with FileNotFoundException for a file that plainly exists on the host.
      - ${FLUSS_REMOTE_DATA_DIR}:${FLUSS_REMOTE_DATA_DIR}:ro
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
    # Writes and commits the paimon files the tiering job produces, and reads the
    # kv snapshots it tiers a primary-key table from (see the jobmanager).
    volumes:
      - ${FLUSS_PAIMON_WAREHOUSE_DIR}:${FLUSS_PAIMON_WAREHOUSE_DIR}
      - ${FLUSS_REMOTE_DATA_DIR}:${FLUSS_REMOTE_DATA_DIR}:ro
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
      # Read-only, and only so that init can wait for the kv snapshots to be
      # written before declaring the environment ready.
      - FLUSS_REMOTE_DATA_DIR=${FLUSS_REMOTE_DATA_DIR}
      # This container also submits the tiering job and waits for it to commit,
      # so it needs both the warehouse path and the paimon database naming. The
      # tiering job is given its lake settings on the command line rather than
      # reading the servers' -- see run-init-sql.sh -- so the credentials have to
      # come this far too, and again when the wait counts the rows it committed.
      - FLUSS_PAIMON_WAREHOUSE=${FLUSS_PAIMON_WAREHOUSE}
      - FLUSS_LAKE_S3_ENDPOINT=${FLUSS_LAKE_S3_ENDPOINT}
      - FLUSS_LAKE_S3_ACCESS_KEY=${FLUSS_LAKE_S3_ACCESS_KEY}
      - FLUSS_LAKE_S3_SECRET_KEY=${FLUSS_LAKE_S3_SECRET_KEY}
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: doris--fluss-jobmanager
        rest.address: doris--fluss-jobmanager
    volumes:
      - ./sql:/opt/fluss-sql:ro
      - ./scripts:/opt/fluss-scripts:ro
      - ${FLUSS_REMOTE_DATA_DIR}:${FLUSS_REMOTE_DATA_DIR}:ro
      # Writable, not read-only like the one above: `flink run` builds the
      # tiering job graph in this container, and building it opens the lake
      # catalog, which creates the warehouse directory if it is not there yet.
      - ${FLUSS_PAIMON_WAREHOUSE_DIR}:${FLUSS_PAIMON_WAREHOUSE_DIR}
    healthcheck:
      test: ["CMD-SHELL", "test -f /tmp/fluss-init/SUCCESS"]
      interval: 5s
      timeout: 10s
      retries: 120
    networks:
      - doris--fluss--network
