#!/usr/bin/env bash
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

# Rendered to fluss.env by run-thirdparties-docker.sh (envsubst).
# build-images.sh also sources this template directly for the image tags, so
# keep the two FLUSS_*_IMAGE lines free of variable references.

DOCKER_FLUSS_ZOOKEEPER_EXTERNAL_PORT=22181
DOCKER_FLUSS_COORDINATOR_EXTERNAL_PORT=19123
DOCKER_FLUSS_TABLET_EXTERNAL_PORT=19124
DOCKER_FLUSS_FLINK_JOBMANAGER_EXTERNAL_PORT=18085

# Fluss 1.0 is not released yet, so there is no published image to pull.
# build-images.sh builds both tags from a local fluss source checkout.
FLUSS_SERVER_IMAGE=doris-fluss-server:1.0-SNAPSHOT-local
FLUSS_FLINK_IMAGE=doris-fluss-flink:1.20.0-fluss-1.0-SNAPSHOT-local

# Address the fluss servers advertise to clients. Doris FE/BE run on the host,
# so the servers must hand out the host address plus the published ports, not
# their in-container hostnames.
FLUSS_HOST_IP=${IP_HOST}

# remote.data.dir holds remote log segments and kv snapshots. Doris BE reads
# those files directly (primary-key table reads), so the directory is bind
# mounted at the SAME absolute path inside the containers and on the host.
FLUSS_REMOTE_DATA_DIR=${FLUSS_COMPOSE_DIR}/data/remote
