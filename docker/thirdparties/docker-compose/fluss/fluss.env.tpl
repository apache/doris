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
# fetch-paimon-s3.sh also sources this template directly, for the paimon
# version, so keep that line free of variable references.

DOCKER_FLUSS_ZOOKEEPER_EXTERNAL_PORT=22181
DOCKER_FLUSS_COORDINATOR_EXTERNAL_PORT=19123
DOCKER_FLUSS_TABLET_EXTERNAL_PORT=19124
DOCKER_FLUSS_FLINK_JOBMANAGER_EXTERNAL_PORT=18085
DOCKER_FLUSS_MINIO_EXTERNAL_PORT=19125

# The images the fluss project publishes for its 1.0.0 release. The server image
# runs the coordinator and the tablet server. The quickstart flink image is
# stock Flink 1.20 plus the fluss connector, the lake tiering job and, kept
# aside under /opt/flink/paimon, the paimon runtime (paimon-flink, paimon-s3,
# fluss-lake-paimon and a hadoop); its init_paimon.sh copies those into lib/ and
# then hands over to the flink entrypoint, which is why the compose file starts
# every flink container through that script. The version is the one fe/pom.xml
# pins fluss.version to, so the cluster the suites run against is the release
# the connector and the BE scanner were compiled against; moving it means
# editing both tags (and fe/pom.xml with them).
FLUSS_SERVER_IMAGE=apache/fluss:1.0.0
FLUSS_FLINK_IMAGE=apache/fluss-quickstart-flink:1.20-1.0.0

# Address the fluss servers advertise to clients. Doris FE/BE run on the host,
# so the servers must hand out the host address plus the published ports, not
# their in-container hostnames.
FLUSS_HOST_IP=${IP_HOST}

# remote.data.dir holds remote log segments and kv snapshots. Doris BE reads
# those files directly (primary-key table reads), so the directory is bind
# mounted at the SAME absolute path inside the containers and on the host.
FLUSS_REMOTE_DATA_DIR=${FLUSS_COMPOSE_DIR}/data/remote

# The paimon warehouse the tiering service writes the lake tables into. It lives
# in the object store this compose brings up rather than in a directory, and that
# choice is what makes the environment able to check the one thing a directory
# never could: fluss deletes every lake option whose name contains key, secret or
# password before it hands a table's properties to a client, so Doris cannot
# learn the credentials from fluss at all -- it can only be told them as catalog
# properties of its own. A warehouse that needs no credentials leaves that whole
# path unexecuted, which is where it stayed until this moved.
#
# The scheme is load bearing: a location with no scheme is read as HDFS
# (StorageRegistry.fromScheme defaults a blank scheme to HDFS), and every
# data-file path paimon recorded then fails to normalize with "Unsupported
# schema: null" at scan time -- long after catalog creation.
#
# To read the lake out of a directory again while debugging -- to tell a storage
# problem from a lake problem -- set this to the FLUSS_PAIMON_WAREHOUSE_DIR path
# below with a file:// scheme, and nothing else has to change: the bind mounts
# are still in place and the s3 settings go inert, because the FileIO paimon
# picks follows the scheme.
#
# The bucket name here, and the port in the endpoint further down, are spelled
# out again rather than substituted: this file is rendered with envsubst, which
# resolves names from the environment and not from the lines above it, so a
# reference to one of them would render empty.
FLUSS_LAKE_S3_BUCKET=fluss-lake
FLUSS_PAIMON_WAREHOUSE=s3://fluss-lake/wh

# Kept for the directory warehouse above, and bind mounted at the same absolute
# path inside the containers and on the host so that the switch needs no other
# edit. Unused while the warehouse is in the object store.
FLUSS_PAIMON_WAREHOUSE_DIR=${FLUSS_COMPOSE_DIR}/data/paimon

# How everything reaches the object store. One address for all of them -- the
# containers and Doris on the host -- for the same reason the fluss servers
# advertise the host address: the endpoint is recorded in the lake table's
# properties, and a container-only hostname there would be a location the host
# cannot resolve. The credentials are the minio root user, which the container
# below is started with.
FLUSS_LAKE_S3_ENDPOINT=http://${IP_HOST}:19125
FLUSS_LAKE_S3_ACCESS_KEY=minioadmin
FLUSS_LAKE_S3_SECRET_KEY=minioadmin

# The one jar the server image lacks. Its plugins/paimon carries
# fluss-lake-paimon, paimon-bundle and a shaded hadoop, but paimon keeps each
# filesystem implementation in a jar of its own and loads it by ServiceLoader,
# and the coordinator opens the s3:// warehouse above the moment a
# datalake-enabled table is created -- so without paimon-s3 in that same
# directory (a plugin directory is a classloader; a jar in another one is
# invisible to it) CREATE TABLE fails. fetch-paimon-s3.sh downloads it into
# cache/ before the stack starts and the compose file bind mounts it into both
# server containers, the arrangement the fluss project's own lakehouse
# quickstart uses. Matched to the paimon.version fluss 1.0.0 was compiled
# against, which is what its images ship as paimon-bundle and paimon-flink; read
# it off the fluss parent pom (org/apache/fluss/fluss/<version>/ on central) when
# the image tags move. NOT matched to Doris's own paimon.version (1.3.1 as of
# this pin): Doris reads the lake half of a table with its own paimon, so the
# tables tiered here double as the check that a paimon 1.x reader still reads
# what a 2.x writer produced. The version is spelled out again in the jar name
# below, because this file is rendered with envsubst, which resolves names from
# the environment and not from the lines above it; fetch-paimon-s3.sh derives
# the same name from FLUSS_PAIMON_VERSION.
FLUSS_PAIMON_VERSION=2.0.0
FLUSS_PAIMON_S3_JAR=${FLUSS_COMPOSE_DIR}/cache/paimon-s3-2.0.0.jar
