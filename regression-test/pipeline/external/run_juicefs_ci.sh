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

########################### Teamcity Build Step: Command Line #######################
: <<EOF
#!/bin/bash
set -euo pipefail

# Required
export teamcity_build_checkoutDir="%teamcity.build.checkoutDir%"
export JFS_META_DSN="%env.JFS_META_DSN%"

# Optional
export JFS_VOLUME="${JFS_VOLUME:-cluster}"
export JFS_BUCKET="${JFS_BUCKET:-/tmp/jfs-bucket}"
export JFS_STORAGE="${JFS_STORAGE:-file}"
export JFS_HMS_PORT="${JFS_HMS_PORT:-9383}"

cd "${teamcity_build_checkoutDir}/regression-test/pipeline/external"
bash -x run_juicefs_ci.sh
EOF
#####################################################################################

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
if [[ -n "${teamcity_build_checkoutDir:-}" ]]; then
    ROOT_DIR="${teamcity_build_checkoutDir}"
fi

JFS_META_DSN="${JFS_META_DSN:-}"
JFS_VOLUME="${JFS_VOLUME:-cluster}"
JFS_BUCKET="${JFS_BUCKET:-/tmp/jfs-bucket}"
JFS_STORAGE="${JFS_STORAGE:-file}"
JFS_HMS_PORT="${JFS_HMS_PORT:-9383}"
JFS_TEST_DIR="${JFS_TEST_DIR:-external_table_p0/refactor_storage_param}"
JFS_TEST_SUITE="${JFS_TEST_SUITE:-test_jfs_hms_catalog_read}"
JFS_RECREATE_METASTORE="${JFS_RECREATE_METASTORE:-true}"
JFS_HADOOP_USER="${JFS_HADOOP_USER:-root}"
JFS_FS="jfs://${JFS_VOLUME}"

if [[ -z "${JFS_REGRESSION_CONF:-}" ]]; then
    if [[ -n "${teamcity_build_checkoutDir:-}" ]]; then
        JFS_REGRESSION_CONF="${ROOT_DIR}/regression-test/pipeline/external/conf/regression-conf.groovy"
    else
        JFS_REGRESSION_CONF="${ROOT_DIR}/regression-test/conf/regression-conf.groovy"
    fi
fi

if [[ -z "${JFS_META_DSN}" ]]; then
    echo "ERROR: JFS_META_DSN is required."
    echo "Example: export JFS_META_DSN='mysql://user:pwd@(127.0.0.1:3316)/juicefs_meta'"
    exit 1
fi

if [[ ! -f "${ROOT_DIR}/run-regression-test.sh" ]]; then
    echo "ERROR: invalid root dir: ${ROOT_DIR}"
    exit 1
fi
if [[ ! -f "${JFS_REGRESSION_CONF}" ]]; then
    echo "ERROR: regression conf not found: ${JFS_REGRESSION_CONF}"
    exit 1
fi

echo "==== [1/4] Prepare JuiceFS metadata (idempotent) ===="
JFS_META_DSN="${JFS_META_DSN}" \
JFS_VOLUME="${JFS_VOLUME}" \
JFS_BUCKET="${JFS_BUCKET}" \
JFS_STORAGE="${JFS_STORAGE}" \
bash "${ROOT_DIR}/regression-test/pipeline/external/prepare_juicefs_ci.sh"

echo "==== [2/4] Refresh Hive3 metastore with JFS meta ===="
if ! command -v envsubst >/dev/null 2>&1; then
    echo "ERROR: envsubst is required."
    exit 1
fi

DOCKER_ROOT="${ROOT_DIR}/docker/thirdparties"
# shellcheck source=/dev/null
. "${DOCKER_ROOT}/custom_settings.env"
export CONTAINER_UID="${CONTAINER_UID}"
export IP_HOST="$(ip -4 addr show scope global | awk '/inet / {print $2}' | cut -d/ -f1 | head -n 1)"
export NEED_LOAD_DATA=0
export LOAD_PARALLEL=$(( $(getconf _NPROCESSORS_ONLN) / 2 ))
export JFS_CLUSTER_META="${JFS_META_DSN}"
# shellcheck source=/dev/null
. "${DOCKER_ROOT}/docker-compose/hive/hive-3x_settings.env"

envsubst <"${DOCKER_ROOT}/docker-compose/hive/hive-3x.yaml.tpl" >"${DOCKER_ROOT}/docker-compose/hive/hive-3x.yaml"
envsubst <"${DOCKER_ROOT}/docker-compose/hive/hadoop-hive.env.tpl" >"${DOCKER_ROOT}/docker-compose/hive/hadoop-hive-3x.env"
envsubst <"${DOCKER_ROOT}/docker-compose/hive/hadoop-hive-3x.env.tpl" >> "${DOCKER_ROOT}/docker-compose/hive/hadoop-hive-3x.env"

# Recreate metastore only, avoid restarting hdfs (port 9866 conflicts on shared hosts).
if [[ "${JFS_RECREATE_METASTORE}" == "true" ]]; then
    sudo docker compose -p "${CONTAINER_UID}hive3" \
        -f "${DOCKER_ROOT}/docker-compose/hive/hive-3x.yaml" \
        --env-file "${DOCKER_ROOT}/docker-compose/hive/hadoop-hive-3x.env" \
        up -d --force-recreate hive-metastore
else
    sudo docker compose -p "${CONTAINER_UID}hive3" \
        -f "${DOCKER_ROOT}/docker-compose/hive/hive-3x.yaml" \
        --env-file "${DOCKER_ROOT}/docker-compose/hive/hadoop-hive-3x.env" \
        up -d hive-metastore
fi

# Detect a writable JFS base path for database locations.
JFS_DB_BASE_PATH="$(sudo docker exec \
    -e JFS_VOLUME="${JFS_VOLUME}" \
    -e JFS_META_DSN="${JFS_META_DSN}" \
    -e JFS_HADOOP_USER="${JFS_HADOOP_USER}" \
    "${CONTAINER_UID}hive3-metastore" \
    bash -c '
set -euo pipefail
HADOOP_BIN=/opt/hadoop-3.2.1/bin/hadoop
COMMON_OPTS=(-Dfs.jfs.impl=io.juicefs.JuiceFileSystem "-Djuicefs.${JFS_VOLUME}.meta=${JFS_META_DSN}")
candidates=(
    "jfs://${JFS_VOLUME}/doris_jfs/${JFS_HADOOP_USER}"
    "jfs://${JFS_VOLUME}/${JFS_HADOOP_USER}"
    "jfs://${JFS_VOLUME}/"
)
for base in "${candidates[@]}"; do
    if "$HADOOP_BIN" fs "${COMMON_OPTS[@]}" -mkdir -p "$base" >/dev/null 2>&1; then
        echo "$base"
        exit 0
    fi
done
echo "ERROR: failed to find writable JFS base path for ${JFS_HADOOP_USER}" >&2
exit 1
')"
echo "Use JFS DB base path: ${JFS_DB_BASE_PATH}"

echo "==== [3/4] Run JuiceFS regression suite ===="
"${ROOT_DIR}/run-regression-test.sh" --run \
    --conf "${JFS_REGRESSION_CONF}" \
    -d "${JFS_TEST_DIR}" \
    -s "${JFS_TEST_SUITE}" \
    -conf enableJfsTest=true \
    -conf enableHiveTest=true \
    -conf externalEnvIp=127.0.0.1 \
    -conf hdfsUser="${JFS_HADOOP_USER}" \
    -conf jfsDbBasePath="${JFS_DB_BASE_PATH}" \
    -conf hive2HmsPort="${JFS_HMS_PORT}" \
    -conf jfsHiveMetastoreUris="thrift://127.0.0.1:${JFS_HMS_PORT}" \
    -conf jfsFs="${JFS_FS}" \
    -conf jfsMeta="${JFS_META_DSN}"

echo "==== [4/4] Done ===="
