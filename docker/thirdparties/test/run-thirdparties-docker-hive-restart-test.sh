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

set -eo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." &>/dev/null && pwd)"
. "${ROOT}/docker-health.sh"

fail() {
    echo "FAIL: $*" >&2
    exit 1
}

assert_success() {
    "$@" || fail "expected success: $*"
}

assert_failure() {
    if "$@"; then
        fail "expected failure: $*"
    fi
}

docker_container_state_cmd() {
    case "$1" in
    doris-external--hadoop2-namenode|\
    doris-external--hadoop2-datanode|\
    doris-external--hive2-server|\
    doris-external--hive2-metastore|\
    doris-external--hive2-metastore-postgresql)
        echo "running|healthy"
        ;;
    doris-stuck--hadoop3-namenode|\
    doris-stuck--hive3-server|\
    doris-stuck--hive3-metastore|\
    doris-stuck--hive3-metastore-postgresql)
        echo "running|healthy"
        ;;
    doris-stuck--hadoop3-datanode)
        echo "running|unhealthy"
        ;;
    *)
        return 1
        ;;
    esac
}

docker_container_env_cmd() {
    case "$1" in
    doris-external--hive2-metastore)
        cat <<'EOF'
HIVE_BOOTSTRAP_GROUPS=common,hive2_only
EOF
        ;;
    doris-stuck--hive3-metastore)
        cat <<'EOF'
HIVE_BOOTSTRAP_GROUPS=common,hive2_only
EOF
        ;;
    *)
        return 1
        ;;
    esac
}

assert_success docker_hive_stack_healthy "doris-external--" "hive2"
assert_failure docker_hive_stack_healthy "doris-stuck--" "hive3"
assert_success docker_hive_stack_reusable "doris-external--" "hive2" "common,hive2_only"
assert_failure docker_hive_stack_reusable "doris-stuck--" "hive3" "common,hive3_only"

echo "PASS"
