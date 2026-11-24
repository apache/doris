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

# Run the real CLI against an isolated fixture tree and a Docker stub.
# Requires Bash 4+, like the launcher; does not access a real daemon.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." &>/dev/null && pwd)"
test_dir="$(mktemp -d /tmp/doris-stop-test.XXXXXX)"
trap 'rm -rf "${test_dir}"' EXIT
fixture="${test_dir}/thirdparties"
mkdir -p "${fixture}" "${test_dir}/bin"
for file in \
    run-thirdparties-docker.sh docker-command.sh docker-health.sh \
    juicefs-helpers.sh custom_settings.env \
    docker-compose/hive/scripts/bootstrap/bootstrap-groups.sh \
    docker-compose/hive/hive-3x_settings.env \
    docker-compose/hive/hadoop-hive.env.tpl \
    docker-compose/hive/hadoop-hive-3x.env.tpl \
    docker-compose/hive/hive-3x.yaml.tpl \
    docker-compose/fluss/fluss.env.tpl docker-compose/fluss/fluss.yaml.tpl; do
    mkdir -p "$(dirname "${fixture}/${file}")"
    cp "${ROOT}/${file}" "${fixture}/${file}"
done

export STOP_TEST_DOCKER_LOG="${test_dir}/docker.log"
export STOP_TEST_PREPARE_MARKER="${test_dir}/prepare-called"
cat >"${fixture}/docker-compose/hive/scripts/prepare-hive-data.sh" <<'EOF'
touch "${STOP_TEST_PREPARE_MARKER}"
exit 99
EOF
cat >"${test_dir}/bin/docker" <<'EOF'
#!/usr/bin/env bash
if [[ "${1:-}" == info ]]; then
    echo '["name=rootless"]'
    exit 0
fi
printf '%s\n' "$*" >>"${STOP_TEST_DOCKER_LOG}"
[[ "${1:-}" == compose ]] || exit 1
for argument in "$@"; do
    if [[ "${argument}" == down ]]; then exit 0; fi
done
exit 1
EOF
cat >"${test_dir}/bin/envsubst" <<'EOF'
#!/bin/sh
cat
EOF
chmod +x "${test_dir}/bin/docker" "${test_dir}/bin/envsubst"

export PATH="${test_dir}/bin:${PATH}"
export CONTAINER_UID=rootless-stop-test-
export DOCKER_HOST=unix:///rootless-stop-test.sock
export IP_HOST=127.0.0.1
export HIVE_HOST_ALIAS=localhost
export JFS_CLUSTER_META=redis://127.0.0.1:6379/0
export HIVE_MODE=refresh
bash "${fixture}/run-thirdparties-docker.sh" -c hive3 --stop \
    >"${test_dir}/launcher.log" 2>&1
[[ ! -e "${STOP_TEST_PREPARE_MARKER}" ]]
grep -q 'compose -p rootless-stop-test-hive3 ' "${STOP_TEST_DOCKER_LOG}"
grep -q ' down$' "${STOP_TEST_DOCKER_LOG}"
bash "${fixture}/run-thirdparties-docker.sh" -c fluss --stop \
    >"${test_dir}/fluss-stop.log" 2>&1
grep -q '/fluss/fluss.yaml ' "${STOP_TEST_DOCKER_LOG}"
echo "Hive stop bypasses data preparation and only tears down compose"
echo "Fluss stop needs no network-address discovery"
