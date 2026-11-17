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
TEST_DIR="$(mktemp -d /tmp/doris-docker-test.XXXXXX)"
TEST_LOG="${TEST_DIR}/commands.log"
ORIGINAL_PATH="${PATH}"
trap 'rm -rf "${TEST_DIR}"' EXIT

fail() {
    echo "FAIL: $*" >&2
    exit 1
}

mkdir -p "${TEST_DIR}/bin"

cat >"${TEST_DIR}/bin/docker" <<'EOF'
#!/usr/bin/env bash
if [[ "${1:-}" == "info" ]]; then
    if [[ "${DOCKER_INFO_EXIT:-0}" -ne 0 ]]; then
        exit "${DOCKER_INFO_EXIT}"
    fi
    if [[ "${2:-}" == "--format" ]]; then
        if [[ "${3}" == '{{.ServerVersion}}' ]]; then
            printf '%s\n' "${DOCKER_SERVER_VERSION}"
        else
            printf '["name=%s"]\n' "${DOCKER_SECURITY_MODE}"
        fi
    fi
    exit 0
fi
printf 'docker %s\n' "$*" >>"${TEST_LOG}"
EOF

cat >"${TEST_DIR}/bin/sudo" <<'EOF'
#!/usr/bin/env bash
printf 'sudo %s\n' "$*" >>"${TEST_LOG}"
exit 125
EOF

chmod +x "${TEST_DIR}/bin/docker" "${TEST_DIR}/bin/sudo"
export PATH="${TEST_DIR}/bin:${ORIGINAL_PATH}"
export TEST_LOG DOCKER_SECURITY_MODE
HOST_OS=Linux

# shellcheck source=../docker-command.sh
. "${ROOT}/docker-command.sh"

docker_cli() {
    docker "$@"
}

unset DOCKER_HOST DOCKER_CONTEXT
DOCKER_SECURITY_MODE=rootless
configure_docker_access
: >"${TEST_LOG}"
export DOCKER_USE_SUDO=1 # A legacy environment must not enable elevation.
docker_cli ps
unset DOCKER_USE_SUDO
grep -Fxq 'docker ps' "${TEST_LOG}" || fail "docker_cli did not call Docker directly"
if grep -q '^sudo ' "${TEST_LOG}"; then
    fail "rootless mode invoked sudo"
fi

export DOCKER_SERVER_VERSION=29.4.0
if require_rootless_host_network 2>/dev/null; then
    fail "rootless Docker before 29.5 was accepted for host networking"
fi
DOCKER_SERVER_VERSION=29.5.0
require_rootless_host_network || fail "rootless Docker 29.5 was rejected"
DOCKER_SERVER_VERSION=30.0.0
require_rootless_host_network || fail "newer rootless Docker was rejected"

DOCKER_SECURITY_MODE=seccomp
: >"${TEST_LOG}"
if configure_docker_access 2>/dev/null; then
    fail "Linux accepted a rootful daemon"
fi
[[ ! -s "${TEST_LOG}" ]] || fail "a rootful daemon triggered elevation"

# macOS keeps direct Docker Desktop access without Linux rootless detection.
HOST_OS=Darwin
configure_docker_access || fail "Docker Desktop was rejected"
require_rootless_host_network || fail "Linux version requirement applied to macOS"
HOST_OS=Linux
DOCKER_SECURITY_MODE=rootless

# Discover the user's socket even if the default system daemon is accessible.
export XDG_RUNTIME_DIR="${TEST_DIR}/runtime"
mkdir -p "${XDG_RUNTIME_DIR}"
python3 - "${XDG_RUNTIME_DIR}/docker.sock" <<'PY'
import socket
import sys
connection = socket.socket(socket.AF_UNIX)
connection.bind(sys.argv[1])
connection.close()
PY
unset DOCKER_HOST DOCKER_CONTEXT
configure_docker_access
[[ "${DOCKER_HOST}" == "unix://${XDG_RUNTIME_DIR}/docker.sock" ]] || fail "user socket was not discovered"

DOCKER_HOST=unix:///explicit/docker.sock
configure_docker_access
[[ "${DOCKER_HOST}" == unix:///explicit/docker.sock ]] || fail "explicit endpoint was replaced"
unset DOCKER_HOST
export DOCKER_CONTEXT=review-rootless
configure_docker_access
[[ -z "${DOCKER_HOST+x}" ]] || fail "explicit context was replaced"
unset DOCKER_CONTEXT

# A failed daemon probe must not run a host operation or silently escalate it.
export DOCKER_INFO_EXIT=17
export DOCKER_HOST="unix://${XDG_RUNTIME_DIR}/docker.sock"
: >"${TEST_LOG}"
status=0
(configure_docker_access && touch "${TEST_DIR}/unexpected") 2>/dev/null || status=$?
[[ "${status}" -eq 1 ]] || fail "failed daemon probe did not propagate its error"
[[ ! -e "${TEST_DIR}/unexpected" ]] || fail "host command ran after daemon probe failed"
[[ ! -s "${TEST_LOG}" ]] || fail "failed daemon probe invoked sudo or another Docker operation"

echo "PASS"
