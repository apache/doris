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

# Integration test: requires the current user's Docker daemon and Python 3.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." &>/dev/null && pwd)"
HOST_OS="$(uname -s)"
. "${ROOT}/docker-command.sh"
configure_docker_access

test_dir="$(mktemp -d /tmp/doris-snapshot-test.XXXXXX)"
prefix="$(basename "${test_dir}" | tr '[:upper:]' '[:lower:]')"
image="${THIRDPARTY_FILESYSTEM_IMAGE:-alpine:3.20}"
snapshot="${ROOT}/docker-compose/hive/scripts/snapshot-hive-baseline.sh"
volumes=()

cleanup() {
    if ((${#volumes[@]})); then
        docker volume rm "${volumes[@]}" >/dev/null
    fi
    rm -rf "${test_dir}"
}
trap cleanup EXIT

# Missing volumes must fail before producing an output file.
if bash "${snapshot}" "${prefix}" "${test_dir}/missing.tar.gz" >"${test_dir}/missing.log" 2>&1; then
    echo "FAIL: snapshot accepted missing volumes" >&2
    exit 1
fi
[[ ! -e "${test_dir}/missing.tar.gz" ]]

for suffix in namenode datanode pgdata state; do
    volumes+=("${prefix}-${suffix}")
    docker volume create "${prefix}-${suffix}" >/dev/null
done

docker run --rm --network none --security-opt no-new-privileges \
    -v "${prefix}-namenode:/fixture/namenode" \
    -v "${prefix}-datanode:/fixture/datanode" \
    -v "${prefix}-pgdata:/fixture/pgdata" \
    -v "${prefix}-state:/fixture/state" \
    "${image}" sh -ec '
        for part in namenode datanode pgdata state; do
            printf "%s" "$part" >"/fixture/$part/payload"
        done
        printf legacy >/fixture/state/baseline.version
        chown 123:456 /fixture/pgdata /fixture/pgdata/payload
        chmod 700 /fixture/pgdata
        chmod 600 /fixture/pgdata/payload
    '

bash "${snapshot}" "${prefix}" "${test_dir}/baseline.tar.gz"
python3 - "${test_dir}/baseline.tar.gz" <<'PY'
import sys
import tarfile

with tarfile.open(sys.argv[1]) as archive:
    for part in ['namenode', 'datanode', 'pgdata', 'state']:
        assert archive.extractfile(part + '/payload').read() == part.encode()
    assert 'state/baseline.version' not in archive.getnames()
    entry = archive.getmember('pgdata/payload')
    assert (entry.uid, entry.gid, entry.mode) == (123, 456, 0o600)
PY
docker run --rm --network none -v "${prefix}-state:/fixture:ro" \
    "${image}" test -f /fixture/baseline.version

# An interrupted exporter must preserve an existing baseline and remove its
# temporary output. Forward info/volume queries to the real rootless daemon.
printf previous-baseline >"${test_dir}/previous.tar.gz"
docker() {
    if [[ "${1:-}" == "run" ]]; then
        return 42
    fi
    command docker "$@"
}
export -f docker
status=0
bash "${snapshot}" "${prefix}" "${test_dir}/previous.tar.gz" \
    >"${test_dir}/failure.log" 2>&1 || status=$?
unset -f docker
[[ "${status}" -eq 42 ]]
[[ "$(cat "${test_dir}/previous.tar.gz")" == previous-baseline ]]
[[ "$(find "${test_dir}" -name 'previous.tar.gz.*' | wc -l | tr -d ' ')" -eq 0 ]]
echo "Snapshot export, UID/GID preservation and failure cleanup passed"
