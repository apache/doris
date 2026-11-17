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

set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." &>/dev/null && pwd)"
. "${ROOT}/docker-compose/fluss/fluss-network.sh"
test_dir="$(mktemp -d /tmp/doris-fluss-network-test.XXXXXX)"
trap 'rm -rf "${test_dir}"' EXIT
mkdir "${test_dir}/bin"
cat >"${test_dir}/bin/ip" <<'EOF'
#!/bin/sh
test "${TEST_ROUTE_FAIL:-0}" = 0 || exit 1
printf '1.1.1.1 via 192.0.2.1 dev eth0 src %s uid 1000\n' "${TEST_ROUTE_IP}"
EOF
cat >"${test_dir}/bin/hostname" <<'EOF'
#!/bin/sh
test "${TEST_HOSTNAME_FAIL:-0}" = 0 || exit 1
printf '%s\n' "${TEST_HOST_IPS}"
EOF
chmod +x "${test_dir}/bin/ip" "${test_dir}/bin/hostname"
export PATH="${test_dir}/bin:${PATH}"
export TEST_ROUTE_IP=192.0.2.10 TEST_HOST_IPS='127.0.0.1 192.0.2.11 ::1'
export HOST_OS=Linux
unset FLUSS_HOST_IP
for IP_HOST in 127.0.0.1 localhost 0.0.0.0 ::1; do
    [[ "$(resolve_fluss_host_ip)" == 192.0.2.10 ]]
done
[[ "${IP_HOST}" == ::1 ]] # Resolving Fluss must not change other fixtures.

TEST_ROUTE_FAIL=1
export TEST_ROUTE_FAIL
IP_HOST=192.0.2.20
[[ "$(resolve_fluss_host_ip)" == 192.0.2.20 ]]
FLUSS_HOST_IP=build-host.example
[[ "$(resolve_fluss_host_ip)" == build-host.example ]]
for FLUSS_HOST_IP in 127.0.0.1 localhost localhost. 0.0.0.0 ::1 '[::1]' :: '[::]'; do
    if resolve_fluss_host_ip >/dev/null 2>&1; then
        echo "FAIL: advertised an unusable address" >&2
        exit 1
    fi
done
unset FLUSS_HOST_IP
IP_HOST=127.0.0.1
[[ "$(resolve_fluss_host_ip)" == 192.0.2.11 ]]
export TEST_HOSTNAME_FAIL=1
if resolve_fluss_host_ip >/dev/null 2>&1; then
    echo "FAIL: accepted missing host addresses" >&2
    exit 1
fi
HOST_OS=Darwin
if resolve_fluss_host_ip >/dev/null 2>&1; then
    echo "FAIL: used loopback for Docker Desktop bridge clients" >&2
    exit 1
fi
FLUSS_HOST_IP=192.0.2.30
[[ "$(resolve_fluss_host_ip)" == 192.0.2.30 ]]
echo "Fluss host-address selection tests passed"
