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
SCRIPT="${ROOT}/docker-compose/kerberos/configure-apt-sources.sh"

fail() {
    echo "FAIL: $*" >&2
    exit 1
}

assert_contains() {
    local expected=$1
    local file=$2
    grep -Fq "${expected}" "${file}" || fail "expected '${expected}' in ${file}"
}

assert_not_contains() {
    local unexpected=$1
    local file=$2
    if grep -Fq "${unexpected}" "${file}"; then
        fail "did not expect '${unexpected}' in ${file}"
    fi
}

[[ -x "${SCRIPT}" ]] || fail "missing executable ${SCRIPT}"

test_root="$(mktemp -d)"
trap 'rm -rf "${test_root}"' EXIT
mkdir -p "${test_root}/sources.list.d"

cat >"${test_root}/sources.list" <<'EOF'
deb http://deb.debian.org/debian bullseye main
deb https://security.debian.org/debian-security bullseye-security main
EOF

cat >"${test_root}/sources.list.d/debian.sources" <<'EOF'
Types: deb
URIs: https://deb.debian.org/debian http://deb.debian.org/debian-security
Suites: bullseye bullseye-updates bullseye-security
Components: main
EOF

cat >"${test_root}/sources.list.d/vendor.list" <<'EOF'
deb https://packages.example.com/debian stable main
EOF

APT_SOURCES_DIR="${test_root}" "${SCRIPT}"

assert_contains "deb https://archive.debian.org/debian bullseye main" \
    "${test_root}/sources.list"
assert_contains "deb https://archive.debian.org/debian-security bullseye-security main" \
    "${test_root}/sources.list"
assert_contains \
    "URIs: https://archive.debian.org/debian https://archive.debian.org/debian-security" \
    "${test_root}/sources.list.d/debian.sources"

for source_file in "${test_root}/sources.list" \
        "${test_root}/sources.list.d/debian.sources"; do
    assert_not_contains "deb.debian.org" "${source_file}"
    assert_not_contains "security.debian.org" "${source_file}"
done

assert_contains "https://packages.example.com/debian" \
    "${test_root}/sources.list.d/vendor.list"

echo "PASS"
