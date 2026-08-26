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

fail() {
    echo "FAIL: $*" >&2
    exit 1
}

tmpdir="$(mktemp -d)"
trap 'rm -rf "${tmpdir}"' EXIT

fixture="${tmpdir}/paimon-codec-dependency"
focused_patch="${fixture}/codec-dependency.patch"
mkdir -p "${fixture}/src/paimon"
{
    printf '%s\n' 'add_paimon_lib(paimon'
    printf '%s\n' '               SOURCES'
    printf '%s\n' '               ${PAIMON_COMMON_SRCS}'
    printf '%s\n' '               ${PAIMON_CORE_SRCS}'
    printf '%s\n' '               DEPENDENCIES'
    printf '%s\n' '               arrow'
    printf '%s\n' '               tbb'
    printf '%s\n' '               glog'
    printf '%s\n' '               fmt'
    printf '%s\n' '               roaring_bitmap'
    printf '%s\n' '               xxhash'
    printf '%s\n' '               Threads::Threads'
    printf '%s\n' '               RapidJSON'
    printf '%s\n' '               STATIC_LINK_LIBS'
    printf '%s\n' '               arrow)'
} >"${fixture}/src/paimon/CMakeLists.txt"

awk '
    /^diff --git / {
        if (found) {
            exit
        }
        if ($0 == "diff --git a/src/paimon/CMakeLists.txt b/src/paimon/CMakeLists.txt") {
            found = 1
        }
    }
    found { print }
' "${ROOT}/patches/paimon-cpp-buildutils-static-deps.patch" >"${focused_patch}"
[[ -s "${focused_patch}" ]] || fail "the Paimon codec dependency patch is missing"
(
    cd "${fixture}"
    patch -s -p1 <"${focused_patch}"
) || fail "the Paimon codec dependency patch did not apply"

dependencies="$(sed -n '/^[[:space:]]*DEPENDENCIES$/,/^[[:space:]]*STATIC_LINK_LIBS$/p' \
    "${fixture}/src/paimon/CMakeLists.txt")"
grep -Eq '^[[:space:]]+zstd$' <<<"${dependencies}" ||
    fail "paimon_objlib does not wait for the ZSTD headers"
grep -Eq '^[[:space:]]+snappy$' <<<"${dependencies}" ||
    fail "paimon_objlib does not wait for the Snappy headers"
grep -Eq '^[[:space:]]+lz4$' <<<"${dependencies}" ||
    fail "paimon_objlib does not wait for the LZ4 headers"

echo "PASS"
