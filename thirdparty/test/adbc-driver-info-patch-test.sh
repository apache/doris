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
ARCHIVE_DIR="${1:?Usage: $0 directory-containing-apache-arrow-adbc-24.tar.gz}"
ARCHIVE_DIR="$(cd "${ARCHIVE_DIR}" && pwd)"
TP_DIR="${ROOT}"
# Load repository-owned definitions, never extracted dependency code.
source "${ROOT}/vars.sh"
mkdir -p "${ROOT}/src"
task_tmp="$(mktemp -d "${ROOT}/src/adbc-info-test.XXXXXX")"
trap 'rm -rf "${task_tmp}"' EXIT

prepare() {
    local dest="$1"
    mkdir -p "${dest}/src"
    cp "${ROOT}/vars.sh" "${dest}/vars.sh"
    ln -s "${ROOT}/patches" "${dest}/patches"
    cp "${ARCHIVE_DIR}/${ARROW_ADBC_NAME}" "${dest}/src/"
}

run_download() {
    TP_DIR="$1" DORIS_HOME="${ROOT}/.." bash "${ROOT}/download-thirdparty.sh" arrow_adbc
}

check_info() {
    local source_dir="$1/src/${ARROW_ADBC_SOURCE}"
    (
        cd "${source_dir}/go/adbc"
        GOWORK=off "${GO_CMD:-go}" test -race ./driver/internal/driverbase ./driver/flightsql \
            -run '^(TestConnectionDriverInfo|TestDorisFlightSql)' -count=1
    )
}

prepare "${task_tmp}/fresh"
run_download "${task_tmp}/fresh"
check_info "${task_tmp}/fresh"
touch "${task_tmp}/fresh/src/${ARROW_ADBC_SOURCE}/cache_reuse_sentinel"
run_download "${task_tmp}/fresh"
[[ -f "${task_tmp}/fresh/src/${ARROW_ADBC_SOURCE}/cache_reuse_sentinel" ]]

# The JNI-header patch marker may already exist in a cached third-party source tree.
# The independent info-patch marker must still install the isolation fix there.
prepare "${task_tmp}/cached"
tar xf "${task_tmp}/cached/src/${ARROW_ADBC_NAME}" -C "${task_tmp}/cached/src"
(
    cd "${task_tmp}/cached/src/${ARROW_ADBC_SOURCE}"
    patch -p1 <"${ROOT}/patches/apache-arrow-adbc-24-pregenerated-jni-header.patch"
    touch patched_mark
)
run_download "${task_tmp}/cached"
check_info "${task_tmp}/cached"
echo "PASS: fresh and previously patched ADBC sources isolate native server information"
