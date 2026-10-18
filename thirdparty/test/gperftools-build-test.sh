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

# Build the pinned gperftools using the production build function, then exercise
# SIGPROF unwinding. Accept a local archive directory for offline reproduction.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." &>/dev/null && pwd)"
TP_DIR="${ROOT}"
source "${ROOT}/vars.sh"
work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT
export TP_SOURCE_DIR="${work}/src"
export TP_INSTALL_DIR="${work}/installed"
export TP_INCLUDE_DIR="${TP_INSTALL_DIR}/include"
export TP_LIB_DIR="${TP_INSTALL_DIR}/lib"
export KERNEL="$(uname -s)"
export PARALLEL="${PARALLEL:-2}"
export CC="${CC:-gcc}" CXX="${CXX:-g++}"
mkdir -p "${TP_SOURCE_DIR}" "${TP_INCLUDE_DIR}" "${TP_LIB_DIR}"
if [[ "$#" == 1 ]]; then
    cp "$1/${GPERFTOOLS_NAME}" "${TP_SOURCE_DIR}/${GPERFTOOLS_NAME}"
else
    curl --fail --location --retry 3 "${GPERFTOOLS_DOWNLOAD}" -o "${TP_SOURCE_DIR}/${GPERFTOOLS_NAME}"
fi
printf '%s  %s\n' "${GPERFTOOLS_MD5SUM}" "${TP_SOURCE_DIR}/${GPERFTOOLS_NAME}" | md5sum -c -
tar xzf "${TP_SOURCE_DIR}/${GPERFTOOLS_NAME}" -C "${TP_SOURCE_DIR}"
# As in the other focused thirdparty tests, load repository-owned function text
# without executing build-thirdparty.sh's whole-tree setup and download steps.
sed -n '/^build_gperftools() {$/,/^}$/p' "${ROOT}/build-thirdparty.sh" >"${work}/build.sh"
grep -q '^}$' "${work}/build.sh"
source "${work}/build.sh"
check_if_source_exist() { test -d "${TP_SOURCE_DIR}/$1"; }
(
    build_gperftools
    make check -j "${PARALLEL}"
)
bash "${ROOT}/test/gperftools-profiler-test.sh"
