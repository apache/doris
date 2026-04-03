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

assert_eq() {
    local expected="$1"
    local actual="$2"
    [[ "${actual}" == "${expected}" ]] || fail "expected '${expected}', got '${actual}'"
}

run_copy_case() {
    local shell_bin="$1"
    local target_arch="$2"
    local tmpdir
    local src_dir
    local target_dir

    tmpdir="$(mktemp -d)"
    src_dir="${tmpdir}/src"
    target_dir="${tmpdir}/target"
    mkdir -p "${src_dir}" "${target_dir}"

    touch "${src_dir}/jindo-core-6.10.4.jar"
    touch "${src_dir}/jindo-sdk-6.10.4.jar"
    touch "${src_dir}/jindo-common-1.0.jar"
    touch "${src_dir}/jindo-core-linux-ubuntu22-x86_64-6.10.4.jar"
    touch "${src_dir}/jindo-core-linux-el7-aarch64-6.10.4.jar"

    "${shell_bin}" -c '
        helper_path="$1"
        source_dir="$2"
        output_dir="$3"
        arch="$4"
        . "${helper_path}"
        jindofs_copy_jars "${source_dir}" "${output_dir}" "Linux" "${arch}"
    ' _ "${ROOT}/jindofs-helpers.sh" "${src_dir}" "${target_dir}" "${target_arch}" || fail "copy failed under ${shell_bin} for ${target_arch}"

    if [[ "${target_arch}" == "x86_64" ]]; then
        assert_eq $'jindo-common-1.0.jar\njindo-core-6.10.4.jar\njindo-core-linux-ubuntu22-x86_64-6.10.4.jar\njindo-sdk-6.10.4.jar' \
            "$(find "${target_dir}" -maxdepth 1 -type f -printf '%f\n' | sort)"
    else
        assert_eq $'jindo-common-1.0.jar\njindo-core-6.10.4.jar\njindo-core-linux-el7-aarch64-6.10.4.jar\njindo-sdk-6.10.4.jar' \
            "$(find "${target_dir}" -maxdepth 1 -type f -printf '%f\n' | sort)"
    fi

    rm -rf "${tmpdir}"
}

run_copy_case bash x86_64
run_copy_case bash aarch64
run_copy_case sh x86_64
run_copy_case sh aarch64

echo "PASS"
