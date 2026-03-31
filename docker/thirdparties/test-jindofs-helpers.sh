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

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." &>/dev/null && pwd)"
. "${ROOT}/docker/thirdparties/jindofs-helpers.sh"

assert_equals() {
    local expected="$1"
    local actual="$2"
    local message="$3"
    if [[ "${expected}" != "${actual}" ]]; then
        echo "ASSERTION FAILED: ${message}" >&2
        echo "Expected:" >&2
        printf '%s\n' "${expected}" >&2
        echo "Actual:" >&2
        printf '%s\n' "${actual}" >&2
        return 1
    fi
}

jar_set_in_dir() {
    local dir="$1"
    find "${dir}" -maxdepth 1 -type f -name '*.jar' -printf '%f\n' | sort
}

basename_set() {
    local jar=""
    while IFS= read -r jar; do
        basename "${jar}"
    done | sort
}

tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT

source_dir="${tmp_dir}/src"
x64_target_dir="${tmp_dir}/x64"
arm_target_dir="${tmp_dir}/arm"
mkdir -p "${source_dir}" "${x64_target_dir}" "${arm_target_dir}"

touch "${source_dir}/bennett-iceberg-plugin-2.8.0.jar"
touch "${source_dir}/jindo-core-6.10.4.jar"
touch "${source_dir}/jindo-core-linux-el7-aarch64-6.10.4.jar"
touch "${source_dir}/jindo-core-linux-ubuntu22-x86_64-6.10.4.jar"
touch "${source_dir}/jindo-sdk-6.10.4.jar"

expected_common_jars=$'bennett-iceberg-plugin-2.8.0.jar\njindo-core-6.10.4.jar\njindo-sdk-6.10.4.jar'
actual_common_jars="$(jindofs_find_common_jars "${source_dir}" | basename_set)"
assert_equals "${expected_common_jars}" "${actual_common_jars}" "common jars should exclude platform-specific jars"

expected_x64_platform_jar='jindo-core-linux-ubuntu22-x86_64-6.10.4.jar'
actual_x64_platform_jar="$(compgen -G "$(jindofs_platform_jar_glob "${source_dir}" Linux x86_64)" | basename_set)"
assert_equals "${expected_x64_platform_jar}" "${actual_x64_platform_jar}" "x86_64 platform jar glob should match the ubuntu22 jar"

expected_arm_platform_jar='jindo-core-linux-el7-aarch64-6.10.4.jar'
actual_arm_platform_jar="$(compgen -G "$(jindofs_platform_jar_glob "${source_dir}" Linux aarch64)" | basename_set)"
assert_equals "${expected_arm_platform_jar}" "${actual_arm_platform_jar}" "aarch64 platform jar glob should match the el7 jar"

jindofs_copy_jars "${source_dir}" "${x64_target_dir}" Linux x86_64
expected_x64_target_jars=$'bennett-iceberg-plugin-2.8.0.jar\njindo-core-6.10.4.jar\njindo-core-linux-ubuntu22-x86_64-6.10.4.jar\njindo-sdk-6.10.4.jar'
actual_x64_target_jars="$(jar_set_in_dir "${x64_target_dir}")"
assert_equals "${expected_x64_target_jars}" "${actual_x64_target_jars}" "x86_64 copy should include common jars and the x86_64 platform jar"

jindofs_copy_jars "${source_dir}" "${arm_target_dir}" Linux aarch64
expected_arm_target_jars=$'bennett-iceberg-plugin-2.8.0.jar\njindo-core-6.10.4.jar\njindo-core-linux-el7-aarch64-6.10.4.jar\njindo-sdk-6.10.4.jar'
actual_arm_target_jars="$(jar_set_in_dir "${arm_target_dir}")"
assert_equals "${expected_arm_target_jars}" "${actual_arm_target_jars}" "aarch64 copy should include common jars and the aarch64 platform jar"

echo "JindoFS helper tests passed"
