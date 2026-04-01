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

# Shared JindoFS helper functions used by build packaging.

JINDOFS_PLATFORM_JAR_PATTERNS_X86_64=(
    'jindo-core-linux-ubuntu22-x86_64-*.jar'
)

JINDOFS_PLATFORM_JAR_PATTERNS_AARCH64=(
    'jindo-core-linux-el7-aarch64-*.jar'
)

jindofs_all_platform_jar_patterns() {
    printf '%s\n' \
        "${JINDOFS_PLATFORM_JAR_PATTERNS_X86_64[@]}" \
        "${JINDOFS_PLATFORM_JAR_PATTERNS_AARCH64[@]}"
}

jindofs_platform_jar_patterns() {
    local target_system="$1"
    local target_arch="$2"

    if [[ "${target_system}" != "Linux" ]]; then
        return 1
    fi

    if [[ "${target_arch}" == "x86_64" ]]; then
        printf '%s\n' "${JINDOFS_PLATFORM_JAR_PATTERNS_X86_64[@]}"
        return 0
    fi

    if [[ "${target_arch}" == "aarch64" ]]; then
        printf '%s\n' "${JINDOFS_PLATFORM_JAR_PATTERNS_AARCH64[@]}"
        return 0
    fi

    return 1
}

jindofs_is_platform_jar() {
    local jar_name="$1"
    local pattern=""
    while IFS= read -r pattern; do
        if [[ -n "${pattern}" && "${jar_name}" == ${pattern} ]]; then
            return 0
        fi
    done < <(jindofs_all_platform_jar_patterns)
    return 1
}

jindofs_find_common_jars() {
    local jindofs_dir="$1"
    local jar=""
    while IFS= read -r jar; do
        if ! jindofs_is_platform_jar "$(basename "${jar}")"; then
            echo "${jar}"
        fi
    done < <(compgen -G "${jindofs_dir}/*.jar" | sort)
}

jindofs_log_source_jars() {
    local jindofs_dir="$1"
    local target_dir="$2"
    local jar=""
    local -a jars=()

    while IFS= read -r jar; do
        jars+=("${jar}")
    done < <(compgen -G "${jindofs_dir}/*.jar" | sort)

    echo "JindoFS source jar count for ${target_dir}: ${#jars[@]}"
    for jar in "${jars[@]}"; do
        echo "JindoFS source jar for ${target_dir}: $(basename "${jar}")"
    done
}

jindofs_copy_jars() {
    local jindofs_dir="$1"
    local target_dir="$2"
    local target_system="$3"
    local target_arch="$4"
    local jar=""
    local platform_jar_pattern=""

    if [[ "${target_system}" != "Linux" ]]; then
        return 0
    fi

    if [[ "${target_arch}" != "x86_64" && "${target_arch}" != "aarch64" ]]; then
        return 0
    fi

    jindofs_log_source_jars "${jindofs_dir}" "${target_dir}"

    while IFS= read -r jar; do
        cp -r -p "${jar}" "${target_dir}/"
        echo "Copy JindoFS jar to ${target_dir}: $(basename "${jar}")"
    done < <(jindofs_find_common_jars "${jindofs_dir}")

    while IFS= read -r platform_jar_pattern; do
        while IFS= read -r jar; do
            cp -r -p "${jar}" "${target_dir}/"
            echo "Copy JindoFS jar to ${target_dir}: $(basename "${jar}")"
        done < <(compgen -G "${jindofs_dir}/${platform_jar_pattern}" | sort)
    done < <(jindofs_platform_jar_patterns "${target_system}" "${target_arch}" || true)
}
