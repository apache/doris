#!/bin/sh
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
# Written in POSIX sh so it is compatible with both bash and dash.

jindofs_all_platform_jar_patterns() {
    printf '%s\n' \
        'jindo-core-linux-ubuntu22-x86_64-*.jar' \
        'jindo-core-linux-el7-aarch64-*.jar'
}

jindofs_platform_jar_patterns() {
    local target_system="$1"
    local target_arch="$2"

    [ "${target_system}" = "Linux" ] || return 1

    case "${target_arch}" in
        x86_64)
            printf '%s\n' 'jindo-core-linux-ubuntu22-x86_64-*.jar'
            ;;
        aarch64)
            printf '%s\n' 'jindo-core-linux-el7-aarch64-*.jar'
            ;;
        *)
            return 1
            ;;
    esac
}

# Prints "match" if jar_name matches any platform jar glob pattern; prints nothing otherwise.
# Callers use: [ -n "$(jindofs_is_platform_jar NAME)" ]
jindofs_is_platform_jar() {
    local jar_name="$1"
    local pattern
    jindofs_all_platform_jar_patterns | while IFS= read -r pattern; do
        case "${jar_name}" in
            ${pattern})
                echo "match"
                return
                ;;
        esac
    done
}

jindofs_find_common_jars() {
    local jindofs_dir="$1"
    local jar
    for jar in "${jindofs_dir}"/*.jar; do
        [ -f "${jar}" ] || continue
        if [ -z "$(jindofs_is_platform_jar "$(basename "${jar}")")" ]; then
            echo "${jar}"
        fi
    done | sort
}

jindofs_log_source_jars() {
    local jindofs_dir="$1"
    local target_dir="$2"
    local jar
    local count=0
    for jar in "${jindofs_dir}"/*.jar; do
        [ -f "${jar}" ] || continue
        count=$((count + 1))
    done
    echo "JindoFS source jar count for ${target_dir}: ${count}"
    for jar in "${jindofs_dir}"/*.jar; do
        [ -f "${jar}" ] || continue
        echo "JindoFS source jar for ${target_dir}: $(basename "${jar}")"
    done
}

jindofs_copy_jars() {
    local jindofs_dir="$1"
    local target_dir="$2"
    local target_system="$3"
    local target_arch="$4"
    local jar
    local platform_jar_pattern

    [ "${target_system}" = "Linux" ] || return 0

    case "${target_arch}" in
        x86_64 | aarch64) ;;
        *) return 0 ;;
    esac

    jindofs_log_source_jars "${jindofs_dir}" "${target_dir}"

    jindofs_find_common_jars "${jindofs_dir}" | while IFS= read -r jar; do
        cp -r -p "${jar}" "${target_dir}/"
        echo "Copy JindoFS jar to ${target_dir}: $(basename "${jar}")"
    done

    jindofs_platform_jar_patterns "${target_system}" "${target_arch}" | while IFS= read -r platform_jar_pattern; do
        for jar in "${jindofs_dir}"/${platform_jar_pattern}; do
            [ -f "${jar}" ] || continue
            cp -r -p "${jar}" "${target_dir}/"
            echo "Copy JindoFS jar to ${target_dir}: $(basename "${jar}")"
        done
    done
}
