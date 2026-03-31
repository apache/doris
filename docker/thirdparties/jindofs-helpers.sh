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

jindofs_find_common_jars() {
    local jindofs_dir="$1"
    local jar=""
    while IFS= read -r jar; do
        case "$(basename "${jar}")" in
        jindo-core-linux-*.jar)
            ;;
        *)
            echo "${jar}"
            ;;
        esac
    done < <(compgen -G "${jindofs_dir}/*.jar" | sort)
}

jindofs_platform_jar_glob() {
    local jindofs_dir="$1"
    local target_system="$2"
    local target_arch="$3"

    if [[ "${target_system}" != "Linux" ]]; then
        return 1
    fi

    if [[ "${target_arch}" == "x86_64" ]]; then
        echo "${jindofs_dir}/jindo-core-linux-ubuntu22-x86_64-[0-9]*.jar"
        return 0
    fi

    if [[ "${target_arch}" == "aarch64" ]]; then
        echo "${jindofs_dir}/jindo-core-linux-el7-aarch64-[0-9]*.jar"
        return 0
    fi

    return 1
}

jindofs_copy_jars() {
    local jindofs_dir="$1"
    local target_dir="$2"
    local target_system="$3"
    local target_arch="$4"
    local jar=""
    local platform_jar_glob=""

    if [[ "${target_system}" != "Linux" ]]; then
        return 0
    fi

    if [[ "${target_arch}" != "x86_64" && "${target_arch}" != "aarch64" ]]; then
        return 0
    fi

    while IFS= read -r jar; do
        cp -r -p "${jar}" "${target_dir}/"
        echo "Copy JindoFS jar to ${target_dir}: $(basename "${jar}")"
    done < <(jindofs_find_common_jars "${jindofs_dir}")

    platform_jar_glob=$(jindofs_platform_jar_glob "${jindofs_dir}" "${target_system}" "${target_arch}" || true)
    if [[ -z "${platform_jar_glob}" ]]; then
        return 0
    fi

    while IFS= read -r jar; do
        cp -r -p "${jar}" "${target_dir}/"
        echo "Copy JindoFS jar to ${target_dir}: $(basename "${jar}")"
    done < <(compgen -G "${platform_jar_glob}" | sort)
}
