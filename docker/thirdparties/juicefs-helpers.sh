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

# Shared JuiceFS helper functions used by build and docker scripts.

JUICEFS_DEFAULT_VERSION="${JUICEFS_DEFAULT_VERSION:-1.3.1}"
MAVEN_REPOSITORY_URL="${MAVEN_REPOSITORY_URL:-https://repo1.maven.org/maven2}"
JUICEFS_HADOOP_MAVEN_REPO="${JUICEFS_HADOOP_MAVEN_REPO:-${MAVEN_REPOSITORY_URL}/io/juicefs/juicefs-hadoop}"

juicefs_find_hadoop_jar_by_globs() {
    local jar_glob=""
    local matched_jar=""
    for jar_glob in "$@"; do
        matched_jar=$(compgen -G "${jar_glob}" | head -n 1 || true)
        if [[ -n "${matched_jar}" ]]; then
            echo "${matched_jar}"
            return 0
        fi
    done
    return 1
}

juicefs_detect_hadoop_version() {
    local juicefs_jar="$1"
    local default_version="${2:-${JUICEFS_DEFAULT_VERSION}}"
    if [[ -z "${juicefs_jar}" ]]; then
        echo "${default_version}"
        return 0
    fi
    juicefs_jar=$(basename "${juicefs_jar}")
    juicefs_jar=${juicefs_jar#juicefs-hadoop-}
    echo "${juicefs_jar%.jar}"
}

juicefs_hadoop_jar_download_url() {
    local juicefs_version="$1"
    local jar_name="juicefs-hadoop-${juicefs_version}.jar"
    echo "${JUICEFS_HADOOP_MAVEN_REPO}/${juicefs_version}/${jar_name}"
}

juicefs_download_hadoop_jar_to_cache() {
    local juicefs_version="$1"
    local cache_dir="$2"
    local jar_name="juicefs-hadoop-${juicefs_version}.jar"
    local target_jar="${cache_dir}/${jar_name}"
    local download_url
    download_url=$(juicefs_hadoop_jar_download_url "${juicefs_version}")

    mkdir -p "${cache_dir}"
    if [[ -s "${target_jar}" ]]; then
        echo "${target_jar}"
        return 0
    fi

    echo "Downloading JuiceFS Hadoop jar ${juicefs_version} from ${download_url}" >&2
    if command -v curl >/dev/null 2>&1; then
        if curl -fL --retry 3 --retry-delay 2 --connect-timeout 10 -o "${target_jar}" "${download_url}"; then
            echo "${target_jar}"
            return 0
        fi
    elif command -v wget >/dev/null 2>&1; then
        if wget -q "${download_url}" -O "${target_jar}"; then
            echo "${target_jar}"
            return 0
        fi
    fi

    rm -f "${target_jar}"
    return 1
}
