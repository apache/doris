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
JUICEFS_THIRDPARTY_REPOSITORY_URL="${JUICEFS_THIRDPARTY_REPOSITORY_URL:-}"
JUICEFS_DEFAULT_THIRDPARTY_REPOSITORY_URL="${JUICEFS_DEFAULT_THIRDPARTY_REPOSITORY_URL:-}"
JUICEFS_HADOOP_MAVEN_REPO="${JUICEFS_HADOOP_MAVEN_REPO:-${MAVEN_REPOSITORY_URL}/io/juicefs/juicefs-hadoop}"
JUICEFS_CLI_RELEASE_REPO="${JUICEFS_CLI_RELEASE_REPO:-https://github.com/juicedata/juicefs/releases/download}"

juicefs_default_thirdparty_repository_url() {
    if [[ -n "${JUICEFS_DEFAULT_THIRDPARTY_REPOSITORY_URL}" ]]; then
        echo "${JUICEFS_DEFAULT_THIRDPARTY_REPOSITORY_URL%/}"
        return 0
    fi
    if [[ -n "${s3BucketName:-}" && -n "${s3Endpoint:-}" ]]; then
        echo "https://${s3BucketName}.${s3Endpoint}/regression/datalake/thirdparty/juicefs"
        return 0
    fi
    echo "https://doris-thirdparty-repo.bj.bcebos.com/thirdparty"
}

juicefs_thirdparty_repository_url() {
    local repository_url="${JUICEFS_THIRDPARTY_REPOSITORY_URL:-${REPOSITORY_URL:-}}"
    if [[ -z "${repository_url}" ]]; then
        repository_url=$(juicefs_default_thirdparty_repository_url)
    fi
    echo "${repository_url%/}"
}

juicefs_repository_file_url() {
    local filename="$1"
    echo "$(juicefs_thirdparty_repository_url)/${filename}"
}

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

juicefs_hadoop_jar_download_urls() {
    local juicefs_version="$1"
    local jar_name="juicefs-hadoop-${juicefs_version}.jar"
    printf '%s\n' \
        "$(juicefs_repository_file_url "${jar_name}")" \
        "$(juicefs_hadoop_jar_download_url "${juicefs_version}")"
}

juicefs_cli_archive_name() {
    local juicefs_version="$1"
    echo "juicefs-${juicefs_version}-linux-amd64.tar.gz"
}

juicefs_cli_archive_mirror_url() {
    local juicefs_version="$1"
    local archive_name
    archive_name=$(juicefs_cli_archive_name "${juicefs_version}")
    juicefs_repository_file_url "${archive_name}"
}

juicefs_cli_archive_download_url() {
    local juicefs_version="$1"
    local archive_name
    archive_name=$(juicefs_cli_archive_name "${juicefs_version}")
    echo "${JUICEFS_CLI_RELEASE_REPO}/v${juicefs_version}/${archive_name}"
}

juicefs_cli_archive_download_urls() {
    local juicefs_version="$1"
    printf '%s\n' \
        "$(juicefs_cli_archive_mirror_url "${juicefs_version}")" \
        "$(juicefs_cli_archive_download_url "${juicefs_version}")"
}

juicefs_download_file() {
    local target_path="$1"
    local download_label="$2"
    shift 2

    local download_url=""
    mkdir -p "$(dirname "${target_path}")"
    for download_url in "$@"; do
        [[ -n "${download_url}" ]] || continue
        echo "Downloading ${download_label} from ${download_url}" >&2
        if command -v curl >/dev/null 2>&1; then
            if curl -fL --retry 3 --retry-delay 2 --connect-timeout 10 -o "${target_path}" "${download_url}"; then
                return 0
            fi
        elif command -v wget >/dev/null 2>&1; then
            if wget -q "${download_url}" -O "${target_path}"; then
                return 0
            fi
        fi
    done

    rm -f "${target_path}"
    return 1
}

juicefs_download_hadoop_jar_to_cache() {
    local juicefs_version="$1"
    local cache_dir="$2"
    local jar_name="juicefs-hadoop-${juicefs_version}.jar"
    local target_jar="${cache_dir}/${jar_name}"
    local -a download_urls=()

    mkdir -p "${cache_dir}"
    if [[ -s "${target_jar}" ]]; then
        echo "${target_jar}"
        return 0
    fi

    mapfile -t download_urls < <(juicefs_hadoop_jar_download_urls "${juicefs_version}")
    if juicefs_download_file "${target_jar}" "JuiceFS Hadoop jar ${juicefs_version}" "${download_urls[@]}"; then
        echo "${target_jar}"
        return 0
    fi

    return 1
}
