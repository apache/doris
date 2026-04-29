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

juicefs_init_runtime_vars() {
    DORIS_ROOT="${DORIS_ROOT:-$(cd "${ROOT}/../.." &>/dev/null && pwd)}"
    JUICEFS_RUNTIME_ROOT="${JUICEFS_RUNTIME_ROOT:-${ROOT}/juicefs}"
    JUICEFS_LOCAL_BIN="${JUICEFS_LOCAL_BIN:-${JUICEFS_RUNTIME_ROOT}/bin/juicefs}"
}

juicefs_find_runtime_hadoop_jar() {
    local -a jar_globs=(
        "${JUICEFS_RUNTIME_ROOT}/lib/juicefs-hadoop-[0-9]*.jar"
        "${ROOT}/docker-compose/hive/scripts/auxlib/juicefs-hadoop-[0-9]*.jar"
        "${DORIS_ROOT}/thirdparty/installed/juicefs_libs/juicefs-hadoop-[0-9]*.jar"
        "${DORIS_ROOT}/output/fe/lib/juicefs/juicefs-hadoop-[0-9]*.jar"
        "${DORIS_ROOT}/output/be/lib/java_extensions/juicefs/juicefs-hadoop-[0-9]*.jar"
    )
    juicefs_find_hadoop_jar_by_globs "${jar_globs[@]}"
}

juicefs_resolve_cli() {
    local juicefs_version
    local archive_name
    local tmp_dir
    local extracted_bin
    local cache_dir
    local -a download_urls=()

    if command -v juicefs >/dev/null 2>&1; then
        command -v juicefs
        return 0
    fi

    if [[ -x "${JUICEFS_LOCAL_BIN}" ]]; then
        echo "${JUICEFS_LOCAL_BIN}"
        return 0
    fi

    juicefs_version=$(juicefs_detect_hadoop_version "$(juicefs_find_runtime_hadoop_jar || true)" "${JUICEFS_DEFAULT_VERSION}")
    archive_name=$(juicefs_cli_archive_name "${juicefs_version}")
    cache_dir="${JUICEFS_RUNTIME_ROOT}/bin"
    mkdir -p "${cache_dir}"
    tmp_dir=$(mktemp -d "${cache_dir}/tmp.XXXXXX")
    mapfile -t download_urls < <(juicefs_cli_archive_download_urls "${juicefs_version}")
    if ! juicefs_download_file "${tmp_dir}/${archive_name}" "JuiceFS CLI ${juicefs_version}" "${download_urls[@]}"; then
        rm -rf "${tmp_dir}"
        return 1
    fi
    tar -xzf "${tmp_dir}/${archive_name}" -C "${tmp_dir}"
    extracted_bin=$(find "${tmp_dir}" -maxdepth 2 -type f -name juicefs | head -n 1)
    if [[ -z "${extracted_bin}" ]]; then
        rm -rf "${tmp_dir}"
        return 1
    fi
    install -m 0755 "${extracted_bin}" "${JUICEFS_LOCAL_BIN}"
    rm -rf "${tmp_dir}"
    echo "${JUICEFS_LOCAL_BIN}"
}

juicefs_ensure_meta_database() {
    local jfs_meta="$1"
    local meta_db
    local mysql_container

    meta_db="${jfs_meta##*/}"
    meta_db="${meta_db%%\?*}"
    if [[ ! "${jfs_meta}" == mysql://* ]] || [[ ! "${meta_db}" =~ ^[A-Za-z0-9_]+$ ]]; then
        return 0
    fi

    mysql_container="${CONTAINER_UID}mysql_57"
    if ! sudo docker ps --format '{{.Names}}' | grep -qx "${mysql_container}"; then
        return 0
    fi

    sudo docker exec "${mysql_container}" mysql -uroot -p123456 -e \
        "CREATE DATABASE IF NOT EXISTS \`${meta_db}\` CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;" >/dev/null
}

juicefs_run_cli() {
    local juicefs_cli
    juicefs_cli=$(juicefs_resolve_cli)
    "${juicefs_cli}" "$@"
}

juicefs_ensure_hadoop_jar_for_hive() {
    local auxlib_dir="${ROOT}/docker-compose/hive/scripts/auxlib"
    local source_jar
    local target_jar
    local source_realpath
    local target_realpath
    local juicefs_version

    source_jar=$(juicefs_find_runtime_hadoop_jar || true)
    if [[ -z "${source_jar}" ]]; then
        juicefs_version=$(juicefs_detect_hadoop_version "" "${JUICEFS_DEFAULT_VERSION}")
        source_jar=$(juicefs_download_hadoop_jar_to_cache "${juicefs_version}" "${JUICEFS_RUNTIME_ROOT}/lib" || true)
    fi
    if [[ -z "${source_jar}" ]]; then
        echo "WARN: skip syncing juicefs-hadoop jar for hive, not found and download failed."
        return 0
    fi

    mkdir -p "${auxlib_dir}"
    target_jar="${auxlib_dir}/$(basename "${source_jar}")"
    source_realpath=$(realpath "${source_jar}")
    if [[ -e "${target_jar}" ]]; then
        target_realpath=$(realpath "${target_jar}")
        if [[ "${source_realpath}" == "${target_realpath}" ]]; then
            echo "JuiceFS Hadoop jar already present in hive auxlib: $(basename "${source_jar}")"
            return 0
        fi
    fi
    cp -f "${source_jar}" "${target_jar}"
    echo "Synced JuiceFS Hadoop jar to hive auxlib: $(basename "${source_jar}")"
}

juicefs_prepare_meta_for_hive() {
    local jfs_meta="$1"
    local volume_name="$2"
    local bucket_dir="/tmp/jfs-bucket/${volume_name}"

    if [[ -z "${jfs_meta}" ]]; then
        return 0
    fi
    if ! juicefs_resolve_cli >/dev/null 2>&1; then
        echo "WARN: JuiceFS-dependent tests will fail. Ensure juicefs binary is on PATH or mirror/github access is available." >&2
        return 0
    fi

    mkdir -p "${bucket_dir}"
    juicefs_ensure_meta_database "${jfs_meta}"
    if juicefs_run_cli status "${jfs_meta}" >/dev/null 2>&1; then
        return 0
    fi
    rm -rf "${bucket_dir:?}/"*
    if ! juicefs_run_cli format --storage file --bucket "${bucket_dir}" "${jfs_meta}" "${volume_name}"; then
        juicefs_run_cli status "${jfs_meta}" >/dev/null 2>&1 || true
    fi
}
