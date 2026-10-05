#!/bin/bash
set -eo pipefail
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

CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
. "${CUR_DIR}/bootstrap/bootstrap-groups.sh"

BOOTSTRAP_GROUPS="$(bootstrap_normalize_groups "${HIVE_BOOTSTRAP_GROUPS:-}")"
PIPELINE_DATA_URL_PREFIX="https://${s3BucketName}.${s3Endpoint}/regression/datalake/pipeline_data"
HIVE_AUXLIB_URL_PREFIX="https://${s3BucketName}.${s3Endpoint}/regression/docker/hive3"
echo "Prepare hive data with bootstrap groups: ${BOOTSTRAP_GROUPS}"

extract_archives=()
while IFS= read -r -d '' archive_path; do
    relative_archive_path="${archive_path#${CUR_DIR}/}"
    if bootstrap_archive_selected "${BOOTSTRAP_GROUPS}" "${relative_archive_path}"; then
        extract_archives+=("${archive_path}")
    fi
done < <(find "${CUR_DIR}/data" -type f -name "*.tar.gz" -print0)

if (( ${#extract_archives[@]} > 0 )); then
    printf '%s\0' "${extract_archives[@]}" | xargs -0 -n1 -P"${LOAD_PARALLEL}" bash -c '
      f="$0"
      echo "Extracting hive data $f"
      dir=$(dirname "$f")
      tar -xzf "$f" -C "$dir"
    '
fi

download_archive_if_missing() {
    local relative_dir="$1"
    local workdir="$2"
    local remote_path="$3"
    local archive_name="$4"
    local target_dir="${CUR_DIR}/${relative_dir}"

    if ! bootstrap_item_selected "${BOOTSTRAP_GROUPS}" "download_dir" "${relative_dir}"; then
        return
    fi

    if [[ ! -d "${target_dir}" ]] || [[ -z "$(find "${target_dir}" -mindepth 1 -print -quit 2>/dev/null)" ]]; then
        echo "${target_dir} is missing or empty"
        rm -rf "${target_dir}"
        pushd "${CUR_DIR}/${workdir}" >/dev/null
        curl -O "${PIPELINE_DATA_URL_PREFIX}/${remote_path}"
        tar -xzf "${archive_name}"
        rm -rf "${archive_name}"
        popd >/dev/null
    else
        echo "${target_dir} exists and is non-empty, continue !"
    fi
}

download_specs=(
    "tpch1.db|.|tpch1.db.tar.gz|tpch1.db.tar.gz"
    "tvf_data|.|tvf_data.tar.gz|tvf_data.tar.gz"
    "data/multi_catalog/test_complex_types/data|data/multi_catalog/test_complex_types|multi_catalog/test_complex_types/data.tar.gz|data.tar.gz"
    "data/multi_catalog/test_compress_partitioned/data|data/multi_catalog/test_compress_partitioned|multi_catalog/test_compress_partitioned/data.tar.gz|data.tar.gz"
    "data/multi_catalog/test_wide_table/data|data/multi_catalog/test_wide_table|multi_catalog/test_wide_table/data.tar.gz|data.tar.gz"
    "data/tvf/test_hdfs_tvf_compression/test_data|data/tvf/test_hdfs_tvf_compression|test_hdfs_tvf_compression/test_data.tar.gz|test_data.tar.gz"
    "data/tvf/test_tvf/tvf|data/tvf/test_tvf|test_tvf/data.tar.gz|data.tar.gz"
    "data/multi_catalog/logs1_parquet/data|data/multi_catalog/logs1_parquet|multi_catalog/logs1_parquet/data.tar.gz|data.tar.gz"
)

download_spec=""
for download_spec in "${download_specs[@]}"; do
    IFS='|' read -r relative_dir workdir remote_path archive_name <<<"${download_spec}"
    download_archive_if_missing "${relative_dir}" "${workdir}" "${remote_path}" "${archive_name}"
done

# download auxiliary jars
jars=(
    jdom-1.1.jar
    aliyun-java-sdk-core-3.4.0.jar
    aliyun-java-sdk-ecs-4.2.0.jar
    aliyun-java-sdk-ram-3.0.0.jar
    aliyun-java-sdk-sts-3.0.0.jar
    jindo-core-6.3.4.jar
    jindo-core-linux-el7-aarch64-6.3.4.jar
    jindo-sdk-6.3.4.jar
    aws-java-sdk-bundle-1.11.375.jar
    hadoop-huaweicloud-3.1.1-hw-54.5.jar
    hadoop-cos-3.1.0-8.3.22.jar
    cos_api-bundle-5.6.244.4.jar
    hadoop-aws-3.2.1.jar
    paimon-hive-connector-3.1-1.3-SNAPSHOT.jar
    gcs-connector-hadoop3-2.2.24-shaded.jar
)

cd ${CUR_DIR}/auxlib
for jar in "${jars[@]}"; do
    if [[ -f "${CUR_DIR}/auxlib/${jar}" ]]; then
        echo "Reuse cached hive aux jar ${jar}"
        continue
    fi
    curl -O "${HIVE_AUXLIB_URL_PREFIX}/${jar}"
done
