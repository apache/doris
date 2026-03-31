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
PREPARE_MODE="${HIVE_PREPARE_MODE:-all}"

prepare_tez_runtime() {
    local tez_runtime_dir="${CUR_DIR}/tez-runtime"
    local tez_conf_dir="${CUR_DIR}/tez-conf"
    local tez_source_image="${HIVE3_TEZ_SOURCE_IMAGE:-doristhirdpartydocker/trinodb:hdp3.1-hive-kerberized_96}"

    if [[ -f "${tez_runtime_dir}/lib/tez.tar.gz" ]] && [[ -f "${tez_conf_dir}/tez-site.xml" ]]; then
        echo "${tez_runtime_dir} and ${tez_conf_dir} exist, continue !"
        return
    fi

    echo "Preparing Tez runtime from ${tez_source_image}"
    if ! docker image inspect "${tez_source_image}" >/dev/null 2>&1; then
        docker pull "${tez_source_image}"
    fi

    local container_id
    container_id="$(docker create "${tez_source_image}")"
    rm -rf "${tez_runtime_dir}" "${tez_conf_dir}"
    mkdir -p "${tez_runtime_dir}" "${tez_conf_dir}"
    docker cp "${container_id}:/usr/hdp/3.1.0.0-78/tez/." "${tez_runtime_dir}/"
    docker cp "${container_id}:/etc/tez/conf/tez-site.xml" "${tez_conf_dir}/tez-site.xml"
    docker rm -f "${container_id}" >/dev/null
}

prepare_tez_runtime
mkdir -p "${CUR_DIR}/nm-local-dir" "${CUR_DIR}/nm-log-dir"
mkdir -p "${CUR_DIR}/hive-local-scratch"

if [[ "${PREPARE_MODE}" == "tez-runtime" ]]; then
    exit 0
fi

# Extract all tar.gz files under the repo
find ${CUR_DIR}/data -type f -name "*.tar.gz" -print0 | \
xargs -0 -n1 -P"${LOAD_PARALLEL}" bash -c '
  f="$0"
  echo "Extracting hive data $f"
  dir=$(dirname "$f")
  tar -xzf "$f" -C "$dir"
'

# download tpch1_data
if [[ ! -d "${CUR_DIR}/tpch1.db" ]]; then
    echo "${CUR_DIR}/tpch1.db does not exist"
    cd ${CUR_DIR}/
    curl -O https://${s3BucketName}.${s3Endpoint}/regression/datalake/pipeline_data/tpch1.db.tar.gz
    tar -zxf tpch1.db.tar.gz
    rm -rf tpch1.db.tar.gz
    cd -
else
    echo "${CUR_DIR}/tpch1.db exist, continue !"
fi

# download tvf_data
if [[ ! -d "${CUR_DIR}/tvf_data" ]]; then
    echo "${CUR_DIR}/tvf_data does not exist"
    cd ${CUR_DIR}/
    curl -O https://${s3BucketName}.${s3Endpoint}/regression/datalake/pipeline_data/tvf_data.tar.gz
    tar -zxf tvf_data.tar.gz
    rm -rf tvf_data.tar.gz
    cd -
else
    echo "${CUR_DIR}/tvf_data exist, continue !"
fi

# download test_complex_types data
if [[ ! -d "${CUR_DIR}/data/multi_catalog/test_complex_types/data" ]]; then
    echo "${CUR_DIR}/data/multi_catalog/test_complex_types/data does not exist"
    cd "${CUR_DIR}/data/multi_catalog/test_complex_types"
    curl -O https://${s3BucketName}.${s3Endpoint}/regression/datalake/pipeline_data/multi_catalog/test_complex_types/data.tar.gz
    tar xzf data.tar.gz
    rm -rf data.tar.gz
    cd -
else
    echo "${CUR_DIR}/data/multi_catalog/test_complex_types/data exist, continue !"
fi

# download test_compress_partitioned data
if [[ ! -d "${CUR_DIR}/data/multi_catalog/test_compress_partitioned/data" ]]; then
    echo "${CUR_DIR}/data/multi_catalog/test_compress_partitioned/data does not exist"
    cd "${CUR_DIR}/data/multi_catalog/test_compress_partitioned"
    curl -O https://${s3BucketName}.${s3Endpoint}/regression/datalake/pipeline_data/multi_catalog/test_compress_partitioned/data.tar.gz
    tar xzf data.tar.gz
    rm -rf data.tar.gz
    cd -
else
    echo "${CUR_DIR}/data/multi_catalog/test_compress_partitioned/data exist, continue !"
fi

# download test_wide_table data
if [[ ! -d "${CUR_DIR}/data/multi_catalog/test_wide_table/data" ]]; then
    echo "${CUR_DIR}/data/multi_catalog/test_wide_table/data does not exist"
    cd "${CUR_DIR}/data/multi_catalog/test_wide_table"
    curl -O https://${s3BucketName}.${s3Endpoint}/regression/datalake/pipeline_data/multi_catalog/test_wide_table/data.tar.gz
    tar xzf data.tar.gz
    rm -rf data.tar.gz
    cd -
else
    echo "${CUR_DIR}/data/multi_catalog/test_wide_table/data exist, continue !"
fi

# download test_hdfs_tvf_compression data
if [[ ! -d "${CUR_DIR}/data/tvf/test_hdfs_tvf_compression/test_data" ]]; then
    echo "${CUR_DIR}/data/tvf/test_hdfs_tvf_compression/test_data does not exist"
    cd "${CUR_DIR}/data/tvf/test_hdfs_tvf_compression"
    curl -O https://${s3BucketName}.${s3Endpoint}/regression/datalake/pipeline_data/test_hdfs_tvf_compression/test_data.tar.gz
    tar xzf test_data.tar.gz
    rm -rf test_data.tar.gz
    cd -
else
    echo "${CUR_DIR}/data/tvf/test_hdfs_tvf_compression/test_data exist, continue !"
fi

# download test_tvf data
if [[ ! -d "${CUR_DIR}/data/tvf/test_tvf/tvf" ]]; then
    echo "${CUR_DIR}/data/tvf/test_tvf/tvf does not exist"
    cd "${CUR_DIR}/data/tvf/test_tvf"
    curl -O https://${s3BucketName}.${s3Endpoint}/regression/datalake/pipeline_data/test_tvf/data.tar.gz
    tar xzf data.tar.gz
    rm -rf data.tar.gz
    cd -
else
    echo "${CUR_DIR}/data/tvf/test_tvf/tvf exist, continue !"
fi

# download logs1_parquet data
if [[ ! -d "${CUR_DIR}/data/multi_catalog/logs1_parquet/data" ]]; then
    echo "${CUR_DIR}/data/multi_catalog/logs1_parquet/data does not exist"
    cd "${CUR_DIR}/data/multi_catalog/logs1_parquet"
    curl -O https://${s3BucketName}.${s3Endpoint}/regression/datalake/pipeline_data/multi_catalog/logs1_parquet/data.tar.gz
    tar xzf data.tar.gz
    rm -rf data.tar.gz
    cd -
else
    echo "${CUR_DIR}/data/multi_catalog/logs1_parquet/data exist, continue !"
fi

# download auxiliary jars
jars=(
    jdom-1.1.jar
    aliyun-java-sdk-core-3.4.0.jar
    aliyun-java-sdk-ecs-4.2.0.jar
    aliyun-java-sdk-ram-3.0.0.jar
    aliyun-java-sdk-sts-3.0.0.jar
    aliyun-sdk-oss-3.4.1.jar
    hadoop-aliyun-3.2.1.jar
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
    curl -O "https://${s3BucketName}.${s3Endpoint}/regression/docker/hive3/${jar}"
done
