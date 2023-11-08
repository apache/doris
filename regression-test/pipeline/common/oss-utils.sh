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

function install_ossutil() {
    if command -v ossutil >/dev/null; then return; fi
    if [[ -z ${OSS_accessKeyID} || -z ${OSS_accessKeySecret} ]]; then
        echo "ERROR: env OSS_accessKeyID or OSS_accessKeySecret not set."
        return 1
    fi
    curl https://gosspublic.alicdn.com/ossutil/install.sh | sudo bash
    echo "[Credentials]
language=EN
endpoint=oss-cn-hongkong-internal.aliyuncs.com
accessKeyID=${OSS_accessKeyID:-}
accessKeySecret=${OSS_accessKeySecret:-}
" >~/.ossutilconfig
}

function check_oss_file_exist() {
    if [[ -z ${ACCESS_KEY_ID} || -z ${ACCESS_KEY_SECRET} ]]; then
        echo "ERROR: env ACCESS_KEY_ID and ACCESS_KEY_SECRET not set"
        return 1
    fi
    # Check if the file exists.
    # file_name like ${pull_request_id}_${commit_id}.tar.gz
    local file_name="$1"
    OSS_DIR="${OSS_DIR:-"oss://opensource-pipeline/compile-release"}"
    install_ossutil
    if ossutil stat \
        -i "${ACCESS_KEY_ID}" \
        -k "${ACCESS_KEY_SECRET}" \
        "${OSS_DIR}/${file_name}"; then
        echo "INFO: ${file_name} file exists." && return 0
    else
        echo "ERROR: ${file_name} file not exits." && return 1
    fi
}

function download_oss_file() {
    # file_name like ${pull_request_id}_${commit_id}.tar.gz
    local file_name="$1"
    if ! check_oss_file_exist "${file_name}"; then return 1; fi
    OSS_DIR="${OSS_DIR:-"oss://opensource-pipeline/compile-release"}"
    install_ossutil
    if ossutil cp -f \
        "${OSS_DIR}/${file_name}" \
        "${file_name}"; then
        echo "INFO: download ${file_name} success" && return 0
    else
        echo "ERROR: download ${file_name} fail" && return 1
    fi
}

function upload_file_to_oss() {
    if [[ -z ${ACCESS_KEY_ID} || -z ${ACCESS_KEY_SECRET} ]]; then
        echo "ERROR: env ACCESS_KEY_ID and ACCESS_KEY_SECRET not set"
        return 1
    fi
    # file_name like ${pull_request_id}_${commit_id}.tar.gz
    local file_name="$1"
    OSS_DIR="${OSS_DIR:-"oss://opensource-pipeline/compile-release"}"
    OSS_URL_PREFIX="${OSS_URL_PREFIX:-"http://opensource-pipeline.oss-cn-hongkong.aliyuncs.com/compile-release"}"
    install_ossutil
    if ossutil cp -f \
        -i "${ACCESS_KEY_ID}" \
        -k "${ACCESS_KEY_SECRET}" \
        "${file_name}" \
        "${OSS_DIR}/${file_name}" \
        --force; then
        if ! check_oss_file_exist "${file_name}"; then return 1; fi
        echo "INFO: success to upload ${file_name} to ${OSS_URL_PREFIX}/${file_name}" && return 0
    else
        echo "ERROR: upload ${file_name} fail" && return 1
    fi
}

function upload_doris_log_to_oss() {
    if [[ -f "$1" ]]; then return 1; fi
    OSS_DIR="oss://opensource-pipeline/regression"
    OSS_URL_PREFIX="http://opensource-pipeline.oss-cn-hongkong.aliyuncs.com/regression"
    upload_file_to_oss "$1"
}
