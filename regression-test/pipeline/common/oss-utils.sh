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
    if command -v ossutil >/dev/null; then return 0; fi
    if [[ -z ${oss_ak} || -z ${oss_sk} ]]; then
        echo "ERROR: env oss_ak or oss_sk not set."
        return 1
    fi
    curl https://gosspublic.alicdn.com/ossutil/install.sh | sudo bash
    echo "[Credentials]
language=EN
endpoint=oss-cn-hongkong-internal.aliyuncs.com
accessKeyID=${oss_ak:-}
accessKeySecret=${oss_sk:-}
" >~/.ossutilconfig
}

function check_oss_file_exist() {
    if [[ -z ${oss_ak} || -z ${oss_sk} ]]; then
        echo "ERROR: env oss_ak and oss_sk not set"
        return 1
    fi
    # Check if the file exists.
    # file_name like ${pull_request_id}_${commit_id}.tar.gz
    local file_name="$1"
    OSS_DIR="${OSS_DIR:-"oss://opensource-pipeline/compile-release"}"
    install_ossutil
    if ossutil stat \
        -i "${oss_ak}" \
        -k "${oss_sk}" \
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
        -i "${oss_ak}" \
        -k "${oss_sk}" \
        "${OSS_DIR}/${file_name}" \
        "${file_name}"; then
        echo "INFO: download ${file_name} success" && return 0
    else
        echo "ERROR: download ${file_name} fail" && return 1
    fi
}

function upload_file_to_oss() {
    if [[ -z ${oss_ak} || -z ${oss_sk} ]]; then
        echo "ERROR: env oss_ak and oss_sk not set"
        return 1
    fi
    if [[ ! -f "$1" ]] || [[ "$1" != "/"* ]]; then
        echo "ERROR: '$1' is not an absolute path"
        return 1
    fi
    # file_name like ${pull_request_id}_${commit_id}.tar.gz
    local file_name
    local dir_name
    dir_name="$(dirname "${1}")"
    file_name="$(basename "${1}")"
    OSS_DIR="${OSS_DIR:-"oss://opensource-pipeline/compile-release"}"
    OSS_URL_PREFIX="${OSS_URL_PREFIX:-"http://opensource-pipeline.oss-cn-hongkong.aliyuncs.com/compile-release"}"
    install_ossutil
    cd "${dir_name}" || return 1
    if ossutil cp -f \
        -i "${oss_ak}" \
        -k "${oss_sk}" \
        "${file_name}" \
        "${OSS_DIR}/${file_name}"; then
        if ! check_oss_file_exist "${file_name}"; then return 1; fi
        cd - || return 1
        echo "INFO: success to upload ${file_name} to ${OSS_URL_PREFIX}/${file_name}" && return 0
    else
        cd - || return 1
        echo "ERROR: upload ${file_name} fail" && return 1
    fi
}

# Operator-provisioned ossutil v2 uses environment-only STS credentials. This path
# neither installs tools nor writes credentials to ~/.ossutilconfig.
function upload_preconfigured_doris_log() (
    set +x
    if [[ "${DORIS_CI_OSSUTIL:-}" != /* || ! -x "${DORIS_CI_OSSUTIL}" ||
        -z "${OSS_ACCESS_KEY_ID:-}" || -z "${OSS_ACCESS_KEY_SECRET:-}" || -z "${OSS_SESSION_TOKEN:-}" ||
        -z "${OSS_REGION:-}" || "${OSS_ENDPOINT:-}" != https://* ||
        ! "${DORIS_CI_LOG_OSS_PREFIX:-}" =~ ^oss://[^/]+/.+ ||
        "$1" != /* || ! -f "$1" ]]; then
        echo "ERROR: preconfigured log upload requires an existing tool, STS, HTTPS endpoint, region, prefix and absolute file." >&2
        return 1
    fi
    local version target
    version=$("${DORIS_CI_OSSUTIL}" version) || return 1
    if [[ "${version}" != *2.4.1* ]]; then
        echo "ERROR: preconfigured log upload requires validated ossutil 2.4.1." >&2
        return 1
    fi
    target="${DORIS_CI_LOG_OSS_PREFIX%/}/$(basename "$1")"
    "${DORIS_CI_OSSUTIL}" --mode StsToken --config-file /dev/null --loglevel off \
        --region "${OSS_REGION}" --endpoint "${OSS_ENDPOINT}" cp "$1" "${target}" || return 1
    "${DORIS_CI_OSSUTIL}" --mode StsToken --config-file /dev/null --loglevel off \
        --region "${OSS_REGION}" --endpoint "${OSS_ENDPOINT}" stat "${target}"
)

function upload_doris_log_to_oss() {
    case "${DORIS_CI_ENV_MODE:-managed}" in
        preconfigured)
            case "${DORIS_CI_LOG_BACKEND:-oss}" in
                artifact)
                    if [[ "${DORIS_CI_LOG_DIR:-}" != /* || ! -d "${DORIS_CI_LOG_DIR}" ||
                        "$1" != /* || ! -f "$1" ]]; then
                        echo "ERROR: artifact log collection requires an existing absolute directory and file." >&2
                        return 1
                    fi
                    local report_dir
                    report_dir=$(mktemp -d "${DORIS_CI_LOG_DIR}/doris-log.XXXXXX") || return 1
                    cp "$1" "${report_dir}/" || return 1
                    echo "INFO: collected log artifact in ${report_dir}; remote publication is a separate step."
                    return 0
                    ;;
                oss) upload_preconfigured_doris_log "$1"; return $? ;;
                *) echo "ERROR: unknown DORIS_CI_LOG_BACKEND." >&2; return 1 ;;
            esac
            ;;
        managed) ;;
        *) echo "ERROR: unknown DORIS_CI_ENV_MODE." >&2; return 1 ;;
    esac
    OSS_DIR="oss://opensource-pipeline/regression"
    OSS_URL_PREFIX="http://opensource-pipeline.oss-cn-hongkong.aliyuncs.com/regression"
    upload_file_to_oss "$1"
}
