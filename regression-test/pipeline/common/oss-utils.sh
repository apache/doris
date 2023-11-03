#!/bin/bash

function check_oss_file_exist() {
    if [[ -z ${ACCESS_KEY_ID} || -z ${ACCESS_KEY_SECRET} ]]; then
        echo "ERROR: env ACCESS_KEY_ID and ACCESS_KEY_SECRET not set"
        return 1
    fi
    # Check if the file exists.
    # file_name like ${pull_request_id}_${commit_id}.tar.gz
    local file_name="$1"
    if ossutil stat \
        -i "${ACCESS_KEY_ID}" \
        -k "${ACCESS_KEY_SECRET}" \
        "oss://opensource-pipeline/compile-release/${file_name}"; then
        echo "INFO: ${file_name} file exists." && return 0
    else
        echo "ERROR: ${file_name} file not exits." && return 1
    fi
}

function download_oss_file() {
    # file_name like ${pull_request_id}_${commit_id}.tar.gz
    local file_name="$1"
    if ! check_oss_file_exist "${file_name}"; then return 1; fi
    if ossutil cp -f \
        oss://opensource-pipeline/compile-release/"${file_name}" \
        "${file_name}"; then
        echo "INFO: download ${file_name} success" && return 0
    else
        echo "ERROR: download ${file_name} fail" && return 1
    fi
}

function upload_oss_file() {
    if [[ -z ${ACCESS_KEY_ID} || -z ${ACCESS_KEY_SECRET} ]]; then
        echo "ERROR: env ACCESS_KEY_ID and ACCESS_KEY_SECRET not set"
        return 1
    fi
    # file_name like ${pull_request_id}_${commit_id}.tar.gz
    local file_name="$1"
    if ossutil cp -f \
        -i "${ACCESS_KEY_ID}" \
        -k "${ACCESS_KEY_SECRET}" \
        "${file_name}" \
        oss://opensource-pipeline/compile-release/ \
        --force; then
        if ! check_oss_file_exist "${file_name}"; then return 1; fi
        echo "INFO: upload ${file_name} success" && return 0
    else
        echo "ERROR: upload ${file_name} fail" && return 1
    fi
}
