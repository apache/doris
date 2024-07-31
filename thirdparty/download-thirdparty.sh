#!/bin/bash
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

################################################################
# This script will download all thirdparties and java libraries
# which are defined in *vars.sh*, unpack patch them if necessary.
# You can run this script multi-times.
# Things will only be downloaded, unpacked and patched once.
################################################################

set -eo pipefail

curdir="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

if [[ -z "${DORIS_HOME}" ]]; then
    DORIS_HOME="${curdir}/.."
fi

# include custom environment variables
if [[ -f "${DORIS_HOME}/custom_env.sh" ]]; then
    # shellcheck disable=1091
    . "${DORIS_HOME}/custom_env.sh"
fi

if [[ -z "${TP_DIR}" ]]; then
    TP_DIR="${curdir}"
fi

if [[ ! -f "${TP_DIR}/vars.sh" ]]; then
    echo 'vars.sh is missing'.
    exit 1
fi

. "${TP_DIR}/vars.sh"

mkdir -p "${TP_DIR}/src"

SPEC_ARCHIVES=(
)
while [[ $# -gt 0 ]]; do
    GIVEN_LIB=$1
    SPEC_LIB=
    for TP_ARCH in "${TP_ARCHIVES[@]}"; do
        if [[ "${GIVEN_LIB,,}" = "${TP_ARCH,,}" ]]; then
            SPEC_LIB=${TP_ARCH}
            break
        fi
    done
    shift 1
    if [[ "${SPEC_LIB}" = "" ]]; then
        echo "given lib: ${GIVEN_LIB} not found"
        exit 1
    fi
    SPEC_ARCHIVES=(
        "${SPEC_ARCHIVES[@]}"
        "${SPEC_LIB}"
    )
done
if [[ "${SPEC_LIB}" != "" ]]; then
    TP_ARCHIVES=("${SPEC_ARCHIVES[@]}")
    echo "Download and build specified libs only: ${TP_ARCHIVES[*]}"
fi

md5sum_bin='md5sum'
if ! command -v "${md5sum_bin}" >/dev/null 2>&1; then
    echo "Warn: md5sum is not installed"
    md5sum_bin=""
fi

md5sum_func() {
    local FILENAME="$1"
    local DESC_DIR="$2"
    local MD5SUM="$3"
    local md5

    if [[ "${md5sum_bin}" == "" ]]; then
        return 0
    else
        md5="$(md5sum "${DESC_DIR}/${FILENAME}")"
        if [[ "${md5}" != "${MD5SUM}  ${DESC_DIR}/${FILENAME}" ]]; then
            echo "${DESC_DIR}/${FILENAME} md5sum check failed!"
            echo -e "except-md5 ${MD5SUM} \nactual-md5 ${md5}"
            return 1
        fi
    fi
    return 0
}

# return 0 if download succeed.
# return 1 if not.
download_func() {
    local FILENAME="$1"
    local DOWNLOAD_URL="$2"
    local DESC_DIR="$3"
    local MD5SUM="$4"

    if [[ -z "${FILENAME}" ]]; then
        echo "Error: No file name specified to download"
        exit 1
    fi
    if [[ -z "${DOWNLOAD_URL}" ]]; then
        echo "Error: No download url specified for ${FILENAME}"
        exit 1
    fi
    if [[ -z "${DESC_DIR}" ]]; then
        echo "Error: No dest dir specified for ${FILENAME}"
        exit 1
    fi

    local STATUS=1
    for attemp in 1 2; do
        if [[ -r "${DESC_DIR}/${FILENAME}" ]]; then
            if md5sum_func "${FILENAME}" "${DESC_DIR}" "${MD5SUM}"; then
                echo "Archive ${FILENAME} already exist."
                STATUS=0
                break
            fi
            echo "Archive ${FILENAME} will be removed and download again."
            rm -f "${DESC_DIR}/${FILENAME}"
        else
            echo "Downloading ${FILENAME} from ${DOWNLOAD_URL} to ${DESC_DIR}"
            if wget --no-check-certificate -q "${DOWNLOAD_URL}" -O "${DESC_DIR}/${FILENAME}"; then
                if md5sum_func "${FILENAME}" "${DESC_DIR}" "${MD5SUM}"; then
                    STATUS=0
                    echo "Success to download ${FILENAME}"
                    break
                fi
                echo "Archive ${FILENAME} will be removed and download again."
                rm -f "${DESC_DIR}/${FILENAME}"
            else
                echo "Failed to download ${FILENAME}. attemp: ${attemp}"
            fi
        fi
    done

    if [[ "${STATUS}" -ne 0 ]]; then
        echo "Failed to download ${FILENAME}"
    fi
    return "${STATUS}"
}

# download thirdparty archives
echo "===== Downloading thirdparty archives..."
for TP_ARCH in "${TP_ARCHIVES[@]}"; do
    NAME="${TP_ARCH}_NAME"
    MD5SUM="${TP_ARCH}_MD5SUM"
    if [[ -z "${REPOSITORY_URL}" ]]; then
        URL="${TP_ARCH}_DOWNLOAD"
        if ! download_func "${!NAME}" "${!URL}" "${TP_SOURCE_DIR}" "${!MD5SUM}"; then
            echo "Failed to download ${!NAME}"
            exit 1
        fi
    else
        URL="${REPOSITORY_URL}/${!NAME}"
        if ! download_func "${!NAME}" "${URL}" "${TP_SOURCE_DIR}" "${!MD5SUM}"; then
            #try to download from home
            URL="${TP_ARCH}_DOWNLOAD"
            if ! download_func "${!NAME}" "${!URL}" "${TP_SOURCE_DIR}" "${!MD5SUM}"; then
                echo "Failed to download ${!NAME}"
                exit 1 # download failed again exit.
            fi
        fi
    fi
done
echo "===== Downloading thirdparty archives...done"

# check if all tp archives exists
echo "===== Checking all thirdpart archives..."
for TP_ARCH in "${TP_ARCHIVES[@]}"; do
    NAME="${TP_ARCH}_NAME"
    if [[ ! -r "${TP_SOURCE_DIR}/${!NAME}" ]]; then
        echo "Failed to fetch ${!NAME}"
        exit 1
    fi
done
echo "===== Checking all thirdpart archives...done"

# unpacking thirdpart archives
echo "===== Unpacking all thirdparty archives..."
TAR_CMD="tar"
UNZIP_CMD="unzip"
SUFFIX_TGZ="\.(tar\.gz|tgz)$"
SUFFIX_XZ="\.tar\.xz$"
SUFFIX_ZIP="\.zip$"
SUFFIX_BZ2="\.tar\.bz2$"
for TP_ARCH in "${TP_ARCHIVES[@]}"; do
    NAME="${TP_ARCH}_NAME"
    SOURCE="${TP_ARCH}_SOURCE"

    if [[ -z "${!SOURCE}" ]]; then
        continue
    fi

    if [[ ! -d "${TP_SOURCE_DIR}/${!SOURCE}" ]]; then
        if [[ "${!NAME}" =~ ${SUFFIX_TGZ} ]]; then
            echo "${TP_SOURCE_DIR}/${!NAME}"
            echo "${TP_SOURCE_DIR}/${!SOURCE}"
            if ! "${TAR_CMD}" xzf "${TP_SOURCE_DIR}/${!NAME}" -C "${TP_SOURCE_DIR}/"; then
                echo "Failed to untar ${!NAME}"
                exit 1
            fi
        elif [[ "${!NAME}" =~ ${SUFFIX_XZ} ]]; then
            echo "${TP_SOURCE_DIR}/${!NAME}"
            echo "${TP_SOURCE_DIR}/${!SOURCE}"
            if ! "${TAR_CMD}" xJf "${TP_SOURCE_DIR}/${!NAME}" -C "${TP_SOURCE_DIR}/"; then
                echo "Failed to untar ${!NAME}"
                exit 1
            fi
        elif [[ "${!NAME}" =~ ${SUFFIX_ZIP} ]]; then
            if ! "${UNZIP_CMD}" -o -qq "${TP_SOURCE_DIR}/${!NAME}" -d "${TP_SOURCE_DIR}/"; then
                echo "Failed to unzip ${!NAME}"
                exit 1
            fi
        elif [[ "${!NAME}" =~ ${SUFFIX_BZ2} ]]; then
            if ! "${TAR_CMD}" xf "${TP_SOURCE_DIR}/${!NAME}" -C "${TP_SOURCE_DIR}/"; then
                echo "Failed to untar ${!NAME}"
                exit 1
            fi
        fi
    else
        echo "${!SOURCE} already unpacked."
    fi
done
echo "===== Unpacking all thirdparty archives...done"

echo "===== Patching thirdparty archives..."

###################################################################################
# PATCHED_MARK is a empty file which will be created in some thirdparty source dir
# only after that thirdparty source is patched.
# This is to avoid duplicated patch.
###################################################################################
PATCHED_MARK="patched_mark"

# abseil patch
if [[ " ${TP_ARCHIVES[*]} " =~ " ABSEIL " ]]; then
    cd "${TP_SOURCE_DIR}/${ABSEIL_SOURCE}"
    if [[ ! -f "${PATCHED_MARK}" ]]; then
        patch -p1 <"${TP_PATCH_DIR}/absl.patch"
        touch "${PATCHED_MARK}"
    fi
    cd -
    echo "Finished patching ${ABSEIL_SOURCE}"
fi

# glog patch
if [[ " ${TP_ARCHIVES[*]} " =~ " GLOG " ]]; then
    if [[ "${GLOG_SOURCE}" == "glog-0.4.0" ]]; then
        cd "${TP_SOURCE_DIR}/${GLOG_SOURCE}"
        if [[ ! -f "${PATCHED_MARK}" ]]; then
            patch -p1 <"${TP_PATCH_DIR}/glog-0.4.0.patch"
            touch "${PATCHED_MARK}"
        fi
        cd -
    elif [[ "${GLOG_SOURCE}" == "glog-0.6.0" ]]; then
        cd "${TP_SOURCE_DIR}/${GLOG_SOURCE}"
        if [[ ! -f "${PATCHED_MARK}" ]]; then
            patch -p1 <"${TP_PATCH_DIR}/glog-0.6.0.patch"
            touch "${PATCHED_MARK}"
        fi
        cd -
    fi
    echo "Finished patching ${GLOG_SOURCE}"
fi

# gtest patch
if [[ " ${TP_ARCHIVES[*]} " =~ " GTEST " ]]; then
    cd "${TP_SOURCE_DIR}/${GTEST_SOURCE}"
    if [[ ! -f "${PATCHED_MARK}" ]]; then
        patch -p1 <"${TP_PATCH_DIR}/googletest-release-1.11.0.patch"
        touch "${PATCHED_MARK}"
    fi
    cd -
    echo "Finished patching ${GTEST_SOURCE}"
fi

# mysql patch
if [[ " ${TP_ARCHIVES[*]} " =~ " MYSQL " ]]; then
    cd "${TP_SOURCE_DIR}/${MYSQL_SOURCE}"
    if [[ ! -f "${PATCHED_MARK}" ]]; then
        patch -p1 <"${TP_PATCH_DIR}/mysql-server-mysql-5.7.18.patch"
        touch "${PATCHED_MARK}"
    fi
    cd -
    echo "Finished patching ${MYSQL_SOURCE}"
fi

# libevent patch
if [[ " ${TP_ARCHIVES[*]} " =~ " LIBEVENT " ]]; then
    cd "${TP_SOURCE_DIR}/${LIBEVENT_SOURCE}"
    if [[ ! -f "${PATCHED_MARK}" ]]; then
        patch -p1 <"${TP_PATCH_DIR}/libevent.patch"
        patch -p1 <"${TP_PATCH_DIR}/libevent-1532.patch"
        patch -p1 <"${TP_PATCH_DIR}/libevent-keepalive-accepted-socket.patch"
        touch "${PATCHED_MARK}"
    fi
    cd -
    echo "Finished patching ${LIBEVENT_SOURCE}"
fi

# gsasl2 patch to fix link error such as mutilple func defination
# when link target with kerberos
if [[ " ${TP_ARCHIVES[*]} " =~ " GSASL " ]]; then
    cd "${TP_SOURCE_DIR}/${GSASL_SOURCE}"
    if [[ ! -f ${PATCHED_MARK} ]]; then
        patch -p1 <"${TP_PATCH_DIR}/libgsasl-1.8.0.patch"
        touch "${PATCHED_MARK}"
    fi
    cd -
    echo "Finished patching ${GSASL_SOURCE}"
fi

# cyrus-sasl patch to force compile gssapi plugin when static linking
# this is for librdkafka with sasl
if [[ " ${TP_ARCHIVES[*]} " =~ " CYRUS_SASL " ]]; then
    cd "${TP_SOURCE_DIR}/${CYRUS_SASL_SOURCE}"
    if [[ ! -f ${PATCHED_MARK} ]]; then
        patch -p1 <"${TP_PATCH_DIR}/cyrus-sasl-2.1.27.patch"
        touch "${PATCHED_MARK}"
    fi
    cd -
    echo "Finished patching ${CYRUS_SASL_SOURCE}"
fi

#patch sqltypes.h, change TCAHR to TWCHAR to avoid conflict with clucene TCAHR
if [[ " ${TP_ARCHIVES[*]} " =~ " ODBC " ]]; then
    cd "${TP_SOURCE_DIR}/${ODBC_SOURCE}"
    if [[ ! -f ${PATCHED_MARK} ]]; then
        patch -p1 <"${TP_PATCH_DIR}/sqltypes.h.patch"
        touch "${PATCHED_MARK}"
    fi
    cd -
    echo "Finished patching ${ODBC_SOURCE}"
fi

# rocksdb patch to fix compile error
if [[ " ${TP_ARCHIVES[*]} " =~ " ROCKSDB " ]]; then
    if [[ "${ROCKSDB_SOURCE}" == "rocksdb-5.14.2" ]]; then
        cd "${TP_SOURCE_DIR}/${ROCKSDB_SOURCE}"
        if [[ ! -f "${PATCHED_MARK}" ]]; then
            patch -p1 <"${TP_PATCH_DIR}/rocksdb-5.14.2.patch"
            touch "${PATCHED_MARK}"
        fi
        cd -
    fi
    echo "Finished patching ${ROCKSDB_SOURCE}"
fi

# arrow patch is used to get the raw orc reader for filter prune.
if [[ " ${TP_ARCHIVES[*]} " =~ " ARROW " ]]; then
    if [[ "${ARROW_SOURCE}" == "arrow-apache-arrow-13.0.0" ]]; then
        cd "${TP_SOURCE_DIR}/${ARROW_SOURCE}"
        if [[ ! -f "${PATCHED_MARK}" ]]; then
            patch -p1 <"${TP_PATCH_DIR}/apache-arrow-13.0.0.patch"
            touch "${PATCHED_MARK}"
        fi
        cd -
    fi
    echo "Finished patching ${ARROW_SOURCE}"
fi

# patch librdkafka to avoid crash
if [[ " ${TP_ARCHIVES[*]} " =~ " LIBRDKAFKA " ]]; then
    if [[ "${LIBRDKAFKA_SOURCE}" == "librdkafka-1.9.2" ]]; then
        cd "${TP_SOURCE_DIR}/${LIBRDKAFKA_SOURCE}"
        if [[ ! -f "${PATCHED_MARK}" ]]; then
            patch -p0 <"${TP_PATCH_DIR}/librdkafka-1.9.2.patch"
            touch "${PATCHED_MARK}"
        fi
        cd -
    fi
    echo "Finished patching ${LIBRDKAFKA_SOURCE}"
fi

# patch jemalloc, disable JEMALLOC_MANGLE for overloading the memory API.
if [[ " ${TP_ARCHIVES[*]} " =~ " JEMALLOC_DORIS " ]]; then
    if [[ "${JEMALLOC_DORIS_SOURCE}" = "jemalloc-5.3.0" ]]; then
        cd "${TP_SOURCE_DIR}/${JEMALLOC_DORIS_SOURCE}"
        if [[ ! -f "${PATCHED_MARK}" ]]; then
            patch -p0 <"${TP_PATCH_DIR}/jemalloc_hook.patch"
            touch "${PATCHED_MARK}"
        fi
        cd -
    fi
    echo "Finished patching ${JEMALLOC_DORIS_SOURCE}"
fi

# patch hyperscan
# https://github.com/intel/hyperscan/issues/292
if [[ " ${TP_ARCHIVES[*]} " =~ " HYPERSCAN " ]]; then
    if [[ "${HYPERSCAN_SOURCE}" == "vectorscan-vectorscan-5.4.11" ]]; then
        cd "${TP_SOURCE_DIR}/${HYPERSCAN_SOURCE}"
        if [[ ! -f "${PATCHED_MARK}" ]]; then
            patch -p1 <"${TP_PATCH_DIR}/vectorscan-5.4.11.patch"
            touch "${PATCHED_MARK}"
        fi
        cd -
    fi
    echo "Finished patching ${HYPERSCAN_SOURCE}"
fi

# patch aws sdk
if [[ " ${TP_ARCHIVES[*]} " =~ " AWS_SDK " ]]; then
    cd "${TP_SOURCE_DIR}/${AWS_SDK_SOURCE}"
    if [[ ! -f "${PATCHED_MARK}" ]]; then
        if [[ "${AWS_SDK_SOURCE}" == "aws-sdk-cpp-1.11.119" ]]; then
            if wget --no-check-certificate -q https://doris-thirdparty-repo.bj.bcebos.com/thirdparty/aws-crt-cpp-1.11.119.tar.gz -O aws-crt-cpp-1.11.119.tar.gz; then
                tar xzf aws-crt-cpp-1.11.119.tar.gz
            else
                bash ./prefetch_crt_dependency.sh
            fi
            patch -p1 <"${TP_PATCH_DIR}/aws-sdk-cpp-1.11.119.patch"
        else
            bash ./prefetch_crt_dependency.sh
        fi
        touch "${PATCHED_MARK}"
    fi
    cd -
    echo "Finished patching ${AWS_SDK_SOURCE}"
fi

# patch simdjson, change simdjson::dom::element_type::BOOL to BOOLEAN to avoid conflict with odbc macro BOOL
if [[ " ${TP_ARCHIVES[*]} " =~ " SIMDJSON " ]]; then
    if [[ "${SIMDJSON_SOURCE}" = "simdjson-3.0.1" ]]; then
        cd "${TP_SOURCE_DIR}/${SIMDJSON_SOURCE}"
        if [[ ! -f "${PATCHED_MARK}" ]]; then
            patch -p1 <"${TP_PATCH_DIR}/simdjson-3.0.1.patch"
            touch "${PATCHED_MARK}"
        fi
        cd -
    fi
    echo "Finished patching ${SIMDJSON_SOURCE}"
fi

# patch brpc
if [[ " ${TP_ARCHIVES[*]} " =~ " BRPC " ]]; then
    if [[ "${BRPC_SOURCE}" == 'brpc-1.4.0' ]]; then
        cd "${TP_SOURCE_DIR}/${BRPC_SOURCE}"
        if [[ ! -f "${PATCHED_MARK}" ]]; then
            for patch_file in "${TP_PATCH_DIR}"/brpc-*; do
                echo "patch ${patch_file}"
                patch -p1 <"${patch_file}"
            done
            touch "${PATCHED_MARK}"
        fi
        cd -
    fi
    echo "Finished patching ${BRPC_SOURCE}"
fi

# patch ali sdk
if [[ " ${TP_ARCHIVES[*]} " =~ " ALI_SDK " ]]; then
    if [[ "${ALI_SDK_SOURCE}" = "aliyun-openapi-cpp-sdk-1.36.1586" ]]; then
        cd "${TP_SOURCE_DIR}/${ALI_SDK_SOURCE}"
        if [[ ! -f "${PATCHED_MARK}" ]]; then
            patch -p1 <"${TP_PATCH_DIR}/ali-sdk-1.36.1586.patch"
            touch "${PATCHED_MARK}"
        fi
        cd -
    fi
    echo "Finished patching ${ALI_SDK_SOURCE}"
fi

# patch base64
if [[ " ${TP_ARCHIVES[*]} " =~ " BASE64 " ]]; then
    if [[ "${BASE64_SOURCE}" = "base64-0.5.2" ]]; then
        cd "${TP_SOURCE_DIR}/${BASE64_SOURCE}"
        if [[ ! -f "${PATCHED_MARK}" ]]; then
            patch -p1 <"${TP_PATCH_DIR}/base64-0.5.2.patch"
            touch "${PATCHED_MARK}"
        fi
        cd -
    fi
    echo "Finished patching ${BASE64_SOURCE}"
fi

# patch krb
if [[ " ${TP_ARCHIVES[*]} " =~ " KRB5 " ]]; then
    if [[ "${KRB5_SOURCE}" = "krb5-1.19" ]]; then
        cd "${TP_SOURCE_DIR}/${KRB5_SOURCE}"
        if [[ ! -f "${PATCHED_MARK}" ]]; then
            patch -p1 <"${TP_PATCH_DIR}/krb5-1.19.patch"
            touch "${PATCHED_MARK}"
        fi
        cd -
    fi
    echo "Finished patching ${KRB5_SOURCE}"
fi

# patch bitshuffle
if [[ " ${TP_ARCHIVES[*]} " =~ " BITSHUFFLE " ]]; then
    if [[ "${BITSHUFFLE_SOURCE}" = "bitshuffle-0.5.1" ]]; then
        cd "${TP_SOURCE_DIR}/${BITSHUFFLE_SOURCE}"
        if [[ ! -f "${PATCHED_MARK}" ]]; then
            patch -p1 <"${TP_PATCH_DIR}/bitshuffle-0.5.1.patch"
            touch "${PATCHED_MARK}"
        fi
        cd -
    fi
    echo "Finished patching ${BITSHUFFLE_SOURCE}"
fi

# vim: ts=4 sw=4 ts=4 tw=100:
