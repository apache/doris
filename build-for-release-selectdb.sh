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

##############################################################
# This script is used to build for SelectDB Enterprise Core
##############################################################

set -eo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

export DORIS_HOME="${ROOT}"

# Check args
usage() {
    echo "
Usage: $0 --version version <options>
  Optional options:
     [no option]        build with avx2
     --noavx2           build without avx2
     --tar              pack the output

  Eg.
    $0 --vendor selectdb --version 1.2.0            build selectdb with avx2
    $0 --vendor velodb --version 1.2.0 --tar        build velodb with avx2 and pack the output
    $0 --vendor selectdb --noavx2 --version 1.2.0   build selectdb without avx2
  "
    exit 1
}

if ! OPTS="$(getopt \
    -n "$0" \
    -o '' \
    -l 'noavx2' \
    -l 'tar' \
    -l 'version:' \
    -l 'vendor:' \
    -l 'help' \
    -- "$@")"; then
    usage
fi

eval set -- "${OPTS}"

_USE_AVX2=1
TAR=0
VERSION=
VENDOR=
if [[ "$#" == 1 ]]; then
    _USE_AVX2=1
else
    while true; do
        case "$1" in
        --noavx2)
            _USE_AVX2=0
            shift
            ;;
        --tar)
            TAR=1
            shift
            ;;
        --version)
            VERSION="$2"
            shift 2
            ;;
        --vendor)
            VENDOR="$2"
            shift 2
            ;;
        --help)
            HELP=1
            shift
            ;;
        --)
            shift
            break
            ;;
        *)
            echo "Internal error"
            exit 1
            ;;
        esac
    done
fi
if [[ "${VENDOR}" == "" ]]; then
    echo "--vendor must be specified, chose one of selectdb, velodb"
    usage
    exit -1
fi
# this env var is used by the doris build script
export DORIS_VENDOR=${VENDOR}

if [[ "${HELP}" -eq 1 ]]; then
    usage
    exit
fi

if [[ -z ${VERSION} ]]; then
    echo "Must specify version"
    usage
    exit 1
fi

echo "Get params:
    VERSION         -- ${VERSION}
    USE_AVX2        -- ${_USE_AVX2}
    TAR             -- ${TAR}
"

ARCH="$(uname -m)"
if [[ "${ARCH}" == "aarch64" ]]; then
    ARCH="arm64"
elif [[ "${ARCH}" == "x86_64" ]]; then
    ARCH="x64"
else
    echo "Unknown arch: ${ARCH}"
    exit 1
fi

bash install-enterprise-dependencies.sh

ORI_OUTPUT="${ROOT}/output"

FE="fe"
BE="be"
BROKER="apache_hdfs_broker"

PACKAGE_NAME="${VENDOR}-doris-${VERSION}-bin-${ARCH}"
if [[ "${_USE_AVX2}" == "0" && "${ARCH}" == "x64" ]]; then
    PACKAGE_NAME="${PACKAGE_NAME}-noavx2"
fi
OUTPUT="${ORI_OUTPUT}/${PACKAGE_NAME}"

rm -rf "${OUTPUT}" && mkdir -p "${OUTPUT}"
echo "Package Path: ${OUTPUT}"

# download and setup java
JAVA17_DOWNLOAD_LINK=
JAVA17_DIR_NAME=
if [[ "${ARCH}" == "x64" ]]; then
    JAVA17_DOWNLOAD_LINK="https://selectdb-doris-1308700295.cos.ap-beijing.myqcloud.com/release/jdbc_driver/openjdk-17.0.2_linux-x64_bin.tar.gz"
    JAVA17_DIR_NAME="jdk-17.0.2"
elif [[ "${ARCH}" == "arm64" ]]; then
    JAVA17_DOWNLOAD_LINK="https://selectdb-doris-1308700295.cos.ap-beijing.myqcloud.com/release/jdbc_driver/bisheng-jdk-17.0.11-linux-aarch64.tar.gz"
    JAVA17_DIR_NAME="bisheng-jdk-17.0.11"
else
    echo "Unknown arch: ${ARCH}"
    exit 1
fi

OUTPUT_JAVA17="${OUTPUT}/java17"
curl -# "${JAVA17_DOWNLOAD_LINK}" | tar xz -C "${OUTPUT}/" && mv "${OUTPUT}/${JAVA17_DIR_NAME}/" "${OUTPUT_JAVA17}"
export JAVA_HOME="${OUTPUT_JAVA17}"
export PATH="${JAVA_HOME}/bin:${PATH}"

# build core
#sh build.sh --clean &&
USE_AVX2="${_USE_AVX2}" WITH_TDE_DIR=enterprise sh build.sh &&
USE_AVX2="${_USE_AVX2}" WITH_TDE_DIR=enterprise sh build.sh --be --meta-tool --cloud

package_fdb() {
    echo "package fdb"
    rm -rf "${ORI_OUTPUT}/fdb"
    cp -r "${ROOT}/tools/fdb" "${ORI_OUTPUT}/"
    pushd "${ORI_OUTPUT}/fdb"
    ## fake a mandatory variable
    mv fdb_vars.sh fdb_vars_bak.sh
    cp fdb_vars_bak.sh fdb_vars.sh
    sed -ri 's#^FDB_HOME=.*#FDB_HOME=/tmp/fdb#' fdb_vars.sh
    rm -rf /tmp/fdb
    mkdir -p /tmp/fdb

    sh fdb_ctl.sh download
    mv fdb_vars_bak.sh fdb_vars.sh
    popd
}

package_fdb

echo "Begin to install"
cp -r "${ORI_OUTPUT}/fe" "${OUTPUT}/fe"
cp -r "${ORI_OUTPUT}/be" "${OUTPUT}/be"
cp -r "${ORI_OUTPUT}/ms" "${OUTPUT}/ms"
cp -r "${ORI_OUTPUT}/apache_hdfs_broker" "${OUTPUT}/apache_hdfs_broker"
cp -r "${ORI_OUTPUT}/tools" "${OUTPUT}/tools"
cp -r "${ORI_OUTPUT}/fdb" "${OUTPUT}/fdb"

JDBC_DRIVERS_DIR="${OUTPUT}/jdbc_drivers/"
mkdir -p "${JDBC_DRIVERS_DIR}"
wget https://selectdb-doris-1308700295.cos.ap-beijing.myqcloud.com/release/jdbc_driver/clickhouse-jdbc-0.3.2-patch11-all.jar -P "${JDBC_DRIVERS_DIR}/"
wget https://selectdb-doris-1308700295.cos.ap-beijing.myqcloud.com/release/jdbc_driver/mssql-jdbc-11.2.0.jre8.jar -P "${JDBC_DRIVERS_DIR}/"
wget https://selectdb-doris-1308700295.cos.ap-beijing.myqcloud.com/release/jdbc_driver/mysql-connector-java-8.0.25.jar -P "${JDBC_DRIVERS_DIR}/"
wget https://selectdb-doris-1308700295.cos.ap-beijing.myqcloud.com/release/jdbc_driver/ojdbc6.jar -P "${JDBC_DRIVERS_DIR}/"
wget https://selectdb-doris-1308700295.cos.ap-beijing.myqcloud.com/release/jdbc_driver/postgresql-42.5.0.jar -P "${JDBC_DRIVERS_DIR}/"

if [[ "${TAR}" -eq 1 ]]; then
    echo "Begin to compress"
    cd "${ORI_OUTPUT}"
    tar -cf - "${PACKAGE_NAME}" | xz -T0 -z - >"${PACKAGE_NAME}".tar.xz
    cd -
fi

echo "Output dir: ${OUTPUT}"
exit 0
