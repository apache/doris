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
# This script is used to build tpcds-dsdgen
# TPC-DS_Tools_v3.2.0.zip is from https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp
# Usage:
#    sh build-tpcds-dsdgen.sh
##############################################################

set -eo pipefail

ROOT=$(dirname "$0")
ROOT=$(
    cd "${ROOT}"
    pwd
)

CURDIR="${ROOT}"
TPCDS_DBGEN_DIR="${CURDIR}/DSGen-software-code-3.2.0rc1/tools"

check_prerequest() {
    local CMD=$1
    local NAME=$2
    if ! ${CMD} >/dev/null; then
        echo "${NAME} is missing. This script depends on unzip to extract files from TPC-DS_Tools_v3.2.0.zip"
        exit 1
    fi
}

check_prerequest "unzip -h" "unzip"

# download tpcds tools package first
if [[ -d "${CURDIR}/DSGen-software-code-3.2.0rc1" ]]; then
    echo "If you want to rebuild TPC-DS_Tools_v3.2.0 again, please delete ${CURDIR}/DSGen-software-code-3.2.0rc1 first."
elif [[ -f "${CURDIR}/TPC-DS_Tools_v3.2.0new.zip" ]]; then
    unzip TPC-DS_Tools_v3.2.0new.zip -d "${CURDIR}/"
else
    wget "https://qa-build.oss-cn-beijing.aliyuncs.com/tools/TPC-DS_Tools_v3.2.0new.zip"
    unzip TPC-DS_Tools_v3.2.0new.zip -d "${CURDIR}/"
fi

# compile tpcds-dsdgen
cd "${TPCDS_DBGEN_DIR}/"
make >/dev/null
cd -

# check
if [[ -f ${TPCDS_DBGEN_DIR}/dsdgen ]]; then
    echo "
################
Build succeed!
################
Run ${TPCDS_DBGEN_DIR}/dsdgen -h"
    exit 0
else
    echo "Build failed!"
    exit 1
fi
