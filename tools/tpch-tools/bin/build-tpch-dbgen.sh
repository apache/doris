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
# This script is used to build tpch-dbgen
# TPC-H_Tools_v3.0.0.zip is from https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp
# Usage:
#    sh build-tpch-dbgen.sh
##############################################################

set -eo pipefail

ROOT=$(dirname "$0")
ROOT=$(
    cd "${ROOT}"
    pwd
)

CURDIR="${ROOT}"
TPCH_DBGEN_DIR="${CURDIR}/TPC-H_Tools_v3.0.0/dbgen"

check_prerequest() {
    local CMD=$1
    local NAME=$2
    if ! ${CMD}; then
        echo "${NAME} is missing. This script depends on unzip to extract files from TPC-H_Tools_v3.0.0new.zip"
        exit 1
    fi
}

check_prerequest "unzip -h" "unzip"

# download tpch tools pacage first
if [[ -d ${TPCH_DBGEN_DIR} ]]; then
    echo "Dir ${TPCH_DBGEN_DIR} already exists. No need to download."
    echo "If you want to download TPC-H_Tools_v3.0.0new again, please delete this dir first."
else
    wget "https://doris-build-1308700295.cos.ap-beijing.myqcloud.com/tools/TPC-H_Tools_v3.0.0new.zip"
    unzip TPC-H_Tools_v3.0.0new.zip -d "${CURDIR}/"
fi

# modify tpcd.h
cd "${TPCH_DBGEN_DIR}/"
printf '%s' '
#ifdef MYSQL
#define GEN_QUERY_PLAN ""
#define START_TRAN "START TRANSACTION"
#define END_TRAN "COMMIT"
#define SET_OUTPUT ""
#define SET_ROWCOUNT "limit %d;\n"
#define SET_DBASE "use %s;\n"
#endif
' >>tpcd.h

# modify makefile
cp makefile.suite makefile
sed -i 's/^CC      =/CC = gcc/g' makefile
sed -i 's/^DATABASE=/DATABASE = MYSQL/g' makefile
sed -i 's/^MACHINE =/MACHINE = LINUX/g' makefile
sed -i 's/^WORKLOAD =/WORKLOAD = TPCH/g' makefile

# compile tpch-dbgen
make >/dev/null
cd -

# check
if [[ -f ${TPCH_DBGEN_DIR}/dbgen ]]; then
    echo "
################
Build succeed!
################
Run ${TPCH_DBGEN_DIR}/dbgen -h"
    exit 0
else
    echo "Build failed!"
    exit 1
fi
