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
# This script is used to generate TPC-DS data set
##############################################################

set -eo pipefail

ROOT=$(dirname "$0")
ROOT=$(
    cd "${ROOT}"
    pwd
)

CURDIR="${ROOT}"
TPCDS_DBGEN_DIR="${CURDIR}/DSGen-software-code-3.2.0rc1/tools"

usage() {
    echo "
Usage: $0 <options>
  Optional options:
     -s             scale factor, default is 1
     -c             parallelism to generate data, default is 10, max is 100

  Eg.
    $0              generate data using default value.
    $0 -s 100        generate data with scale factor 100.
    $0 -s 1000 -c 100   generate data with scale factor 1000. And using 1000 threads to generate data concurrently.
  "
    exit 1
}

OPTS=$(getopt \
    -n "$0" \
    -o '' \
    -o 'hs:c:' \
    -- "$@")

eval set -- "${OPTS}"

SCALE_FACTOR=100
PARALLEL=10
HELP=0

if [[ $# == 0 ]]; then
    usage
fi

while true; do
    case "$1" in
    -h)
        HELP=1
        shift
        ;;
    -s)
        SCALE_FACTOR=$2
        shift 2
        ;;
    -c)
        PARALLEL=$2
        shift 2
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

if [[ "${HELP}" -eq 1 ]]; then
    usage
fi

TPCDS_DATA_DIR="${CURDIR}/tpcds-data"
echo "Scale Factor: ${SCALE_FACTOR}"
echo "Parallelism: ${PARALLEL}"

# check if dsdgen exists
if [[ ! -f ${TPCDS_DBGEN_DIR}/dsdgen ]]; then
    echo "${TPCDS_DBGEN_DIR}/dsdgen does not exist. Run build-tpcds-dsdgen.sh first to build it first."
    exit 1
fi

if [[ -d ${TPCDS_DATA_DIR}/ ]]; then
    echo "${TPCDS_DATA_DIR} exists. Remove it before generating data"
    exit 1
fi

mkdir "${TPCDS_DATA_DIR}"/

# gen data
echo "Begin to generate data..."
date
cd "${TPCDS_DBGEN_DIR}"
if [[ ${PARALLEL} -eq 1 ]] && "${TPCDS_DBGEN_DIR}"/dsdgen -SCALE "${SCALE_FACTOR}" -TERMINATE N -DIR "${TPCDS_DATA_DIR}"; then
    echo "data genarated."
elif [[ ${PARALLEL} -gt 1 ]] && [[ ${PARALLEL} -le 100 ]]; then
    for c in $(seq 1 "${PARALLEL}"); do
        "${TPCDS_DBGEN_DIR}"/dsdgen -SCALE "${SCALE_FACTOR}" -PARALLEL "${PARALLEL}" -CHILD "${c}" -TERMINATE N -DIR "${TPCDS_DATA_DIR}" &
    done
    wait
    echo "data genarated."
else
    echo "ERROR: bad parallelism ${PARALLEL}" && exit 1
fi
cd "${TPCDS_DATA_DIR}"
echo "Convert encoding of customer table files from one iso-8859-1 to utf-8."
for i in $(seq 1 "${PARALLEL}"); do
    if ! [[ -f "customer_${i}_${PARALLEL}.dat" ]]; then continue; fi
    mv "customer_${i}_${PARALLEL}.dat" "customer_${i}_${PARALLEL}.dat.bak"
    iconv -f iso-8859-1 -t utf-8 "customer_${i}_${PARALLEL}.dat.bak" -o "customer_${i}_${PARALLEL}.dat"
    rm "customer_${i}_${PARALLEL}.dat.bak"
done
date

# check data
du -sh "${TPCDS_DATA_DIR}"/*.dat*
