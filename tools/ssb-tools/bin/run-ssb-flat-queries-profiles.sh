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

#################################################################
# This script is used to create ssb flat queries and get profiles
#################################################################

set -eo pipefail

ROOT=$(dirname "$0")
ROOT=$(
    cd "${ROOT}"
    pwd
)

CURDIR="${ROOT}"
QUERIES_DIR="${CURDIR}/../ssb-flat-queries"

usage() {
    echo "
This script is used to run SSB flat 13queries, 
will use mysql client to connect Doris server which parameter is specified in 'doris-cluster.conf' file.
Usage: $0 
  "
    exit 1
}

OPTS=$(getopt \
    -n "$0" \
    -o '' \
    -o 'h' \
    -- "$@")

eval set -- "${OPTS}"
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

check_prerequest() {
    local CMD=$1
    local NAME=$2
    if ! ${CMD}; then
        echo "${NAME} is missing. This script depends on mysql to create tables in Doris."
        exit 1
    fi
}

check_prerequest "mysql --version" "mysql"

source "${CURDIR}/../conf/doris-cluster.conf"
export MYSQL_PWD=${PASSWORD:-}

echo "FE_HOST: ${FE_HOST:='127.0.0.1'}"
echo "FE_QUERY_PORT: ${FE_QUERY_PORT:='9030'}"
echo "FE_HTTP_PORT: ${FE_HTTP_PORT:='8030'}"
echo "USER: ${USER:='root'}"
echo "DB: ${DB:='ssb'}"

urlHeader="http"
if [[ "a${ENABLE_MTLS}" == "atrue" ]] && \
   [[ -n "${CERT_PATH}" ]] && \
   [[ -n "${KEY_PATH}" ]] && \
   [[ -n "${CACERT_PATH}" ]]; then
    export mysqlMTLSInfo="--ssl-mode=VERIFY_CA --tls-version=TLSv1.2 --ssl-ca=${CACERT_PATH} --ssl-cert=${CERT_PATH} --ssl-key=${KEY_PATH}"
    export curlMTLSInfo="--cert ${CERT_PATH} --key ${KEY_PATH} --cacert ${CACERT_PATH}"
    urlHeader="https"
fi

run_sql() {
    echo "$@"
    mysql ${mysqlMTLSInfo} -h"${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${USER}" -D"${DB}" -e "$@"
}

if [[ "a${ENABLE_PROFILE}" == "atrue" ]]; then
    run_sql "set global enable_profile=true;"
else
    run_sql "set global enable_profile=false;"
fi

echo '============================================'
run_sql "show variables;"
echo '============================================'
run_sql "show table status;"
echo '============================================'

PROFILE_DIR="profiles"
if [[ -d "${PROFILE_DIR}" ]]; then
    rm -r "${PROFILE_DIR}"
fi
mkdir -p "${PROFILE_DIR}"
touch result.csv

cold_run_sum=0
best_hot_run_sum=0

# =========================
# add function: get latest query's profile
# =========================
get_and_save_profile() {
    local runName=$1  # e.g. q1.1_cold / q1.1_hot1
    local queryId
    queryId=$(curl --user ${USER}:${PASSWORD} ${urlHeader}://${FE_HOST}:${FE_HTTP_PORT}/rest/v1/query_profile ${curlMTLSInfo}|jq -r '.data.rows[0]["Profile ID"]')
    if [[ -z "${queryId}" ]]; then
        echo "Fail to get query_id for ${runName}"
        return
    fi
    echo "${runName} -> query_id=${queryId}"

    # Get profile
    local profileFile="${PROFILE_DIR}/${runName}_${queryId}.profile"
    curl -s ${curlMTLSInfo} -u "${USER}:${PASSWORD}" "${urlHeader}://${FE_HOST}:${FE_HTTP_PORT}/api/profile/text?query_id=${queryId}" -o "${profileFile}"
    echo "profile saved: ${profileFile}"
}

for i in '1.1' '1.2' '1.3' '2.1' '2.2' '2.3' '3.1' '3.2' '3.3' '3.4' '4.1' '4.2' '4.3'; do
    cold=0
    hot1=0
    hot2=0
    echo -ne "q${i}\t" | tee -a result.csv
    start=$(date +%s%3N)
    if ! output=$(mysql ${mysqlMTLSInfo} -h"${FE_HOST}" -u"${USER}" -P"${FE_QUERY_PORT}" -D"${DB}" --comments \
        <"${QUERIES_DIR}/q${i}.sql" 2>&1); then
        printf "Error: Failed to execute query q%s (cold run). Output:\n%s\n" "${i}" "${output}" >&2
        continue
    fi
    end=$(date +%s%3N)
    cold=$((end - start))
    echo -ne "${cold}\t" | tee -a result.csv
    # Get profile after executing query
    get_and_save_profile "q${i}.sql_cold"

    start=$(date +%s%3N)
    if ! output=$(mysql ${mysqlMTLSInfo} -h"${FE_HOST}" -u"${USER}" -P"${FE_QUERY_PORT}" -D"${DB}" --comments \
        <"${QUERIES_DIR}/q${i}.sql" 2>&1); then
        printf "Error: Failed to execute query q%s (hot run 1). Output:\n%s\n" "${i}" "${output}" >&2
        continue
    fi
    end=$(date +%s%3N)
    hot1=$((end - start))
    echo -ne "${hot1}\t" | tee -a result.csv
    get_and_save_profile "q${i}.sql_hot1"

    start=$(date +%s%3N)
    if ! output=$(mysql ${mysqlMTLSInfo} -h"${FE_HOST}" -u"${USER}" -P"${FE_QUERY_PORT}" -D"${DB}" --comments \
        <"${QUERIES_DIR}/q${i}.sql" 2>&1); then
        printf "Error: Failed to execute query q%s (hot run 2). Output:\n%s\n" "${i}" "${output}" >&2
        continue
    fi
    end=$(date +%s%3N)
    hot2=$((end - start))
    echo -ne "${hot2}\t" | tee -a result.csv
    get_and_save_profile "q${i}.sql_hot2"

    cold_run_sum=$((cold_run_sum + cold))
    if [[ ${hot1} -lt ${hot2} ]]; then
        best_hot_run_sum=$((best_hot_run_sum + hot1))
        echo -ne "${hot1}" | tee -a result.csv
        echo "" | tee -a result.csv
    else
        best_hot_run_sum=$((best_hot_run_sum + hot2))
        echo -ne "${hot2}" | tee -a result.csv
        echo "" | tee -a result.csv
    fi
done

echo "Total cold run time: ${cold_run_sum} ms"
echo "Total hot run time: ${best_hot_run_sum} ms"
echo 'Finish ssb-flat queries.'
