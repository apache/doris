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
# This script is used to test EMR cloud service
# Usage:
#    provide your env arguments in default_emr_env.sh
#    sh emr_tools.sh --case ping --endpoint oss-cn-beijing-internal.aliyuncs.com --region cn-beijing  --service ali --ak ak --sk sk
##############################################################

set -eo pipefail

usage() {
    echo "
Usage: $0 <options>
  Optional options:
     [no option]            
     --case             regression case runner: ping, data_set
     --profile          cloud credential profile
     --ak               cloud access key
     --sk               cloud secret key
     --endpoint         cloud endpoint
     --region           cloud region
     --service          cloud optional service provider: ali, tx, hw
     --host             doris mysql cli host, example: 127.0.0.1
     --user             doris username, example: user
     --port             doris port, example: 9030
  Example:
    sh emr_tools.sh --case ping --endpoint oss-cn-beijing-internal.aliyuncs.com --region cn-beijing  --service ali --ak ak --sk sk
  "
    exit 1
}

if ! OPTS="$(getopt \
    -n "$0" \
    -o '' \
    -l 'case:' \
    -l 'profile:' \
    -l 'ak:' \
    -l 'sk:' \
    -l 'endpoint:' \
    -l 'region:' \
    -l 'service:' \
    -l 'host:' \
    -l 'user:' \
    -l 'port:' \
    -l 'test:' \
    -o 'h' \
    -- "$@")"; then
    usage
fi
eval set -- "${OPTS}"

while true; do
    case "$1" in
    --profile)
        PROFILE="$2"
        # can use custom profile: sh emr_tools.sh --profile default_emr_env.sh
        if [[ -n "${PROFILE}" ]]; then
            # example: "$(pwd)/default_emr_env.sh"
            # shellcheck disable=SC1090
            source "${PROFILE}"
        fi
        shift 2
        break
        ;;
    --case)
        CASE="$2"
        shift 2
        ;;
    --ak)
        AK="$2"
        shift 2
        ;;
    --sk)
        SK="$2"
        shift 2
        ;;
    --endpoint)
        ENDPOINT="$2"
        shift 2
        ;;
    --region)
        REGION="$2"
        shift 2
        ;;
    --test)
        TEST_SET="$2"
        shift 2
        ;;
    --service)
        SERVICE="$2"
        shift 2
        ;;
    --host)
        HOST="$2"
        shift 2
        ;;
    --user)
        USER="$2"
        shift 2
        ;;
    --port)
        PORT="$2"
        shift 2
        ;;
    -h)
        usage
        ;;
    --)
        shift
        break
        ;;
    *)
        echo "$1"
        echo "Internal error"
        exit 1
        ;;
    esac
done

export FE_HOST=${HOST}
export USER=${USER}
export FE_QUERY_PORT=${PORT}

if [[ ${CASE} == 'ping' ]]; then
    if [[ ${SERVICE} == 'hw' ]]; then
        # shellcheck disable=SC2269
        HMS_META_URI="${HMS_META_URI}"
        # shellcheck disable=SC2269
        HMS_WAREHOUSE="${HMS_WAREHOUSE}"
        # shellcheck disable=SC2269
        BEELINE_URI="${BEELINE_URI}"
    elif [[ ${SERVICE} == 'ali' ]]; then
        # shellcheck disable=SC2269
        HMS_META_URI="${HMS_META_URI}"
        # shellcheck disable=SC2269
        HMS_WAREHOUSE="${HMS_WAREHOUSE}"
    else
        # [[ ${SERVICE} == 'tx' ]];
        # shellcheck disable=SC2269
        HMS_META_URI="${HMS_META_URI}"
        # shellcheck disable=SC2269
        HMS_WAREHOUSE="${HMS_WAREHOUSE}"
    fi
    sh ping_test/ping_poc.sh "${ENDPOINT}" "${REGION}" "${SERVICE}" "${AK}" "${SK}" "${HMS_META_URI}" "${HMS_WAREHOUSE}" "${BEELINE_URI}"
elif [[ ${CASE} == 'data_set' ]]; then
    if [[ ${SERVICE} == 'tx' ]]; then
        BUCKET=cosn://datalake-bench-cos-1308700295
    elif [[ ${SERVICE} == 'ali' ]]; then
        BUCKET=oss://benchmark-oss
    fi
    # gen table for spark
    if ! sh stardard_set/gen_spark_create_sql.sh "${BUCKET}" obj; then
        echo "Fail to generate spark obj table for test set"
        exit 1
    fi
    if ! sh stardard_set/gen_spark_create_sql.sh hdfs:///benchmark-hdfs hdfs; then
        echo "Fail to generate spark hdfs table for test set, import hdfs data first"
        exit 1
    fi
    # FE_HOST=172.16.1.163
    # USER=root
    # PORT=9035
    if [[ -z ${TEST_SET} ]]; then
        TEST_SET='all'
    fi
    TYPE=hdfs sh stardard_set/run_standard_set.sh "${FE_HOST}" "${USER}" "${PORT}" hms_hdfs "${TEST_SET}"
    TYPE=hdfs sh stardard_set/run_standard_set.sh "${FE_HOST}" "${USER}" "${PORT}" iceberg_hms "${TEST_SET}"
    if [[ ${SERVICE} == 'tx' ]]; then
        sh stardard_set/run_standard_set.sh "${FE_HOST}" "${USER}" "${PORT}" hms_cos "${TEST_SET}"
        sh stardard_set/run_standard_set.sh "${FE_HOST}" "${USER}" "${PORT}" iceberg_hms_cos "${TEST_SET}"
    elif [[ ${SERVICE} == 'ali' ]]; then
        sh stardard_set/run_standard_set.sh "${FE_HOST}" "${USER}" "${PORT}" hms_oss "${TEST_SET}"
        sh stardard_set/run_standard_set.sh "${FE_HOST}" "${USER}" "${PORT}" iceberg_hms_oss "${TEST_SET}"
    fi
fi
