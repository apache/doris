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
# See emr_tools.sh
##############################################################

set -e

FE_HOST=$1
USER=$2
FE_QUERY_PORT=$3
DB=$4

TRIES=3
QUERY_NUM=1
RESULT_FILE=result-master-"${DB}".csv
touch "${RESULT_FILE}"
truncate -s 0 "${RESULT_FILE}"

while read -r query; do
    echo -n "query${QUERY_NUM}," | tee -a "${RESULT_FILE}"
    for i in $(seq 1 "${TRIES}"); do
        RES=$(mysql -vvv -h"${FE_HOST}" -u"${USER}" -P"${FE_QUERY_PORT}" -D"${DB}" -e "${query}" | perl -nle 'print $1 if /((\d+\.\d+)+ sec)/' || :)
        echo -n "${RES}" | tee -a "${RESULT_FILE}"
        [[ "${i}" != "${TRIES}" ]] && echo -n "," | tee -a "${RESULT_FILE}"
    done
    echo "" | tee -a "${RESULT_FILE}"

    QUERY_NUM=$((QUERY_NUM + 1))
done <"$5"
