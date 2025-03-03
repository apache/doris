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

curdir="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

DORIS_HOME="$(
    cd "${curdir}/.." || exit 1
    pwd
)"
export DORIS_HOME
echo "DORIS_HOME: ${DORIS_HOME}"

if [[ -z "${JAVA_HOME}" ]]; then
    if ! command -v jcmd &>/dev/null; then
        JCMD=""
    else
        JCMD="$(command -v jcmd)"
    fi
else
    JCMD="${JAVA_HOME}/bin/jcmd"
fi
echo "JCMD: ${JCMD}"

if [[ ! -x "${JCMD}" ]]; then
    echo "The JAVA_HOME environment variable is not set correctly"
    echo "This environment variable is required to run this program"
    echo "Note: JAVA_HOME should point to a JDK and not a JRE"
    echo "You can set JAVA_HOME in the fe.conf configuration file"
    exit 1
fi

FE_PID=$(${JAVA_HOME}/bin/jps | grep DorisFE | awk '{print $1}')
if [[ -z "${FE_PID}" ]]; then
    echo "DorisFe not started"
    exit 1
fi
echo "DorisFE pid: ${FE_PID}"

mkdir -p "${DORIS_HOME}/log"
NOW=$(date +'%Y%m%d%H%M%S')
RECORD_OUTPUT="${DORIS_HOME}/log/flight_record_${NOW}.jfr"
if [[ -z "${RECORD_SECONDS}" ]]; then
    RECORD_SECONDS="30"
fi

# add shutdown hook to stop flight record
cleanup() {
    echo "Exec shutdown hook"
    ${JCMD} "${FE_PID}" JFR.stop name="jfr_${NOW}"
    echo "Generated flight record to ${RECORD_OUTPUT}"
    exit 1
}

trap cleanup SIGINT

echo "Begin flight record ${RECORD_SECONDS} seconds and generate to ${RECORD_OUTPUT}..."
${JCMD} "${FE_PID}" JFR.start name="jfr_${NOW}" settings=profile filename="${RECORD_OUTPUT}"

echo "wait ${RECORD_SECONDS} seconds..."
sleep "${RECORD_SECONDS}"
${JCMD} "${FE_PID}" JFR.stop name="jfr_${NOW}"

echo "Generated flight record to ${RECORD_OUTPUT}"
