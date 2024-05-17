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
    if ! command -v java &>/dev/null; then
        JAVA=""
    else
        JAVA="$(command -v java)"
    fi
else
    JAVA="${JAVA_HOME}/bin/java"
fi
echo "JAVA: ${JAVA}"

if [[ ! -x "${JAVA}" ]]; then
    echo "The JAVA_HOME environment variable is not set correctly"
    echo "This environment variable is required to run this program"
    echo "Note: JAVA_HOME should point to a JDK and not a JRE"
    echo "You can set JAVA_HOME in the fe.conf configuration file"
    exit 1
fi

FE_PID=$(jps | grep DorisFE | awk '{print $1}')
if [[ -z "${FE_PID}" ]]; then
    echo "DorisFe not started"
    exit 1
fi
echo "DorisFE pid: ${FE_PID}"

mkdir -p "${DORIS_HOME}/log"
NOW=$(date +'%Y%m%d%H%M%S')
PROFILE_OUTPUT="${DORIS_HOME}/log/profile_${NOW}.html"
if [[ -z "${PROFILE_SECONDS}" ]]; then
    PROFILE_SECONDS="10"
fi

echo "Begin profiling ${PROFILE_SECONDS} seconds and generate flame graph to ${PROFILE_OUTPUT}..."
${JAVA} -jar "${DORIS_HOME}"/lib/ap-loader-all-*.jar profiler -a -n -l -i 200us -d "${PROFILE_SECONDS}" -f "${PROFILE_OUTPUT}" "${FE_PID}"
echo "Generated flame graph to ${PROFILE_OUTPUT}"
