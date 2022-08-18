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

set -eo pipefail

curdir="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

OPTS="$(getopt \
    -n "$0" \
    -o '' \
    -l 'daemon' \
    -- "$@")"

eval set -- "${OPTS}"

RUN_DAEMON=0
RUN_IN_AWS=0
while true; do
    case "$1" in
    --daemon)
        RUN_DAEMON=1
        shift
        ;;
    --aws)
        RUN_IN_AWS=1
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

DORIS_HOME="$(
    cd "${curdir}/.."
    pwd
)"
export DORIS_HOME

MAX_MAP_COUNT="$(sysctl -n vm.max_map_count)"
if [[ "${MAX_MAP_COUNT}" -lt 2000000 ]]; then
    echo "Please set vm.max_map_count to be 2000000. sysctl -w vm.max_map_count=2000000"
    exit 1
fi

# add libs to CLASSPATH
for f in "${DORIS_HOME}/lib"/*.jar; do
    if [[ -z "${DORIS_JNI_CLASSPATH_PARAMETER}" ]]; then
        export DORIS_JNI_CLASSPATH_PARAMETER="${f}"
    else
        export DORIS_JNI_CLASSPATH_PARAMETER="${f}:${DORIS_JNI_CLASSPATH_PARAMETER}"
    fi
done
# DORIS_JNI_CLASSPATH_PARAMETER is used to configure additional jar path to jvm. e.g. -Djava.class.path=$DORIS_HOME/lib/java-udf.jar
export DORIS_JNI_CLASSPATH_PARAMETER="-Djava.class.path=${DORIS_JNI_CLASSPATH_PARAMETER}"

jdk_version() {
    local result
    local java_cmd="${JAVA_HOME:-.}/bin/java"
    local IFS=$'\n'

    if [[ -z "${java_cmd}" ]]; then
        result=no_java
        return 1
    else
        local version
        # remove \r for Cygwin
        version="$("${java_cmd}" -Xms32M -Xmx32M -version 2>&1 | tr '\r' '\n' | grep version | awk '{print $3}')"
        version="${version//\"/}"
        if [[ "${version}" =~ ^1\. ]]; then
            result="$(echo "${version}" | awk -F '.' '{print $2}')"
        else
            result="$(echo "${version}" | awk -F '.' '{print $1}')"
        fi
    fi
    echo "${result}"
    return 0
}

jvm_arch="amd64"
MACHINE_TYPE="$(uname -m)"
if [[ "${MACHINE_TYPE}" == "aarch64" ]]; then
    jvm_arch="aarch64"
fi
java_version="$(
    set -e
    jdk_version
)"
if [[ "${java_version}" -gt 8 ]]; then
    export LD_LIBRARY_PATH="${JAVA_HOME}/lib/server:${JAVA_HOME}/lib:${LD_LIBRARY_PATH}"
# JAVA_HOME is jdk
elif [[ -d "${JAVA_HOME}/jre" ]]; then
    export LD_LIBRARY_PATH="${JAVA_HOME}/jre/lib/${jvm_arch}/server:${JAVA_HOME}/jre/lib/${jvm_arch}:${LD_LIBRARY_PATH}"
# JAVA_HOME is jre
else
    export LD_LIBRARY_PATH="${JAVA_HOME}/lib/${jvm_arch}/server:${JAVA_HOME}/lib/${jvm_arch}:${LD_LIBRARY_PATH}"
fi

# export env variables from be.conf
#
# UDF_RUNTIME_DIR
# LOG_DIR
# PID_DIR
export UDF_RUNTIME_DIR="${DORIS_HOME}/lib/udf-runtime"
export LOG_DIR="${DORIS_HOME}/log"
PID_DIR="$(
    cd "${curdir}"
    pwd
)"
export PID_DIR

# set odbc conf path
export ODBCSYSINI="${DORIS_HOME}/conf"

# support utf8 for oracle database
export NLS_LANG='AMERICAN_AMERICA.AL32UTF8'

#filter known leak for lsan.
export LSAN_OPTIONS="suppressions=${DORIS_HOME}/conf/asan_suppr.conf"

while read -r line; do
    envline="$(echo "${line}" |
        sed 's/[[:blank:]]*=[[:blank:]]*/=/g' |
        sed 's/^[[:blank:]]*//g' |
        grep -E "^[[:upper:]]([[:upper:]]|_|[[:digit:]])*=" ||
        true)"
    envline="$(eval "echo ${envline}")"
    if [[ "${envline}" == *"="* ]]; then
        eval 'export "${envline}"'
    fi
done <"${DORIS_HOME}/conf/be.conf"

if [[ -e "${DORIS_HOME}/bin/palo_env.sh" ]]; then
    # shellcheck disable=1091
    source "${DORIS_HOME}/bin/palo_env.sh"
fi

if [[ ! -d "${LOG_DIR}" ]]; then
    mkdir -p "${LOG_DIR}"
fi

if [[ ! -d "${UDF_RUNTIME_DIR}" ]]; then
    mkdir -p "${UDF_RUNTIME_DIR}"
fi

rm -f "${UDF_RUNTIME_DIR}"/*

pidfile="${PID_DIR}/be.pid"

if [[ -f "${pidfile}" ]]; then
    if kill -0 "$(cat "${pidfile}")" >/dev/null 2>&1; then
        echo "Backend running as process $(cat "${pidfile}"). Stop it first."
        exit 1
    else
        rm "${pidfile}"
    fi
fi

chmod 755 "${DORIS_HOME}/lib/doris_be"
echo "start time: $(date)" >>"${LOG_DIR}/be.out"

if [[ ! -f '/bin/limit3' ]]; then
    LIMIT=''
else
    LIMIT="/bin/limit3 -c 0 -n 65536"
fi

## If you are not running in aws cloud, disable this env since https://github.com/aws/aws-sdk-cpp/issues/1410.
if [[ "${RUN_IN_AWS}" -eq 0 ]]; then
    export AWS_EC2_METADATA_DISABLED=true
fi

## set hdfs conf
export LIBHDFS3_CONF="${DORIS_HOME}/conf/hdfs-site.xml"

if [[ "${RUN_DAEMON}" -eq 1 ]]; then
    nohup ${LIMIT:+${LIMIT}} "${DORIS_HOME}/lib/doris_be" "$@" >>"${LOG_DIR}/be.out" 2>&1 </dev/null &
else
    export DORIS_LOG_TO_STDERR=1
    ${LIMIT:+${LIMIT}} "${DORIS_HOME}/lib/doris_be" "$@" 2>&1 </dev/null
fi
