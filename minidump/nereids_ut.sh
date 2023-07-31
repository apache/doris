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

cur_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

if [[ "$(uname -s)" == 'Darwin' ]] && command -v brew &>/dev/null; then
    PATH="$(brew --prefix)/opt/gnu-getopt/bin:${PATH}"
    export PATH
fi

OPTS="$(getopt \
    -n "$0" \
    -o '' \
    -l 'daemon' \
    -l 'helper:' \
    -l 'd:' \
    -l 'version' \
    -- "$@")"

eval set -- "${OPTS}"

DUMP_FILE_DIR=''
OPT_VERSION=''
while true; do
    case "$1" in
    --version)
        OPT_VERSION="--version"
        shift
        ;;
    --d)
        DUMP_FILE_DIR="$2"
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

DORIS_HOME="$(
    cd "${cur_dir}/.."
    pwd
)"
export DORIS_HOME

# export env variables from fe.conf
#
# JAVA_OPTS
# LOG_DIR
# PID_DIR
export JAVA_OPTS="-Xmx1024m"
export LOG_DIR="${DORIS_HOME}/minidump"

while read -r line; do
    env_line="$(echo "${line}" |
        sed 's/[[:blank:]]*=[[:blank:]]*/=/g' |
        sed 's/^[[:blank:]]*//g' |
        grep -E "^[[:upper:]]([[:upper:]]|_|[[:digit:]])*=" ||
        true)"
    env_line="$(eval "echo ${env_line}")"
    if [[ "${env_line}" == *"="* ]]; then
        eval 'export "${env_line}"'
    fi
done <"${DORIS_HOME}/minidump/minidump.conf"

if [[ -e "${DORIS_HOME}/bin/palo_env.sh" ]]; then
    # shellcheck disable=1091
    source "${DORIS_HOME}/bin/palo_env.sh"
fi

if [[ -z "${JAVA_HOME}" ]]; then
    JAVA="$(command -v java)"
else
    JAVA="${JAVA_HOME}/bin/java"
fi

if [[ ! -x "${JAVA}" ]]; then
    echo "The JAVA_HOME environment variable is not defined correctly"
    echo "This environment variable is needed to run this program"
    echo "NB: JAVA_HOME should point to a JDK not a JRE"
    exit 1
fi

# get jdk version, return version as an Integer.
# 1.8 => 8, 13.0 => 13
jdk_version() {
    local java_cmd="${1}"
    local result
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

# need check and create if the log directory existed before outing message to the log file.
if [[ ! -d "${LOG_DIR}" ]]; then
    mkdir -p "${LOG_DIR}"
fi

# check java version and choose correct JAVA_OPTS
java_version="$(
    set -e
    jdk_version "${JAVA}"
)"
final_java_opt="${JAVA_OPTS}"
if [[ "${java_version}" -gt 8 ]]; then
    if [[ -z "${JAVA_OPTS_FOR_JDK_9}" ]]; then
        echo "JAVA_OPTS_FOR_JDK_9 is not set in fe.conf" >>"${LOG_DIR}/minidump.out"
        exit 1
    fi
    final_java_opt="${JAVA_OPTS_FOR_JDK_9}"
fi

# add libs to CLASSPATH
DORIS_FE_JAR=
for f in "${DORIS_HOME}/lib"/*.jar; do
    if [[ "${f}" == *"doris-fe.jar" ]]; then
        DORIS_FE_JAR="${f}"
        continue
    fi
    CLASSPATH="${f}:${CLASSPATH}"
done

# make sure the doris-fe.jar is at first order, so that some classed
# with same qualified name can be loaded priority from doris-fe.jar
CLASSPATH="${DORIS_FE_JAR}:${CLASSPATH}"
export CLASSPATH="${CLASSPATH}:${DORIS_HOME}/lib:${DORIS_HOME}/conf"

date >>"${LOG_DIR}/minidump.out"
query_id_head="|QueryId="
sql_head="|Sql="
# shellcheck disable=SC2045
for path in $(ls "${DUMP_FILE_DIR}"); do
    query_id=${path:0:33}
    temp_file=${LOG_DIR}/temp
    "${JAVA}" ${final_java_opt:+${final_java_opt}} -Xmx2g org.apache.doris.nereids.minidump.Minidump "${DUMP_FILE_DIR}/${path}" "${OPT_VERSION}" "$@" >"${temp_file}"
    result=$(cat "${temp_file}")
    echo "${query_id_head} ${query_id} ${sql_head} ${result}" | tee >>"${LOG_DIR}/minidump.out"
done
date >>"${LOG_DIR}/minidump.out"
