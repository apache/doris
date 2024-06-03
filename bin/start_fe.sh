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

if [[ "$(uname -s)" == 'Darwin' ]] && command -v brew &>/dev/null; then
    PATH="$(brew --prefix)/opt/gnu-getopt/bin:${PATH}"
    export PATH
fi

OPTS="$(getopt \
    -n "$0" \
    -o '' \
    -l 'daemon' \
    -l 'helper:' \
    -l 'image:' \
    -l 'version' \
    -l 'metadata_failure_recovery' \
    -l 'console' \
    -- "$@")"

eval set -- "${OPTS}"

RUN_DAEMON=0
RUN_CONSOLE=0
HELPER=''
IMAGE_PATH=''
IMAGE_TOOL=''
OPT_VERSION=''
METADATA_FAILURE_RECOVERY=''
while true; do
    case "$1" in
    --daemon)
        RUN_DAEMON=1
        shift
        ;;
    --console)
        RUN_CONSOLE=1
        shift
        ;;
    --version)
        OPT_VERSION="--version"
        shift
        ;;
    --metadata_failure_recovery)
        METADATA_FAILURE_RECOVERY="-r"
        shift
        ;;
    --helper)
        HELPER="$2"
        shift 2
        ;;
    --image)
        IMAGE_TOOL=1
        IMAGE_PATH="$2"
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
    cd "${curdir}/.."
    pwd
)"
export DORIS_HOME

# export env variables from fe.conf
#
# JAVA_OPTS
# LOG_DIR
# PID_DIR
export JAVA_OPTS="-Xmx1024m"
export LOG_DIR="${DORIS_HOME}/log"
PID_DIR="$(
    cd "${curdir}"
    pwd
)"
export PID_DIR

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
done <"${DORIS_HOME}/conf/fe.conf"

if [[ -e "${DORIS_HOME}/bin/palo_env.sh" ]]; then
    # shellcheck disable=1091
    source "${DORIS_HOME}/bin/palo_env.sh"
fi

#Due to the machine not being configured with Java home, in this case, when FE cannot start, it is necessary to prompt an error message indicating that it has not yet been configured with Java home.

if [[ -z "${JAVA_HOME}" ]]; then
    if ! command -v java &>/dev/null; then
        JAVA=""
    else
        JAVA="$(command -v java)"
    fi
else
    JAVA="${JAVA_HOME}/bin/java"
fi

if [[ ! -x "${JAVA}" ]]; then
    echo "The JAVA_HOME environment variable is not set correctly"
    echo "This environment variable is required to run this program"
    echo "Note: JAVA_HOME should point to a JDK and not a JRE"
    echo "You can set JAVA_HOME in the fe.conf configuration file"
    exit 1
fi

for var in http_proxy HTTP_PROXY https_proxy HTTPS_PROXY; do
    if [[ -n ${!var} ]]; then
        echo "env '${var}' = '${!var}', need unset it using 'unset ${var}'"
        exit 1
    fi
done

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

STDOUT_LOGGER="${LOG_DIR}/fe.out"
log() {
    # same datetime format as in fe.log: 2024-06-03 14:54:41,478
    cur_date=$(date +"%Y-%m-%d %H:%M:%S,$(date +%3N)")
    if [[ "${RUN_CONSOLE}" -eq 1 ]]; then
        echo "StdoutLogger ${cur_date} $1"
    else
        echo "StdoutLogger ${cur_date} $1" >>"${STDOUT_LOGGER}"
    fi
}

# check java version and choose correct JAVA_OPTS
java_version="$(
    set -e
    jdk_version "${JAVA}"
)"
if [[ "${java_version}" -eq 17 ]]; then
    if [[ -z "${JAVA_OPTS_FOR_JDK_17}" ]]; then
        log "JAVA_OPTS_FOR_JDK_17 is not set in fe.conf"
        exit 1
    fi
    final_java_opt="${JAVA_OPTS_FOR_JDK_17}"
else
    log "ERROR: The jdk_version is ${java_version}, must be 17."
    exit 1
fi
log "Using Java version ${java_version}"
log "${final_java_opt}"

# add libs to CLASSPATH
DORIS_FE_JAR=
for f in "${DORIS_HOME}/lib"/*.jar; do
    if [[ "${f}" == *"doris-fe.jar" ]]; then
        DORIS_FE_JAR="${f}"
        continue
    fi
    CLASSPATH="${f}:${CLASSPATH}"
done

# add custom_libs to CLASSPATH
if [[ -d "${DORIS_HOME}/custom_lib" ]]; then
    for f in "${DORIS_HOME}/custom_lib"/*.jar; do
        CLASSPATH="${CLASSPATH}:${f}"
    done
fi

# make sure the doris-fe.jar is at first order, so that some classed
# with same qualified name can be loaded priority from doris-fe.jar
CLASSPATH="${DORIS_FE_JAR}:${CLASSPATH}"
if [[ -n "${HADOOP_CONF_DIR}" ]]; then
    CLASSPATH="${HADOOP_CONF_DIR}:${CLASSPATH}"
fi
export CLASSPATH="${DORIS_HOME}/conf:${CLASSPATH}:${DORIS_HOME}/lib"

pidfile="${PID_DIR}/fe.pid"

if [[ -f "${pidfile}" ]] && [[ "${OPT_VERSION}" == "" ]]; then
    if kill -0 "$(cat "${pidfile}")" >/dev/null 2>&1; then
        echo "Frontend running as process $(cat "${pidfile}"). Stop it first."
        exit 1
    fi
fi

if [[ ! -f "/bin/limit" ]]; then
    LIMIT=''
else
    LIMIT=/bin/limit
fi

coverage_opt=""
if [[ -n "${JACOCO_COVERAGE_OPT}" ]]; then
    coverage_opt="${JACOCO_COVERAGE_OPT}"
fi

CUR_DATE=$(date)
log "start time: ${CUR_DATE}"

if [[ "${HELPER}" != "" ]]; then
    # change it to '-helper' to be compatible with code in Frontend
    HELPER="-helper ${HELPER}"
fi

if [[ "${IMAGE_TOOL}" -eq 1 ]]; then
    if [[ -n "${IMAGE_PATH}" ]]; then
        ${LIMIT:+${LIMIT}} "${JAVA}" ${final_java_opt:+${final_java_opt}} ${coverage_opt:+${coverage_opt}} org.apache.doris.DorisFE -i "${IMAGE_PATH}"
    else
        echo "Internal error, USE IMAGE_TOOL like: ./start_fe.sh --image image_path"
    fi
elif [[ "${RUN_DAEMON}" -eq 1 ]]; then
    nohup ${LIMIT:+${LIMIT}} "${JAVA}" ${final_java_opt:+${final_java_opt}} -XX:-OmitStackTraceInFastThrow -XX:OnOutOfMemoryError="kill -9 %p" ${coverage_opt:+${coverage_opt}} org.apache.doris.DorisFE ${HELPER:+${HELPER}} "${METADATA_FAILURE_RECOVERY}" "$@" >>"${STDOUT_LOGGER}" 2>&1 </dev/null &
elif [[ "${RUN_CONSOLE}" -eq 1 ]]; then
    export DORIS_LOG_TO_STDERR=1
    ${LIMIT:+${LIMIT}} "${JAVA}" ${final_java_opt:+${final_java_opt}} -XX:-OmitStackTraceInFastThrow -XX:OnOutOfMemoryError="kill -9 %p" ${coverage_opt:+${coverage_opt}} org.apache.doris.DorisFE ${HELPER:+${HELPER}} ${OPT_VERSION:+${OPT_VERSION}} "${METADATA_FAILURE_RECOVERY}" "$@" </dev/null
else
    ${LIMIT:+${LIMIT}} "${JAVA}" ${final_java_opt:+${final_java_opt}} -XX:-OmitStackTraceInFastThrow -XX:OnOutOfMemoryError="kill -9 %p" ${coverage_opt:+${coverage_opt}} org.apache.doris.DorisFE ${HELPER:+${HELPER}} ${OPT_VERSION:+${OPT_VERSION}} "${METADATA_FAILURE_RECOVERY}" "$@" >>"${STDOUT_LOGGER}" 2>&1 </dev/null
fi

if [[ "${OPT_VERSION}" != "" ]]; then
    exit 0
fi
echo $! >"${pidfile}"
