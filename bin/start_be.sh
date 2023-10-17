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

MACHINE_OS=$(uname -s)
if [[ "$(uname -s)" == 'Darwin' ]] && command -v brew &>/dev/null; then
    PATH="$(brew --prefix)/opt/gnu-getopt/bin:${PATH}"
    export PATH
fi

OPTS="$(getopt \
    -n "$0" \
    -o '' \
    -l 'daemon' \
    -l 'console' \
    -- "$@")"

eval set -- "${OPTS}"

RUN_DAEMON=0
RUN_IN_AWS=0
RUN_CONSOLE=0
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
    --console)
        RUN_CONSOLE=1
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

if [[ "$(uname -s)" != 'Darwin' ]]; then
    MAX_MAP_COUNT="$(cat /proc/sys/vm/max_map_count)"
    if [[ "${MAX_MAP_COUNT}" -lt 2000000 ]]; then
        echo "Please set vm.max_map_count to be 2000000 under root using 'sysctl -w vm.max_map_count=2000000'."
        exit 1
    fi
fi

MAX_FILE_COUNT="$(ulimit -n)"
if [[ "${MAX_FILE_COUNT}" -lt 60000 ]]; then
    echo "Please set the maximum number of open file descriptors larger than 60000, eg: 'ulimit -n 60000'."
    exit 1
fi

if [[ "$(swapon -s | wc -l)" -gt 1 ]]; then
    echo "Please disable swap memory before installation."
    exit 1
fi

# add java libs
preload_jars=("preload-extensions")
preload_jars+=("java-udf")

for preload_jar_dir in "${preload_jars[@]}"; do
    for f in "${DORIS_HOME}/lib/java_extensions/${preload_jar_dir}"/*.jar; do
        if [[ -z "${DORIS_CLASSPATH}" ]]; then
            export DORIS_CLASSPATH="${f}"
        else
            export DORIS_CLASSPATH="${DORIS_CLASSPATH}:${f}"
        fi
    done
done

if [[ -d "${DORIS_HOME}/lib/hadoop_hdfs/" ]]; then
    # add hadoop libs
    for f in "${DORIS_HOME}/lib/hadoop_hdfs/common"/*.jar; do
        DORIS_CLASSPATH="${DORIS_CLASSPATH}:${f}"
    done
    for f in "${DORIS_HOME}/lib/hadoop_hdfs/common/lib"/*.jar; do
        DORIS_CLASSPATH="${DORIS_CLASSPATH}:${f}"
    done
    for f in "${DORIS_HOME}/lib/hadoop_hdfs/hdfs"/*.jar; do
        DORIS_CLASSPATH="${DORIS_CLASSPATH}:${f}"
    done
    for f in "${DORIS_HOME}/lib/hadoop_hdfs/hdfs/lib"/*.jar; do
        DORIS_CLASSPATH="${DORIS_CLASSPATH}:${f}"
    done
fi

# add custom_libs to CLASSPATH
if [[ -d "${DORIS_HOME}/custom_lib" ]]; then
    for f in "${DORIS_HOME}/custom_lib"/*.jar; do
        DORIS_CLASSPATH="${DORIS_CLASSPATH}:${f}"
    done
fi

if [[ -n "${HADOOP_CONF_DIR}" ]]; then
    export DORIS_CLASSPATH="${DORIS_CLASSPATH}:${HADOOP_CONF_DIR}"
fi

# the CLASSPATH and LIBHDFS_OPTS is used for hadoop libhdfs
# and conf/ dir so that hadoop libhdfs can read .xml config file in conf/
export CLASSPATH="${DORIS_HOME}/conf/:${DORIS_CLASSPATH}:${CLASSPATH}"
# DORIS_CLASSPATH is for self-managed jni
export DORIS_CLASSPATH="-Djava.class.path=${DORIS_CLASSPATH}"

export LD_LIBRARY_PATH="${DORIS_HOME}/lib/hadoop_hdfs/native:${LD_LIBRARY_PATH}"

jdk_version() {
    local java_cmd="${1}"
    local result
    local IFS=$'\n'

    if ! command -v "${java_cmd}" >/dev/null; then
        echo "ERROR: invalid java_cmd ${java_cmd}" >>"${LOG_DIR}/be.out"
        result=no_java
        return 1
    else
        echo "INFO: java_cmd ${java_cmd}" >>"${LOG_DIR}/be.out"
        local version
        # remove \r for Cygwin
        version="$("${java_cmd}" -Xms32M -Xmx32M -version 2>&1 | tr '\r' '\n' | grep version | awk '{print $3}')"
        version="${version//\"/}"
        if [[ "${version}" =~ ^1\. ]]; then
            result="$(echo "${version}" | awk -F '.' '{print $2}')"
        else
            result="$(echo "${version}" | awk -F '.' '{print $1}')"
        fi
        echo "INFO: jdk_version ${result}" >>"${LOG_DIR}/be.out"
    fi
    echo "${result}"
    return 0
}

# export env variables from be.conf
#
# LOG_DIR
# PID_DIR
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

# filter known leak.
export LSAN_OPTIONS="suppressions=${DORIS_HOME}/conf/lsan_suppr.conf"
export ASAN_OPTIONS="suppressions=${DORIS_HOME}/conf/asan_suppr.conf"

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

if [[ -z "${JAVA_HOME}" ]]; then
    echo "The JAVA_HOME environment variable is not defined correctly"
    echo "This environment variable is needed to run this program"
    echo "NB: JAVA_HOME should point to a JDK not a JRE"
    echo "You can set it in be.conf"
    exit 1
fi

for var in http_proxy HTTP_PROXY https_proxy HTTPS_PROXY; do
    if [[ -n ${!var} ]]; then
        echo "env '${var}' = '${!var}', need unset it using 'unset ${var}'"
        exit 1
    fi
done

if [[ ! -d "${LOG_DIR}" ]]; then
    mkdir -p "${LOG_DIR}"
fi

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

## set asan and ubsan env to generate core file
export ASAN_OPTIONS=symbolize=1:abort_on_error=1:disable_coredump=0:unmap_shadow_on_exit=1:detect_container_overflow=0
export UBSAN_OPTIONS=print_stacktrace=1

## set TCMALLOC_HEAP_LIMIT_MB to limit memory used by tcmalloc
set_tcmalloc_heap_limit() {
    local total_mem_mb
    local mem_limit_str

    if [[ "$(uname -s)" != 'Darwin' ]]; then
        total_mem_mb="$(free -m | grep Mem | awk '{print $2}')"
    else
        total_mem_mb="$(($(sysctl -a hw.memsize | awk '{print $NF}') / 1024))"
    fi
    mem_limit_str=$(grep ^mem_limit "${DORIS_HOME}"/conf/be.conf)
    local digits_unit=${mem_limit_str##*=}
    digits_unit="${digits_unit#"${digits_unit%%[![:space:]]*}"}"
    digits_unit="${digits_unit%"${digits_unit##*[![:space:]]}"}"
    local digits=${digits_unit%%[^[:digit:]]*}
    local unit=${digits_unit##*[[:digit:] ]}

    mem_limit_mb=0
    case ${unit} in
    t | T) mem_limit_mb=$((digits * 1024 * 1024)) ;;
    g | G) mem_limit_mb=$((digits * 1024)) ;;
    m | M) mem_limit_mb=$((digits)) ;;
    k | K) mem_limit_mb=$((digits / 1024)) ;;
    %) mem_limit_mb=$((total_mem_mb * digits / 100)) ;;
    *) mem_limit_mb=$((digits / 1024 / 1024 / 1024)) ;;
    esac

    if [[ "${mem_limit_mb}" -eq 0 ]]; then
        mem_limit_mb=$((total_mem_mb * 90 / 100))
    fi

    if [[ "${mem_limit_mb}" -gt "${total_mem_mb}" ]]; then
        echo "mem_limit is larger than whole memory of the server. ${mem_limit_mb} > ${total_mem_mb}."
        return 1
    fi
    export TCMALLOC_HEAP_LIMIT_MB=${mem_limit_mb}
}

# set_tcmalloc_heap_limit || exit 1

## set hdfs3 conf
if [[ -f "${DORIS_HOME}/conf/hdfs-site.xml" ]]; then
    export LIBHDFS3_CONF="${DORIS_HOME}/conf/hdfs-site.xml"
fi

# check java version and choose correct JAVA_OPTS
java_version="$(
    set -e
    jdk_version "${JAVA_HOME}/bin/java"
)"

CUR_DATE=$(date +%Y%m%d-%H%M%S)
LOG_PATH="-DlogPath=${DORIS_HOME}/log/jni.log"
COMMON_OPTS="-Dsun.java.command=DorisBE -XX:-CriticalJNINatives"
JDBC_OPTS="-DJDBC_MIN_POOL=1 -DJDBC_MAX_POOL=100 -DJDBC_MAX_IDLE_TIME=300000 -DJDBC_MAX_WAIT_TIME=5000"

if [[ "${java_version}" -gt 8 ]]; then
    if [[ -z ${JAVA_OPTS_FOR_JDK_9} ]]; then
        JAVA_OPTS_FOR_JDK_9="-Xmx1024m ${LOG_PATH} -Xlog:gc:${DORIS_HOME}/log/be.gc.log.${CUR_DATE} ${COMMON_OPTS} ${JDBC_OPTS}"
    fi
    final_java_opt="${JAVA_OPTS_FOR_JDK_9}"
else
    if [[ -z ${JAVA_OPTS} ]]; then
        JAVA_OPTS="-Xmx1024m ${LOG_PATH} -Xloggc:${DORIS_HOME}/log/be.gc.log.${CUR_DATE} ${COMMON_OPTS} ${JDBC_OPTS}"
    fi
    final_java_opt="${JAVA_OPTS}"
fi

if [[ "${MACHINE_OS}" == "Darwin" ]]; then
    max_fd_limit='-XX:-MaxFDLimit'

    if ! echo "${final_java_opt}" | grep "${max_fd_limit/-/\\-}" >/dev/null; then
        final_java_opt="${final_java_opt} ${max_fd_limit}"
    fi

    if [[ -n "${JAVA_OPTS}" ]] && ! echo "${JAVA_OPTS}" | grep "${max_fd_limit/-/\\-}" >/dev/null; then
        JAVA_OPTS="${JAVA_OPTS} ${max_fd_limit}"
    fi
fi

# set LIBHDFS_OPTS for hadoop libhdfs
export LIBHDFS_OPTS="${final_java_opt}"

#echo "CLASSPATH: ${CLASSPATH}"
#echo "LD_LIBRARY_PATH: ${LD_LIBRARY_PATH}"
#echo "LIBHDFS_OPTS: ${LIBHDFS_OPTS}"

if [[ -z ${JEMALLOC_CONF} ]]; then
    JEMALLOC_CONF="percpu_arena:percpu,background_thread:true,metadata_thp:auto,muzzy_decay_ms:15000,dirty_decay_ms:15000,oversize_threshold:0,lg_tcache_max:20,prof:false,lg_prof_interval:32,lg_prof_sample:19,prof_gdump:false,prof_accum:false,prof_leak:false,prof_final:false"
fi

if [[ -z ${JEMALLOC_PROF_PRFIX} ]]; then
    export JEMALLOC_CONF="${JEMALLOC_CONF},prof_prefix:"
else
    JEMALLOC_PROF_PRFIX="${DORIS_HOME}/log/${JEMALLOC_PROF_PRFIX}"
    export JEMALLOC_CONF="${JEMALLOC_CONF},prof_prefix:${JEMALLOC_PROF_PRFIX}"
fi

export AWS_EC2_METADATA_DISABLED=true
export AWS_MAX_ATTEMPTS=2

if [[ "${RUN_DAEMON}" -eq 1 ]]; then
    nohup ${LIMIT:+${LIMIT}} "${DORIS_HOME}/lib/doris_be" "$@" >>"${LOG_DIR}/be.out" 2>&1 </dev/null &
elif [[ "${RUN_CONSOLE}" -eq 1 ]]; then
    export DORIS_LOG_TO_STDERR=1
    ${LIMIT:+${LIMIT}} "${DORIS_HOME}/lib/doris_be" "$@" 2>&1 </dev/null
else
    ${LIMIT:+${LIMIT}} "${DORIS_HOME}/lib/doris_be" "$@" >>"${LOG_DIR}/be.out" 2>&1 </dev/null
fi
