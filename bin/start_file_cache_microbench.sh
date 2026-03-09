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

DORIS_HOME="$(
    cd "${curdir}/.."
    pwd
)"
export DORIS_HOME

export LOG_DIR="${DORIS_HOME}/log"

# read from be.conf (for JAVA_HOME and other envs)
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

jdk_version() {
    local java_cmd="${1}"
    local result
    local IFS=$'\n'

    if ! command -v "${java_cmd}" >/dev/null; then
        result=no_java
        return 1
    else
        local version
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

setup_java_env() {
    local java_version

    if [[ -z "${JAVA_HOME}" ]]; then
        return 1
    fi

    local jvm_arch='amd64'
    if [[ "$(uname -m)" == 'aarch64' ]]; then
        jvm_arch='aarch64'
    fi
    java_version="$(
        set -e
        jdk_version "${JAVA_HOME}/bin/java"
    )"
    if [[ "${java_version}" -gt 8 ]]; then
        export LD_LIBRARY_PATH="${JAVA_HOME}/lib/server:${JAVA_HOME}/lib:${LD_LIBRARY_PATH}"
        if [[ "$(uname -s)" == 'Darwin' ]]; then
            export DYLD_LIBRARY_PATH="${JAVA_HOME}/lib/server:${JAVA_HOME}/lib:${DYLD_LIBRARY_PATH}"
        fi
    elif [[ -d "${JAVA_HOME}/jre" ]]; then
        export LD_LIBRARY_PATH="${JAVA_HOME}/jre/lib/${jvm_arch}/server:${JAVA_HOME}/jre/lib/${jvm_arch}:${LD_LIBRARY_PATH}"
        if [[ "$(uname -s)" == 'Darwin' ]]; then
            export DYLD_LIBRARY_PATH="${JAVA_HOME}/jre/lib/server:${JAVA_HOME}/jre/lib:${DYLD_LIBRARY_PATH}"
        fi
    else
        export LD_LIBRARY_PATH="${JAVA_HOME}/lib/${jvm_arch}/server:${JAVA_HOME}/lib/${jvm_arch}:${LD_LIBRARY_PATH}"
        if [[ "$(uname -s)" == 'Darwin' ]]; then
            export DYLD_LIBRARY_PATH="${JAVA_HOME}/lib/server:${JAVA_HOME}/lib:${DYLD_LIBRARY_PATH}"
        fi
    fi
}

setup_java_env || true

if [[ -z "${JAVA_HOME}" ]]; then
    echo "The JAVA_HOME environment variable is not set correctly"
    echo "This environment variable is required to run this program"
    echo "Note: JAVA_HOME should point to a JDK and not a JRE"
    echo "You can set JAVA_HOME in the be.conf configuration file"
    exit 1
fi

java_version="$(
    set -e
    jdk_version "${JAVA_HOME}/bin/java"
)"
if [[ "${java_version}" -ne 17 ]]; then
    echo "ERROR: The jdk_version is ${java_version}, it must be 17."
    exit 1
fi


JEMALLOC_CONF="percpu_arena:percpu,background_thread:true,metadata_thp:auto,muzzy_decay_ms:5000,dirty_decay_ms:5000,oversize_threshold:0,prof:true,lg_prof_interval:30,lg_prof_sample:19,prof_final:false,prof_active:true"
JEMALLOC_PROF_PRFIX="jeprofile_doris_cloud"

export JEMALLOC_CONF="${JEMALLOC_CONF},prof_prefix:${JEMALLOC_PROF_PRFIX}"
sysctl -w vm.max_map_count=2000000 && ulimit -n 65535 && ulimit -c unlimited && swapoff -a && export ASAN_SYMBOLIZER_PATH=/var/local/ldb-toolchain/bin/llvm-symbolizer && nohup ./file_cache_microbench --port=8060 &
