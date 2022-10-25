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

DORIS_HOME="$(
    cd "${curdir}/.."
    pwd
)"
export DORIS_HOME

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
        # JAVA_HOME is jdk
    elif [[ -d "${JAVA_HOME}/jre" ]]; then
        export LD_LIBRARY_PATH="${JAVA_HOME}/jre/lib/${jvm_arch}/server:${JAVA_HOME}/jre/lib/${jvm_arch}:${LD_LIBRARY_PATH}"
        # JAVA_HOME is jre
    else
        export LD_LIBRARY_PATH="${JAVA_HOME}/lib/${jvm_arch}/server:${JAVA_HOME}/lib/${jvm_arch}:${LD_LIBRARY_PATH}"
    fi
}

# prepare jvm if needed
setup_java_env || true

if [[ -e "${DORIS_HOME}/bin/palo_env.sh" ]]; then
    # shellcheck disable=1091
    source "${DORIS_HOME}/bin/palo_env.sh"
fi

chmod 755 "${DORIS_HOME}/lib/doris_be"

"${DORIS_HOME}"/lib/doris_be --version
