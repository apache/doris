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

java_version=$(jdk_version "${JAVA_HOME:-}/bin/java")
jvm_arch='amd64'
if [[ "$(uname -m)" == 'aarch64' ]]; then
    jvm_arch='aarch64'
fi
if [[ "${java_version}" -gt 8 ]]; then
    export LIBJVM_PATH="${JAVA_HOME}/lib"
# JAVA_HOME is jdk
elif [[ -d "${JAVA_HOME}/jre" ]]; then
    export LIBJVM_PATH="${JAVA_HOME}/jre/lib/${jvm_arch}"
# JAVA_HOME is jre
else
    export LIBJVM_PATH="${JAVA_HOME}/lib/${jvm_arch}"
fi

if [[ "$(uname -s)" != 'Darwin' ]]; then
    echo "${LIBJVM_PATH}"/*/libjvm.so
else
    echo "${LIBJVM_PATH}"/*/libjvm.dylib
fi
