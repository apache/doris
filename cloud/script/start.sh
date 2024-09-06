#!/usr/bin/bash
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
    cd "${curdir}/.." || exit 1
    pwd
)"

cd "${DORIS_HOME}" || exit 1

if [[ ! -d bin || ! -d conf || ! -d lib ]]; then
    echo "$0 must be invoked at the directory which contains bin, conf and lib"
    exit 1
fi

RUN_DAEMON=0
RUN_VERSION=0
RUN_CONSOLE=0
for arg; do
    shift
    [[ "${arg}" = "--daemonized" ]] && RUN_DAEMON=1 && continue
    [[ "${arg}" = "-daemonized" ]] && RUN_DAEMON=1 && continue
    [[ "${arg}" = "--daemon" ]] && RUN_DAEMON=1 && continue
    [[ "${arg}" = "--version" ]] && RUN_VERSION=1 && continue
    [[ "${arg}" = "--console" ]] && RUN_CONSOLE=1 && continue
    set -- "$@" "${arg}"
done
# echo "$@" "daemonized=${daemonized}"}

# export env variables from doris_cloud.conf
# read from doris_cloud.conf
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
done <"${DORIS_HOME}/conf/doris_cloud.conf"

process=doris_cloud

if [[ -f "${DORIS_HOME}/bin/${process}.pid" ]]; then
    pid=$(cat "${DORIS_HOME}/bin/${process}.pid")
    if [[ "${pid}" != "" ]]; then
        if kill -0 "$(cat "${DORIS_HOME}/bin/${process}.pid")" >/dev/null 2>&1; then
            echo "pid file existed, ${process} have already started, pid=${pid}"
            exit 1
        fi
    fi
    echo "pid file existed but process not alive, remove it, pid=${pid}"
    rm -f "${DORIS_HOME}/bin/${process}.pid"
fi

lib_path="${DORIS_HOME}/lib"
bin="${DORIS_HOME}/lib/doris_cloud"
if ldd "${bin}" | grep -Ei 'libfdb_c.*not found' &>/dev/null; then
    if ! command -v patchelf &>/dev/null; then
        echo "patchelf is needed to launch meta_service"
        exit 1
    fi
    patchelf --set-rpath "${lib_path}" "${bin}"
    # ldd "${bin}"
fi

chmod 550 "${DORIS_HOME}/lib/doris_cloud"

if [[ -z "${JAVA_HOME}" ]]; then
    echo "The JAVA_HOME environment variable is not defined correctly"
    echo "This environment variable is needed to run this program"
    echo "NB: JAVA_HOME should point to a JDK not a JRE"
    echo "You can set it in doris_cloud.conf"
    exit 1
fi

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

export CLASSPATH="${DORIS_CLASSPATH}"

export LD_LIBRARY_PATH="${JAVA_HOME}/lib/server:${LD_LIBRARY_PATH}"

## set libhdfs3 conf
if [[ -f "${DORIS_HOME}/conf/hdfs-site.xml" ]]; then
    export LIBHDFS3_CONF="${DORIS_HOME}/conf/hdfs-site.xml"
fi

echo "LIBHDFS3_CONF=${LIBHDFS3_CONF}"

# to enable dump jeprof heap stats prodigally, change `prof:false` to `prof:true`
# to control the dump interval change `lg_prof_interval` to a specific value, it is pow/exponent of 2 in size of bytes, default 34 means 2 ** 34 = 16GB
# to control the dump path, change `prof_prefix` to a specific path, e.g. /doris_cloud/log/ms_, by default it dumps at the path where the start command called
export JEMALLOC_CONF="percpu_arena:percpu,background_thread:true,metadata_thp:auto,muzzy_decay_ms:5000,dirty_decay_ms:5000,oversize_threshold:0,prof_prefix:ms_,prof:false,lg_prof_interval:34"

if [[ "${RUN_VERSION}" -eq 1 ]]; then
    "${bin}" --version
    exit 0
fi

mkdir -p "${DORIS_HOME}/log"
echo "starts ${process} with args: $*"
out_file=${DORIS_HOME}/log/${process}.out
if [[ "${RUN_DAEMON}" -eq 1 ]]; then
    # append 10 blank lines to ensure the following tail -n10 works correctly
    printf "\n\n\n\n\n\n\n\n\n\n" >>"${out_file}"
    echo "$(date +'%F %T') try to start ${process}" >>"${out_file}"
    nohup "${bin}" "$@" >>"${out_file}" 2>&1 &
    echo "wait and check ${process} start successfully"
    sleep 3
    tail -n10 "${out_file}" | grep 'successfully started brpc'
    ret=$?
    if [[ ${ret} -ne 0 ]]; then
        echo "${process} may not start successfully please check process log for more details"
        exit 1
    fi
    echo "${process} start successfully"
    exit 0
elif [[ "${RUN_CONSOLE}" -eq 1 ]]; then
    export DORIS_LOG_TO_STDERR=1
    date
    "${bin}" "$@" 2>&1
else
    "${bin}" "$@"
fi

# vim: et ts=2 sw=2:
