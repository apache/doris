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
    -- "$@")"

eval set -- "${OPTS}"

RUN_DAEMON=0
while true; do
    case "$1" in
    --daemon)
        RUN_DAEMON=1
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

BROKER_HOME="$(
    cd "${curdir}/.."
    pwd
)"
export BROKER_HOME

PID_DIR="$(
    cd "${curdir}"
    pwd
)"
export PID_DIR

export JAVA_OPTS="-Xmx1024m -Dfile.encoding=UTF-8 --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens java.base/jdk.internal.ref=ALL-UNNAMED"
export BROKER_LOG_DIR="${BROKER_HOME}/log"
# java
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

# add libs to CLASSPATH
for f in "${BROKER_HOME}/lib"/*.jar; do
    CLASSPATH="${f}:${CLASSPATH}"
done
export CLASSPATH="${CLASSPATH}:${BROKER_HOME}/lib:${BROKER_HOME}/conf"

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
done <"${BROKER_HOME}/conf/apache_hdfs_broker.conf"

pidfile="${PID_DIR}/apache_hdfs_broker.pid"

if [[ -f "${pidfile}" ]]; then
    if kill -0 "$(cat "${pidfile}")" >/dev/null 2>&1; then
        echo "Broker running as process $(cat "${pidfile}").  Stop it first."
        exit 1
    fi
fi

if [[ ! -d "${BROKER_LOG_DIR}" ]]; then
    mkdir -p "${BROKER_LOG_DIR}"
fi

date >>"${BROKER_LOG_DIR}/apache_hdfs_broker.out"

if [[ ${RUN_DAEMON} -eq 1 ]]; then
    nohup ${LIMIT:+${LIMIT}} "${JAVA}" ${JAVA_OPTS:+${JAVA_OPTS}} org.apache.doris.broker.hdfs.BrokerBootstrap "$@" >>"${BROKER_LOG_DIR}/apache_hdfs_broker.out" 2>&1 </dev/null &
else
    ${LIMIT:+${LIMIT}} "${JAVA}" ${JAVA_OPTS:+${JAVA_OPTS}} org.apache.doris.broker.hdfs.BrokerBootstrap "$@" 2>&1 </dev/null
fi

echo $! >"${pidfile}"
