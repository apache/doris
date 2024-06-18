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

signum=9
if [[ "$1" = "--grace" ]]; then
    signum=15
fi

pidfile="${PID_DIR}/fe.pid"

if [[ -f "${pidfile}" ]]; then
    pid="$(cat "${pidfile}")"

    # check if pid valid
    if test -z "${pid}"; then
        echo "ERROR: Invalid PID"
        exit 1
    fi

    # check if pid process exist
    if ! kill -0 "${pid}" 2>&1; then
        echo "ERROR: fe process with PID ${pid} does not exist"
        exit 1
    fi

    pidcomm="$(basename "$(ps -p "${pid}" -o comm=)")"
    # check if pid process is frontend process
    if [[ "java" != "${pidcomm}" ]]; then
        echo "ERROR: PID may not be the fe process"
        exit 1
    fi

    # kill pid process and check it
    if kill "-${signum}" "${pid}" >/dev/null 2>&1; then
        while true; do
            if kill -0 "${pid}" >/dev/null 2>&1; then
                echo "Waiting for fe process with PID ${pid} to terminate"
                sleep 2
            else
                echo "stop ${pidcomm} and remove PID file"
                if [[ -f "${pidfile}" ]]; then rm "${pidfile}"; fi
                exit 0
            fi
        done
    else
        echo "ERROR: Failed to stop process with PID ${pid}"
        exit 1
    fi
else
    echo "ERROR: ${pidfile} does not exist"
    exit 1
fi
