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

# init and export env variables

PRG="$0"
while [[ -h "${PRG}" ]] ; do
  ls=$(ls -ld "${PRG}")
  link=$(expr "${ls}" : '.*-> \(.*\)$')
  if expr "${link}" : '/.*' > /dev/null; then
    PRG="${link}"
  else
    PRG=$(dirname "${PRG}")/"${link}"
  fi
done

PRGDIR=$(dirname "${PRG}")

export DORIS_HOME="$(cd "${PRGDIR}/.." >/dev/null; pwd)"
export PID_DIR="$(cd "${PRGDIR}" >/dev/null; pwd)"
PID_FILE="${PID_DIR}/be.pid"

signum=9
if [[ "$1" = "--grace" ]]; then
    signum=15
fi

if [[ -f "${PID_FILE}" ]]; then
    pid="$(cat "${PID_FILE}")"

    # check if pid valid
    if test -z "${pid}"; then
        echo "ERROR: invalid pid."
        exit 1
    fi

    # check if pid process exist
    if ! kill -0 "${pid}" 2>&1; then
        echo "ERROR: be process ${pid} does not exist."
        exit 1
    fi

    pidcomm="$(ps -p "${pid}" -o comm=)"
    # check if pid process is backend process
    if [[ "doris_be" != "${pidcomm}" ]]; then
        echo "ERROR: pid process may not be be. "
        exit 1
    fi

    # kill pid process and check it
    if kill "-${signum}" "${pid}" >/dev/null 2>&1; then
        while true; do
            if kill -0 "${pid}" >/dev/null 2>&1; then
                echo "waiting be to stop, pid: ${pid}"
                sleep 2
            else
                echo "stop ${pidcomm}, and remove pid file. "
                if [[ -f "${PID_FILE}" ]]; then rm "${PID_FILE}"; fi
                exit 0
            fi
        done
    else
        echo "ERROR: failed to stop ${pid}"
        exit 1
    fi
else
    echo "ERROR: ${PID_FILE} does not exist"
    exit 1
fi
