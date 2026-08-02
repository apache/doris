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

################################################################
# Entrypoint of the fluss sql-client container: creates the regression
# fixtures through the Flink SQL client, then idles so that the compose
# healthcheck has something to gate on. The SUCCESS marker is written only
# after every statement succeeded.
################################################################

set -eo pipefail

MARKER_DIR=/tmp/fluss-init
SQL_TEMPLATE=/opt/fluss-sql/init.sql
JOBMANAGER_PORT=8081
WAIT_SECONDS=180
SQL_TIMEOUT_SECONDS=900
ATTEMPTS=3

rm -rf "${MARKER_DIR}"
mkdir -p "${MARKER_DIR}"

wait_for_jobmanager() {
    local waited=0
    while ! (exec 3<>"/dev/tcp/${FLUSS_JOBMANAGER_HOST}/${JOBMANAGER_PORT}") >/dev/null 2>&1; do
        if ((waited >= WAIT_SECONDS)); then
            echo "ERROR: jobmanager ${FLUSS_JOBMANAGER_HOST}:${JOBMANAGER_PORT} not reachable after ${WAIT_SECONDS}s" >&2
            return 1
        fi
        sleep 2
        waited=$((waited + 2))
    done
}

wait_for_jobmanager

# The bootstrap address is only known at compose time, and the Flink SQL client
# does not expand environment variables inside SQL files.
sed "s|__FLUSS_BOOTSTRAP_SERVERS__|${FLUSS_BOOTSTRAP_SERVERS}|g" \
    "${SQL_TEMPLATE}" >"${MARKER_DIR}/init.sql"

run_attempt() {
    local log="$1"
    local status=0

    # Timeout, because a write that the servers keep rejecting is retried by the
    # fluss client practically forever: without it the container just hangs.
    timeout "${SQL_TIMEOUT_SECONDS}" /opt/flink/bin/sql-client.sh -f "${MARKER_DIR}/init.sql" 2>&1 | tee "${log}"
    status="${PIPESTATUS[0]}"
    if ((status != 0)); then
        return "${status}"
    fi
    # The SQL client stops at the first failing statement but still exits 0, so
    # the only honest completion signal is its own output.
    if grep -q '\[ERROR\]' "${log}"; then
        return 1
    fi
    return 0
}

# init.sql drops and recreates its database up front, so a retry always starts
# from the same state. Retries exist because the tablet server may still be
# registering with the coordinator when the ports are already open.
for ((attempt = 1; attempt <= ATTEMPTS; attempt++)); do
    echo "Running fluss init SQL (attempt ${attempt}/${ATTEMPTS})"
    if run_attempt "${MARKER_DIR}/init-attempt-${attempt}.log"; then
        touch "${MARKER_DIR}/SUCCESS"
        echo "Fluss init SQL finished"
        exec tail -f /dev/null
    fi
    echo "Fluss init SQL failed on attempt ${attempt}" >&2
    sleep 10
done

echo "ERROR: fluss init SQL failed after ${ATTEMPTS} attempts" >&2
exit 1
