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
# Primary-key fixtures whose buckets must have been snapshotted before the
# environment counts as ready. See wait_for_kv_snapshots.
SNAPSHOT_TABLES=(pk_basic pk_types pk_part)
SNAPSHOT_WAIT_SECONDS=120

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

# Waits until every primary-key fixture has a kv snapshot on disk.
#
# Doris BE reads those files directly, from the host, at the path this container
# wrote them to -- a bind mount at the same absolute path on both sides. Nothing
# but an end-to-end run covers that, and without this wait it would only be
# covered by luck: with no snapshot a primary-key table is read by replaying its
# whole change log, which is equally correct and takes a different code path
# entirely. Baking the snapshot into the environment makes every later suite
# exercise the file-reading path instead of racing the ten-second interval.
#
# What it proves is that a snapshot was taken, not that the coordinator has
# committed it -- completion is registered in ZooKeeper, and the directory here
# is created before the upload. That gap is milliseconds, and losing it costs a
# suite nothing: the read falls back to the change log and still returns the
# right rows. Never seeing a snapshot at all is the real problem, and that is
# what the timeout reports.
wait_for_kv_snapshots() {
    local waited=0
    local table
    local missing
    while :; do
        missing=""
        for table in "${SNAPSHOT_TABLES[@]}"; do
            # Two shapes, because a partition sits between the table and the
            # bucket ({partitionName}-p{partitionId}); the trailing /* requires
            # the snapshot directory to hold a file, not merely to exist.
            #   {remote.data.dir}/kv/{db}/{table}-{tableId}/{bucket}/snap-{id}/
            #   {remote.data.dir}/kv/{db}/{table}-{tableId}/{partition}/{bucket}/snap-{id}/
            local root="${FLUSS_REMOTE_DATA_DIR}/kv/fluss_test/${table}-"
            if ! compgen -G "${root}*/*/snap-*/*" >/dev/null 2>&1 \
                && ! compgen -G "${root}*/*/*/snap-*/*" >/dev/null 2>&1; then
                missing="${missing} ${table}"
            fi
        done
        if [[ -z "${missing}" ]]; then
            echo "Kv snapshots present for:${SNAPSHOT_TABLES[*]}"
            return 0
        fi
        if ((waited >= SNAPSHOT_WAIT_SECONDS)); then
            echo "ERROR: no kv snapshot after ${SNAPSHOT_WAIT_SECONDS}s for:${missing}" >&2
            echo "ERROR: expected under ${FLUSS_REMOTE_DATA_DIR}/kv/fluss_test/" >&2
            ls -R "${FLUSS_REMOTE_DATA_DIR}/kv" >&2 2>/dev/null || true
            return 1
        fi
        sleep 5
        waited=$((waited + 5))
    done
}

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
        echo "Fluss init SQL finished; waiting for kv snapshots"
        if ! wait_for_kv_snapshots; then
            exit 1
        fi
        touch "${MARKER_DIR}/SUCCESS"
        echo "Fluss environment ready"
        exec tail -f /dev/null
    fi
    echo "Fluss init SQL failed on attempt ${attempt}" >&2
    sleep 10
done

echo "ERROR: fluss init SQL failed after ${ATTEMPTS} attempts" >&2
exit 1
