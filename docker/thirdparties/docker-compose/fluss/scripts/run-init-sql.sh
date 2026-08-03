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
#
# The lake fixtures are built in three steps, because what makes them useful is
# WHERE each row ends up:
#   1. init.sql writes the rows that belong in paimon, with the tiering service
#      running;
#   2. the tiering service is stopped once it has committed all of them;
#   3. init-lake-tail.sql writes the rows that must stay in the fluss log.
# Stopping the service is the point: it freezes the division between the two
# halves. Left running, it would keep consuming the tail, and a suite asserting
# that a table is read as "lake plus log" would slowly turn into one asserting
# "lake only" -- passing or failing by how long the environment had been up.
################################################################

set -eo pipefail

MARKER_DIR=/tmp/fluss-init
SQL_TEMPLATE=/opt/fluss-sql/init.sql
LAKE_TAIL_TEMPLATE=/opt/fluss-sql/init-lake-tail.sql
LAKE_COUNTS_TEMPLATE=/opt/fluss-sql/lake-row-counts.sql
JOBMANAGER_PORT=8081
WAIT_SECONDS=180
SQL_TIMEOUT_SECONDS=900
ATTEMPTS=3
# Primary-key fixtures whose buckets must have been snapshotted before the
# environment counts as ready. See wait_for_kv_snapshots.
SNAPSHOT_TABLES=(pk_basic pk_types pk_part lake_pk lake_pk_multi lake_pk_part lake_pk_cold)
SNAPSHOT_WAIT_SECONDS=120

# What each lake fixture must hold in paimon before the tail is written -- the
# row counts init.sql writes, merged where the table has a primary key. Keep in
# step with init.sql; a mismatch here stalls the environment instead of
# corrupting a fixture, and the timeout prints both sides.
LAKE_EXPECTED_ROWS=(
    "lake_log=4"
    "lake_cold=3"
    "lake_types=1"
    "lake_part=3"
    "lake_pk=3"
    "lake_pk_multi=9"
    "lake_pk_part=4"
    "lake_pk_cold=3"
)
LAKE_TIERING_WAIT_SECONDS=300
TIERING_JAR_GLOB='/opt/flink/opt/fluss-flink-tiering-*.jar'
FLINK_BIN=/opt/flink/bin/flink

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

# The bootstrap address and the warehouse path are only known at compose time,
# and the Flink SQL client does not expand environment variables inside SQL
# files.
sed "s|__FLUSS_BOOTSTRAP_SERVERS__|${FLUSS_BOOTSTRAP_SERVERS}|g" \
    "${SQL_TEMPLATE}" >"${MARKER_DIR}/init.sql"
sed "s|__FLUSS_BOOTSTRAP_SERVERS__|${FLUSS_BOOTSTRAP_SERVERS}|g" \
    "${LAKE_TAIL_TEMPLATE}" >"${MARKER_DIR}/init-lake-tail.sql"
sed "s|__FLUSS_PAIMON_WAREHOUSE__|${FLUSS_PAIMON_WAREHOUSE}|g" \
    "${LAKE_COUNTS_TEMPLATE}" >"${MARKER_DIR}/lake-row-counts.sql"

# Cancels every job on the cluster. This cluster runs nothing but the tiering
# service, and a retry must not leave the previous attempt's job consuming the
# database the next attempt is about to drop and recreate.
cancel_all_jobs() {
    local ids
    ids="$("${FLINK_BIN}" list -r 2>/dev/null | grep -oE '[0-9a-f]{32}' || true)"
    local id
    for id in ${ids}; do
        echo "Cancelling flink job ${id}"
        "${FLINK_BIN}" cancel "${id}" >/dev/null 2>&1 || true
    done
}

# Submits the fluss -> paimon tiering service. Detached, because it is a
# streaming job that has to keep running while init.sql writes.
start_tiering_job() {
    local jar
    # shellcheck disable=SC2086
    jar="$(ls ${TIERING_JAR_GLOB} 2>/dev/null | head -n 1)"
    if [[ -z "${jar}" ]]; then
        echo "ERROR: no tiering jar matching ${TIERING_JAR_GLOB}" >&2
        return 1
    fi
    echo "Submitting tiering service from ${jar}"
    # The lake settings repeat the ones the fluss servers carry. They have to:
    # the servers use them to create the paimon table, this job uses them to
    # write it, and neither reads the other's configuration.
    "${FLINK_BIN}" run -d "${jar}" \
        --fluss.bootstrap.servers "${FLUSS_BOOTSTRAP_SERVERS}" \
        --datalake.format paimon \
        --datalake.paimon.metastore filesystem \
        --datalake.paimon.warehouse "${FLUSS_PAIMON_WAREHOUSE}"
}

# Waits until paimon holds every row init.sql wrote to a lake table, by counting
# them in the warehouse itself (sql/lake-row-counts.sql) rather than by checking
# that some snapshot exists. Tiering commits what it has consumed so far, so a
# snapshot proves only that it started; freezing a fixture half-tiered would
# leave a table meant to be lake-only with a log tail on some runs and not
# others.
wait_for_lake_rows() {
    local waited=0
    local log="${MARKER_DIR}/lake-row-counts.log"
    local expected missing
    while :; do
        missing=""
        if timeout "${SQL_TIMEOUT_SECONDS}" /opt/flink/bin/sql-client.sh \
            -f "${MARKER_DIR}/lake-row-counts.sql" >"${log}" 2>&1; then
            for expected in "${LAKE_EXPECTED_ROWS[@]}"; do
                grep -qF "LAKEROWS:${expected}" "${log}" || missing="${missing} ${expected}"
            done
            if [[ -z "${missing}" ]]; then
                echo "Tiered to paimon:${LAKE_EXPECTED_ROWS[*]}"
                return 0
            fi
        else
            missing=" (count query failed)"
        fi
        if ((waited >= LAKE_TIERING_WAIT_SECONDS)); then
            echo "ERROR: tiering did not reach the expected row counts after ${LAKE_TIERING_WAIT_SECONDS}s:${missing}" >&2
            echo "ERROR: last count output follows" >&2
            cat "${log}" >&2 || true
            return 1
        fi
        sleep 10
        waited=$((waited + 10))
    done
}

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

run_sql() {
    local sql="$1"
    local log="$2"
    local status=0

    # Timeout, because a write that the servers keep rejecting is retried by the
    # fluss client practically forever: without it the container just hangs.
    timeout "${SQL_TIMEOUT_SECONDS}" /opt/flink/bin/sql-client.sh -f "${sql}" 2>&1 | tee "${log}"
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

# Removes the paimon side of the lake tables, which is what makes a retry
# possible at all. init.sql drops its fluss database, but that leaves the paimon
# tables where they are -- and fluss refuses to create a lake table whose paimon
# table already exists and holds rows. Without this, an attempt that failed after
# creating one lake table dooms every attempt after it, and the environment comes
# up "failed after 3 attempts" with the real cause three hundred lines up.
#
# Deleting the directory IS dropping the database here: the warehouse is a
# filesystem catalog, mounted writable for exactly this kind of work, and the
# host script empties the same directory before the containers start.
drop_lake_warehouse() {
    local warehouse="${FLUSS_PAIMON_WAREHOUSE#file://}"
    if [[ -d "${warehouse}/fluss_test.db" ]]; then
        echo "Removing the paimon side of the previous attempt: ${warehouse}/fluss_test.db"
        rm -rf "${warehouse}/fluss_test.db"
    fi
}

run_attempt() {
    local attempt="$1"

    # A previous attempt's tiering job would still be consuming the database
    # init.sql is about to drop.
    cancel_all_jobs
    drop_lake_warehouse
    start_tiering_job || return 1

    run_sql "${MARKER_DIR}/init.sql" "${MARKER_DIR}/init-attempt-${attempt}.log" || return 1

    echo "Fluss init SQL finished; waiting for the tiering service to commit"
    wait_for_lake_rows || return 1

    # Everything meant for the lake is in the lake. Stop tiering BEFORE writing
    # the tail, so the tail stays in the fluss log for good.
    cancel_all_jobs

    run_sql "${MARKER_DIR}/init-lake-tail.sql" \
        "${MARKER_DIR}/init-lake-tail-attempt-${attempt}.log" || return 1
    return 0
}

# init.sql drops and recreates its database up front, so a retry always starts
# from the same state. Retries exist because the tablet server may still be
# registering with the coordinator when the ports are already open.
for ((attempt = 1; attempt <= ATTEMPTS; attempt++)); do
    echo "Running fluss init SQL (attempt ${attempt}/${ATTEMPTS})"
    if run_attempt "${attempt}"; then
        echo "Fluss fixtures written; waiting for kv snapshots"
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
