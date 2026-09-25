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
#   2. the tiering service is stopped once it has committed all of them and the
#      coordinator has published those commits as readable snapshots;
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
LAKE_READABLE_COUNTS_TEMPLATE=/opt/fluss-sql/lake-readable-counts.sql
JOBMANAGER_PORT=8081
WAIT_SECONDS=180
SQL_TIMEOUT_SECONDS=900
ATTEMPTS=3
# One wall-clock budget covers startup, all SQL probes and every retry. Stage
# limits below keep one failed probe from consuming it all, but never extend it.
INIT_TIMEOUT_SECONDS=3000
INIT_DEADLINE_EPOCH=$(($(date +%s) + INIT_TIMEOUT_SECONDS))
# Primary-key fixtures whose buckets must have been snapshotted before the
# environment counts as ready. See wait_for_kv_snapshots.
#
# pk_empty is deliberately absent: nothing was ever written to it, so no bucket
# will ever be snapshotted and waiting would hang on the state it exists to have.
SNAPSHOT_TABLES=(pk_basic pk_types pk_part pk_nested lake_pk lake_pk_multi lake_pk_part
    lake_pk_cold lake_pk_part_int big_pk)
SNAPSHOT_WAIT_SECONDS=180
MINIO_CONTROL_DIR=/tmp/fluss-minio-control
MINIO_CLEANUP_WAIT_SECONDS=120
ZOOKEEPER_CONTROL_DIR=/tmp/fluss-zookeeper-control
ZOOKEEPER_EXPORT_WAIT_SECONDS=120
TIERING_CANCEL_WAIT_SECONDS=120

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
    "lake_nested=1"
    "lake_part_int=3"
    "lake_pk_part_int=4"
    "big_log=100000"
    "big_pk=100000"
)
# The two large fixtures put 200000 rows through the tiering service, which is
# most of what this wait is now for; the small ones commit within a round.
LAKE_TIERING_WAIT_SECONDS=900
LAKE_READABLE_WAIT_SECONDS=900
TIERING_JAR_GLOB='/opt/flink/opt/fluss-flink-tiering-*.jar'
FLINK_BIN=/opt/flink/bin/flink

rm -rf "${MARKER_DIR}"
mkdir -p "${MARKER_DIR}"

stage_deadline() {
    local candidate
    candidate=$(($(date +%s) + $1))
    if ((candidate > INIT_DEADLINE_EPOCH)); then
        candidate=${INIT_DEADLINE_EPOCH}
    fi
    printf '%s\n' "${candidate}"
}

remaining_until() {
    local remaining
    remaining=$(($1 - $(date +%s)))
    if ((remaining <= 0)); then
        return 1
    fi
    printf '%s\n' "${remaining}"
}

bounded_command_timeout() {
    local deadline="$1"
    local remaining
    remaining="$(remaining_until "${deadline}")" || return 1
    if ((remaining > SQL_TIMEOUT_SECONDS)); then
        remaining=${SQL_TIMEOUT_SECONDS}
    fi
    printf '%s\n' "${remaining}"
}

sleep_before() {
    local seconds="$1"
    local deadline="$2"
    local remaining
    remaining="$(remaining_until "${deadline}")" || return 1
    if ((seconds > remaining)); then
        seconds=${remaining}
    fi
    sleep "${seconds}"
}

# shellcheck source=fluss-job-control.sh
source /opt/fluss-scripts/fluss-job-control.sh

run_sql_probe() {
    local sql="$1"
    local log="$2"
    local deadline="$3"
    local command_timeout
    command_timeout="$(bounded_command_timeout "${deadline}")" || return 124
    timeout "${command_timeout}" /opt/flink/bin/sql-client.sh -f "${sql}" >"${log}" 2>&1
}

wait_for_jobmanager() {
    local deadline
    deadline="$(stage_deadline "${WAIT_SECONDS}")"
    while ! (exec 3<>"/dev/tcp/${FLUSS_JOBMANAGER_HOST}/${JOBMANAGER_PORT}") >/dev/null 2>&1; do
        if ! remaining_until "${deadline}" >/dev/null; then
            echo "ERROR: jobmanager ${FLUSS_JOBMANAGER_HOST}:${JOBMANAGER_PORT} did not become reachable before its deadline" >&2
            return 1
        fi
        sleep_before 2 "${deadline}" || return 1
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
sed -e "s|__FLUSS_PAIMON_WAREHOUSE__|${FLUSS_PAIMON_WAREHOUSE}|g" \
    -e "s|__FLUSS_LAKE_S3_ENDPOINT__|${FLUSS_LAKE_S3_ENDPOINT}|g" \
    -e "s|__FLUSS_LAKE_S3_ACCESS_KEY__|${FLUSS_LAKE_S3_ACCESS_KEY}|g" \
    -e "s|__FLUSS_LAKE_S3_SECRET_KEY__|${FLUSS_LAKE_S3_SECRET_KEY}|g" \
    "${LAKE_COUNTS_TEMPLATE}" >"${MARKER_DIR}/lake-row-counts.sql"
sed -e "s|__FLUSS_PAIMON_WAREHOUSE__|${FLUSS_PAIMON_WAREHOUSE}|g" \
    -e "s|__FLUSS_LAKE_S3_ENDPOINT__|${FLUSS_LAKE_S3_ENDPOINT}|g" \
    -e "s|__FLUSS_LAKE_S3_ACCESS_KEY__|${FLUSS_LAKE_S3_ACCESS_KEY}|g" \
    -e "s|__FLUSS_LAKE_S3_SECRET_KEY__|${FLUSS_LAKE_S3_SECRET_KEY}|g" \
    "${LAKE_READABLE_COUNTS_TEMPLATE}" >"${MARKER_DIR}/lake-readable-counts-header.sql"

# Submits the fluss -> paimon tiering service. Detached, because it is a
# streaming job that has to keep running while init.sql writes.
start_tiering_job() {
    local jar command_timeout
    # shellcheck disable=SC2086
    jar="$(ls ${TIERING_JAR_GLOB} 2>/dev/null | head -n 1)"
    if [[ -z "${jar}" ]]; then
        echo "ERROR: no tiering jar matching ${TIERING_JAR_GLOB}" >&2
        return 1
    fi
    echo "Submitting tiering service from ${jar}"
    # The lake settings repeat the ones the fluss servers carry. They have to:
    # the servers use them to create the paimon table, this job uses them to
    # write it, and neither reads the other's configuration. The credentials in
    # particular cannot be picked up from the servers even in principle -- fluss
    # strips them out of anything it hands to a client.
    command_timeout="$(bounded_command_timeout "${INIT_DEADLINE_EPOCH}")" || return 1
    timeout "${command_timeout}" "${FLINK_BIN}" run -d "${jar}" \
        --fluss.bootstrap.servers "${FLUSS_BOOTSTRAP_SERVERS}" \
        --datalake.format paimon \
        --datalake.paimon.metastore filesystem \
        --datalake.paimon.warehouse "${FLUSS_PAIMON_WAREHOUSE}" \
        --datalake.paimon.s3.endpoint "${FLUSS_LAKE_S3_ENDPOINT}" \
        --datalake.paimon.s3.path.style.access true \
        --datalake.paimon.s3.access-key "${FLUSS_LAKE_S3_ACCESS_KEY}" \
        --datalake.paimon.s3.secret-key "${FLUSS_LAKE_S3_SECRET_KEY}"
}

# Waits until paimon holds every row init.sql wrote to a lake table, by counting
# them in the warehouse itself (sql/lake-row-counts.sql) rather than by checking
# that some snapshot exists. Tiering commits what it has consumed so far, so a
# snapshot proves only that it started; freezing a fixture half-tiered would
# leave a table meant to be lake-only with a log tail on some runs and not
# others.
wait_for_lake_rows() {
    local deadline
    deadline="$(stage_deadline "${LAKE_TIERING_WAIT_SECONDS}")"
    local log="${MARKER_DIR}/lake-row-counts.log"
    local expected missing
    while :; do
        missing=""
        if run_sql_probe "${MARKER_DIR}/lake-row-counts.sql" "${log}" "${deadline}"; then
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
        if ! remaining_until "${deadline}" >/dev/null; then
            echo "ERROR: tiering did not reach the expected row counts before its deadline:${missing}" >&2
            echo "ERROR: last count output follows" >&2
            cat "${log}" >&2 || true
            return 1
        fi
        sleep_before 10 "${deadline}" || return 1
    done
}

# Asks the ZooKeeper sidecar for exactly the snapshot IDs the coordinator has
# published as readable. Replies are generation-qualified: an export that
# finishes after its caller timed out cannot be mistaken for a later request.
request_readable_snapshot_ids() {
    local parent_deadline="$1"
    local request_id
    request_id="$(date +%s)-$$-${RANDOM}"
    local snapshot_file="${ZOOKEEPER_CONTROL_DIR}/SNAPSHOTS.${request_id}"
    local failure_file="${ZOOKEEPER_CONTROL_DIR}/EXPORT_FAILED.${request_id}"
    local request_tmp request_deadline candidate

    request_deadline="${parent_deadline}"
    candidate=$(($(date +%s) + ZOOKEEPER_EXPORT_WAIT_SECONDS))
    if ((candidate < request_deadline)); then
        request_deadline=${candidate}
    fi

    rm -f "${snapshot_file}" "${failure_file}"
    request_tmp="$(mktemp "${ZOOKEEPER_CONTROL_DIR}/EXPORT_REQUEST.XXXXXX")"
    printf '%s\n' "${request_id}" >"${request_tmp}"
    mv "${request_tmp}" "${ZOOKEEPER_CONTROL_DIR}/EXPORT_REQUEST"

    while :; do
        if [[ -f "${snapshot_file}" ]]; then
            if grep -qF "generation=${request_id}" "${snapshot_file}"; then
                printf '%s\n' "${snapshot_file}"
                return 0
            fi
            echo "ERROR: ZooKeeper snapshot export returned the wrong generation" \
                >"${MARKER_DIR}/lake-readable-export.log"
            return 1
        fi
        if [[ -f "${failure_file}" ]]; then
            cp "${failure_file}" "${MARKER_DIR}/lake-readable-export.log"
            return 1
        fi
        if ! remaining_until "${request_deadline}" >/dev/null; then
            echo "ZooKeeper readable-snapshot export timed out" \
                >"${MARKER_DIR}/lake-readable-export.log"
            return 1
        fi
        sleep_before 1 "${request_deadline}" || return 1
    done
}

build_readable_count_sql() {
    local snapshots="$1"
    local sql="${MARKER_DIR}/lake-readable-counts.sql"
    local expected table snapshot first=1

    cp "${MARKER_DIR}/lake-readable-counts-header.sql" "${sql}"
    for expected in "${LAKE_EXPECTED_ROWS[@]}"; do
        table="${expected%%=*}"
        snapshot="$(sed -n "s/^${table}=//p" "${snapshots}")"
        if [[ ! "${snapshot}" =~ ^[0-9]+$ ]]; then
            echo "ERROR: no readable snapshot id exported for fluss_test.${table}" >&2
            return 1
        fi
        if ((first == 0)); then
            printf 'UNION ALL\n' >>"${sql}"
        fi
        printf "SELECT CONCAT('READABLE:%s=', CAST(COUNT(*) AS STRING)) AS marker FROM \`%s\` /*+ OPTIONS('scan.snapshot-id'='%s') */\n" \
            "${table}" "${table}" "${snapshot}" >>"${sql}"
        first=0
    done
    printf ';\n' >>"${sql}"
}

# A Paimon commit is visible in object storage before the tiering committer
# publishes it to Fluss. Query each coordinator-published snapshot ID directly:
# an older readable snapshot plus a log tail cannot satisfy this count, while a
# latest-but-not-readable Paimon snapshot is never selected in the first place.
wait_for_readable_lake_snapshots() {
    local deadline snapshots
    deadline="$(stage_deadline "${LAKE_READABLE_WAIT_SECONDS}")"
    local sql_log="${MARKER_DIR}/lake-readable-counts.log"
    local expected missing
    while :; do
        missing=""
        if snapshots="$(request_readable_snapshot_ids "${deadline}")" \
            && build_readable_count_sql "${snapshots}" \
            && run_sql_probe "${MARKER_DIR}/lake-readable-counts.sql" "${sql_log}" "${deadline}"; then
            for expected in "${LAKE_EXPECTED_ROWS[@]}"; do
                grep -qF "READABLE:${expected}" "${sql_log}" || missing="${missing} ${expected}"
            done
        else
            missing=" (exact readable-snapshot query failed)"
        fi
        if [[ -z "${missing}" ]]; then
            echo "Exact readable lake snapshots contain:${LAKE_EXPECTED_ROWS[*]}"
            return 0
        fi
        if ! remaining_until "${deadline}" >/dev/null; then
            echo "ERROR: Fluss did not publish every expected row in a readable lake snapshot before its deadline:${missing}" >&2
            echo "ERROR: last readable query and snapshot-export diagnostics follow" >&2
            cat "${sql_log}" >&2 2>/dev/null || true
            cat "${MARKER_DIR}/lake-readable-export.log" >&2 2>/dev/null || true
            return 1
        fi
        sleep_before 10 "${deadline}" || return 1
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
    local deadline
    deadline="$(stage_deadline "${SNAPSHOT_WAIT_SECONDS}")"
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
        if ! remaining_until "${deadline}" >/dev/null; then
            echo "ERROR: no kv snapshot before the snapshot deadline for:${missing}" >&2
            echo "ERROR: expected under ${FLUSS_REMOTE_DATA_DIR}/kv/fluss_test/" >&2
            ls -R "${FLUSS_REMOTE_DATA_DIR}/kv" >&2 2>/dev/null || true
            return 1
        fi
        sleep_before 5 "${deadline}" || return 1
    done
}

run_sql() {
    local sql="$1"
    local log="$2"
    local status=0

    # Timeout, because a write that the servers keep rejecting is retried by the
    # fluss client practically forever: without it the container just hangs.
    local command_timeout
    command_timeout="$(bounded_command_timeout "${INIT_DEADLINE_EPOCH}")" || return 124
    timeout "${command_timeout}" /opt/flink/bin/sql-client.sh -f "${sql}" 2>&1 | tee "${log}"
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
drop_lake_warehouse() {
    case "${FLUSS_PAIMON_WAREHOUSE}" in
        s3://*)
            local target="${FLUSS_PAIMON_WAREHOUSE#s3://}/fluss_test.db"
            local deadline
            deadline="$(stage_deadline "${MINIO_CLEANUP_WAIT_SECONDS}")"
            rm -f "${MINIO_CONTROL_DIR}/CLEANUP_DONE" \
                "${MINIO_CONTROL_DIR}/CLEANUP_FAILED"
            printf '%s\n' "${target}" >"${MINIO_CONTROL_DIR}/CLEANUP_REQUEST.tmp"
            mv "${MINIO_CONTROL_DIR}/CLEANUP_REQUEST.tmp" \
                "${MINIO_CONTROL_DIR}/CLEANUP_REQUEST"
            while [[ ! -f "${MINIO_CONTROL_DIR}/CLEANUP_DONE" ]]; do
                if [[ -f "${MINIO_CONTROL_DIR}/CLEANUP_FAILED" ]]; then
                    cat "${MINIO_CONTROL_DIR}/CLEANUP_FAILED" >&2
                    return 1
                fi
                if ! remaining_until "${deadline}" >/dev/null; then
                    echo "ERROR: minio did not clean s3://${target} before its cleanup deadline" >&2
                    return 1
                fi
                sleep_before 1 "${deadline}" || return 1
            done
            ;;
        file://*)
            # Kept for the documented local-directory debugging switch.
            local warehouse="${FLUSS_PAIMON_WAREHOUSE#file://}"
            if [[ -d "${warehouse}/fluss_test.db" ]]; then
                echo "Removing the paimon side of the previous attempt: ${warehouse}/fluss_test.db"
                rm -rf "${warehouse}/fluss_test.db"
            fi
            ;;
        *)
            echo "ERROR: unsupported paimon warehouse for retry cleanup: ${FLUSS_PAIMON_WAREHOUSE}" >&2
            return 1
            ;;
    esac
}

run_attempt() {
    local attempt="$1"

    # A previous attempt's tiering job would still be consuming the database
    # init.sql is about to drop.
    cancel_all_jobs || return 1
    drop_lake_warehouse || return 1
    start_tiering_job || return 1

    run_sql "${MARKER_DIR}/init.sql" "${MARKER_DIR}/init-attempt-${attempt}.log" || return 1

    echo "Fluss init SQL finished; waiting for the tiering service to commit"
    wait_for_lake_rows || return 1
    wait_for_readable_lake_snapshots || return 1

    # Everything meant for the lake is both committed and published as readable.
    # Stop tiering BEFORE writing the tail, so the tail stays in the fluss log.
    cancel_all_jobs || return 1

    run_sql "${MARKER_DIR}/init-lake-tail.sql" \
        "${MARKER_DIR}/init-lake-tail-attempt-${attempt}.log" || return 1
    return 0
}

# init.sql drops and recreates its database up front, so a retry always starts
# from the same state. Retries exist because the tablet server may still be
# registering with the coordinator when the ports are already open.
for ((attempt = 1; attempt <= ATTEMPTS; attempt++)); do
    if ! remaining_until "${INIT_DEADLINE_EPOCH}" >/dev/null; then
        echo "ERROR: fluss fixture initialization exhausted its ${INIT_TIMEOUT_SECONDS}s wall-clock budget" >&2
        exit 1
    fi
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
    if ((attempt < ATTEMPTS)); then
        sleep_before 10 "${INIT_DEADLINE_EPOCH}" || break
    fi
done

echo "ERROR: fluss init SQL failed after ${ATTEMPTS} attempts or the ${INIT_TIMEOUT_SECONDS}s wall-clock budget" >&2
exit 1
