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

# Exports the snapshot IDs that the Fluss coordinator has actually published
# as readable. The SQL-client image has the Fluss client jars but only a JRE,
# so it cannot compile a one-off Admin client. ZooKeeper is the coordinator's
# durable source for the same answer: each lake-table node lists its snapshots,
# and a readable_offsets member marks the snapshots getReadableLakeSnapshot may
# return. Requests and replies go through a tiny shared control directory.

set -eo pipefail

CONTROL_DIR=/tmp/fluss-zookeeper-control
ZK_SERVER=${ZK_SERVER:-doris--fluss-zookeeper:2181}
ZK_ROOT=${ZK_ROOT:-/fluss}
ZK_CLI=${ZK_CLI:-/apache-zookeeper-3.9.2-bin/bin/zkCli.sh}
ZK_COMMAND_TIMEOUT_SECONDS=20
TABLES=(lake_log lake_cold lake_types lake_part lake_pk lake_pk_multi
    lake_pk_part lake_pk_cold lake_nested lake_part_int lake_pk_part_int
    big_log big_pk)

mkdir -p "${CONTROL_DIR}"
chmod 0777 "${CONTROL_DIR}"
rm -f "${CONTROL_DIR}/EXPORT_REQUEST" "${CONTROL_DIR}/READY"

if [[ ! -x "${ZK_CLI}" ]]; then
    echo "ERROR: ZooKeeper CLI not found at ${ZK_CLI}" >&2
    exit 1
fi

zk_get() {
    local path="$1"
    local namespaced_path
    local output
    # Fluss' Curator client connects with zookeeper.path.root as its namespace;
    # zkCli connects to the server root, so it must add that namespace itself.
    namespaced_path="${ZK_ROOT%/}${path}"
    output="$(timeout "${ZK_COMMAND_TIMEOUT_SECONDS}" "${ZK_CLI}" \
        -server "${ZK_SERVER}" get "${namespaced_path}" 2>/dev/null)" || return 1
    # Fluss' serdes write one compact JSON object. Keep the last such line so
    # CLI banners, prompts and JVM diagnostics cannot be mistaken for data.
    printf '%s\n' "${output}" | awk '/^\{.*\}$/ { json=$0 } END { if (json != "") print json }'
}

table_id() {
    printf '%s\n' "$1" \
        | sed -n 's/.*"table_id"[[:space:]]*:[[:space:]]*\([0-9][0-9]*\).*/\1/p'
}

latest_readable_snapshot() {
    local json="$1"
    if printf '%s\n' "${json}" | grep -q '"version"[[:space:]]*:[[:space:]]*1'; then
        # Legacy v1 stored one readable snapshot directly in the znode.
        printf '%s\n' "${json}" \
            | sed -n 's/.*"snapshot_id"[[:space:]]*:[[:space:]]*\([0-9][0-9]*\).*/\1/p'
        return
    fi
    # V2 retains a timeline. getReadableLakeSnapshot walks it backwards and
    # picks the last entry carrying readable_offsets; do exactly the same.
    printf '%s\n' "${json}" \
        | sed 's/},[[:space:]]*{/}\
{/g' \
        | grep '"readable_offsets"' \
        | tail -n 1 \
        | sed -n 's/.*"snapshot_id"[[:space:]]*:[[:space:]]*\([0-9][0-9]*\).*/\1/p' \
        || true
}

touch "${CONTROL_DIR}/READY"

while :; do
    if [[ -f "${CONTROL_DIR}/EXPORT_REQUEST" ]]; then
        request_id="$(sed -n '1p' "${CONTROL_DIR}/EXPORT_REQUEST")"
        if [[ ! "${request_id}" =~ ^[0-9]+-[0-9]+-[0-9]+$ ]]; then
            rm -f "${CONTROL_DIR}/EXPORT_REQUEST"
            sleep 1
            continue
        fi
        snapshots_tmp="$(mktemp "${CONTROL_DIR}/SNAPSHOTS.XXXXXX")"
        printf 'generation=%s\n' "${request_id}" >>"${snapshots_tmp}"
        failure=""

        for table in "${TABLES[@]}"; do
            registration="$(zk_get "/metadata/databases/fluss_test/tables/${table}")" \
                || registration=""
            id="$(table_id "${registration}")"
            if [[ -z "${id}" ]]; then
                failure="table metadata is not published for fluss_test.${table}"
                break
            fi

            lake_table="$(zk_get "/tabletservers/tables/${id}/laketable")" \
                || lake_table=""
            snapshot="$(latest_readable_snapshot "${lake_table}")"
            if [[ -z "${snapshot}" ]]; then
                failure="no readable lake snapshot is published for fluss_test.${table} (table id ${id})"
                break
            fi
            printf '%s=%s\n' "${table}" "${snapshot}" >>"${snapshots_tmp}"
        done

        if [[ -z "${failure}" ]]; then
            chmod 0644 "${snapshots_tmp}"
            mv "${snapshots_tmp}" "${CONTROL_DIR}/SNAPSHOTS.${request_id}"
        else
            rm -f "${snapshots_tmp}"
            failed_tmp="$(mktemp "${CONTROL_DIR}/EXPORT_FAILED.XXXXXX")"
            printf '%s\n' "${failure}" >"${failed_tmp}"
            chmod 0644 "${failed_tmp}"
            mv "${failed_tmp}" "${CONTROL_DIR}/EXPORT_FAILED.${request_id}"
        fi
        # A requester may replace a timed-out generation while this export is
        # still running. Remove only the request we actually answered.
        if [[ "$(sed -n '1p' "${CONTROL_DIR}/EXPORT_REQUEST" 2>/dev/null || true)" == "${request_id}" ]]; then
            rm -f "${CONTROL_DIR}/EXPORT_REQUEST"
        fi
    fi
    sleep 1
done
