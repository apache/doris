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
#
# Smoke test for the compose topologies: brings one up under its own project
# name, subnet and host ports (so a cluster you are working with stays
# untouched), exercises what the topology is for, tears it down.
#
#   ./smoke-test.sh multi-node [image:tag]
#   ./smoke-test.sh cloud      [image:tag]
#
# Everything runs through the client service, so the host needs only docker.

set -euo pipefail

TOPOLOGY=${1:?usage: smoke-test.sh <multi-node|cloud> [image:tag]}
export DORIS_IMAGE=${2:-${DORIS_IMAGE:-apache/doris:all-in-one-4.1.3}}
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WAIT_SECONDS=${WAIT_SECONDS:-600}

case "${TOPOLOGY}" in
    multi-node) export SUBNET=172.31.91 ;;
    cloud)      export SUBNET=172.31.92 ;;
    *) echo "unknown topology ${TOPOLOGY}, expected multi-node|cloud" >&2; exit 1 ;;
esac
export COMPOSE_PROJECT_NAME="doris-smoke-${TOPOLOGY}"
export FE_PORT=29030 FE2_PORT=29031 FE3_PORT=29032 \
       FE_HTTP_PORT=28030 FE2_HTTP_PORT=28031 FE3_HTTP_PORT=28032 \
       BE_HTTP_PORT=28040 MS_PORT=25000 MINIO_PORT=29000 MINIO_CONSOLE_PORT=29001

compose() { docker compose -f "${HERE}/${TOPOLOGY}.yml" --profile ha "$@"; }
cleanup() {
    local rc=$?
    if ((rc != 0)); then
        echo "--- compose state ---" >&2
        compose ps >&2 || true
        echo "--- recent logs ---" >&2
        compose logs --tail 40 --no-log-prefix fe-1 be-1 2>&1 | tail -80 >&2 || true
    fi
    compose down --remove-orphans >/dev/null 2>&1 || true
    exit "${rc}"
}
trap cleanup EXIT

step() { printf '\n== %s\n' "$*"; }
fail() { echo "FAIL: $*" >&2; exit 1; }
# q <fe host> <sql>: through the client container, batch output, no header
q() { compose exec -T client mysql -uroot -h"$1" -P9030 -N --batch --connect-timeout=3 -e "$2"; }
# column <fe host> <show statement> <column name> [<filter column> <value>]
column() {
    compose exec -T client mysql -uroot -h"$1" -P9030 --batch --connect-timeout=3 -e "$2" \
        | awk -F'\t' -v want="$3" -v fc="${4:-}" -v fv="${5:-}" '
            NR == 1 { for (i = 1; i <= NF; i++) col[$i] = i; next }
            fc == "" || $col[fc] == fv { print $col[want] }'
}
master_fe() { column "$1" 'show frontends' Host IsMaster true; }
wait_for() {   # wait_for <seconds> <description> <command...>
    local timeout=$1 what=$2; shift 2
    local deadline=$((SECONDS + timeout))
    while ((SECONDS < deadline)); do
        if "$@" >/dev/null 2>&1; then return 0; fi
        sleep 2
    done
    fail "timed out waiting for ${what}"
}
healthy() { [[ "$(docker inspect -f '{{.State.Health.Status}}' "${COMPOSE_PROJECT_NAME}-$1-1" 2>/dev/null)" == healthy ]]; }
stream_load() {   # stream_load <fe host> <db.table> <csv lines>
    local target=${2/./\/}
    compose exec -T client bash -c "printf '$3' | curl -sS --location-trusted -u root: \
        -H 'column_separator:,' -H 'Expect:100-continue' -T - http://$1:8030/api/${target}/_stream_load" \
        | grep -q '"Status": *"Success"'
}

step "starting ${TOPOLOGY} from ${DORIS_IMAGE} (project ${COMPOSE_PROJECT_NAME}, subnet ${SUBNET}.0/24)"
compose up --wait --wait-timeout "${WAIT_SECONDS}" >/dev/null
echo "  up after ${SECONDS}s"
compose ps --format 'table {{.Service}}\t{{.Status}}' | sed 's/^/  /'

step "three FEs and three BEs alive"
[[ "$(column fe-1 'show frontends' Alive | grep -c true)" == 3 ]] || fail "expected 3 live FEs"
[[ "$(column fe-1 'show backends' Alive | grep -c true)" == 3 ]] || fail "expected 3 live BEs"
echo "  master is $(master_fe fe-1)"

step "create, insert, read back"
q fe-1 "create database if not exists smoke"
q fe-1 "drop table if exists smoke.t"
q fe-1 "create table smoke.t (k int, v varchar(32)) duplicate key(k) distributed by hash(k) buckets 4"
q fe-1 "insert into smoke.t values (1,'a'),(2,'b'),(3,'c')"
[[ "$(q fe-1 'select count(*) from smoke.t')" == 3 ]] || fail "insert/select mismatch"
echo "  3 rows"

if [[ "${TOPOLOGY}" == multi-node ]]; then
    step "three replicas, one per backend"
    q fe-1 "show create table smoke.t" | grep -q 'tag.location.default: 3' || fail "table is not 3-replica"
    per_be=$(q fe-1 "show tablets from smoke.t" | awk -F'\t' '{n[$3]++} END {print length(n)}')
    [[ "${per_be}" == 3 ]] || fail "replicas spread over ${per_be} backends, expected 3"
    echo "  12 replicas over 3 backends"
else
    step "two compute groups, default storage vault on MinIO"
    [[ "$(column fe-1 'show compute groups' BackendNum Name cg_a)" == 2 ]] || fail "cg_a should have 2 BEs"
    [[ "$(column fe-1 'show compute groups' BackendNum Name cg_b)" == 1 ]] || fail "cg_b should have 1 BE"
    [[ "$(column fe-1 'show storage vaults' IsDefault Name built_in_storage_vault)" == true ]] \
        || fail "built_in_storage_vault is not the default"
    [[ "$(q fe-1 'use @cg_b; select count(*) from smoke.t')" == 3 ]] || fail "read via cg_b failed"
    echo "  cg_a=2 cg_b=1, reads work from cg_b"
    objects=$(compose exec -T minio sh -c 'mc alias set local http://127.0.0.1:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null && mc ls -r local/doris | wc -l')
    ((objects > 0)) || fail "no objects in MinIO after the insert"
    echo "  ${objects} objects in MinIO"
fi

step "stream load through the client (redirect to a BE address)"
stream_load fe-1 smoke.t '4,d\n5,e\n' || fail "stream load did not report Success"
[[ "$(q fe-1 'select count(*) from smoke.t')" == 5 ]] || fail "row count after stream load"
echo "  5 rows"

step "kill the master FE, expect a new one"
old_master=$(master_fe fe-1)
compose kill fe-1 >/dev/null 2>&1
new_master_elected() {
    new_master=$(master_fe fe-2 2>/dev/null || true)
    [[ -n "${new_master}" && "${new_master}" != "${old_master}" ]]
}
wait_for 120 "a new master" new_master_elected
echo "  ${old_master} -> ${new_master}"
q fe-2 "insert into smoke.t values (6,'f')"
[[ "$(q fe-2 'select count(*) from smoke.t')" == 6 ]] || fail "write through the new master failed"
echo "  write through fe-2 ok"

step "restart a BE while the old master is down, then bring the old master back"
compose restart be-2 >/dev/null 2>&1
wait_for 180 "be-2 healthy" healthy be-2
compose start fe-1 >/dev/null 2>&1
wait_for 180 "fe-1 healthy" healthy fe-1
[[ "$(column fe-1 'show frontends' Alive | grep -c true)" == 3 ]] || fail "expected 3 live FEs after the restart"
[[ "$(column fe-1 'show backends' Alive | grep -c true)" == 3 ]] || fail "expected 3 live BEs after the restart"
echo "  everything alive again, master is $(master_fe fe-1)"

step "cleanup"
q fe-1 "drop database smoke force" >/dev/null 2>&1 || q fe-1 "drop database smoke"
printf '\nsmoke test passed: %s (%s)\n' "${TOPOLOGY}" "${DORIS_IMAGE}"
