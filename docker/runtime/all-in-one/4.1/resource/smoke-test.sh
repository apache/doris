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
#   smoke-test.sh <image:tag> [base|full]
#
# DOCKER_RUN_OPTS passes extra flags to docker run, e.g. to reproduce a
# memory-constrained CI runner:
#   DOCKER_RUN_OPTS='--memory 6g' smoke-test.sh apache/doris:all-in-one-4.1.3
#
# Guards the prune list. Everything runs through `docker exec`, using the
# client tools already inside the image, so the host needs nothing but docker
# and no ports have to be published.
#
# What it does not cover: real external-table reads. Those need the fixtures
# under docker/thirdparties and are a separate exercise -- see README.md.

set -euo pipefail

IMAGE=${1:?usage: smoke-test.sh <image:tag> [base|full]}
FLAVOR=${2:-base}
WAIT_SECONDS=${WAIT_SECONDS:-300}

CID=

cleanup() {
    local rc=$?
    if [[ -n "${CID}" ]]; then
        if ((rc != 0)); then
            echo "--- container log (tail) ---" >&2
            docker logs --tail 120 "${CID}" 2>&1 >&2 || true
        fi
        docker rm -f "${CID}" >/dev/null 2>&1 || true
    fi
    exit "${rc}"
}
trap cleanup EXIT

step() { printf '\n== %s\n' "$*"; }
fail() { echo "FAIL: $*" >&2; exit 1; }

q() { docker exec "${CID}" mysql -uroot -h127.0.0.1 -P9030 -N --batch -e "$1"; }

in_image() { docker exec "${CID}" test -d "/opt/apache-doris/$1"; }

step "starting ${IMAGE}"
CID=$(docker run -d ${DOCKER_RUN_OPTS:-} "${IMAGE}")
# Record what was actually exercised: the image is multi-arch capable and this
# is the one line that says which variant this run proves anything about.
echo "  image arch : $(docker image inspect "${IMAGE}" --format '{{.Os}}/{{.Architecture}}')"
echo "  host arch  : $(uname -m)"
echo "  container  : $(docker exec "${CID}" uname -m 2>/dev/null || echo '?')"

step "waiting for health status"
deadline=$((SECONDS + WAIT_SECONDS))
while ((SECONDS < deadline)); do
    status=$(docker inspect -f '{{.State.Health.Status}}' "${CID}" 2>/dev/null || echo missing)
    [[ "${status}" == healthy ]] && break
    if [[ "$(docker inspect -f '{{.State.Running}}' "${CID}" 2>/dev/null)" != "true" ]]; then
        fail "container exited before becoming healthy"
    fi
    sleep 2
done
[[ "$(docker inspect -f '{{.State.Health.Status}}' "${CID}")" == healthy ]] \
    || fail "never became healthy within ${WAIT_SECONDS}s"
echo "healthy after ${SECONDS}s"

step "JNI baseline is present"
# These three are what every external-table read goes through. If a future
# edit to prune.txt takes one out, this is where it shows up.
for d in be/lib/java_extensions/preload-extensions \
         be/lib/java_extensions/java-udf \
         be/lib/hadoop_hdfs; do
    in_image "${d}" || fail "missing JNI baseline directory: ${d}"
    echo "  ok ${d}"
done

step "flavor payload matches ${FLAVOR}"
# Present in both tags.
for d in paimon-scanner iceberg-metadata-scanner jdbc-scanner java-writer; do
    in_image "be/lib/java_extensions/${d}" || fail "missing from base payload: ${d}"
    echo "  ok ${d}"
done
# Present only in -full.
for d in hadoop-hudi-scanner trino-connector-scanner max-compute-connector; do
    if [[ "${FLAVOR}" == full ]]; then
        in_image "be/lib/java_extensions/${d}" || fail "full flavor is missing ${d}"
        echo "  ok ${d} (full)"
    else
        ! in_image "be/lib/java_extensions/${d}" || fail "base flavor should not ship ${d}"
        echo "  ok ${d} absent (base)"
    fi
done
# Dropped from both tags.
for d in be/lib/meta_tool be/lib/cdc_client fe/arthas; do
    ! in_image "${d}" || fail "${d} should have been pruned"
    echo "  ok ${d} absent"
done

step "create database and table"
# No replication_num: force_olap_table_replication_num must supply it.
q "create database if not exists smoke"
q "drop table if exists smoke.t"
q "create table smoke.t (k int, v varchar(32), d date)
   duplicate key(k) distributed by hash(k) buckets 3"
[[ "$(q "show create table smoke.t" | grep -c 'replication_allocation')" -ge 1 ]] \
    || echo "  note: replication_allocation not shown, continuing"

step "insert and read back"
q "insert into smoke.t values (1,'a','2026-01-01'),(2,'b','2026-01-02')"
[[ "$(q 'select count(*) from smoke.t')" == "2" ]] || fail "insert/select mismatch"
echo "  2 rows"

step "stream load"
printf '3,c,2026-01-03\n' >/tmp/smoke_load.csv
docker cp /tmp/smoke_load.csv "${CID}:/tmp/smoke_load.csv" >/dev/null
rm -f /tmp/smoke_load.csv
docker exec "${CID}" curl -sS -u root: \
    -H "column_separator:," -H "Expect:100-continue" \
    -T /tmp/smoke_load.csv \
    -XPUT "http://127.0.0.1:8040/api/smoke/t/_stream_load" | grep -q '"Status": *"Success"' \
    || fail "stream load did not report Success"
[[ "$(q 'select count(*) from smoke.t')" == "3" ]] || fail "row count after stream load is wrong"
echo "  3 rows"

step "aggregation and schema change"
[[ "$(q "select count(distinct v) from smoke.t")" == "3" ]] || fail "aggregation result is wrong"
q "alter table smoke.t add column c1 int default '0'"
for _ in $(seq 1 60); do
    [[ "$(q "show alter table column from smoke order by CreateTime desc limit 1" | awk '{print $10}')" == "FINISHED" ]] && break
    sleep 2
done
q "select k, c1 from smoke.t order by k limit 1" >/dev/null || fail "cannot read after schema change"
echo "  schema change applied"

step "cleanup"
q "drop database smoke"

printf '\nsmoke test passed: %s (%s)\n' "${IMAGE}" "${FLAVOR}"
