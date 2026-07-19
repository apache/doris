#!/bin/bash
#
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
# Self-test for tools/check-authz-cache-sharding.sh.
#
# The gate exits 0 on the real (already-marked) tree, so a controlled RED/GREEN fixture is the only way
# to prove it catches what it must. Each seeded field targets one gate property:
#   SILENT — a cache field with 'authz-cache-session-user-disabled' on the decl line   (trailing marker)
#   SILENT — a cache field with 'authz-cache-exempt' on the line ABOVE                  (above-line marker)
#   RED    — an unmarked 'private final Iceberg*Cache' field                            (the core violation)
#   RED    — an unmarked raw 'private final Cache<...>' field                           (raw Caffeine form)
#   RED    — an unmarked 'protected static final LoadingCache<...>' field               (visibility/static form)
#   SILENT — a non-cache field (type does not end in Cache)                             (type boundary)
# Plus: exit 0 on a fully-marked tree, and the marker is load-bearing (strip it => the field is flagged).
#
# Usage:  bash tools/check-authz-cache-sharding.test.sh   # exit 0 = pass, 1 = fail

set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GATE="${SCRIPT_DIR}/check-authz-cache-sharding.sh"

FX="$(mktemp -d)"
trap 'rm -rf "${FX}"' EXIT

DIR="${FX}/fe-connector-iceberg/src/main/java/org/apache/doris/connector/iceberg"
mkdir -p "${DIR}"
TARGET="${DIR}/IcebergConnector.java"

# Fixture: three marked caches (trailing + above), three UNMARKED caches in varied forms (RED), one
# non-cache field. The raw Cache<...> and the protected-static LoadingCache<...> exercise the broadened
# type/visibility coverage (a future authz cache need not be spelled "private final Iceberg*Cache").
cat > "${TARGET}" <<'EOF'
package org.apache.doris.connector.iceberg;
public class IcebergConnector {
    private final IcebergLatestSnapshotCache latestSnapshotCache; // authz-cache-session-user-disabled
    private final IcebergTableCache tableCache; // authz-cache-session-user-disabled
    // authz-cache-exempt: pure metadata, read only after a per-user load
    private final IcebergManifestCache manifestCache = new IcebergManifestCache();
    private final IcebergBrokenCache brokenCache;
    private final Cache<TableIdentifier, Object> rawCache;
    protected static final LoadingCache<Object, Object> loadingCache;
    private final Object notACache = null;
}
EOF

FAILED=0
fail() { echo "FAIL: $1"; FAILED=1; }

# ---- run 1: mixed fixture -> the three UNMARKED caches (all forms) flagged ----
OUT="$(bash "${GATE}" "${FX}" 2>&1)"; EC=$?
REPORTED="$(printf '%s\n' "${OUT}" | grep -E "^${FX}.*:[0-9]+:" || true)"
N="$(printf '%s\n' "${REPORTED}" | grep -c 'Cache' || true)"

[ "${EC}" -eq 1 ] || fail "expected exit 1 (violations present), got ${EC}"
[ "${N}" -eq 3 ] || fail "expected exactly 3 reported violations, got ${N}"$'\n'"${REPORTED}"

printf '%s\n' "${REPORTED}" | grep -qF 'brokenCache' || fail "unmarked Iceberg*Cache NOT reported: brokenCache"
printf '%s\n' "${REPORTED}" | grep -qF 'rawCache' || fail "unmarked raw Cache<...> NOT reported: rawCache"
printf '%s\n' "${REPORTED}" | grep -qF 'loadingCache' || fail "unmarked static LoadingCache NOT reported: loadingCache"

must_not_report() {
    printf '%s\n' "${REPORTED}" | grep -qF "$1" && fail "should NOT be reported: $1" || true
}
must_not_report 'latestSnapshotCache'   # trailing marker
must_not_report 'tableCache'            # trailing marker
must_not_report 'manifestCache'         # marker on line above
must_not_report 'notACache'             # type does not end in Cache

# ---- run 2: remove the three unmarked caches -> fully-marked tree exits 0 ----
sed -i '/IcebergBrokenCache brokenCache;/d; /Cache<TableIdentifier, Object> rawCache;/d; /LoadingCache<Object, Object> loadingCache;/d' "${TARGET}"
bash "${GATE}" "${FX}" >/dev/null 2>&1 && CLEAN_EC=0 || CLEAN_EC=$?
[ "${CLEAN_EC}" -eq 0 ] || fail "expected exit 0 on a fully-marked tree, got ${CLEAN_EC}"

# ---- run 3: the 'disabled' marker is load-bearing -> strip it and its two caches are flagged ----
# (the 'exempt' manifest stays silent, proving the two markers are independent.)
sed -i 's/authz-cache-session-user-disabled/marker-removed-here/g' "${TARGET}"
OUT3="$(bash "${GATE}" "${FX}" 2>&1)"; EC3=$?
REP3="$(printf '%s\n' "${OUT3}" | grep -E "^${FX}.*:[0-9]+:" || true)"
N3="$(printf '%s\n' "${REP3}" | grep -c 'Cache' || true)"
[ "${EC3}" -eq 1 ] || fail "expected exit 1 after stripping the disabled marker, got ${EC3}"
# latestSnapshotCache + tableCache (both were 'disabled') = 2 flagged; the exempt manifest stays silent.
[ "${N3}" -eq 2 ] || fail "expected 2 flagged after stripping the disabled marker, got ${N3}"$'\n'"${REP3}"
printf '%s\n' "${REP3}" | grep -qF 'latestSnapshotCache' || fail "disabled cache not flagged after marker strip"
printf '%s\n' "${REP3}" | grep -qF 'manifestCache' && fail "exempt cache wrongly flagged (markers not independent)" || true

if [ "${FAILED}" -eq 0 ]; then
    echo "PASS: authz-cache gate flags unmarked cache fields; disabled/exempt markers stay silent and are load-bearing."
    exit 0
fi
echo "---- run1 output ----"; printf '%s\n' "${OUT}"
exit 1
