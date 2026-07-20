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
# Arch gate: authorization-sensitive cross-query cache isolation on IcebergConnector.
#
# Invariant: under iceberg.rest.session=user the per-user authorization lives INSIDE the delegated
# loadTable round-trip, so any SHARED cross-query cache on IcebergConnector that can return a value
# without that per-user load bypasses authorization — a "list != load" metadata disclosure (a user who
# can list a table but is not authorized to load it receives a cached projection produced for another
# user). Every cache HOLDER field on IcebergConnector must therefore declare its isolation discipline
# with a marker on the field-declaration line or the line directly above it:
#
#   authz-cache-session-user-disabled  — the field is null under isUserSessionEnabled() (constructor gate)
#   authz-cache-exempt                 — justified as not authz-bypassing (e.g. default-off + read ONLY
#                                        after a preceding per-user resolveTable/loadTable)
#
# A cache holder field carrying NEITHER marker fails the build, so "added a new cross-query cache and
# forgot to isolate it for session=user" becomes a build failure instead of a silent leak.
#
# This gate is marker-based (like tools/check-fecore-metadata-funnel.sh): the marker is a reviewed claim
# and the self-test (tools/check-authz-cache-sharding.test.sh) locks the RED/GREEN behavior. The fe-core
# generic schema cache is a different, name-keyed cache protected separately by
# ExternalCatalog.shouldBypassSchemaCache, not by this gate.
#
# Usage:
#   tools/check-authz-cache-sharding.sh            # default root fe/fe-connector
#   tools/check-authz-cache-sharding.sh <root>     # supplied root (the dir containing fe-connector-iceberg/)
#
# Exit code:
#   0 — every cache holder field carries a marker
#   1 — at least one cache holder field is unmarked (offending lines printed)
#   2 — the cache-holder file was not found under the root

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEFAULT_ROOT="${SCRIPT_DIR}/../fe/fe-connector"
ROOT="${1:-${DEFAULT_ROOT}}"

# The single connector that owns the authorization-sensitive cross-query caches.
TARGET_REL='fe-connector-iceberg/src/main/java/org/apache/doris/connector/iceberg/IcebergConnector.java'
TARGET="${ROOT}/${TARGET_REL}"

if [ ! -f "${TARGET}" ]; then
    echo "check-authz-cache-sharding: cache-holder file not found: ${TARGET}" >&2
    exit 2
fi

MARKER_DISABLED='authz-cache-session-user-disabled'
MARKER_EXEMPT='authz-cache-exempt'

# A cache holder field declaration: any "final <Something>Cache " / "final <Something>Cache<" field,
# regardless of visibility (private/protected/public/package) or static-ness. This deliberately catches
# more than the shipped "private final Iceberg*Cache" convention — a future authz-sensitive cross-query
# cache added as a raw Caffeine/Guava Cache<...> or LoadingCache<...>, or with a different visibility,
# must still be classified. The internal MetaCacheEntry plumbing inside the *Cache classes lives in other
# files (this gate scans only IcebergConnector), so it never matches here. A holder whose type name does
# NOT end in "Cache" is out of scope by convention (cross-query caches are dedicated *Cache types).
FIELD_DECL='final ([A-Za-z_][A-Za-z0-9_]*)?Cache[ <]'

CANDIDATES=$(grep -nE "${FIELD_DECL}" "${TARGET}" 2>/dev/null || true)

RESULT=""
if [ -n "${CANDIDATES}" ]; then
    while IFS= read -r line; do
        [ -z "${line}" ] && continue
        # line = <lineno>:<code>
        lineno="${line%%:*}"
        code="${line#*:}"

        # marker on the declaration line ...
        case "${code}" in *"${MARKER_DISABLED}"*|*"${MARKER_EXEMPT}"*) continue ;; esac
        # ... or on the line immediately above it (long decls / block-comment carriers).
        if [ "${lineno}" -gt 1 ]; then
            prev="$(sed -n "$((lineno - 1))p" "${TARGET}" 2>/dev/null || true)"
            case "${prev}" in *"${MARKER_DISABLED}"*|*"${MARKER_EXEMPT}"*) continue ;; esac
        fi

        RESULT="${RESULT}${TARGET}:${line}"$'\n'
    done <<< "${CANDIDATES}"
fi
RESULT=$(printf '%s' "${RESULT}" | sed '/^$/d')

if [ -n "${RESULT}" ]; then
    echo "AUTHZ-SENSITIVE cache holder field(s) in IcebergConnector without an isolation marker:" >&2
    echo "${RESULT}" >&2
    echo "" >&2
    echo "Under iceberg.rest.session=user a shared, un-partitioned cross-query cache bypasses the per-user" >&2
    echo "loadTable authorization (a metadata disclosure). Each cache holder field must carry, on its" >&2
    echo "declaration line or the line directly above it, exactly ONE of:" >&2
    echo "  ${MARKER_DISABLED}  — the field is null under isUserSessionEnabled()" >&2
    echo "  ${MARKER_EXEMPT}    — justified as not authz-bypassing (e.g. read only after a per-user load)" >&2
    exit 1
fi
