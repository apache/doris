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
# Arch gate: the per-statement ConnectorMetadata funnel.
#
# Invariant: within a statement, fe-core must reuse exactly ONE ConnectorMetadata
# instance per catalog. Every read / scan / DDL / MVCC seam therefore acquires its
# metadata through the engine-side funnel
#     org.apache.doris.datasource.plugin.PluginDrivenMetadata.get(session, connector)
# which memoizes connector.getMetadata(session) on the statement's ConnectorStatementScope
# and closes it deterministically at statement end. A bare connector.getMetadata(session)
# anywhere else in fe-core silently mints a second, unmanaged instance and breaks the
# invariant with no compile error and no test failure — this gate fails the build on it.
#
# Allowed (kept silent):
#   1. PluginDrivenMetadata.java — the funnel itself, the ONE legal direct call site
#      (its javadoc mentions of the call live here too).
#   2. Any bare call whose line — or the line immediately above it — carries the marker
#      "getMetadata-funnel-exempt": the write-path seams (INSERT / DELETE / MERGE sink,
#      bind, row-level DML), temporarily exempt until the write-sharing step reroutes them
#      through the funnel. Deleting a marker there auto-tightens the gate onto that site.
#   3. A no-argument getMetadata() call (RuntimeProfile's node probe, TestExternalCatalog's
#      static map): a different method, not the Connector#getMetadata(ConnectorSession) SPI.
#   4. A comment line that merely names the call — never executable.
#
# The match is anchored to the call form (".getMetadata(" with an argument), NOT to
# getMetadataTableRows(...) or the API declaration.
#
# Self-test: tools/check-fecore-metadata-funnel.test.sh.
#
# Usage:
#   tools/check-fecore-metadata-funnel.sh                # default root fe/fe-core
#   tools/check-fecore-metadata-funnel.sh <fe-core-dir>  # supplied root
#
# Exit code:
#   0 — no un-exempt bare calls
#   1 — at least one found (offending lines printed)
#   2 — search root not found

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEFAULT_ROOT="${SCRIPT_DIR}/../fe/fe-core"
ROOT="${1:-${DEFAULT_ROOT}}"

if [ ! -d "${ROOT}" ]; then
    echo "check-fecore-metadata-funnel: search root not found: ${ROOT}" >&2
    exit 2
fi

# The funnel carrier — the ONE file allowed to call Connector#getMetadata directly.
FUNNEL_REL='datasource/plugin/PluginDrivenMetadata.java'

# Inline marker for the write-path seams still bypassing the funnel (removed by the write-sharing step).
# It may sit on the call line or on the line directly above it (long call lines keep the marker above).
MARKER='getMetadata-funnel-exempt'

# An invocation of getMetadata with an argument. Two forms so a call whose argument is wrapped onto the
# next line (open paren at end of line) is still caught, not just the single-line form:
ARG_SAMELINE='\.getMetadata\([[:space:]]*[^[:space:])]'   # ".getMetadata(session"  — arg on this line
ARG_WRAPPED='\.getMetadata\([[:space:]]*$'                # ".getMetadata("         — arg on next line

# Candidate lines: any ".getMetadata(" under main sources. Narrowed to real arg calls in the loop.
CANDIDATES=$(grep -rEn '\.getMetadata\(' "${ROOT}/src/main/java" 2>/dev/null || true)

RESULT=""
if [ -n "${CANDIDATES}" ]; then
    while IFS= read -r line; do
        [ -z "${line}" ] && continue
        # line = <file>:<lineno>:<code>
        file="${line%%:*}"
        rest="${line#*:}"
        lineno="${rest%%:*}"
        code="${rest#*:}"

        # (4) comment line merely naming the call — never executable.
        trimmed="${code#"${code%%[![:space:]]*}"}"
        case "${trimmed}" in '*'*|'//'*|'/*'*) continue ;; esac

        # (3) no-argument getMetadata() — a different method, not the SPI call.
        if ! { [[ "${code}" =~ ${ARG_SAMELINE} ]] || [[ "${code}" =~ ${ARG_WRAPPED} ]]; }; then
            continue
        fi

        # (1) the funnel file itself.
        case "${file}" in *"${FUNNEL_REL}") continue ;; esac

        # (2) tracked write-path exemption marker, on the call line ...
        case "${code}" in *"${MARKER}"*) continue ;; esac
        # ... or on the line immediately above it.
        if [ "${lineno}" -gt 1 ]; then
            prev="$(sed -n "$((lineno - 1))p" "${file}" 2>/dev/null || true)"
            case "${prev}" in *"${MARKER}"*) continue ;; esac
        fi

        RESULT="${RESULT}${line}"$'\n'
    done <<< "${CANDIDATES}"
fi
RESULT=$(printf '%s' "${RESULT}" | sed '/^$/d')

if [ -n "${RESULT}" ]; then
    echo "BARE Connector#getMetadata() calls in fe-core (must route through PluginDrivenMetadata.get):" >&2
    echo "${RESULT}" >&2
    echo "" >&2
    echo "A statement must reuse ONE ConnectorMetadata per catalog. Acquire it via" >&2
    echo "PluginDrivenMetadata.get(session, connector), not connector.getMetadata(session)." >&2
    echo "Write-path seams pending the write-sharing step carry a '${MARKER}' marker" >&2
    echo "(on the call line or the line directly above it)." >&2
    exit 1
fi
