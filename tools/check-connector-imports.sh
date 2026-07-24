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
# Forbidden-import gate for fe-connector modules.
# See plan-doc/01-spi-extensions-rfc.md §15.4.
#
# Connector modules MUST NOT import fe-core internals (catalog / common /
# datasource / qe / analysis / nereids / planner / persist / transaction / fs /
# statistics / mysql / service). Anything they need from fe-core has to be
# exposed through the SPI in
#   org.apache.doris.connector.{api,spi,extension,...}
# or shared types in org.apache.doris.thrift / org.apache.doris.filesystem.
#
# The gate matches both plain and `import static` imports, scans src/main/java
# AND src/test/java, and anchors the SPI-allowed exclusion to the import target
# (not the file path). Self-test: tools/check-connector-imports.test.sh.
#
# Usage:
#   tools/check-connector-imports.sh                  # search default root
#   tools/check-connector-imports.sh <fe-connector>   # search supplied root
#
# Exit code:
#   0 — no forbidden imports
#   1 — at least one forbidden import found (offending lines printed)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEFAULT_ROOT="${SCRIPT_DIR}/../fe/fe-connector"
ROOT="${1:-${DEFAULT_ROOT}}"

if [ ! -d "${ROOT}" ]; then
    echo "check-connector-imports: search root not found: ${ROOT}" >&2
    exit 2
fi

FORBIDDEN='org\.apache\.doris\.(catalog|common|datasource|qe|analysis|nereids|planner|persist|transaction|fs|statistics|mysql|service)'

# SPI-shared packages a connector MAY import. Anchor the exclusion to the IMPORT TARGET (":import ..."), NOT
# the whole "<path>:<lineno>:import ..." line: a bare 'grep -v org.apache.doris.connector' matches the FILE
# PATH too (regex '.' matches '/'), so a forbidden import inside a src/.../org/apache/doris/connector/** file
# would be dropped by its location — blinding the gate to the very modules it guards. Import-anchored keeps the
# real allowance (imports OF these packages) without the path-collision false negative.
ALLOWED_IMPORT=':import[[:space:]]+(static[[:space:]]+)?org[.]apache[.]doris[.](connector|thrift|extension|filesystem)[.]'

CANDIDATES=$(grep -rEn "^import[[:space:]]+(static[[:space:]]+)?${FORBIDDEN}[.]" \
        "${ROOT}"/*/src/main/java "${ROOT}"/*/src/test/java 2>/dev/null \
        | grep -vE "${ALLOWED_IMPORT}" || true)

# A flagged import is a FALSE POSITIVE when the connector module VENDORS its own self-contained copy of the
# class: a real source file inside a fe-connector module whose package happens to match a fe-core prefix (e.g.
# fe-connector-hms's patched HiveMetaStoreClient imports org.apache.doris.datasource.hive.HiveVersionUtil,
# which resolves to fe-connector-hms's OWN vendored HiveVersionUtil.java, not fe-core). The naive package-prefix
# grep cannot tell those apart. Skip an import when a connector-owned source file defines it; keep only imports
# that have NO in-tree definition (i.e. genuinely reach into fe-core).
is_vendored() {
    local fqn="$1" path last
    while [ -n "${fqn}" ]; do
        path="${fqn//.//}.java"
        if find "${ROOT}" -path "*/src/main/java/${path}" -print -quit 2>/dev/null | grep -q .; then
            return 0
        fi
        # Peel a trailing nested-class segment (Uppercase) so a nested-type import resolves to its top-level
        # file; stop at a package segment (lowercase) — a genuine fe-core import has no vendored file at any level.
        last="${fqn##*.}"
        case "${last}" in
            [A-Z]*) case "${fqn}" in *.*) fqn="${fqn%.*}" ;; *) return 1 ;; esac ;;
            *) return 1 ;;
        esac
    done
    return 1
}

RESULT=""
if [ -n "${CANDIDATES}" ]; then
    while IFS= read -r line; do
        [ -z "${line}" ] && continue
        # line = <file>:<lineno>:import <fqn>;
        fqn=$(printf '%s\n' "${line}" | sed -E 's/.*import[[:space:]]+(static[[:space:]]+)?//; s/;.*//')
        if is_vendored "${fqn}"; then
            echo "check-connector-imports: skipping vendored same-module import: ${fqn}" >&2
            continue
        fi
        RESULT="${RESULT}${line}"$'\n'
    done <<< "${CANDIDATES}"
fi
RESULT=$(printf '%s' "${RESULT}" | sed '/^$/d')

if [ -n "${RESULT}" ]; then
    echo "FORBIDDEN IMPORTS in fe-connector modules:" >&2
    echo "${RESULT}" >&2
    echo "" >&2
    echo "fe-connector modules MUST NOT depend on fe-core internals." >&2
    echo "Expose what you need through the connector SPI instead." >&2
    echo "See plan-doc/01-spi-extensions-rfc.md §15.4." >&2
    exit 1
fi
