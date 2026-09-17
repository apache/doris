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
#
# Arch gate: fe-core carries no JDBC data-source implementation.
#
# Invariant: every JDBC dialect client, type mapping, driver-jar policy and connection pool lives in
# fe/fe-connector (the jdbc plugin and the shared spi policy), never in fe-core. fe-core reaches a JDBC
# source only through the connector SPI (PluginDrivenExternalCatalog for catalogs, StreamingSourceClient
# for streaming jobs). A dialect fix that lands in fe-core again would silently fork the implementation
# — the exact drift this gate exists to prevent — with no compile error and no test failure.
#
# Forbidden in fe-core main sources:
#   1. import org.apache.doris.datasource.jdbc.*   — the deleted legacy client package; it must not return.
#   2. import com.zaxxer.hikari.*                  — a connection pool is a connector concern.
#   3. import java.sql.<connection or statement type> — Connection, DriverManager, Driver, Statement,
#      PreparedStatement, ResultSet, ResultSetMetaData, DatabaseMetaData, DataSource, SQLException.
#      The value types (java.sql.Timestamp/Date/Time/Types) are not JDBC access and stay allowed.
#
# Exempt (kept silent):
#   - org/apache/doris/httpv2/** — the FE's HTTP SQL gateway, which talks to the FE ITSELF over the
#     MySQL protocol through a JDBC driver. That is not an external data source.
#   - A comment line that merely names an import — never executable.
#
# Self-test: build-support/tests/test-fe-core-jdbc-free.sh.
#
# Usage:
#   build-support/check-fe-core-jdbc-free.sh                # default root fe/fe-core
#   build-support/check-fe-core-jdbc-free.sh <fe-core-dir>  # supplied root
#
# Exit code:
#   0 — no forbidden imports
#   1 — at least one found (offending lines printed)
#   2 — search root not found

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEFAULT_ROOT="${SCRIPT_DIR}/../fe/fe-core"
ROOT="${1:-${DEFAULT_ROOT}}"

if [ ! -d "${ROOT}" ]; then
    echo "check-fe-core-jdbc-free: search root not found: ${ROOT}" >&2
    exit 2
fi

# The FE's own HTTP SQL gateway: JDBC to the FE itself, exempt by path.
EXEMPT_DIR='org/apache/doris/httpv2/'

# The three forbidden import shapes (anchored to an import statement, so a comment or a string that
# names the package is not matched).
LEGACY_PKG='^[[:space:]]*import[[:space:]]+(static[[:space:]]+)?org\.apache\.doris\.datasource\.jdbc\.'
POOL='^[[:space:]]*import[[:space:]]+(static[[:space:]]+)?com\.zaxxer\.hikari\.'
JDBC_ACCESS='^[[:space:]]*import[[:space:]]+(static[[:space:]]+)?java\.sql\.(Connection|DriverManager|Driver|Statement|PreparedStatement|CallableStatement|ResultSet|ResultSetMetaData|DatabaseMetaData|DataSource|SQLException)([[:space:].;]|$)'

CANDIDATES=$(grep -rEn "${LEGACY_PKG}|${POOL}|${JDBC_ACCESS}" "${ROOT}/src/main/java" 2>/dev/null || true)

RESULT=""
if [ -n "${CANDIDATES}" ]; then
    while IFS= read -r line; do
        [ -z "${line}" ] && continue
        file="${line%%:*}"
        case "${file}" in *"${EXEMPT_DIR}"*) continue ;; esac
        RESULT="${RESULT}${line}"$'\n'
    done <<< "${CANDIDATES}"
fi
RESULT=$(printf '%s' "${RESULT}" | sed '/^$/d')

if [ -n "${RESULT}" ]; then
    echo "JDBC data-source access in fe-core (must live in fe/fe-connector and be reached through the SPI):" >&2
    echo "${RESULT}" >&2
    echo "" >&2
    echo "fe-core must not import org.apache.doris.datasource.jdbc.*, com.zaxxer.hikari.* or the" >&2
    echo "java.sql connection/statement types. A JDBC source is reached through the connector SPI" >&2
    echo "(PluginDrivenExternalCatalog / StreamingSourceClient); dialect logic belongs in" >&2
    echo "fe/fe-connector/fe-connector-jdbc. Only ${EXEMPT_DIR} (JDBC to the FE itself) is exempt." >&2
    exit 1
fi
