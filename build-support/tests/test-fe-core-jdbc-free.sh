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
# Self-test for build-support/check-fe-core-jdbc-free.sh.
#
# The gate exits 0 on the real (already-clean) tree, so a controlled RED/GREEN fixture is the only way
# to prove it catches what it must and to lock the behavior against silent regression.
# Each seeded case targets one gate property:
#   RED    — import of the deleted legacy client package        (the core violation)
#   RED    — import of the HikariCP pool                         (pool is a connector concern)
#   RED    — import of java.sql.Connection / a static import of DriverManager.getConnection
#   SILENT — java.sql.Timestamp / java.sql.Types                 (value types, not JDBC access)
#   SILENT — the same forbidden imports under httpv2/            (JDBC to the FE itself, exempt by path)
#   SILENT — a comment or string that names a forbidden package  (never an import)
#   SILENT — a class whose simple name merely starts with a forbidden one (java.sql.Statement vs
#            a hypothetical java.sql.StatementEvent is matched on the whole simple name)
# Plus: exit 0 on a clean tree.
#
# Usage:  bash build-support/tests/test-fe-core-jdbc-free.sh   # exit 0 = pass, 1 = fail

set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GATE="${SCRIPT_DIR}/../check-fe-core-jdbc-free.sh"

FX="$(mktemp -d)"
trap 'rm -rf "${FX}"' EXIT

SRC="${FX}/src/main/java/org/apache/doris"
mkdir -p "${SRC}/job" "${SRC}/httpv2/util" "${SRC}/common"

# RED: the legacy package, the pool, and a JDBC connection type.
cat > "${SRC}/job/LegacyClientUser.java" <<'EOF2'
package org.apache.doris.job;
import org.apache.doris.datasource.jdbc.client.JdbcClient;
public class LegacyClientUser {
}
EOF2

cat > "${SRC}/job/PoolUser.java" <<'EOF2'
package org.apache.doris.job;
import com.zaxxer.hikari.HikariDataSource;
public class PoolUser {
}
EOF2

cat > "${SRC}/job/ConnectionUser.java" <<'EOF2'
package org.apache.doris.job;
import java.sql.Connection;
import static java.sql.DriverManager.getConnection;
public class ConnectionUser {
}
EOF2

# SILENT: value types, comments and strings, and a longer simple name.
cat > "${SRC}/common/ValueTypes.java" <<'EOF2'
package org.apache.doris.common;
import java.sql.Timestamp;
import java.sql.Types;
import java.sql.StatementEvent;
public class ValueTypes {
    // import java.sql.Connection; is only mentioned here
    String s = "import org.apache.doris.datasource.jdbc.client.JdbcClient";
}
EOF2

# SILENT: the FE's own HTTP SQL gateway is exempt by path.
cat > "${SRC}/httpv2/util/StatementSubmitter.java" <<'EOF2'
package org.apache.doris.httpv2.util;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
public class StatementSubmitter {
}
EOF2

FAILED=0
fail() { echo "FAIL: $1"; FAILED=1; }

# ---- run 1: mixed fixture -> exactly the RED imports flagged ----
OUT="$(bash "${GATE}" "${FX}" 2>&1)"; EC=$?
REPORTED="$(printf '%s\n' "${OUT}" | grep -E "^${FX}.*:[0-9]+:" || true)"
N="$(printf '%s\n' "${REPORTED}" | grep -c 'import' || true)"

[ "${EC}" -eq 1 ] || fail "expected exit 1 (violations present), got ${EC}"
[ "${N}" -eq 4 ] || fail "expected exactly 4 reported violations, got ${N}"$'\n'"${REPORTED}"

must_report() { printf '%s\n' "${REPORTED}" | grep -qF "$1" || fail "violation NOT reported: $1"; }
must_report 'job/LegacyClientUser.java:'
must_report 'job/PoolUser.java:'
must_report 'import java.sql.Connection;'
must_report 'import static java.sql.DriverManager.getConnection;'

must_not_report() {
    printf '%s\n' "${REPORTED}" | grep -qF "$1" && fail "should NOT be reported: $1" || true
}
must_not_report 'common/ValueTypes.java:'
must_not_report 'httpv2/util/StatementSubmitter.java:'

# ---- run 2: remove the RED cases -> clean tree exits 0 ----
rm -f "${SRC}/job/LegacyClientUser.java" "${SRC}/job/PoolUser.java" "${SRC}/job/ConnectionUser.java"
bash "${GATE}" "${FX}" >/dev/null 2>&1; EC=$?
[ "${EC}" -eq 0 ] || fail "expected exit 0 on the clean fixture, got ${EC}"

# ---- run 3: a missing root is reported as such, not as clean ----
bash "${GATE}" "${FX}/does-not-exist" >/dev/null 2>&1; EC=$?
[ "${EC}" -eq 2 ] || fail "expected exit 2 for a missing search root, got ${EC}"

if [ "${FAILED}" -ne 0 ]; then
    echo "test-fe-core-jdbc-free: FAILED"
    exit 1
fi
echo "test-fe-core-jdbc-free: OK"
