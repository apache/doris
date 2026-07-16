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
# Self-test for tools/check-connector-imports.sh.
#
# The forbidden-import gate exits 0 on the real (clean) tree both before and
# after the hardening, so a controlled RED/GREEN fixture is the only way to prove
# the gate actually catches what it must and to lock the behavior against silent
# regression. Each seeded violation below targets one gate property:
#   - a static import of a forbidden package        (must NOT slip past `import static`)
#   - imports of the 6 added packages               (persist/transaction/fs/statistics/mysql/service)
#   - a forbidden import in a src/test/java file     (test sources must be scanned)
#   - a forbidden import in an org.apache.doris.connector.** file
#                                                    (must be judged by import target, not file path)
#   - a static import of a VENDORED class            (must be SKIPPED, not falsely reported)
# and the allow-cases (thrift/filesystem/connector SPI + the non-static vendored import) must stay silent.
#
# Usage:  bash tools/check-connector-imports.test.sh   # exit 0 = pass, 1 = fail

set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GATE="${SCRIPT_DIR}/check-connector-imports.sh"

FX="$(mktemp -d)"
trap 'rm -rf "${FX}"' EXIT

MOD="${FX}/fe-connector-fake"
mkdir -p "${MOD}/src/main/java/org/apache/doris/fake" \
         "${MOD}/src/main/java/org/apache/doris/connector/fake" \
         "${MOD}/src/main/java/org/apache/doris/datasource/hive" \
         "${MOD}/src/test/java/org/apache/doris/fake"

# token-free package: holes 1 (static) + 2 (six packages), legit allow-imports, both vendored imports.
cat > "${MOD}/src/main/java/org/apache/doris/fake/FakeConn.java" <<'EOF'
package org.apache.doris.fake;
import static org.apache.doris.catalog.Type.INT;
import org.apache.doris.persist.EditLog;
import org.apache.doris.fs.remote.RemoteFileSystem;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.service.FrontendOptions;
import org.apache.doris.thrift.TFoo;
import org.apache.doris.filesystem.Bar;
import org.apache.doris.connector.api.Baz;
import org.apache.doris.datasource.hive.HiveVersionUtil;
import static org.apache.doris.datasource.hive.HiveVersionUtil.SOME_CONST;
public class FakeConn {}
EOF

# vendored self-defined class: same-module definition => both imports above must be skipped.
cat > "${MOD}/src/main/java/org/apache/doris/datasource/hive/HiveVersionUtil.java" <<'EOF'
package org.apache.doris.datasource.hive;
public class HiveVersionUtil { public static final int SOME_CONST = 1; }
EOF

# hole 4: a forbidden import inside a connector-namespaced file. The gate must report it by import TARGET;
# a path-matching whitelist would drop it because the path contains org/apache/doris/connector/.
cat > "${MOD}/src/main/java/org/apache/doris/connector/fake/ConnPathConn.java" <<'EOF'
package org.apache.doris.connector.fake;
import org.apache.doris.catalog.Type;
public class ConnPathConn {}
EOF

# hole 3: forbidden import in a test source.
cat > "${MOD}/src/test/java/org/apache/doris/fake/FakeConnTest.java" <<'EOF'
package org.apache.doris.fake;
import org.apache.doris.transaction.TransactionState;
public class FakeConnTest {}
EOF

OUT="$(bash "${GATE}" "${FX}" 2>&1)"; EC=$?

FAILED=0
fail() { echo "FAIL: $1"; FAILED=1; }

# 1) The gate must reject the fixture.
[ "${EC}" -eq 1 ] || fail "expected exit 1 (violations present), got ${EC}"

# 2) The reported-violation lines are those printed as <fixturepath>:<lineno>:import ...
REPORTED="$(printf '%s\n' "${OUT}" | grep -E "^${FX}.*:[0-9]+:import" || true)"
N="$(printf '%s\n' "${REPORTED}" | grep -c ':import' )"
[ "${N}" -eq 8 ] || fail "expected exactly 8 reported violations, got ${N}"$'\n'"${REPORTED}"

# 3) Every seeded violation must be reported (one property each).
must_report() {
    printf '%s\n' "${REPORTED}" | grep -qF "$1" || fail "violation NOT reported: $1"
}
must_report 'import static org.apache.doris.catalog.Type.INT;'   # hole 1 (static)
must_report 'import org.apache.doris.persist.EditLog;'           # hole 2
must_report 'import org.apache.doris.fs.remote.RemoteFileSystem;' # hole 2 (fs != filesystem)
must_report 'import org.apache.doris.statistics.ColumnStatistic;' # hole 2
must_report 'import org.apache.doris.mysql.privilege.Auth;'      # hole 2
must_report 'import org.apache.doris.service.FrontendOptions;'   # hole 2
must_report 'ConnPathConn.java:2:import org.apache.doris.catalog.Type;'  # hole 4 (connector-namespaced)
must_report 'FakeConnTest.java:2:import org.apache.doris.transaction.TransactionState;'  # hole 3 (test src)

# 4) Allow-cases and vendored imports must NOT be reported as violations.
must_not_report() {
    printf '%s\n' "${REPORTED}" | grep -qF "$1" && fail "should NOT be reported: $1" || true
}
must_not_report 'org.apache.doris.thrift.TFoo'          # SPI-shared
must_not_report 'org.apache.doris.filesystem.Bar'       # SPI-shared
must_not_report 'org.apache.doris.connector.api.Baz'    # SPI
must_not_report 'HiveVersionUtil;'                      # vendored (non-static)
must_not_report 'HiveVersionUtil.SOME_CONST;'           # vendored (static) — E3

# 5) Both vendored imports must have been actively skipped (proves is_vendored ran on the static one too).
printf '%s\n' "${OUT}" | grep -q 'skipping vendored .*HiveVersionUtil$' \
    || fail "non-static vendored import was not skipped"
printf '%s\n' "${OUT}" | grep -q 'skipping vendored .*HiveVersionUtil.SOME_CONST$' \
    || fail "static vendored import was not skipped (E3 static-strip regressed)"

if [ "${FAILED}" -eq 0 ]; then
    echo "PASS: check-connector-imports.sh catches all 4 holes; allow-cases and vendored imports stay silent."
    exit 0
fi
echo "---- gate output ----"
printf '%s\n' "${OUT}"
exit 1
