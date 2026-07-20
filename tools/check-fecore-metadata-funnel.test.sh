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
# Self-test for tools/check-fecore-metadata-funnel.sh.
#
# The gate exits 0 on the real (already-routed) tree, so a controlled RED/GREEN fixture is the
# only way to prove it catches what it must and to lock the behavior against silent regression.
# Each seeded case targets one gate property:
#   RED  — a bare arg call in a normal file (no marker)     (the core violation)
#   RED  — a bare call whose arg is WRAPPED to the next line (open paren at EOL must still catch)
#   SILENT — the funnel file's own direct call + javadoc     (whitelisted by path)
#   SILENT — a marked call, marker on the call line          (same-line exemption)
#   SILENT — a marked call, marker on the line above         (above-line exemption, for long lines)
#   SILENT — a no-argument getMetadata()                     (different method, not the SPI call)
#   SILENT — getMetadataTableRows(...)                       (method-name boundary)
#   SILENT — a comment line naming the call                  (never executable)
# Plus: exit 0 on a clean tree, and the marker is load-bearing (strip it => the call is flagged).
#
# Usage:  bash tools/check-fecore-metadata-funnel.test.sh   # exit 0 = pass, 1 = fail

set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GATE="${SCRIPT_DIR}/check-fecore-metadata-funnel.sh"

FX="$(mktemp -d)"
trap 'rm -rf "${FX}"' EXIT

SRC="${FX}/src/main/java/org/apache/doris"
mkdir -p "${SRC}/datasource/plugin" "${SRC}/read" "${SRC}/write" "${SRC}/misc"

# SILENT: the funnel — the ONE legal direct call site, plus a javadoc mention of the call.
cat > "${SRC}/datasource/plugin/PluginDrivenMetadata.java" <<'EOF'
package org.apache.doris.datasource.plugin;
/**
 * Memoizes {@code connector.getMetadata(session)} once per statement.
 */
public final class PluginDrivenMetadata {
    static Object get(Object session, Object connector) {
        return scope.getOrCreateMetadata(key, () -> connector.getMetadata(session));
    }
}
EOF

# RED: bare arg call, no marker, not the funnel.
cat > "${SRC}/read/Reader.java" <<'EOF'
package org.apache.doris.read;
public class Reader {
    void f() {
        Object m = connector.getMetadata(session);
    }
}
EOF

# RED: bare call whose argument is wrapped onto the next line (open paren at end of line).
cat > "${SRC}/read/Wrapped.java" <<'EOF'
package org.apache.doris.read;
public class Wrapped {
    void f() {
        Object m = connector.getMetadata(
                session);
    }
}
EOF

# SILENT: write-path call, marker on the SAME line.
cat > "${SRC}/write/WriterTrailing.java" <<'EOF'
package org.apache.doris.write;
public class WriterTrailing {
    void f() {
        Object m = connector.getMetadata(session); // getMetadata-funnel-exempt: write path
    }
}
EOF

# SILENT: write-path call, marker on the line ABOVE (how long call lines carry it).
cat > "${SRC}/write/WriterAbove.java" <<'EOF'
package org.apache.doris.write;
public class WriterAbove {
    void f() {
        // getMetadata-funnel-exempt: write path, rerouted through the funnel in the write-sharing step
        Object m = catalog.getConnector().getMetadata(session);
    }
}
EOF

# SILENT: no-arg getMetadata(), getMetadataTableRows(...), and a comment naming the call.
cat > "${SRC}/misc/Misc.java" <<'EOF'
package org.apache.doris.misc;
public class Misc {
    void f() {
        int id = (int) node.getMetadata();
        Object rows = table.getMetadataTableRows(session);
        // comment: connector.getMetadata(session) must stay silent
        Object keys = catalogProvider.getMetadata().keySet();
    }
}
EOF

FAILED=0
fail() { echo "FAIL: $1"; FAILED=1; }

# ---- run 1: mixed fixture -> exactly the two RED cases flagged ----
OUT="$(bash "${GATE}" "${FX}" 2>&1)"; EC=$?
REPORTED="$(printf '%s\n' "${OUT}" | grep -E "^${FX}.*:[0-9]+:" || true)"
N="$(printf '%s\n' "${REPORTED}" | grep -c 'getMetadata' || true)"

[ "${EC}" -eq 1 ] || fail "expected exit 1 (violations present), got ${EC}"
[ "${N}" -eq 2 ] || fail "expected exactly 2 reported violations, got ${N}"$'\n'"${REPORTED}"

must_report() { printf '%s\n' "${REPORTED}" | grep -qF "$1" || fail "violation NOT reported: $1"; }
must_report 'read/Reader.java:'    # core: bare arg call, no marker
must_report 'read/Wrapped.java:'   # wrapped-arg call (open paren at EOL)

must_not_report() {
    printf '%s\n' "${REPORTED}" | grep -qF "$1" && fail "should NOT be reported: $1" || true
}
must_not_report 'PluginDrivenMetadata.java:'   # funnel (whitelisted by path)
must_not_report 'WriterTrailing.java:'         # marker on call line
must_not_report 'WriterAbove.java:'            # marker on line above
must_not_report 'Misc.java:'                   # no-arg / getMetadataTableRows / comment

# ---- run 2: remove the RED cases -> clean tree exits 0 (proves all SILENT cases stay silent) ----
rm -f "${SRC}/read/Reader.java" "${SRC}/read/Wrapped.java"
bash "${GATE}" "${FX}" >/dev/null 2>&1 && CLEAN_EC=0 || CLEAN_EC=$?
[ "${CLEAN_EC}" -eq 0 ] || fail "expected exit 0 on clean tree (only funnel/marked/no-arg/comment), got ${CLEAN_EC}"

# ---- run 3: the marker is load-bearing -> strip it and both write calls are flagged ----
sed -i 's/getMetadata-funnel-exempt/marker-removed-here/' \
    "${SRC}/write/WriterTrailing.java" "${SRC}/write/WriterAbove.java"
OUT3="$(bash "${GATE}" "${FX}" 2>&1)"; EC3=$?
REP3="$(printf '%s\n' "${OUT3}" | grep -E "^${FX}.*:[0-9]+:" || true)"
N3="$(printf '%s\n' "${REP3}" | grep -c 'getMetadata' || true)"
[ "${EC3}" -eq 1 ] || fail "expected exit 1 after stripping markers, got ${EC3}"
[ "${N3}" -eq 2 ] || fail "expected 2 flagged after stripping markers, got ${N3}"$'\n'"${REP3}"
printf '%s\n' "${REP3}" | grep -qF 'WriterTrailing.java:' || fail "same-line call not flagged after marker strip"
printf '%s\n' "${REP3}" | grep -qF 'WriterAbove.java:' || fail "above-line call not flagged after marker strip"

if [ "${FAILED}" -eq 0 ]; then
    echo "PASS: funnel gate catches bare (incl. wrapped) calls; funnel/marker/no-arg/comment stay silent; marker is load-bearing."
    exit 0
fi
echo "---- run1 output ----"; printf '%s\n' "${OUT}"
exit 1
