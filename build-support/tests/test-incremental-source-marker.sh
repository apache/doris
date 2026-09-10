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
# Self-test for fe-core's IncrementalSourceMarker (the -Pfast-fe profile).
#
# The tool decides which sources `mvn -Pfast-fe compile` hands to javac, so a wrong answer is either
# a slow build (too many files) or a stale class (too few). This test pins both sides on a fixture
# tree: it is the missing-dependency direction which is dangerous, so the cases below assert that a
# referrer is always marked and that an unrelated file never is.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TOOL="${ROOT}/fe/fe-core/src/main/java/org/apache/doris/buildtools/IncrementalSourceMarker.java"

JAVA_BIN="${JAVA_HOME:+${JAVA_HOME}/bin/}java"
JAVAC_BIN="${JAVA_HOME:+${JAVA_HOME}/bin/}javac"
for binary in "${JAVA_BIN}" "${JAVAC_BIN}"; do
    if ! command -v "${binary}" > /dev/null 2>&1; then
        echo "test-incremental-source-marker: ${binary} not found, set JAVA_HOME or put a jdk on PATH" >&2
        exit 1
    fi
done

WORK="$(mktemp -d)"
trap 'rm -rf "${WORK}"' EXIT

SRC="${WORK}/src/main/java"
CLASSES="${WORK}/target/classes"
TOOL_CLASSES="${WORK}/tool-classes"
CACHE="${WORK}/index.bin"
STAMP="${WORK}/stamp"
mkdir -p "${SRC}/demo" "${CLASSES}/demo" "${TOOL_CLASSES}"

"${JAVAC_BIN}" -d "${TOOL_CLASSES}" "${TOOL}"

failures=0

check() {
    local name="$1" expected="$2" actual="$3"
    if [[ "${expected}" == "${actual}" ]]; then
        echo "  ok   ${name}"
    else
        echo "  FAIL ${name}: expected [${expected}] got [${actual}]"
        failures=$((failures + 1))
    fi
}

write() {
    mkdir -p "$(dirname "$1")"
    printf '%s\n' "$2" > "$1"
}

run() {
    "${JAVA_BIN}" -cp "${TOOL_CLASSES}" org.apache.doris.buildtools.IncrementalSourceMarker \
        --sources="${SRC}" --touch-root="${SRC}" --cache="${CACHE}" --classes="${CLASSES}" > /dev/null
}

# the sources the tool marked, i.e. the ones it made newer than the stamp
marked() {
    touch "${STAMP}"
    sleep 0.05
    run
    find "${SRC}" -name '*.java' -newer "${STAMP}" | sed "s|${SRC}/||" | sort | tr '\n' ' '
}

class_states() {
    local result=""
    local name state
    for name in 'Removed.class' 'Removed$Inner.class' 'Removedish.class'; do
        state="kept"
        if [[ ! -e "${CLASSES}/demo/${name}" ]]; then
            state="gone"
        fi
        result="${result}${result:+ }${name}=${state}"
    done
    printf '%s' "${result}"
}

# Alpha is the changed type, Beta mentions it, Gamma is unrelated.
write "${SRC}/demo/Alpha.java" 'package demo; public class Alpha { public void a() {} }'
write "${SRC}/demo/Beta.java"  'package demo; public class Beta { Alpha a; }'
write "${SRC}/demo/Gamma.java" 'package demo; public class Gamma { }'

run > /dev/null
check "a build with no change marks nothing" "" "$(marked)"

write "${SRC}/demo/Alpha.java" 'package demo; public class Alpha { public void a() { int probe = 1; } }'
check "editing Alpha marks Alpha and its referrer Beta, not Gamma" \
    "demo/Alpha.java demo/Beta.java " "$(marked)"

# A mention inside a comment or a string is not a dependency, and must not mark the file. Gamma is
# changed and settled first, so any mark on the next run could only come from Alpha.
write "${SRC}/demo/Gamma.java" 'package demo; public class Gamma { /* Alpha */ String s = "Alpha"; }'
run > /dev/null
write "${SRC}/demo/Alpha.java" 'package demo; public class Alpha { public void a() { int probe = 2; } }'
check "a comment or string literal alone does not mark Gamma" \
    "demo/Alpha.java demo/Beta.java " "$(marked)"

# A new type invalidates the files which already mention the new name.
write "${SRC}/demo/Delta.java" 'package demo; public class Delta extends Alpha { }'
check "adding Delta marks only Delta" "demo/Delta.java " "$(marked)"

# A removed source drops its classes, and only its own.
write "${SRC}/demo/Removed.java" 'package demo; public class Removed { }'
run > /dev/null
touch "${CLASSES}/demo/Removed.class" "${CLASSES}/demo/Removed\$Inner.class" "${CLASSES}/demo/Removedish.class"
rm "${SRC}/demo/Removed.java"
run > /dev/null
check "removing a source deletes its classes, inner classes included" \
    "Removed.class=gone Removed\$Inner.class=gone Removedish.class=kept" "$(class_states)"

if [[ "${failures}" -ne 0 ]]; then
    echo "test-incremental-source-marker: ${failures} failure(s)"
    exit 1
fi
echo "test-incremental-source-marker: OK"
