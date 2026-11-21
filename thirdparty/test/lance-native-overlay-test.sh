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

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." &>/dev/null && pwd)"
task_dir="$(mktemp -d)"
trap 'rm -rf "${task_dir}"' EXIT
mkdir -p "${task_dir}/src" "${task_dir}/patches" "${task_dir}/fixture"

fail() { echo "FAIL: $*" >&2; exit 1; }
assert_value() {
    [[ "$(cat "${task_dir}/src/$1/value")" == "$2" ]] || fail "unexpected $1 source contents"
}
archive_fixture() {
    local source_name="$1" archive_name="$2" value="$3"
    mkdir -p "${task_dir}/fixture/${source_name}"
    printf '%s\n' "${value}" > "${task_dir}/fixture/${source_name}/value"
    tar czf "${task_dir}/src/${archive_name}" -C "${task_dir}/fixture" "${source_name}"
}
write_patch() {
    local patch_name="$1" before="$2" after="$3"
    cat > "${task_dir}/patches/${patch_name}" <<EOF
--- a/value
+++ b/value
@@ -1 +1 @@
-${before}
+${after}
EOF
}
digest() {
    if command -v md5sum >/dev/null 2>&1; then
        md5sum "$1" | awk '{print $1}'
    else
        md5 -q "$1"
    fi
}

archive_fixture lance-c-native lance-c-native.tar.gz native0
archive_fixture lance-native lance-native.tar.gz core0
archive_fixture datafusion-runtime datafusion-runtime.crate runtime0
archive_fixture fst-runtime fst-runtime.crate fst0
archive_fixture arrow-select-runtime arrow-select-runtime.crate arrow0
write_patch lance-c-foyer.patch native0 native1
write_patch lance-c-memory.patch native1 native2
write_patch lance-native-memory.patch core0 core1
write_patch lance-df-runtime-memory.patch runtime0 runtime1
write_patch lance-fst-memory.patch fst0 fst1
write_patch lance-arrow-select-memory.patch arrow0 arrow1
cat > "${task_dir}/vars.sh" <<EOF
export TP_SOURCE_DIR="${task_dir}/src"
export TP_INSTALL_DIR="${task_dir}/installed"
export TP_PATCH_DIR="${task_dir}/patches"
export TP_INCLUDE_DIR="${task_dir}/installed/include"
export TP_LIB_DIR="${task_dir}/installed/lib"
export TP_JAR_DIR="${task_dir}/installed/lib/jar"
LANCE_C_DOWNLOAD=https://example.invalid/native
LANCE_C_NAME=lance-c-native.tar.gz
LANCE_C_SOURCE=lance-c-native
LANCE_C_MD5SUM=$(digest "${task_dir}/src/lance-c-native.tar.gz")
LANCE_NATIVE_DOWNLOAD=https://example.invalid/lance
LANCE_NATIVE_NAME=lance-native.tar.gz
LANCE_NATIVE_SOURCE=lance-native
LANCE_NATIVE_MD5SUM=$(digest "${task_dir}/src/lance-native.tar.gz")
LANCE_DF_RUNTIME_DOWNLOAD=https://example.invalid/runtime
LANCE_DF_RUNTIME_NAME=datafusion-runtime.crate
LANCE_DF_RUNTIME_SOURCE=datafusion-runtime
LANCE_DF_RUNTIME_MD5SUM=$(digest "${task_dir}/src/datafusion-runtime.crate")
LANCE_FST_DOWNLOAD=https://example.invalid/fst
LANCE_FST_NAME=fst-runtime.crate
LANCE_FST_SOURCE=fst-runtime
LANCE_FST_MD5SUM=$(digest "${task_dir}/src/fst-runtime.crate")
LANCE_ARROW_SELECT_DOWNLOAD=https://example.invalid/arrow-select
LANCE_ARROW_SELECT_NAME=arrow-select-runtime.crate
LANCE_ARROW_SELECT_SOURCE=arrow-select-runtime
LANCE_ARROW_SELECT_MD5SUM=$(digest "${task_dir}/src/arrow-select-runtime.crate")
export TP_ARCHIVES=('LANCE_C' 'LANCE_NATIVE' 'LANCE_DF_RUNTIME' 'LANCE_FST' 'LANCE_ARROW_SELECT')
EOF
run_download() {
    TP_DIR="${task_dir}" DORIS_HOME="${task_dir}" bash "${ROOT}/download-thirdparty.sh" lance_c
}

run_download > "${task_dir}/first.log" 2>&1 || { cat "${task_dir}/first.log"; fail 'initial source overlays'; }
assert_value lance-c-native native2
assert_value lance-native core1
assert_value datafusion-runtime runtime1
assert_value fst-runtime fst1
assert_value arrow-select-runtime arrow1
[[ -f "${task_dir}/src/lance-c-native/patched_mark_foyer" ]] || fail 'missing native marker'
for source_name in lance-native datafusion-runtime fst-runtime arrow-select-runtime; do
    [[ -f "${task_dir}/src/${source_name}/patched_mark_memory" ]] || fail 'missing companion marker'
done

# Every overlay checksum invalidates its own extracted source. A changed ABI
# patch also reconstructs the native source and reapplies the cache prerequisite.
write_patch lance-c-memory.patch native1 native3
write_patch lance-native-memory.patch core0 core2
write_patch lance-df-runtime-memory.patch runtime0 runtime2
write_patch lance-fst-memory.patch fst0 fst2
write_patch lance-arrow-select-memory.patch arrow0 arrow2
run_download > "${task_dir}/second.log" 2>&1 || { cat "${task_dir}/second.log"; fail 'changed source overlays'; }
assert_value lance-c-native native3
assert_value lance-native core2
assert_value datafusion-runtime runtime2
assert_value fst-runtime fst2
assert_value arrow-select-runtime arrow2

# A failed patch must not publish the checksum marker used by a subsequent build.
cat > "${task_dir}/patches/lance-native-memory.patch" <<'EOF'
--- /dev/null
+++ b/partial
@@ -0,0 +1 @@
+partial overlay
--- a/value
+++ b/value
@@ -1 +1 @@
-incorrect-before
+core3
EOF
if run_download > "${task_dir}/failure.log" 2>&1; then
    fail 'invalid source patch accepted'
fi
[[ ! -f "${task_dir}/src/lance-native/patched_mark_memory" ]] || fail 'failed patch published marker'
assert_value lance-native core0
[[ -f "${task_dir}/src/lance-native/partial" ]] || fail 'partial application was not exercised'
write_patch lance-native-memory.patch core0 core3
run_download > "${task_dir}/recover.log" 2>&1 || { cat "${task_dir}/recover.log"; fail 'partial overlay recovery'; }
assert_value lance-native core3
[[ ! -f "${task_dir}/src/lance-native/partial" ]] || fail 'partial source survived recovery'

# A failure in the second native overlay also leaves the prerequisite applied.
# Retrying must restore the archive before applying both patches again.
write_patch lance-c-memory.patch incorrect-before native4
if run_download > "${task_dir}/native-failure.log" 2>&1; then
    fail 'invalid native ABI patch accepted'
fi
[[ ! -f "${task_dir}/src/lance-c-native/patched_mark_foyer" ]] || fail 'failed ABI patch published marker'
assert_value lance-c-native native1
write_patch lance-c-memory.patch native1 native4
run_download > "${task_dir}/native-recover.log" 2>&1 || { cat "${task_dir}/native-recover.log"; fail 'native overlay recovery'; }
assert_value lance-c-native native4
echo 'PASS: Lance companions, crate extraction, overlay invalidation, and partial-failure recovery'
