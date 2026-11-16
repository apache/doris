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

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
install_dir="${TP_INSTALL_DIR:-${root}/installed}"
artifact_dir="${AUTOPILOT_ARTIFACT_DIR:-${TMPDIR:-/tmp}}"

[[ "$(uname -s)" == Linux ]] || { echo "SKIP: GNU libunwind is Linux-only"; exit 0; }
[[ -f "${install_dir}/lib/libunwind.a" ]] || { echo "missing libunwind archive" >&2; exit 1; }

tmpdir="$(mktemp -d "${artifact_dir}/libunwind-minidebug.XXXXXX")"
trap 'rm -rf "${tmpdir}"' EXIT
cat >"${tmpdir}/fixture.c" <<'EOF'
#define UNW_LOCAL_ONLY
#include <libunwind.h>
#include <string.h>

__attribute__((noinline)) static int minidebug_marker(void) {
    unw_context_t context;
    unw_cursor_t cursor;
    if (unw_getcontext(&context) < 0 || unw_init_local(&cursor, &context) < 0) return 2;
    for (int i = 0; i < 16; ++i) {
        char name[256];
        unw_word_t offset;
        if (unw_get_proc_name(&cursor, name, sizeof(name), &offset) == 0 &&
            strstr(name, "minidebug_marker") != 0) return 0;
        if (unw_step(&cursor) <= 0) break;
    }
    return 3;
}

int main(void) { return minidebug_marker(); }
EOF

cc="${CC:-cc}"
"${cc}" -O0 -g -fno-inline -fno-omit-frame-pointer -I"${install_dir}/include" \
    "${tmpdir}/fixture.c" -L"${install_dir}/lib" \
    -L"${TP_LZMA_LIB_DIR:-${install_dir}/lib}" \
    -Wl,--start-group -lunwind -lunwind-generic -llzma -lz -Wl,--end-group \
    -ldl -pthread -o "${tmpdir}/fixture"

objcopy --only-keep-debug "${tmpdir}/fixture" "${tmpdir}/debug"
xz -c "${tmpdir}/debug" >"${tmpdir}/debug.xz"
strip --strip-all "${tmpdir}/fixture"
objcopy --add-section ".gnu_debugdata=${tmpdir}/debug.xz" "${tmpdir}/fixture"
! nm "${tmpdir}/fixture" 2>/dev/null | grep -q minidebug_marker
readelf -S "${tmpdir}/fixture" | grep -Fq .gnu_debugdata
"${tmpdir}/fixture"
echo "mini-debug symbol lookup passed with the patched libunwind archive"
