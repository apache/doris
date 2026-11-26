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
test_tmp="$(mktemp -d)"
trap 'rm -rf "${test_tmp}"' EXIT

fail() { echo "FAIL: $*" >&2; exit 1; }

mkdir -p "${test_tmp}/bin" "${test_tmp}/fixtures" "${test_tmp}/jdk/include"
touch "${test_tmp}/jdk/include/jni.h"
for tool in bash dirname mkdir rm cp cat awk sed tr tar gzip uname md5sum sha256sum touch; do
    ln -s "$(command -v "${tool}")" "${test_tmp}/bin/${tool}"
done
cat > "${test_tmp}/bin/compiler" <<'STUB'
#!/bin/bash
while [[ $# -gt 0 ]]; do
    if [[ "$1" == -o ]]; then touch "$2"; exit 0; fi
    shift
done
STUB
cat > "${test_tmp}/bin/ar" <<'STUB'
#!/bin/bash
touch "$2"
STUB
cat > "${test_tmp}/bin/wget" <<'STUB'
#!/bin/bash
output=''
url=''
while [[ $# -gt 0 ]]; do
    case "$1" in
        -O) output="$2"; shift 2 ;;
        https://*) url="$1"; shift ;;
        *) shift ;;
    esac
done
cp "${FIXTURES}/${url##*/}" "${output}"
STUB
chmod +x "${test_tmp}/bin/"{compiler,ar,wget}

# Exercise the real package function without rebuilding unrelated C/C++ fixtures.
source <(sed -n '/^build_arrow_adbc() {$/,/^}$/p' "${ROOT}/build-thirdparty.sh")
check_if_source_exist() { [[ -d "${TP_SOURCE_DIR}/$1" ]]; }

fixture_src="${test_tmp}/fixtures/adbc-source"
mkdir -p "${fixture_src}/c/vendor/sqlite3"
touch "${fixture_src}/c/vendor/sqlite3/sqlite3.c" "${fixture_src}/c/vendor/sqlite3/sqlite3.h"
touch "${fixture_src}/patched_mark" "${fixture_src}/patched_mark_connection_info"
tar czf "${test_tmp}/fixtures/adbc-source.tar.gz" -C "${test_tmp}/fixtures" adbc-source
mkdir -p "${test_tmp}/fixtures/patched-flight"
printf '%s\n' 'isolated Flight driver fixture' > "${test_tmp}/fixtures/patched-flight/libadbc_driver_flightsql.so"
tar czf "${test_tmp}/fixtures/patched-flight.tar.gz" -C "${test_tmp}/fixtures" patched-flight
archive_sha="$(sha256sum "${test_tmp}/fixtures/patched-flight.tar.gz" | awk '{print $1}')"
library_sha="$(sha256sum "${test_tmp}/fixtures/patched-flight/libadbc_driver_flightsql.so" | awk '{print $1}')"
source_md5="$(md5sum "${test_tmp}/fixtures/adbc-source.tar.gz" | awk '{print $1}')"
flight_md5="$(md5sum "${test_tmp}/fixtures/patched-flight.tar.gz" | awk '{print $1}')"

prepare() {
    local dest="$1"
    mkdir -p "${dest}/src"
    cat > "${dest}/vars.sh" <<VARS
TP_SOURCE_DIR="${dest}/src"
TP_INSTALL_DIR="${dest}/installed"
TP_PATCH_DIR="${ROOT}/patches"
ARROW_ADBC_SOURCE=adbc-source
ARROW_ADBC_NAME=adbc-source.tar.gz
ARROW_ADBC_DOWNLOAD=https://example.com/adbc-source.tar.gz
ARROW_ADBC_MD5SUM=${source_md5}
ARROW_ADBC_FLIGHTSQL_SOURCE=patched-flight
ARROW_ADBC_FLIGHTSQL_NAME=patched-flight.tar.gz
ARROW_ADBC_FLIGHTSQL_DOWNLOAD=https://example.com/patched-flight.tar.gz
ARROW_ADBC_FLIGHTSQL_MD5SUM=${flight_md5}
ARROW_ADBC_FLIGHTSQL_SHA256=${archive_sha}
ARROW_ADBC_FLIGHTSQL_LIBRARY_SHA256=${library_sha}
TP_ARCHIVES=(ARROW_ADBC ARROW_ADBC_FLIGHTSQL)
VARS
}

run_download() {
    PATH="${test_tmp}/bin" FIXTURES="${test_tmp}/fixtures" TP_DIR="$1" DORIS_HOME="${test_tmp}" \
        /bin/bash "${ROOT}/download-thirdparty.sh" arrow_adbc
}

run_build() (
    source "$1/vars.sh"
    export PATH="${test_tmp}/bin"
    command -v go >/dev/null 2>&1 && fail 'Go leaked into the fixture PATH'
    KERNEL=Linux
    MACHINE_TYPE=x86_64
    BUILD_DIR=build
    JAVA_HOME="${test_tmp}/jdk"
    CC="${test_tmp}/bin/compiler"
    CXX="${test_tmp}/bin/compiler"
    TP_CXX_STANDARD=20
    TP_INCLUDE_DIR="${TP_INSTALL_DIR}/include"
    CMAKE_CMD=/bin/true
    BUILD_SYSTEM=/bin/true
    GENERATOR=Ninja
    PARALLEL=1
    GO_CMD=go
    mkdir -p "${TP_INSTALL_DIR}/lib64"
    build_arrow_adbc
)

prepare "${test_tmp}/existing"
cp "${test_tmp}/fixtures/"*.tar.gz "${test_tmp}/existing/src/"
tar xzf "${test_tmp}/existing/src/adbc-source.tar.gz" -C "${test_tmp}/existing/src"
tar xzf "${test_tmp}/existing/src/patched-flight.tar.gz" -C "${test_tmp}/existing/src"
run_build "${test_tmp}/existing"
cmp "${test_tmp}/fixtures/patched-flight/libadbc_driver_flightsql.so" \
    "${test_tmp}/existing/installed/lib64/libadbc_driver_flightsql.so"
echo 'PASS: existing prebuilt installation without Go'

prepare "${test_tmp}/fresh"
run_download "${test_tmp}/fresh"
[[ -f "${test_tmp}/fresh/src/patched-flight/libadbc_driver_flightsql.so" ]] \
    || fail 'arrow_adbc did not download its prebuilt companion'
run_build "${test_tmp}/fresh"
cmp "${test_tmp}/fixtures/patched-flight/libadbc_driver_flightsql.so" \
    "${test_tmp}/fresh/installed/lib64/libadbc_driver_flightsql.so"
echo 'PASS: fresh download and installation without Go'

touch "${test_tmp}/fresh/src/patched-flight/cache-sentinel"
run_download "${test_tmp}/fresh"
[[ -f "${test_tmp}/fresh/src/patched-flight/cache-sentinel" ]] || fail 'cached source replaced'
run_build "${test_tmp}/fresh"
echo 'PASS: cached prebuilt installation without Go'

printf '%s\n' 'stale unpatched driver' > "${test_tmp}/fresh/src/patched-flight/libadbc_driver_flightsql.so"
if run_build "${test_tmp}/fresh" > "${test_tmp}/corrupt.log" 2>&1; then
    fail 'modified extracted driver was accepted'
fi
grep -q 'SHA256\|checksum' "${test_tmp}/corrupt.log" || fail 'wrong failure for modified driver'
echo 'PASS: modified cached driver rejected'

cp "${test_tmp}/fixtures/patched-flight/libadbc_driver_flightsql.so" \
    "${test_tmp}/fresh/src/patched-flight/libadbc_driver_flightsql.so"
printf '%s\n' 'modified archive' > "${test_tmp}/fresh/src/patched-flight.tar.gz"
if run_build "${test_tmp}/fresh" > "${test_tmp}/archive.log" 2>&1; then
    fail 'modified archive was accepted'
fi
grep -q 'SHA256\|checksum' "${test_tmp}/archive.log" || fail 'wrong failure for modified archive'
echo 'PASS: modified cached archive rejected'

prepare "${test_tmp}/invalid-checksum"
printf 'ARROW_ADBC_FLIGHTSQL_SHA256=%064d\n' 0 >> "${test_tmp}/invalid-checksum/vars.sh"
if run_download "${test_tmp}/invalid-checksum" > "${test_tmp}/download.log" 2>&1; then
    fail 'archive with a mismatched SHA-256 was unpacked'
fi
grep -q 'checksum' "${test_tmp}/download.log" || fail 'wrong failure for archive SHA-256 mismatch'
[[ ! -d "${test_tmp}/invalid-checksum/src/patched-flight" ]] || fail 'checksum checked after extraction'
echo 'PASS: archive SHA-256 checked before extraction'

for platform in 'Linux x86_64 x86_64' 'Linux aarch64 aarch64' 'Linux arm64 aarch64' \
    'Linux ppc64le none' 'Darwin x86_64 none' 'Darwin arm64 none'; do
    read -r test_os test_arch expected_arch <<< "${platform}"
    TEST_OS="${test_os}" TEST_ARCH="${test_arch}" EXPECTED_ARCH="${expected_arch}" bash -eu -c '
        uname() { if [[ "$1" == -s ]]; then echo "$TEST_OS"; else echo "$TEST_ARCH"; fi; }
        TP_DIR="$1"
        source "$TP_DIR/vars.sh"
        if [[ "$EXPECTED_ARCH" == none ]]; then
            [[ -z "$ARROW_ADBC_FLIGHTSQL_SOURCE" ]]
            [[ " ${TP_ARCHIVES[*]} " != *" ARROW_ADBC_FLIGHTSQL "* ]]
        else
            [[ "$ARROW_ADBC_FLIGHTSQL_SOURCE" == *"-${EXPECTED_ARCH}-glibc2.17" ]]
            [[ " ${TP_ARCHIVES[*]} " == *" ARROW_ADBC_FLIGHTSQL "* ]]
            [[ ${#ARROW_ADBC_FLIGHTSQL_SHA256} == 64 ]]
            [[ ${#ARROW_ADBC_FLIGHTSQL_LIBRARY_SHA256} == 64 ]]
        fi
    ' _ "${ROOT}"
done
echo 'PASS: Linux architectures and unsupported platforms under nounset'
