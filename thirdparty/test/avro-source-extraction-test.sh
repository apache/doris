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

set -eo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." &>/dev/null && pwd)"
tmpdir="$(mktemp -d)"
trap 'rm -rf "${tmpdir}"' EXIT

fail() {
    echo "FAIL: $*" >&2
    exit 1
}

digest() {
    if command -v md5sum >/dev/null 2>&1; then
        md5sum "$1" | awk '{print $1}'
    else
        md5 -q "$1"
    fi
}

mkdir -p "${tmpdir}/input/avro-fixture/lang/c++/include/avro" \
    "${tmpdir}/input/avro-fixture/lang/java/fixtures" \
    "${tmpdir}/input/other-fixture/lang/java" "${tmpdir}/bin" "${tmpdir}/home" "${tmpdir}/src"
printf 'header\n' >"${tmpdir}/input/avro-fixture/lang/c++/include/avro/Generic.hh"
printf 'source\n' >"${tmpdir}/input/avro-fixture/lang/c++/CMakeLists.txt"
printf 'license\n' >"${tmpdir}/input/avro-fixture/LICENSE.txt"
printf 'notice\n' >"${tmpdir}/input/avro-fixture/NOTICE.txt"
# Hadoop CRC sidecars are Java test data, not AppleDouble metadata or C++ build inputs.
printf 'crc\000\000\000\002\000' >"${tmpdir}/input/avro-fixture/lang/java/fixtures/._SUCCESS.crc"
printf 'other payload\n' >"${tmpdir}/input/other-fixture/lang/java/payload"
tar czf "${tmpdir}/src/avro.tar.gz" -C "${tmpdir}/input" avro-fixture
tar czf "${tmpdir}/src/other.tar.gz" -C "${tmpdir}/input" other-fixture

# Every archive is cached and checksummed; an unexpected network request must fail the test.
cat >"${tmpdir}/bin/wget" <<'EOF'
#!/usr/bin/env bash
echo 'Unexpected download' >&2
exit 1
EOF
chmod +x "${tmpdir}/bin/wget"

write_vars() {
    cat >"${tmpdir}/vars.sh" <<EOF
export TP_SOURCE_DIR="${tmpdir}/src"
export TP_INSTALL_DIR="${tmpdir}/installed"
export TP_PATCH_DIR="${tmpdir}/patches"
AVRO_DOWNLOAD="https://example.com/avro.tar.gz"
AVRO_NAME="avro.tar.gz"
AVRO_SOURCE="avro-fixture"
AVRO_MD5SUM="$(digest "${tmpdir}/src/avro.tar.gz")"
OTHER_DOWNLOAD="https://example.com/other.tar.gz"
OTHER_NAME="other.tar.gz"
OTHER_SOURCE="other-fixture"
OTHER_MD5SUM="$(digest "${tmpdir}/src/other.tar.gz")"
export TP_ARCHIVES=('AVRO' 'OTHER')
EOF
}

run_download() {
    PATH="${tmpdir}/bin:${PATH}" TP_DIR="${tmpdir}" DORIS_HOME="${tmpdir}/home" \
        bash "${ROOT}/download-thirdparty.sh" "$@" >"${tmpdir}/download.log" 2>&1
}

write_vars
if ! run_download; then
    cat "${tmpdir}/download.log"
    fail 'expected cached source extraction to succeed'
fi
[[ -f "${tmpdir}/src/avro-fixture/lang/c++/include/avro/Generic.hh" ]] || fail 'C++ headers missing'
[[ -f "${tmpdir}/src/avro-fixture/lang/c++/CMakeLists.txt" ]] || fail 'C++ build files missing'
[[ -f "${tmpdir}/src/avro-fixture/LICENSE.txt" ]] || fail 'license missing'
[[ -f "${tmpdir}/src/avro-fixture/NOTICE.txt" ]] || fail 'notice missing'
[[ ! -e "${tmpdir}/src/avro-fixture/lang/java" ]] || fail 'unrelated Java fixtures were extracted'
[[ -f "${tmpdir}/src/other-fixture/lang/java/payload" ]] || fail 'other archives were filtered'
run_download || fail 'reusing extracted sources failed'

# Selecting required C++ members must not turn a malformed source archive into a successful build.
rm -rf "${tmpdir}/src/avro-fixture" "${tmpdir}/input/avro-fixture/lang/c++"
tar czf "${tmpdir}/src/avro.tar.gz" -C "${tmpdir}/input" avro-fixture
write_vars
if run_download avro; then
    fail 'archive without the required C++ tree was accepted'
fi

echo 'PASS'
