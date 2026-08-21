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

fail() {
    echo "FAIL: $*" >&2
    exit 1
}

assert_eq() {
    local expected="$1"
    local actual="$2"
    [[ "${actual}" == "${expected}" ]] || fail "expected '${expected}', got '${actual}'"
}

tmpdir="$(mktemp -d)"
trap 'rm -rf "${tmpdir}"' EXIT

stub_bin="${tmpdir}/bin"
mkdir -p "${stub_bin}"
download_log="${tmpdir}/wget.log"
payload="juicefs jar payload"
payload_md5="$(printf '%s' "${payload}" | md5sum | awk '{print $1}')"

cat > "${tmpdir}/vars.sh" <<EOF
#!/bin/bash
export TP_SOURCE_DIR="${tmpdir}/src"
export TP_INSTALL_DIR="${tmpdir}/installed"
export TP_PATCH_DIR="${tmpdir}/patches"
export TP_INCLUDE_DIR="\${TP_INSTALL_DIR}/include"
export TP_LIB_DIR="\${TP_INSTALL_DIR}/lib"
export TP_JAR_DIR="\${TP_INSTALL_DIR}/lib/jar"
JUICEFS_DOWNLOAD="https://repo1.maven.org/maven2/io/juicefs/juicefs-hadoop/1.3.1/juicefs-hadoop-1.3.1.jar"
JUICEFS_NAME="juicefs-hadoop-1.3.1.jar"
JUICEFS_SOURCE=
JUICEFS_MD5SUM="${payload_md5}"
export TP_ARCHIVES=('JUICEFS')
EOF

cat > "${stub_bin}/wget" <<EOF
#!/usr/bin/env bash
set -eo pipefail
url=""
output=""
expect_output=0
for arg in "\$@"; do
    if [[ "\${expect_output}" -eq 1 ]]; then
        output="\${arg}"
        expect_output=0
        continue
    fi
    if [[ "\${arg}" == "-O" ]]; then
        expect_output=1
        continue
    fi
    if [[ "\${arg}" != -* && -z "\${url}" ]]; then
        url="\${arg}"
    fi
done
echo "\${url}" >> "${download_log}"
if [[ "\${url}" == "https://repo1.maven.org/maven2/io/juicefs/juicefs-hadoop/1.3.1/juicefs-hadoop-1.3.1.jar" ]]; then
    printf '%s' "${payload}" > "\${output}"
    exit 0
fi
exit 1
EOF
chmod +x "${stub_bin}/wget"

if ! PATH="${stub_bin}:${PATH}" s3BucketName="test-bucket" s3Endpoint="oss.example.com" \
    TP_DIR="${tmpdir}" DORIS_HOME="${ROOT}/.." \
    bash "${ROOT}/download-thirdparty.sh"; then
    fail "expected download-thirdparty.sh to fall back from S3 mirror to Maven for JUICEFS"
fi

[[ -f "${tmpdir}/src/juicefs-hadoop-1.3.1.jar" ]] || fail "expected downloaded archive"
assert_eq "${payload}" "$(cat "${tmpdir}/src/juicefs-hadoop-1.3.1.jar")"

expected_log=$'https://test-bucket.oss.example.com/regression/datalake/thirdparty/juicefs/juicefs-hadoop-1.3.1.jar\nhttps://test-bucket.oss.example.com/regression/datalake/thirdparty/juicefs/juicefs-hadoop-1.3.1.jar\nhttps://repo1.maven.org/maven2/io/juicefs/juicefs-hadoop/1.3.1/juicefs-hadoop-1.3.1.jar'
assert_eq "${expected_log}" "$(cat "${download_log}")"

echo "PASS"
