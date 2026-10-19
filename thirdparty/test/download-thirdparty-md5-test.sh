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

# download-thirdparty.sh used to disable every checksum when GNU md5sum was
# absent, which is the normal state on macOS. That is not a neutral loss of
# coverage: an empty or truncated response is then indistinguishable from a
# valid archive, and gets treated as one. A 0-byte header reached the published
# thirdparty source bundle exactly that way, because the job that builds it runs
# on macOS. This test pins the invariant that a bad download is rejected on a
# host that only has BSD md5.

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

command -v md5sum >/dev/null 2>&1 || fail "this test needs md5sum on the host to build its fixtures"

tmpdir="$(mktemp -d)"
trap 'rm -rf "${tmpdir}"' EXIT

payload='thirdparty archive payload'
payload_md5="$(printf '%s' "${payload}" | md5sum | awk '{ print $1 }')"

# download-thirdparty.sh resolves its helpers through PATH, so imitating macOS
# only takes a PATH that holds everything the script needs except md5sum. That
# runs on any host, which is the point -- the platform this protects is the one
# CI has the least of.
sandbox_bin="${tmpdir}/bin"
mkdir -p "${sandbox_bin}"
for tool in bash awk sed tr rm mkdir dirname basename uname cat cut head ls find sort tar unzip git curl env readlink; do
    tool_path="$(command -v "${tool}" 2>/dev/null)" || continue
    ln -sf "${tool_path}" "${sandbox_bin}/${tool}"
done

# BSD md5 as macOS ships it: `md5 -q FILE` prints the digest and nothing else.
cat >"${sandbox_bin}/md5" <<EOF
#!/usr/bin/env bash
set -eo pipefail
[[ "\$1" == '-q' ]] || { echo 'stub md5 only implements -q' >&2; exit 64; }
$(command -v md5sum) "\$2" | awk '{ print \$1 }'
EOF
chmod +x "${sandbox_bin}/md5"

PATH="${sandbox_bin}" command -v md5sum >/dev/null 2>&1 &&
    fail "sandbox PATH still exposes md5sum, the test would not prove anything"

cat >"${tmpdir}/vars.sh" <<EOF
#!/bin/bash
export TP_SOURCE_DIR="${tmpdir}/src"
export TP_INSTALL_DIR="${tmpdir}/installed"
export TP_PATCH_DIR="${tmpdir}/patches"
export TP_INCLUDE_DIR="\${TP_INSTALL_DIR}/include"
export TP_LIB_DIR="\${TP_INSTALL_DIR}/lib"
export TP_JAR_DIR="\${TP_INSTALL_DIR}/lib/jar"
FOO_DOWNLOAD="https://example.com/foo.jar"
FOO_NAME="foo.jar"
FOO_SOURCE=
FOO_MD5SUM="${payload_md5}"
export TP_ARCHIVES=('FOO')
EOF

# A wget that reports success and writes nothing, which is what an empty 200
# response looks like. Only a checksum can catch this one, so it fails the run
# if and only if verification really happened.
write_wget() {
    local body="$1"
    cat >"${sandbox_bin}/wget" <<EOF
#!/usr/bin/env bash
set -eo pipefail
output=''
expect_output=0
for arg in "\$@"; do
    if [[ "\${expect_output}" -eq 1 ]]; then
        output="\${arg}"
        expect_output=0
        continue
    fi
    [[ "\${arg}" == '-O' ]] && expect_output=1
done
printf '%s' '${body}' >"\${output}"
exit 0
EOF
    chmod +x "${sandbox_bin}/wget"
}

run_download() {
    PATH="${sandbox_bin}" TP_DIR="${tmpdir}" DORIS_HOME="${ROOT}/.." \
        bash "${ROOT}/download-thirdparty.sh" 2>&1
}

echo '== an empty response must not pass as a valid archive =='
rm -rf "${tmpdir}/src"
write_wget ''
if output="$(run_download)"; then
    echo "${output}"
    fail 'download-thirdparty.sh accepted an empty archive'
fi
case "${output}" in
*'md5sum check failed'*) ;;
*)
    echo "${output}"
    fail 'expected the empty archive to be rejected by its checksum'
    ;;
esac
case "${output}" in
*'will not be verified'*)
    echo "${output}"
    fail 'verification was skipped, BSD md5 was not picked up'
    ;;
esac
[[ -f "${tmpdir}/src/foo.jar" ]] && fail 'the rejected archive was left behind for the next run to reuse'

echo '== a good archive must still be accepted through BSD md5 =='
rm -rf "${tmpdir}/src"
write_wget "${payload}"
if ! output="$(run_download)"; then
    echo "${output}"
    fail 'download-thirdparty.sh rejected a valid archive'
fi
[[ -f "${tmpdir}/src/foo.jar" ]] || fail "expected downloaded archive at ${tmpdir}/src/foo.jar"
assert_eq "${payload}" "$(cat "${tmpdir}/src/foo.jar")"

echo 'PASS'
