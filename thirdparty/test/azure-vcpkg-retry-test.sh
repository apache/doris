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

# azure is the last package build-thirdparty.sh builds, and it is the only one
# that fetches sources itself: vcpkg downloads curl, libxml2, openssl and zlib
# while cmake configures, outside download-thirdparty.sh and its mirror. In
# apache/doris run 32032837067 all three jobs spent between 1h26m and 2h19m
# building the other 73 packages and then threw the tree away because
# github.com/madler/zlib/archive/v1.3.1.tar.gz answered 429. This test pins the
# two halves of the answer to that: a failed download is waited out, and any
# other failure still stops on the first attempt rather than repeating a broken
# build four more times.

set -eo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." &>/dev/null && pwd)"

fail() {
    echo "FAIL: $*" >&2
    exit 1
}

assert_eq() {
    local expected="$1"
    local actual="$2"
    local what="$3"
    [[ "${actual}" == "${expected}" ]] || fail "${what}: expected '${expected}', got '${actual}'"
}

tmpdir="$(mktemp -d)"
trap 'rm -rf "${tmpdir}"' EXIT

# build-thirdparty.sh downloads and compiles the whole tree before it defines a
# single build_* function, so the function under test is lifted out of it rather
# than sourced. Extracting it keeps the test honest: it runs the same text that
# ships, and it breaks loudly if build_azure is renamed or reindented.
azure_fn="${tmpdir}/build_azure.sh"
sed -n '/^build_azure() {$/,/^}$/p' "${ROOT}/build-thirdparty.sh" >"${azure_fn}"
[[ -s "${azure_fn}" ]] || fail "could not extract build_azure() from build-thirdparty.sh"
grep -q '^}$' "${azure_fn}" || fail "extracted build_azure() is not terminated"

# A cmake that fails the first ${CMAKE_FAILURES} times it is called, the way
# vcpkg reports whatever CMAKE_FAILURE_KIND asks for, and counts its calls.
cmake_stub="${tmpdir}/cmake"
cat >"${cmake_stub}" <<'EOF'
#!/usr/bin/env bash
set -eo pipefail
calls="$(cat "${CMAKE_CALL_COUNT}" 2>/dev/null || echo 0)"
calls=$((calls + 1))
echo "${calls}" >"${CMAKE_CALL_COUNT}"
if [[ "${calls}" -gt "${CMAKE_FAILURES}" ]]; then
    echo '-- Configuring done'
    exit 0
fi
if [[ "${CMAKE_FAILURE_KIND}" == 'download' ]]; then
    echo 'error: curl: (22) The requested URL returned error: 429'
    echo 'CMake Error at scripts/cmake/vcpkg_download_distfile.cmake:136 (message):'
    echo '  Download failed, halting portfile.'
else
    echo 'CMake Error: CMAKE_CXX_COMPILER not set, after EnableLanguage'
fi
exit 1
EOF
chmod +x "${cmake_stub}"

ninja_stub="${tmpdir}/ninja"
printf '#!/usr/bin/env bash\nexit 0\n' >"${ninja_stub}"
chmod +x "${ninja_stub}"

# Everything build_azure reads that the script normally sets up around it.
export BUILD_AZURE='ON'
export AZURE_SOURCE='azure-sdk-for-cpp-azure-core_1.16.0'
export BUILD_DIR='doris_build'
export TP_SOURCE_DIR="${tmpdir}/src"
export TP_INSTALL_DIR="${tmpdir}/installed"
export CMAKE_CMD="${cmake_stub}"
export BUILD_SYSTEM="${ninja_stub}"
export GENERATOR='Ninja'
export PARALLEL=1
KERNEL="$(uname -s)"
export KERNEL
export CMAKE_CALL_COUNT="${tmpdir}/cmake-calls"
mkdir -p "${TP_SOURCE_DIR}/${AZURE_SOURCE}/vcpkg-custom-ports" "${TP_INSTALL_DIR}"

# shellcheck source=/dev/null
. "${azure_fn}"

# The script checks the unpacked source is there; the fixture above is enough.
check_if_source_exist() { :; }

SLEEP_LOG="${tmpdir}/sleeps"
recorded_downloads="${tmpdir}/vcpkg-downloads"

# build_azure calls exit on a failure it will not retry, so each case runs in a
# subshell and reports back through files. The backoff is minutes by design; the
# stub records what it was asked to wait instead of waiting.
run_case() {
    export CMAKE_FAILURES="$1"
    export CMAKE_FAILURE_KIND="$2"
    rm -f "${CMAKE_CALL_COUNT}" "${SLEEP_LOG}"
    : >"${SLEEP_LOG}"
    status=0
    (
        # Called by build_azure, and VCPKG_DOWNLOADS is set by it.
        # shellcheck disable=SC2329
        sleep() { printf '%s\n' "$1" >>"${SLEEP_LOG}"; }
        build_azure
        # shellcheck disable=SC2153,SC2154
        printf '%s\n' "${VCPKG_DOWNLOADS}" >"${recorded_downloads}"
    ) >"${tmpdir}/out.log" 2>&1 || status=$?
    calls="$(cat "${CMAKE_CALL_COUNT}" 2>/dev/null || echo 0)"
    sleeps="$(awk 'END { print NR }' "${SLEEP_LOG}")"
}

# 1. Two failed downloads then a good one: build_azure rides it out.
run_case 2 download
assert_eq 0 "${status}" 'a download that recovers should not fail the build'
assert_eq 3 "${calls}" 'cmake should be retried until the download works'
assert_eq 2 "${sleeps}" 'each retry should back off first'
first_backoff="$(sed -n '1p' "${SLEEP_LOG}")"
second_backoff="$(sed -n '2p' "${SLEEP_LOG}")"
[[ "${first_backoff}" -lt "${second_backoff}" ]] ||
    fail "backoff should grow, got ${first_backoff} then ${second_backoff}"

# 2. vcpkg keeps its downloads outside the build directory, which build_azure
#    wipes on entry, so a rerun does not refetch what already landed.
vcpkg_downloads="$(cat "${recorded_downloads}")"
[[ -n "${vcpkg_downloads}" ]] || fail 'VCPKG_DOWNLOADS should be set for vcpkg'
[[ -d "${vcpkg_downloads}" ]] || fail "VCPKG_DOWNLOADS ${vcpkg_downloads} should exist"
case "${vcpkg_downloads}" in
*"/${BUILD_DIR}/"* | *"/${BUILD_DIR}")
    fail "VCPKG_DOWNLOADS ${vcpkg_downloads} is inside the wiped build directory"
    ;;
*) ;;
esac

# 3. A failure that is not a download stops on the first attempt. Repeating a
#    configure that cannot work only buries the error under four more copies.
run_case 99 hard
[[ "${status}" -ne 0 ]] || fail 'a broken configure should fail the build'
assert_eq 1 "${calls}" 'a non-download failure should not be retried'
assert_eq 0 "${sleeps}" 'a non-download failure should not sleep'
grep -q 'not on a download' "${tmpdir}/out.log" ||
    fail 'the build should say why it did not retry'

# 4. A download that never recovers gives up, but only after it has waited.
run_case 99 download
[[ "${status}" -ne 0 ]] || fail 'an unreachable source should fail the build'
[[ "${calls}" -ge 3 ]] || fail "expected several attempts, got ${calls}"
assert_eq "$((calls - 1))" "${sleeps}" 'every attempt but the first should follow a sleep'

echo 'PASS: build_azure waits out a failed vcpkg download and fails fast on anything else'
