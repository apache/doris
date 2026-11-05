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

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
. "${ROOT}/docker/thirdparties/juicefs-helpers.sh"
TEST_DIR=$(mktemp -d)
trap 'rm -rf "${TEST_DIR}"' EXIT

fail() {
    echo "FAIL: $*" >&2
    exit 1
}

# Test the real packaging entry point with an isolated artifact and mirror.
FIXTURE="${TEST_DIR}/repo"
mkdir -p "${FIXTURE}/docker/thirdparties" "${FIXTURE}/thirdparty" "${TEST_DIR}/bin"
cp "${ROOT}/post-build.sh" "${FIXTURE}/"
cp "${ROOT}/docker/thirdparties/"*-helpers.sh "${FIXTURE}/docker/thirdparties/"
cp "${ROOT}/thirdparty/jindofs_version.sh" "${FIXTURE}/thirdparty/"
printf 'verified test artifact\n' > "${TEST_DIR}/artifact.jar"
if command -v md5sum >/dev/null 2>&1; then
    TEST_MD5=$(md5sum "${TEST_DIR}/artifact.jar")
    TEST_MD5=${TEST_MD5%% *}
else
    TEST_MD5=$(md5 -q "${TEST_DIR}/artifact.jar")
fi
printf 'JUICEFS_NAME=juicefs-hadoop-%s.jar\nJUICEFS_MD5SUM="%s"\n' \
    "${JUICEFS_PINNED_VERSION}" "${TEST_MD5}" > "${FIXTURE}/thirdparty/vars.sh"
cat > "${TEST_DIR}/bin/curl" <<'CURL'
#!/usr/bin/env bash
[[ "${TEST_DOWNLOAD_FAIL:-0}" == 0 ]] || exit 1
while [[ $# -gt 0 ]]; do
    if [[ "$1" == -o ]]; then
        cp "${TEST_ARTIFACT}" "$2"
        exit 0
    fi
    shift
done
exit 1
CURL
chmod +x "${TEST_DIR}/bin/curl"
export TEST_ARTIFACT="${TEST_DIR}/artifact.jar"
export PATH="${TEST_DIR}/bin:${PATH}"

run_packaging() {
    DORIS_HOME="${FIXTURE}" DORIS_THIRDPARTY="${TEST_DIR}/thirdparty" \
        TARGET_SYSTEM=Darwin TARGET_ARCH=arm64 \
        bash "${FIXTURE}/post-build.sh" --fe --be --juicefs --output "${TEST_DIR}/output"
}

assert_outputs() {
    local directory
    for directory in "${TEST_DIR}/output/fe/lib/juicefs" \
            "${TEST_DIR}/output/be/lib/java_extensions/juicefs"; do
        cmp "${TEST_ARTIFACT}" "${directory}/juicefs-hadoop-${JUICEFS_PINNED_VERSION}.jar" \
            || fail "wrong artifact in ${directory}"
        [[ $(find "${directory}" -name '*.jar' | wc -l) -eq 1 ]] \
            || fail "old artifact remains in ${directory}"
    done
}

# Empty cache downloads the pinned version for both FE and BE.
run_packaging
assert_outputs

# An old cache and old output must not take precedence over the selected release.
rm -rf "${FIXTURE}/thirdparty/installed" "${TEST_DIR}/output"
mkdir -p "${TEST_DIR}/thirdparty/installed/juicefs_libs" \
    "${TEST_DIR}/output/fe/lib/juicefs" "${TEST_DIR}/output/be/lib/java_extensions/juicefs"
printf 'old version' > "${TEST_DIR}/thirdparty/installed/juicefs_libs/juicefs-hadoop-1.3.1.jar"
cp "${TEST_DIR}/thirdparty/installed/juicefs_libs/juicefs-hadoop-1.3.1.jar" \
    "${TEST_DIR}/output/fe/lib/juicefs/"
cp "${TEST_DIR}/thirdparty/installed/juicefs_libs/juicefs-hadoop-1.3.1.jar" \
    "${TEST_DIR}/output/be/lib/java_extensions/juicefs/"
run_packaging
assert_outputs

# A mixed cache still selects only the pinned release, with no network access.
cp "${TEST_ARTIFACT}" \
    "${TEST_DIR}/thirdparty/installed/juicefs_libs/juicefs-hadoop-${JUICEFS_PINNED_VERSION}.jar"
TEST_DOWNLOAD_FAIL=1 run_packaging
assert_outputs

# A wrong checksum must fail packaging rather than silently retain an old release.
printf 'corrupt version' > \
    "${TEST_DIR}/thirdparty/installed/juicefs_libs/juicefs-hadoop-${JUICEFS_PINNED_VERSION}.jar"
if run_packaging; then
    fail "corrupt cache was accepted"
fi
assert_outputs

rm -f "${TEST_DIR}/thirdparty/installed/juicefs_libs/juicefs-hadoop-${JUICEFS_PINNED_VERSION}.jar"
rm -rf "${FIXTURE}/thirdparty/installed"
if TEST_DOWNLOAD_FAIL=1 run_packaging; then
    fail "failed download was accepted"
fi
assert_outputs

echo "PASS: empty cache, old cache/output, mixed cache, corrupt cache, failed download"
