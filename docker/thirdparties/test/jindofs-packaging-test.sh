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
. "${ROOT}/thirdparty/jindofs_version.sh"
TEST_DIR=$(mktemp -d)
trap 'rm -rf "${TEST_DIR}"' EXIT
FIXTURE="${TEST_DIR}/repo"
mkdir -p "${FIXTURE}/docker/thirdparties" "${FIXTURE}/thirdparty" "${TEST_DIR}/bin"
cp "${ROOT}/post-build.sh" "${FIXTURE}/"
cp "${ROOT}/docker/thirdparties/"*-helpers.sh "${FIXTURE}/docker/thirdparties/"
# Lance JNI replacement is a separate post-build operation, outside this fixture.
printf 'lance_jni_replace() { :; }\n' > "${FIXTURE}/docker/thirdparties/lance-jni-helpers.sh"
printf 'verified test artifact\n' > "${TEST_DIR}/artifact.jar"
TEST_MD5=$(md5sum "${TEST_DIR}/artifact.jar")
TEST_MD5=${TEST_MD5%% *}
sed "s/^\(JINDOFS[A-Z0-9_]*_MD5SUM\)=.*/\1=${TEST_MD5}/" \
    "${ROOT}/thirdparty/jindofs_version.sh" > "${FIXTURE}/thirdparty/jindofs_version.sh"
printf 'JUICEFS_NAME=juicefs-hadoop-1.4.2.jar\nJUICEFS_MD5SUM="%s"\n' \
    "${TEST_MD5}" > "${FIXTURE}/thirdparty/vars.sh"
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
        TARGET_SYSTEM=Linux TARGET_ARCH="$1" \
        bash "${FIXTURE}/post-build.sh" --fe --be --jindofs --output "${TEST_DIR}/output"
}

assert_outputs() {
    local platform_jar="$1"
    local directory jar
    for directory in "${TEST_DIR}/output/fe/lib/jindofs" \
            "${TEST_DIR}/output/be/lib/java_extensions/jindofs"; do
        [[ $(find "${directory}" -name '*.jar' | wc -l) -eq 3 ]] || return 1
        for jar in "${JINDOFS_NAME}" "${JINDOFS_SDK_NAME}" "${platform_jar}"; do
            cmp "${TEST_ARTIFACT}" "${directory}/${jar}" || return 1
        done
    done
}

run_packaging aarch64
assert_outputs "${JINDOFS_AARCH64_NAME}"

# A reused output and mixed cache must contain only the selected architecture/release.
printf old > "${TEST_DIR}/thirdparty/installed/jindofs_libs/jindo-core-6.10.4.jar"
cp "${TEST_DIR}/thirdparty/installed/jindofs_libs/jindo-core-6.10.4.jar" \
    "${TEST_DIR}/output/fe/lib/jindofs/"
cp "${TEST_DIR}/thirdparty/installed/jindofs_libs/jindo-core-6.10.4.jar" \
    "${TEST_DIR}/output/be/lib/java_extensions/jindofs/"
run_packaging x86_64
assert_outputs "${JINDOFS_UBUNTU22_NAME}"

TEST_DOWNLOAD_FAIL=1 run_packaging x86_64
assert_outputs "${JINDOFS_UBUNTU22_NAME}"

printf corrupt > "${TEST_DIR}/thirdparty/installed/jindofs_libs/${JINDOFS_NAME}"
if TEST_DOWNLOAD_FAIL=1 run_packaging x86_64; then
    echo 'FAIL: corrupt cache and failed download were accepted' >&2
    exit 1
fi
assert_outputs "${JINDOFS_UBUNTU22_NAME}"
echo 'PASS: ARM64/x86_64 packaging, stale output, mixed cache, corrupt cache, failed download'
