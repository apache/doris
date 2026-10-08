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
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT
mkdir -p "${work}/repo/thirdparty/patches" "${work}/external"
cp "${ROOT}/thirdparty/vars.sh" "${work}/repo/thirdparty/"
cp "${ROOT}/thirdparty/patches/lance-c-foyer.patch" "${work}/repo/thirdparty/patches/"
# Extract the real build gate; all destructive operations stay inside this temporary install.
echo 'set -eo pipefail' > "${work}/gate.sh"
sed -n '/^# build thirdparty libraries if necessary/,/^update_submodule()/p' "${ROOT}/build.sh" \
    | sed '$d' >> "${work}/gate.sh"
export DORIS_HOME="${work}/repo" DORIS_THIRDPARTY="${work}/external"
export TARGET_SYSTEM=Linux CLEAN=0 PARALLEL=1
export TEST_HELPER="${ROOT}/thirdparty/lance-install.sh"
cp "${TEST_HELPER}" "${work}/repo/thirdparty/"
cp -r "${DORIS_HOME}/thirdparty/." "${DORIS_THIRDPARTY}/"
printf '#!/usr/bin/env bash\nexit 0\n' > "${DORIS_THIRDPARTY}/download-thirdparty.sh"
cat > "${DORIS_THIRDPARTY}/build-thirdparty.sh" <<'BUILDER'
set -euo pipefail
TP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${TP_DIR}/lance-install.sh"
fingerprint="$(lance_c_install_fingerprint "${TP_DIR}")"
echo rebuilt >> "${DORIS_THIRDPARTY}/builds"
installed="${DORIS_THIRDPARTY}/installed"
mkdir -p "${installed}/lib/hadoop_hdfs/native" "${installed}/lib/hadoop_hdfs_3_4/native" "${installed}/lib64" \
    "${installed}/include/lance" "${installed}/include/paimon_rust"
for file in lib/hadoop_hdfs/native/libhdfs.a lib/hadoop_hdfs_3_4/native/libhdfs.a \
    lib64/liblance_c.a lib64/libpaimon_c.a \
    include/lance/lance.h include/lance/lance.hpp include/paimon_rust/paimon.h; do
    echo artifact > "${installed}/${file}"
done
printf '%s\n' "${fingerprint}" > "${installed}/lib64/.lance-c-fingerprint"
BUILDER
# A complete legacy image has all old sentinels but no Lance revision marker.
installed="${DORIS_THIRDPARTY}/installed"
mkdir -p "${installed}/lib/hadoop_hdfs/native" "${installed}/lib/hadoop_hdfs_3_4/native" "${installed}/lib64" \
    "${installed}/include/lance" "${installed}/include/paimon_rust"
for file in lib/hadoop_hdfs/native/libhdfs.a lib/hadoop_hdfs_3_4/native/libhdfs.a \
    lib64/liblance_c.a lib64/libpaimon_c.a \
    include/lance/lance.h include/lance/lance.hpp include/paimon_rust/paimon.h; do
    echo legacy > "${installed}/${file}"
done
bash "${work}/gate.sh"
[[ -f "${DORIS_THIRDPARTY}/builds" ]] || { echo 'FAIL: reused unversioned Lance archive'; exit 1; }
source "${TEST_HELPER}"
lance_c_install_is_current "${DORIS_HOME}/thirdparty" "${installed}"
check_builds() {
    [[ "$(wc -l < "${DORIS_THIRDPARTY}/builds")" -eq "$1" ]]
}
check_builds 1
bash "${work}/gate.sh"
check_builds 1
echo 'PASS: legacy image rebuilt; matching install reused'
# Failed preflight must preserve every dependency, not just the Lance archive.
expect_preserved_install() {
    cp -a "${installed}" "${work}/saved-install"
    cp "${DORIS_THIRDPARTY}/builds" "${work}/saved-builds"
    if bash "${work}/gate.sh" > "${work}/preflight.log" 2>&1; then
        echo 'FAIL: accepted incomplete or mismatched rebuild sources'; exit 1
    fi
    if ! diff -r "${work}/saved-install" "${installed}"; then
        echo 'FAIL: rejected rebuild sources after changing installed dependencies'; exit 1
    fi
    cmp "${work}/saved-builds" "${DORIS_THIRDPARTY}/builds"
    rm -rf "${work}/saved-install"
}
echo stale > "${installed}/lib64/.lance-c-fingerprint"
for file in lance-install.sh vars.sh patches/lance-c-foyer.patch download-thirdparty.sh build-thirdparty.sh; do
    mv "${DORIS_THIRDPARTY}/${file}" "${work}/missing-input"
    expect_preserved_install
    mv "${work}/missing-input" "${DORIS_THIRDPARTY}/${file}"
done
echo 'PASS: incomplete external sources rejected before changing installed dependencies'
echo stale > "${installed}/lib64/.lance-c-fingerprint"
bash "${work}/gate.sh"
check_builds 2
# Pin and patch updates both invalidate installed artifacts, even with identical ABI.
sed 's/^LANCE_C_SOURCE=.*/LANCE_C_SOURCE="lance-c-test-revision"/' "${DORIS_HOME}/thirdparty/vars.sh" \
    > "${work}/new-vars.sh"
mv "${work}/new-vars.sh" "${DORIS_HOME}/thirdparty/vars.sh"
expect_preserved_install
cp "${DORIS_HOME}/thirdparty/vars.sh" "${DORIS_THIRDPARTY}/vars.sh"
bash "${work}/gate.sh"
check_builds 3
echo '# test patch update' >> "${DORIS_HOME}/thirdparty/patches/lance-c-foyer.patch"
expect_preserved_install
cp "${DORIS_HOME}/thirdparty/patches/lance-c-foyer.patch" "${DORIS_THIRDPARTY}/patches/"
bash "${work}/gate.sh"
check_builds 4
echo 'PASS: mismatched sources rejected; synchronized pin and patch updates rebuild'
rm "${installed}/include/lance/lance.h"
bash "${work}/gate.sh"
check_builds 5
: > "${installed}/lib64/liblance_c.a"
bash "${work}/gate.sh"
check_builds 6
echo 'PASS: incomplete header/archive install rebuilt'
# A legacy external builder may exit successfully without installing the new revision.
cp "${DORIS_THIRDPARTY}/build-thirdparty.sh" "${work}/good-builder.sh"
printf '#!/usr/bin/env bash\nexit 0\n' > "${DORIS_THIRDPARTY}/build-thirdparty.sh"
rm "${installed}/lib64/.lance-c-fingerprint"
if bash "${work}/gate.sh" > "${work}/old-builder.log" 2>&1; then
    echo 'FAIL: accepted output from a stale external builder'; exit 1
fi
grep -q 'Lance dependency revision does not match' "${work}/old-builder.log"
cp "${work}/good-builder.sh" "${DORIS_THIRDPARTY}/build-thirdparty.sh"
bash "${DORIS_THIRDPARTY}/build-thirdparty.sh"
echo 'PASS: stale external builder cannot silently satisfy the revision gate'
# Images without rebuild sources must fail before deleting installed dependencies.
rm "${DORIS_THIRDPARTY}/build-thirdparty.sh" "${installed}/lib64/.lance-c-fingerprint"
if bash "${work}/gate.sh" > "${work}/missing-source.log" 2>&1; then
    echo 'FAIL: accepted stale compilation image without build sources'; exit 1
fi
[[ -s "${installed}/lib64/liblance_c.a" ]]
echo 'PASS: missing rebuild source fails without deleting installed artifacts'
# Execute the actual publication function with a tiny Cargo stand-in. A failed
# archive copy must invalidate the old marker, and only a complete retry may stamp it.
sed -n '/^build_lance_c()/,/^}/p' "${ROOT}/thirdparty/build-thirdparty.sh" > "${work}/publish-function.sh"
export TP_DIR="${DORIS_HOME}/thirdparty"
source "${TP_DIR}/vars.sh"
export LANCE_C_SOURCE TP_SOURCE_DIR TP_INSTALL_DIR
export LANCE_C_INSTALL_FINGERPRINT="$(lance_c_install_fingerprint "${TP_DIR}")"
mkdir -p "${TP_SOURCE_DIR}/${LANCE_C_SOURCE}/include/lance" "${TP_INSTALL_DIR}/bin"
echo header > "${TP_SOURCE_DIR}/${LANCE_C_SOURCE}/include/lance/lance.h"
echo header > "${TP_SOURCE_DIR}/${LANCE_C_SOURCE}/include/lance/lance.hpp"
printf '#!/bin/sh\nexit 0\n' > "${TP_INSTALL_DIR}/bin/protoc"
chmod +x "${TP_INSTALL_DIR}/bin/protoc"
cat > "${work}/cargo" <<'CARGO'
#!/usr/bin/env bash
set -eu
if [[ "$1" == --version ]]; then
    echo 'cargo 1.94.0'; exit 0
fi
mkdir -p "${CARGO_TARGET_DIR}/release"
echo archive > "${CARGO_TARGET_DIR}/release/liblance_c.a"
CARGO
chmod +x "${work}/cargo"
export LANCE_C_CARGO="${work}/cargo" RUSTUP_TOOLCHAIN=1.94.0
export BUILD_DIR=build KERNEL=Linux LANCE_C_CARGO_OFFLINE=OFF
cat > "${work}/publish.sh" <<'PUBLISH'
set -eo pipefail
check_if_source_exist() { :; }
install_rust_archive() {
    if [[ "${FAIL_INSTALL:-0}" == 1 ]]; then return 1; fi
    cp "$1" "${TP_INSTALL_DIR}/lib64/liblance_c.a"
}
source "$1"
build_lance_c
PUBLISH
bash "${work}/publish.sh" "${work}/publish-function.sh" > "${work}/publish.log" 2>&1
lance_c_install_is_current "${TP_DIR}" "${TP_INSTALL_DIR}"
if FAIL_INSTALL=1 bash "${work}/publish.sh" "${work}/publish-function.sh" >> "${work}/publish.log" 2>&1; then
    echo 'FAIL: expected archive publication failure'; exit 1
fi
[[ ! -e "${TP_INSTALL_DIR}/lib64/.lance-c-fingerprint" ]]
bash "${work}/publish.sh" "${work}/publish-function.sh" >> "${work}/publish.log" 2>&1
lance_c_install_is_current "${TP_DIR}" "${TP_INSTALL_DIR}"
echo 'PASS: successful install stamped; failed publication invalidated; retry repaired'
