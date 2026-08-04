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

tmpdir="$(mktemp -d)"
trap 'rm -rf "${tmpdir}"' EXIT

harness="${tmpdir}/harness"
mkdir -p "${harness}/src" "${harness}/patches" "${harness}/installed"
cp "${ROOT}/download-thirdparty.sh" "${harness}/download-thirdparty.sh"

create_patch() {
    local patch_file="$1"
    local source_file="$2"
    {
        printf '%s\n' "--- a/${source_file}"
        printf '%s\n' "+++ b/${source_file}"
        printf '%s\n' '@@ -1 +1 @@'
        printf '%s\n' '-original'
        printf '%s\n' '+patched'
    } >"${patch_file}"
}

create_archive() {
    local source_name="$1"
    local archive_name="$2"
    local prefix="$3"
    local index

    mkdir -p "${harness}/src/${source_name}"
    for index in 1 2 3; do
        printf '%s\n' original >"${harness}/src/${source_name}/${prefix}-${index}.txt"
    done
    tar -czf "${harness}/src/${archive_name}" -C "${harness}/src" "${source_name}"
    rm -rf "${harness}/src/${source_name}"
}

arrow_source="arrow-apache-arrow-24.0.0"
arrow_archive="apache-arrow-24.0.0.tar.gz"
paimon_source="doris-thirdparty-paimon-cpp-0a4f4e2"
paimon_archive="paimon-cpp-0a4f4e2.tar.gz"

create_archive "${arrow_source}" "${arrow_archive}" arrow
create_archive "${paimon_source}" "${paimon_archive}" paimon

arrow_patches=(
    apache-arrow-24.0.0-paimon.patch
    apache-arrow-24.0.0-force-write-int96-timestamps.patch
    apache-arrow-24.0.0-lzo.patch
)
paimon_patches=(
    paimon-cpp-buildutils-static-deps.patch
    paimon-cpp-arrow-24-compatibility.patch
    paimon-cpp-arrow-24-compute.patch
)

for index in 0 1 2; do
    create_patch "${harness}/patches/${arrow_patches[${index}]}" "arrow-$((index + 1)).txt"
    create_patch "${harness}/patches/${paimon_patches[${index}]}" "paimon-$((index + 1)).txt"
done

arrow_md5="$(md5sum "${harness}/src/${arrow_archive}" | awk '{print $1}')"
paimon_md5="$(md5sum "${harness}/src/${paimon_archive}" | awk '{print $1}')"
{
    printf 'TP_SOURCE_DIR="%s"\n' "${harness}/src"
    printf 'TP_INSTALL_DIR="%s"\n' "${harness}/installed"
    printf 'TP_PATCH_DIR="%s"\n' "${harness}/patches"
    printf '%s\n' 'TP_ARCHIVES=(ARROW PAIMON_CPP)'
    printf 'ARROW_NAME="%s"\n' "${arrow_archive}"
    printf 'ARROW_SOURCE="%s"\n' "${arrow_source}"
    printf 'ARROW_MD5SUM="%s"\n' "${arrow_md5}"
    printf '%s\n' 'ARROW_DOWNLOAD="unused"'
    printf 'PAIMON_CPP_NAME="%s"\n' "${paimon_archive}"
    printf 'PAIMON_CPP_SOURCE="%s"\n' "${paimon_source}"
    printf 'PAIMON_CPP_MD5SUM="%s"\n' "${paimon_md5}"
    printf '%s\n' 'PAIMON_CPP_DOWNLOAD="unused"'
    printf '%s\n' 'arrow_paimon_build_fingerprint() { printf "%s\n" test-fingerprint; }'
} >"${harness}/vars.sh"

exercise_interrupted_patch_set() {
    local package="$1"
    local source_name="$2"
    local archive_name="$3"
    local prefix="$4"
    shift 4
    local patches=("$@")
    local applied
    local index

    for applied in 1 2 3; do
        rm -rf "${harness}/src/${source_name}"
        tar -xzf "${harness}/src/${archive_name}" -C "${harness}/src"
        for ((index = 0; index < applied; ++index)); do
            (
                cd "${harness}/src/${source_name}"
                patch -s -p1 <"${harness}/patches/${patches[${index}]}"
            )
        done

        TP_DIR="${harness}" DORIS_HOME="${tmpdir}" \
            bash "${harness}/download-thirdparty.sh" "${package}" >/dev/null

        for index in 1 2 3; do
            [[ "$(<"${harness}/src/${source_name}/${prefix}-${index}.txt")" == "patched" ]] ||
                fail "${package} did not recover after interruption boundary ${applied}"
        done
        [[ "$(<"${harness}/src/${source_name}/patched_mark_arrow_paimon_fingerprint")" == "test-fingerprint" ]] ||
            fail "${package} fingerprint marker is missing"

        touch "${harness}/src/${source_name}/idempotence-sentinel"
        TP_DIR="${harness}" DORIS_HOME="${tmpdir}" \
            bash "${harness}/download-thirdparty.sh" "${package}" >/dev/null
        [[ -f "${harness}/src/${source_name}/idempotence-sentinel" ]] ||
            fail "${package} reset a completely patched source tree"
    done
}

exercise_interrupted_patch_set ARROW "${arrow_source}" "${arrow_archive}" arrow \
    "${arrow_patches[@]}"
exercise_interrupted_patch_set PAIMON_CPP "${paimon_source}" "${paimon_archive}" paimon \
    "${paimon_patches[@]}"

# A Paimon-only build may publish only its own fingerprint. It must not make a
# stale Arrow installation pass the shared prebuilt validation.
. "${ROOT}/arrow-paimon-vars.sh"
prebuilt="${tmpdir}/prebuilt"
mkdir -p "${prebuilt}/include/arrow/util" "${prebuilt}/lib64"
printf '#define ARROW_VERSION_STRING "%s"\n' "${ARROW_VERSION}" \
    >"${prebuilt}/include/arrow/util/config.h"
for library in "${ARROW_PAIMON_REQUIRED_LIBRARIES[@]}"; do
    touch "${prebuilt}/lib64/${library}"
done

# A legacy prebuilt may have the old combined marker but no component markers.
# Generic build.sh consumers must reject it before importing Arrow Compute.
arrow_paimon_build_fingerprint >"${prebuilt}/arrow-paimon-build-fingerprint.txt"
if arrow_paimon_prebuilt_valid "${prebuilt}" >/dev/null 2>&1; then
    fail "legacy combined marker certified an unversioned component closure"
fi

arrow_build_fingerprint >"${prebuilt}/arrow-build-fingerprint.txt"
paimon_build_fingerprint >"${prebuilt}/paimon-build-fingerprint.txt"
rm "${prebuilt}/lib64/libarrow_compute.a"
if arrow_paimon_prebuilt_valid "${prebuilt}" >/dev/null 2>&1; then
    fail "prebuilt validation accepted a missing Arrow Compute archive"
fi
touch "${prebuilt}/lib64/libarrow_compute.a"

printf '%s\n' stale-arrow >"${prebuilt}/arrow-build-fingerprint.txt"
if arrow_paimon_prebuilt_valid "${prebuilt}" >/dev/null 2>&1; then
    fail "Paimon-only marker update certified a stale Arrow build"
fi

arrow_build_fingerprint >"${prebuilt}/arrow-build-fingerprint.txt"
arrow_paimon_prebuilt_valid "${prebuilt}" || fail "matching component markers were rejected"

echo "PASS"
