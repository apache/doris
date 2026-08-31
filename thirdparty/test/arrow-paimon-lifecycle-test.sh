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

create_nonempty_fixture_file() {
    local path="$1"
    mkdir -p "$(dirname "${path}")"
    printf '%s\n' fixture >"${path}"
}

create_static_archive_fixture() {
    local path="$1"
    local member="${tmpdir}/static-archive-member"
    mkdir -p "$(dirname "${path}")"
    printf '%s\n' fixture >"${member}"
    ar rcs "${path}" "${member}"
}

create_sdk_header_fixtures() {
    local install_dir="$1"
    local header

    for header in "${ARROW_REQUIRED_HEADERS[@]}" "${PAIMON_REQUIRED_HEADERS[@]}"; do
        create_nonempty_fixture_file "${install_dir}/include/${header}"
    done
}

create_library_fixtures() {
    local install_dir="$1"
    shift
    local library

    for library in "$@"; do
        create_static_archive_fixture "${install_dir}/lib64/${library}"
    done
}

create_fingerprint_fixture() {
    local destination="$1"

    mkdir -p "${destination}/patches"
    cp "${ROOT}/arrow-paimon-vars.sh" "${destination}/arrow-paimon-vars.sh"
    cp "${ROOT}/paimon-cpp-cache.cmake" "${destination}/paimon-cpp-cache.cmake"
    cp "${ROOT}"/patches/apache-arrow-24.0.0-*.patch "${destination}/patches/"
    cp "${ROOT}"/patches/paimon-cpp-*.patch "${destination}/patches/"
}

fingerprint_from_fixture() {
    local fixture="$1"
    local component="$2"
    local result_variable="$3"
    local fingerprint

    fingerprint="$(
        set -e
        # shellcheck source=/dev/null
        . "${fixture}/arrow-paimon-vars.sh"
        "${component}_build_fingerprint"
    )"
    printf -v "${result_variable}" '%s' "${fingerprint}"
}

exercise_semantic_fingerprints() {
    local first_fixture="${tmpdir}/fingerprint-first"
    local second_fixture="${tmpdir}/fingerprint-second"
    local first_arrow
    local first_paimon
    local second_arrow
    local second_paimon
    local changed_arrow
    local changed_paimon

    create_fingerprint_fixture "${first_fixture}"
    create_fingerprint_fixture "${second_fixture}"

    printf '%s\n' first-env >"${first_fixture}/env.sh"
    printf '%s\n' second-env >"${second_fixture}/env.sh"
    printf '%s\n' first-vars >"${first_fixture}/vars.sh"
    printf '%s\n' second-vars >"${second_fixture}/vars.sh"
    printf '%s\n' first-download >"${first_fixture}/download-thirdparty.sh"
    printf '%s\n' second-download >"${second_fixture}/download-thirdparty.sh"
    printf '%s\n' first-build >"${first_fixture}/build-thirdparty.sh"
    printf '%s\n' second-build >"${second_fixture}/build-thirdparty.sh"
    printf '%s\n' '# release-branch-only comment' >>"${second_fixture}/arrow-paimon-vars.sh"

    fingerprint_from_fixture "${first_fixture}" arrow first_arrow
    fingerprint_from_fixture "${first_fixture}" paimon first_paimon
    [[ "${first_arrow}" != "${ARROW_LEGACY_COMPATIBLE_SEMANTIC_FINGERPRINT}" ]] ||
        fail "the empty-row-group fix unexpectedly accepts the legacy Arrow marker"
    [[ "${first_paimon}" != "${PAIMON_LEGACY_COMPATIBLE_SEMANTIC_FINGERPRINT}" ]] ||
        fail "the dual-prefix Paimon build unexpectedly accepts the legacy root-prefix marker"
    fingerprint_from_fixture "${second_fixture}" arrow second_arrow
    fingerprint_from_fixture "${second_fixture}" paimon second_paimon
    [[ "${first_arrow}" == "${second_arrow}" ]] ||
        fail "unrelated branch scripts changed the Arrow fingerprint"
    [[ "${first_paimon}" == "${second_paimon}" ]] ||
        fail "unrelated branch scripts changed the Paimon fingerprint"

    printf '%s\n' semantic-change \
        >>"${second_fixture}/patches/apache-arrow-24.0.0-lzo.patch"
    fingerprint_from_fixture "${second_fixture}" arrow changed_arrow
    fingerprint_from_fixture "${second_fixture}" paimon changed_paimon
    [[ "${changed_arrow}" != "${first_arrow}" ]] ||
        fail "an Arrow patch change did not change the Arrow fingerprint"
    [[ "${changed_paimon}" != "${first_paimon}" ]] ||
        fail "an Arrow patch change did not change the Paimon fingerprint"

    cp "${first_fixture}/patches/apache-arrow-24.0.0-lzo.patch" \
        "${second_fixture}/patches/apache-arrow-24.0.0-lzo.patch"
    printf '%s\n' semantic-change >>"${second_fixture}/paimon-cpp-cache.cmake"
    fingerprint_from_fixture "${second_fixture}" arrow second_arrow
    fingerprint_from_fixture "${second_fixture}" paimon changed_paimon
    [[ "${second_arrow}" == "${first_arrow}" ]] ||
        fail "a Paimon-only cache change changed the Arrow fingerprint"
    [[ "${changed_paimon}" != "${first_paimon}" ]] ||
        fail "a Paimon cache change did not change the Paimon fingerprint"

    changed_arrow="$(
        set -e
        # shellcheck source=/dev/null
        . "${first_fixture}/arrow-paimon-vars.sh"
        ARROW_BUILD_SCHEMA_VERSION="${ARROW_BUILD_SCHEMA_VERSION}-changed"
        arrow_build_fingerprint
    )"
    [[ "${changed_arrow}" != "${first_arrow}" ]] ||
        fail "an Arrow build-schema change did not change its fingerprint"
}

# shellcheck source=../arrow-paimon-vars.sh
. "${ROOT}/arrow-paimon-vars.sh"
for entrypoint in "${ROOT}/../build.sh" "${ROOT}/../run-be-ut.sh"; do
    grep -Fq 'select_arrow_paimon_home_from_install' "${entrypoint}" ||
        fail "$(basename "${entrypoint}") does not enforce the selected Arrow/Paimon home"
done
grep -Fq 'set(CMAKE_INSTALL_LIBDIR "lib64"' "${ROOT}/paimon-cpp-cache.cmake" ||
    fail "the Paimon cache does not enforce the lib64 install contract"

exercise_home_selection() {
    local install_dir="${tmpdir}/home-selection/installed"
    local expected_home
    expected_home="$(arrow_install_dir "${install_dir}")"

    (
        unset ARROW_HOME PAIMON_HOME
        select_arrow_paimon_home_from_install "${install_dir}" "lifecycle test"
        [[ "${ARROW_HOME}" == "${expected_home}" && "${PAIMON_HOME}" == "${expected_home}" ]]
    ) || fail "default Arrow/Paimon homes were not selected together"

    if (
        export PAIMON_HOME="${install_dir}"
        unset ARROW_HOME
        select_arrow_paimon_home_from_install "${install_dir}" "lifecycle test"
    ) >/dev/null 2>&1; then
        fail "a split Arrow/Paimon home was accepted"
    fi
    if (
        export ARROW_HOME="${install_dir}" PAIMON_HOME="${install_dir}"
        select_arrow_paimon_home_from_install "${install_dir}" "lifecycle test"
    ) >/dev/null 2>&1; then
        fail "the legacy root Arrow/Paimon home was accepted by the current consumer"
    fi
}

exercise_home_selection
exercise_semantic_fingerprints

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
    local file_count="${4:-3}"
    local index

    mkdir -p "${harness}/src/${source_name}"
    for ((index = 1; index <= file_count; ++index)); do
        printf '%s\n' original >"${harness}/src/${source_name}/${prefix}-${index}.txt"
    done
    tar -czf "${harness}/src/${archive_name}" -C "${harness}/src" "${source_name}"
    rm -rf "${harness}/src/${source_name}"
}

arrow_source="arrow-apache-arrow-24.0.0"
arrow_archive="apache-arrow-24.0.0.tar.gz"
arrow_17_source="arrow-apache-arrow-17.0.0"
arrow_17_archive="apache-arrow-17.0.0.tar.gz"
paimon_source="doris-thirdparty-paimon-cpp-0a4f4e2"
paimon_17_source="${paimon_source}-arrow-17"
paimon_archive="paimon-cpp-0a4f4e2.tar.gz"

create_archive "${arrow_source}" "${arrow_archive}" arrow
create_archive "${arrow_17_source}" "${arrow_17_archive}" arrow17
create_archive "${paimon_source}" "${paimon_archive}" paimon 4

arrow_patches=(
    apache-arrow-24.0.0-paimon.patch
    apache-arrow-24.0.0-force-write-int96-timestamps.patch
    apache-arrow-24.0.0-lzo.patch
)
arrow_17_patches=(
    apache-arrow-17.0.0-paimon.patch
    apache-arrow-17.0.0-force-write-int96-timestamps.patch
    apache-arrow-17.0.0-lzo.patch
)
paimon_patches=(
    paimon-cpp-buildutils-static-deps.patch
    paimon-cpp-empty-row-groups.patch
    paimon-cpp-arrow-24-compatibility.patch
    paimon-cpp-arrow-24-compute.patch
)

for index in "${!arrow_patches[@]}"; do
    create_patch "${harness}/patches/${arrow_patches[${index}]}" "arrow-$((index + 1)).txt"
done
for index in "${!arrow_17_patches[@]}"; do
    create_patch "${harness}/patches/${arrow_17_patches[${index}]}" "arrow17-$((index + 1)).txt"
done
for index in "${!paimon_patches[@]}"; do
    create_patch "${harness}/patches/${paimon_patches[${index}]}" "paimon-$((index + 1)).txt"
done

arrow_md5="$(md5sum "${harness}/src/${arrow_archive}" | awk '{print $1}')"
arrow_17_md5="$(md5sum "${harness}/src/${arrow_17_archive}" | awk '{print $1}')"
paimon_md5="$(md5sum "${harness}/src/${paimon_archive}" | awk '{print $1}')"
{
    printf 'TP_SOURCE_DIR="%s"\n' "${harness}/src"
    printf 'TP_INSTALL_DIR="%s"\n' "${harness}/installed"
    printf 'TP_PATCH_DIR="%s"\n' "${harness}/patches"
    printf '%s\n' 'TP_ARCHIVES=(ARROW_17 ARROW PAIMON_CPP_17 PAIMON_CPP)'
    printf 'ARROW_17_NAME="%s"\n' "${arrow_17_archive}"
    printf 'ARROW_17_SOURCE="%s"\n' "${arrow_17_source}"
    printf 'ARROW_17_MD5SUM="%s"\n' "${arrow_17_md5}"
    printf '%s\n' 'ARROW_17_DOWNLOAD="unused"'
    printf 'ARROW_NAME="%s"\n' "${arrow_archive}"
    printf 'ARROW_SOURCE="%s"\n' "${arrow_source}"
    printf 'ARROW_MD5SUM="%s"\n' "${arrow_md5}"
    printf '%s\n' 'ARROW_DOWNLOAD="unused"'
    printf 'PAIMON_CPP_NAME="%s"\n' "${paimon_archive}"
    printf 'PAIMON_CPP_SOURCE="%s"\n' "${paimon_source}"
    printf 'PAIMON_CPP_MD5SUM="%s"\n' "${paimon_md5}"
    printf '%s\n' 'PAIMON_CPP_DOWNLOAD="unused"'
    printf 'PAIMON_CPP_17_NAME="%s"\n' "${paimon_archive}"
    printf 'PAIMON_CPP_17_ARCHIVE_SOURCE="%s"\n' "${paimon_source}"
    printf 'PAIMON_CPP_17_SOURCE="%s"\n' "${paimon_17_source}"
    printf 'PAIMON_CPP_17_MD5SUM="%s"\n' "${paimon_md5}"
    printf '%s\n' 'PAIMON_CPP_17_DOWNLOAD="unused"'
    printf '%s\n' 'arrow_paimon_build_fingerprint() { printf "%s\n" test-fingerprint; }'
    printf '%s\n' 'arrow_paimon_17_build_fingerprint() { printf "%s\n" test-17-fingerprint; }'
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

    for ((applied = 1; applied <= ${#patches[@]}; ++applied)); do
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

        for ((index = 1; index <= ${#patches[@]}; ++index)); do
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

exercise_legacy_source_isolation() {
    local index

    touch "${harness}/src/${arrow_source}/current-arrow-sentinel"
    TP_DIR="${harness}" DORIS_HOME="${tmpdir}" \
        bash "${harness}/download-thirdparty.sh" ARROW_17 >/dev/null
    for index in 1 2 3; do
        [[ "$(<"${harness}/src/${arrow_17_source}/arrow17-${index}.txt")" == "patched" ]] ||
            fail "Arrow 17 patch ${index} was not applied"
    done
    [[ -f "${harness}/src/${arrow_source}/current-arrow-sentinel" ]] ||
        fail "extracting Arrow 17 modified the Arrow 24 source"

    touch "${harness}/src/${paimon_source}/current-paimon-sentinel"
    TP_DIR="${harness}" DORIS_HOME="${tmpdir}" \
        bash "${harness}/download-thirdparty.sh" PAIMON_CPP_17 >/dev/null
    for index in 1 2; do
        [[ "$(<"${harness}/src/${paimon_17_source}/paimon-${index}.txt")" == "patched" ]] ||
            fail "Paimon Arrow 17 common patch ${index} was not applied"
    done
    for index in 3 4; do
        [[ "$(<"${harness}/src/${paimon_17_source}/paimon-${index}.txt")" == "original" ]] ||
            fail "Paimon Arrow 17 received an Arrow 24-only patch"
    done
    [[ -f "${harness}/src/${paimon_source}/current-paimon-sentinel" ]] ||
        fail "extracting Paimon for Arrow 17 modified the Arrow 24 source"
}

exercise_legacy_source_isolation

exercise_generic_recovery_dispatch() {
    local generic="${tmpdir}/generic-recovery"
    local thirdparty_dir="${generic}/thirdparty"
    local external_thirdparty_dir="${generic}/external-thirdparty"
    local args_file="${generic}/build-args.txt"
    local output_file="${generic}/build-output.txt"
    local external_builder_called="${generic}/external-builder-called"
    local fake_mvn="${generic}/fake-mvn"
    local non_native_status=79
    local non_native_target
    local status
    local flag
    local parallel
    local clean
    local package1
    local package2
    local package3
    local package4
    local extra

    mkdir -p "${thirdparty_dir}/installed/lib/hadoop_hdfs/native" \
        "${external_thirdparty_dir}/installed/lib/hadoop_hdfs/native" \
        "${generic}/gensrc" "${generic}/fe" "${generic}/be/build_Release" \
        "${generic}/be/output"
    cp "${ROOT}/../build.sh" "${generic}/build.sh"
    cp "${ROOT}/arrow-paimon-vars.sh" "${thirdparty_dir}/arrow-paimon-vars.sh"
    touch "${thirdparty_dir}/installed/lib/hadoop_hdfs/native/libhdfs.a"
    touch "${external_thirdparty_dir}/installed/lib/hadoop_hdfs/native/libhdfs.a"
    printf '%s\n' 'clean: ; @:' >"${generic}/gensrc/Makefile"
    # shellcheck disable=SC2016
    printf '%s\n' '#!/usr/bin/env bash' '[[ "$1" == "clean" ]]' >"${fake_mvn}"
    chmod +x "${fake_mvn}"
    {
        printf '%s\n' '#!/usr/bin/env bash'
        printf 'exit %q\n' "${non_native_status}"
    } >"${generic}/generated-source.sh"
    chmod +x "${generic}/generated-source.sh"
    {
        printf '%s\n' 'DORIS_BUILD_PROFILE=0'
        printf '%s\n' 'TARGET_SYSTEM=Linux'
        printf 'MVN_CMD=%q\n' "${fake_mvn}"
    } >"${generic}/env.sh"
    {
        printf '%s\n' '#!/usr/bin/env bash'
        # RECOVERY_ARGS_FILE is expanded when the generated builder runs.
        # shellcheck disable=SC2016
        printf '%s\n' 'printf "%s\n" "$*" >"${RECOVERY_ARGS_FILE:?}"'
        # shellcheck disable=SC2016
        printf '%s\n' 'exit "${RECOVERY_EXIT_STATUS:-73}"'
    } >"${thirdparty_dir}/build-thirdparty.sh"

    DORIS_THIRDPARTY="${thirdparty_dir}" RECOVERY_ARGS_FILE="${args_file}" \
        bash "${generic}/build.sh" --clean >"${output_file}" 2>&1 ||
        fail "bare clean failed before reaching its clean-only exit"
    [[ ! -e "${args_file}" ]] || fail "bare clean invoked the thirdparty builder"
    [[ ! -d "${generic}/be/build_Release" && ! -d "${generic}/be/output" ]] ||
        fail "bare clean did not remove BE build outputs"

    for non_native_target in --fe --hive-udf; do
        if DORIS_THIRDPARTY="${thirdparty_dir}" RECOVERY_ARGS_FILE="${args_file}" \
            bash "${generic}/build.sh" "${non_native_target}" >"${output_file}" 2>&1; then
            fail "${non_native_target} did not reach the generated-source sentinel"
        else
            status=$?
        fi
        [[ "${status}" -eq "${non_native_status}" ]] ||
            fail "${non_native_target} failed before the generated-source sentinel"
        [[ ! -e "${args_file}" ]] ||
            fail "${non_native_target} invoked the native Arrow/Paimon builder"
    done

    rm -f "${args_file}"
    if DORIS_THIRDPARTY="${external_thirdparty_dir}" \
        bash "${generic}/build.sh" --cloud >"${output_file}" 2>&1; then
        fail "--cloud did not reach the generated-source sentinel"
    else
        status=$?
    fi
    [[ "${status}" -eq "${non_native_status}" ]] ||
        fail "--cloud incorrectly required the BE Arrow/Paimon stack"
    [[ ! -e "${args_file}" ]] ||
        fail "--cloud invoked the native Arrow/Paimon builder"

    if DORIS_THIRDPARTY="${thirdparty_dir}" RECOVERY_ARGS_FILE="${args_file}" \
        bash "${generic}/build.sh" --fe --clean >"${output_file}" 2>&1; then
        fail "--fe --clean did not reach the generated-source sentinel"
    else
        status=$?
    fi
    [[ "${status}" -eq "${non_native_status}" ]] ||
        fail "--fe --clean failed before the generated-source sentinel"
    [[ ! -e "${args_file}" ]] ||
        fail "--fe --clean invoked the native Arrow/Paimon builder"

    rm -f "${args_file}"
    if ARROW_HOME="${generic}/explicit-arrow" PAIMON_HOME="${generic}/explicit-paimon" \
        DORIS_THIRDPARTY="${thirdparty_dir}" RECOVERY_ARGS_FILE="${args_file}" \
        bash "${generic}/build.sh" --be >"${output_file}" 2>&1; then
        fail "build.sh accepted unsupported explicit Arrow/Paimon prefixes"
    fi
    grep -Fq "only supports the Arrow/Paimon stack selected from DORIS_THIRDPARTY" \
        "${output_file}" || fail "build.sh did not explain its supported Arrow/Paimon selection"
    [[ ! -e "${args_file}" ]] ||
        fail "an unsupported explicit Arrow/Paimon selection invoked the builder"

    if DORIS_THIRDPARTY="${thirdparty_dir}" RECOVERY_ARGS_FILE="${args_file}" \
        bash "${generic}/build.sh" --be >"${output_file}" 2>&1; then
        fail "generic stale-prebuilt recovery did not invoke the focused builder"
    else
        status=$?
    fi
    [[ "${status}" -eq 73 ]] || fail "generic recovery failed before invoking its builder"
    read -r flag parallel package1 package2 package3 package4 extra <"${args_file}"
    [[ "${flag}" == "-j" && "${parallel}" =~ ^[0-9]+$ &&
        "${package1}" == "arrow_17" && "${package2}" == "paimon_cpp_17" &&
        "${package3}" == "arrow" && "${package4}" == "paimon_cpp" && -z "${extra}" ]] ||
        fail "generic recovery dispatched the wrong build package set"

    if DORIS_THIRDPARTY="${thirdparty_dir}" RECOVERY_ARGS_FILE="${args_file}" \
        bash "${generic}/build.sh" --be --clean >"${output_file}" 2>&1; then
        fail "generic clean recovery did not invoke the focused builder"
    else
        status=$?
    fi
    [[ "${status}" -eq 73 ]] || fail "generic clean recovery failed before invoking its builder"
    read -r flag parallel clean package1 package2 package3 package4 extra <"${args_file}"
    [[ "${flag}" == "-j" && "${parallel}" =~ ^[0-9]+$ && "${clean}" == "--clean" &&
        "${package1}" == "arrow_17" && "${package2}" == "paimon_cpp_17" &&
        "${package3}" == "arrow" && "${package4}" == "paimon_cpp" && -z "${extra}" ]] ||
        fail "generic clean recovery dispatched the wrong build package set"

    if DORIS_THIRDPARTY="${thirdparty_dir}" RECOVERY_ARGS_FILE="${args_file}" \
        RECOVERY_EXIT_STATUS=0 bash "${generic}/build.sh" --be >"${output_file}" 2>&1; then
        fail "generic recovery accepted artifacts that failed current-checkout validation"
    fi
    grep -Fq "Rebuilt Arrow/Paimon artifacts do not match this checkout's selected inputs" \
        "${output_file}" || fail "generic recovery did not validate artifacts after its builder returned"

    if DORIS_THIRDPARTY="${external_thirdparty_dir}" \
        bash "${generic}/build.sh" --be >"${output_file}" 2>&1; then
        fail "generic recovery accepted an invalid install-only thirdparty prefix"
    fi
    grep -Fq "is an install-only or incomplete prefix" "${output_file}" ||
        fail "generic recovery did not explain how to refresh an install-only prebuilt"

    {
        printf '%s\n' '#!/usr/bin/env bash'
        # EXTERNAL_BUILDER_CALLED is expanded when the generated builder runs.
        # shellcheck disable=SC2016
        printf '%s\n' 'touch "${EXTERNAL_BUILDER_CALLED:?}"'
    } >"${external_thirdparty_dir}/build-thirdparty.sh"
    if DORIS_THIRDPARTY="${external_thirdparty_dir}" \
        EXTERNAL_BUILDER_CALLED="${external_builder_called}" \
        bash "${generic}/build.sh" --be >"${output_file}" 2>&1; then
        fail "generic recovery accepted an external thirdparty source tree"
    fi
    grep -Fq "Cannot rebuild thirdparty libraries with an external source tree" "${output_file}" ||
        fail "generic recovery did not reject an external thirdparty source tree"
    [[ ! -e "${external_builder_called}" ]] ||
        fail "generic recovery invoked an external thirdparty builder"
}

exercise_generic_recovery_dispatch

# A Paimon-only build may publish only its own fingerprint. It must not make a
# stale Arrow installation pass the shared prebuilt validation.
prebuilt="${tmpdir}/prebuilt"
selected_prebuilt="$(arrow_install_dir "${prebuilt}")"
create_sdk_header_fixtures "${selected_prebuilt}"
printf '#define ARROW_VERSION_STRING "%s"\n' "${ARROW_VERSION}" \
    >"${selected_prebuilt}/include/arrow/util/config.h"
create_library_fixtures "${selected_prebuilt}" "${ARROW_PAIMON_REQUIRED_LIBRARIES[@]}"

prepare_arrow_paimon_download_packages "${ARROW_PAIMON_BUILD_PACKAGES[@]}"
[[ "${ARROW_PAIMON_BUILD_PACKAGES[*]}" == "arrow paimon_cpp" ]] ||
    fail "focused recovery dispatches a bundled source package as a build target"
[[ "${ARROW_PAIMON_SHARED_BUILD_PACKAGES[*]}" == "arrow_17 paimon_cpp_17 arrow paimon_cpp" ]] ||
    fail "shared recovery does not cover both installed Arrow/Paimon stacks"
[[ "${ARROW_PAIMON_DOWNLOAD_PACKAGES[*]}" == "arrow paimon_cpp xsimd brotli" ]] ||
    fail "focused recovery does not download the complete Arrow source closure"
prepare_arrow_paimon_download_packages arrow_17 paimon_cpp_17
[[ "${ARROW_PAIMON_DOWNLOAD_PACKAGES[*]}" == "arrow_17 paimon_cpp_17 xsimd_17 brotli" ]] ||
    fail "Arrow 17 build does not download its independent source closure"

# A legacy prebuilt may have the old combined marker but no component markers.
# Generic build.sh consumers must reject it before importing Arrow Compute.
arrow_paimon_build_fingerprint >"${prebuilt}/arrow-paimon-build-fingerprint.txt"
if arrow_paimon_prebuilt_valid "${prebuilt}" >/dev/null 2>&1; then
    fail "legacy combined marker certified an unversioned component closure"
fi

printf '%s\n' "${ARROW_LEGACY_BUILD_FINGERPRINTS[0]}" \
    >"${selected_prebuilt}/arrow-build-fingerprint.txt"
printf '%s\n' "${PAIMON_LEGACY_BUILD_FINGERPRINTS[0]}" \
    >"${selected_prebuilt}/paimon-build-fingerprint.txt"
if arrow_prebuilt_valid "${prebuilt}" >/dev/null 2>&1; then
    fail "the empty-row-group fix accepted a legacy Arrow prebuilt"
fi
if paimon_prebuilt_valid "${prebuilt}" >/dev/null 2>&1; then
    fail "the empty-row-group fix accepted a legacy Paimon prebuilt"
fi
if arrow_paimon_prebuilt_valid "${prebuilt}" >/dev/null 2>&1; then
    fail "the shared prebuilt accepted a stale Paimon marker"
fi
publish_arrow_prebuilt_marker "${prebuilt}"
publish_paimon_prebuilt_marker "${prebuilt}"
arrow_paimon_prebuilt_valid "${prebuilt}" ||
    fail "the refreshed Arrow and Paimon component markers were rejected"
if (
    ARROW_BUILD_SCHEMA_VERSION="${ARROW_BUILD_SCHEMA_VERSION}-changed"
    arrow_prebuilt_valid "${prebuilt}"
) >/dev/null 2>&1; then
    fail "the legacy Arrow marker survived a semantic fingerprint change"
fi
if (
    PAIMON_BUILD_SCHEMA_VERSION="${PAIMON_BUILD_SCHEMA_VERSION}-changed"
    paimon_prebuilt_valid "${prebuilt}"
) >/dev/null 2>&1; then
    fail "the legacy Paimon marker survived a semantic fingerprint change"
fi

# A shared prebuilt is complete only when the legacy root stack and the
# versioned current stack are both present and independently valid.
create_sdk_header_fixtures "${prebuilt}"
printf '#define ARROW_VERSION_STRING "%s"\n' "${ARROW_17_VERSION}" \
    >"${prebuilt}/include/arrow/util/config.h"
create_library_fixtures "${prebuilt}" "${ARROW_17_REQUIRED_LIBRARIES[@]}" \
    "${PAIMON_REQUIRED_LIBRARIES[@]}"
publish_arrow_17_prebuilt_marker "${prebuilt}"
publish_paimon_17_prebuilt_marker "${prebuilt}"
shared_arrow_paimon_prebuilt_valid "${prebuilt}" ||
    fail "matching shared Arrow/Paimon stacks were rejected"

prebuilt_archive_root="${tmpdir}/prebuilt-archive-root"
prebuilt_archive="${tmpdir}/prebuilt.tar.xz"
mkdir -p "${prebuilt_archive_root}/installed"
cp -a "${prebuilt}/." "${prebuilt_archive_root}/installed/"
tar -C "${prebuilt_archive_root}" -cJf "${prebuilt_archive}" installed

external_thirdparty="${tmpdir}/external-thirdparty"
mkdir -p "${external_thirdparty}/installed/${ARROW_INSTALL_SUBDIR}"
cp -a "${selected_prebuilt}/." \
    "${external_thirdparty}/installed/${ARROW_INSTALL_SUBDIR}/"
touch "${external_thirdparty}/installed/old-image-artifact"
ensure_arrow_paimon_prebuilt_from_url "${external_thirdparty}" \
    "file://${prebuilt_archive}" >/dev/null 2>&1 ||
    fail "BE UT could not refresh an outdated build-image prebuilt"
shared_arrow_paimon_prebuilt_valid "${external_thirdparty}/installed" ||
    fail "BE UT installed an invalid build-image prebuilt"
[[ ! -e "${external_thirdparty}/installed/old-image-artifact" ]] ||
    fail "BE UT accepted a current-only prebuilt without refreshing the legacy stack"
ensure_arrow_paimon_prebuilt_from_url "${external_thirdparty}" \
    "file://${tmpdir}/missing-prebuilt.tar.xz" >/dev/null 2>&1 ||
    fail "BE UT tried to download over a valid build-image prebuilt"

missing_legacy_archive_root="${tmpdir}/missing-legacy-prebuilt-archive-root"
missing_legacy_archive="${tmpdir}/missing-legacy-prebuilt.tar.xz"
mkdir -p "${missing_legacy_archive_root}/installed"
cp -a "${prebuilt}/." "${missing_legacy_archive_root}/installed/"
rm "${missing_legacy_archive_root}/installed/include/arrow/util/config.h"
tar -C "${missing_legacy_archive_root}" -cJf "${missing_legacy_archive}" installed

missing_legacy_thirdparty="${tmpdir}/missing-legacy-thirdparty"
mkdir -p "${missing_legacy_thirdparty}/installed"
touch "${missing_legacy_thirdparty}/installed/preserved-after-missing-legacy-download"
if ensure_arrow_paimon_prebuilt_from_url "${missing_legacy_thirdparty}" \
    "file://${missing_legacy_archive}" >/dev/null 2>&1; then
    fail "BE UT accepted a downloaded prebuilt without Arrow 17"
fi
[[ -e "${missing_legacy_thirdparty}/installed/preserved-after-missing-legacy-download" ]] ||
    fail "BE UT replaced the build-image prebuilt before validating the legacy stack"

exercise_rejected_prebuilt_artifact() {
    local name="$1"
    local relative_path="$2"
    local mutation="$3"
    local archive_root="${tmpdir}/${name}-archive-root"
    local archive="${tmpdir}/${name}.tar.xz"
    local thirdparty_root="${tmpdir}/${name}-thirdparty"

    mkdir -p "${archive_root}/installed"
    cp -a "${prebuilt}/." "${archive_root}/installed/"
    if [[ "${mutation}" == "remove" ]]; then
        rm "${archive_root}/installed/${relative_path}"
    else
        : >"${archive_root}/installed/${relative_path}"
    fi
    tar -C "${archive_root}" -cJf "${archive}" installed

    mkdir -p "${thirdparty_root}/installed"
    touch "${thirdparty_root}/installed/preserved-after-invalid-sdk-download"
    if ensure_arrow_paimon_prebuilt_from_url "${thirdparty_root}" \
        "file://${archive}" >/dev/null 2>&1; then
        fail "BE UT accepted ${name}"
    fi
    [[ -e "${thirdparty_root}/installed/preserved-after-invalid-sdk-download" ]] ||
        fail "BE UT replaced the build-image prebuilt with ${name}"
}

exercise_rejected_prebuilt_artifact missing-legacy-paimon-header \
    include/paimon/defs.h remove
exercise_rejected_prebuilt_artifact missing-current-paimon-header \
    "${ARROW_INSTALL_SUBDIR}/include/paimon/defs.h" remove
exercise_rejected_prebuilt_artifact missing-current-parquet-header \
    "${ARROW_INSTALL_SUBDIR}/include/parquet/arrow/reader.h" remove
exercise_rejected_prebuilt_artifact empty-legacy-paimon-library \
    lib64/libpaimon.a empty

invalid_archive_root="${tmpdir}/invalid-prebuilt-archive-root"
invalid_archive="${tmpdir}/invalid-prebuilt.tar.xz"
mkdir -p "${invalid_archive_root}/installed"
cp -a "${prebuilt}/." "${invalid_archive_root}/installed/"
rm "${invalid_archive_root}/installed/${ARROW_INSTALL_SUBDIR}/lib64/libarrow_compute.a"
tar -C "${invalid_archive_root}" -cJf "${invalid_archive}" installed

invalid_external_thirdparty="${tmpdir}/invalid-external-thirdparty"
mkdir -p "${invalid_external_thirdparty}/installed"
touch "${invalid_external_thirdparty}/installed/preserved-after-invalid-download"
if ensure_arrow_paimon_prebuilt_from_url "${invalid_external_thirdparty}" \
    "file://${invalid_archive}" >/dev/null 2>&1; then
    fail "BE UT accepted a downloaded prebuilt without Arrow Compute"
fi
[[ -e "${invalid_external_thirdparty}/installed/preserved-after-invalid-download" ]] ||
    fail "BE UT replaced the build-image prebuilt before validating its download"

publish_arrow_prebuilt_marker "${prebuilt}"
publish_paimon_prebuilt_marker "${prebuilt}"
rm "${selected_prebuilt}/lib64/libarrow_compute.a"
if arrow_paimon_prebuilt_valid "${prebuilt}" >/dev/null 2>&1; then
    fail "prebuilt validation accepted a missing Arrow Compute archive"
fi
create_static_archive_fixture "${selected_prebuilt}/lib64/libarrow_compute.a"

printf '%s\n' stale-arrow >"${selected_prebuilt}/arrow-build-fingerprint.txt"
if arrow_paimon_prebuilt_valid "${prebuilt}" >/dev/null 2>&1; then
    fail "Paimon-only marker update certified a stale Arrow build"
fi
if require_arrow_prebuilt_for_paimon "${prebuilt}" >/dev/null 2>&1; then
    fail "Paimon build accepted a stale installed Arrow"
fi

publish_arrow_prebuilt_marker "${prebuilt}"
require_arrow_prebuilt_for_paimon "${prebuilt}" ||
    fail "Paimon build rejected the selected installed Arrow"
arrow_paimon_prebuilt_valid "${prebuilt}" || fail "matching component markers were rejected"

invalidate_paimon_prebuilt_marker "${prebuilt}"
if arrow_paimon_prebuilt_valid "${prebuilt}" >/dev/null 2>&1; then
    fail "an interrupted Paimon rebuild left its old marker valid"
fi
publish_paimon_prebuilt_marker "${prebuilt}"

invalidate_arrow_prebuilt_marker "${prebuilt}"
if require_arrow_prebuilt_for_paimon "${prebuilt}" >/dev/null 2>&1; then
    fail "an interrupted Arrow rebuild left its old marker valid"
fi
publish_arrow_prebuilt_marker "${prebuilt}"
arrow_paimon_prebuilt_valid "${prebuilt}" ||
    fail "republished component markers were rejected"
shared_arrow_paimon_prebuilt_valid "${prebuilt}" ||
    fail "republishing the Arrow 24 stack invalidated Arrow 17"
select_arrow_paimon_rebuild_packages "${prebuilt}"
[[ "${#ARROW_PAIMON_REBUILD_PACKAGES[@]}" -eq 0 ]] ||
    fail "recovery rebuilt an already valid shared stack"

# Rebuilding or invalidating either stack must not affect the other branch's
# artifacts or fingerprints.
invalidate_arrow_prebuilt_marker "${prebuilt}"
arrow_paimon_17_prebuilt_valid "${prebuilt}" ||
    fail "invalidating Arrow 24 affected the Arrow 17 stack"
select_arrow_paimon_rebuild_packages "${prebuilt}" >/dev/null 2>&1
[[ "${ARROW_PAIMON_REBUILD_PACKAGES[*]}" == "arrow paimon_cpp" ]] ||
    fail "recovery did not isolate an invalid Arrow 24 stack"
publish_arrow_prebuilt_marker "${prebuilt}"

invalidate_arrow_17_prebuilt_marker "${prebuilt}"
arrow_paimon_prebuilt_valid "${prebuilt}" ||
    fail "invalidating Arrow 17 affected the Arrow 24 stack"
select_arrow_paimon_rebuild_packages "${prebuilt}" >/dev/null 2>&1
[[ "${ARROW_PAIMON_REBUILD_PACKAGES[*]}" == "arrow_17 paimon_cpp_17" ]] ||
    fail "recovery did not isolate an invalid Arrow 17 stack"
publish_arrow_17_prebuilt_marker "${prebuilt}"
arrow_paimon_17_prebuilt_valid "${prebuilt}" ||
    fail "republished Arrow 17 component markers were rejected"

# Preparing a root-prefix Arrow downgrade removes Paimon first. An interrupted
# Arrow 17 build therefore cannot expose a root Arrow 17/Paimon 24 mixture to an
# unchanged branch-4.1 consumer.
migration_prefix="${tmpdir}/interrupted-arrow-17-migration"
migration_selected_prefix="$(arrow_install_dir "${migration_prefix}")"
mkdir -p \
    "${migration_prefix}/include/arrow" \
    "${migration_prefix}/include/parquet" \
    "${migration_prefix}/include/paimon" \
    "${migration_prefix}/include/unrelated" \
    "${migration_prefix}/lib64" \
    "${migration_selected_prefix}"
for library in "${ARROW_REQUIRED_LIBRARIES[@]}" "${PAIMON_REQUIRED_LIBRARIES[@]}"; do
    touch "${migration_prefix}/lib64/${library}"
done
touch \
    "${migration_prefix}/arrow-build-fingerprint.txt" \
    "${migration_prefix}/paimon-build-fingerprint.txt" \
    "${migration_prefix}/arrow-paimon-build-fingerprint.txt" \
    "${migration_prefix}/arrow-17-build-fingerprint.txt" \
    "${migration_prefix}/paimon-arrow-17-build-fingerprint.txt" \
    "${migration_prefix}/arrow-paimon-17-build-fingerprint.txt" \
    "${migration_prefix}/include/unrelated/sentinel" \
    "${migration_selected_prefix}/sentinel"

prepare_arrow_17_install_prefix "${migration_prefix}"
[[ ! -e "${migration_prefix}/include/arrow" &&
    ! -e "${migration_prefix}/include/parquet" &&
    ! -e "${migration_prefix}/include/paimon" &&
    ! -e "${migration_prefix}/lib64/libarrow.a" &&
    ! -e "${migration_prefix}/lib64/libpaimon.a" &&
    ! -e "${migration_prefix}/arrow-build-fingerprint.txt" &&
    ! -e "${migration_prefix}/paimon-build-fingerprint.txt" &&
    ! -e "${migration_prefix}/arrow-17-build-fingerprint.txt" &&
    ! -e "${migration_prefix}/paimon-arrow-17-build-fingerprint.txt" ]] ||
    fail "an interrupted Arrow 17 migration left a mixed root-prefix stack"
[[ -e "${migration_prefix}/include/unrelated/sentinel" &&
    -e "${migration_selected_prefix}/sentinel" ]] ||
    fail "preparing the Arrow 17 migration removed another stack's artifacts"

# Reinstalling one stack cleans only files owned by that stack. In particular,
# a downgrade of the legacy prefix must remove Arrow 24-only artifacts without
# deleting Paimon or unrelated thirdparty files.
cleanup_prefix="${tmpdir}/cleanup-prefix"
mkdir -p \
    "${cleanup_prefix}/include/arrow" \
    "${cleanup_prefix}/include/parquet" \
    "${cleanup_prefix}/include/paimon" \
    "${cleanup_prefix}/include/unrelated" \
    "${cleanup_prefix}/lib/cmake/Paimon" \
    "${cleanup_prefix}/lib64/cmake/ArrowCompute" \
    "${cleanup_prefix}/lib64/cmake/Paimon" \
    "${cleanup_prefix}/lib64/pkgconfig" \
    "${cleanup_prefix}/share/arrow" \
    "${cleanup_prefix}/share/doc/arrow"
touch \
    "${cleanup_prefix}/lib64/libarrow_compute.a" \
    "${cleanup_prefix}/lib64/libparquet.a" \
    "${cleanup_prefix}/lib64/libpaimon.a" \
    "${cleanup_prefix}/lib64/libfmt_paimon.a" \
    "${cleanup_prefix}/lib64/libunrelated.a" \
    "${cleanup_prefix}/lib64/pkgconfig/arrow-compute.pc" \
    "${cleanup_prefix}/include/unrelated/sentinel"

clean_arrow_artifacts_in "${cleanup_prefix}"
[[ ! -e "${cleanup_prefix}/include/arrow" &&
    ! -e "${cleanup_prefix}/include/parquet" &&
    ! -e "${cleanup_prefix}/lib64/libarrow_compute.a" &&
    ! -e "${cleanup_prefix}/lib64/cmake/ArrowCompute" &&
    ! -e "${cleanup_prefix}/lib64/pkgconfig/arrow-compute.pc" ]] ||
    fail "Arrow cleanup left stale artifacts in the selected prefix"
[[ -e "${cleanup_prefix}/include/paimon" &&
    -e "${cleanup_prefix}/lib64/libpaimon.a" &&
    -e "${cleanup_prefix}/include/unrelated/sentinel" ]] ||
    fail "Arrow cleanup removed another package's artifacts"

clean_paimon_artifacts_in "${cleanup_prefix}"
[[ ! -e "${cleanup_prefix}/include/paimon" &&
    ! -e "${cleanup_prefix}/lib64/libpaimon.a" &&
    ! -e "${cleanup_prefix}/lib64/libfmt_paimon.a" &&
    ! -e "${cleanup_prefix}/lib/cmake/Paimon" &&
    ! -e "${cleanup_prefix}/lib64/cmake/Paimon" ]] ||
    fail "Paimon cleanup left stale artifacts in the selected prefix"
[[ -e "${cleanup_prefix}/lib64/libunrelated.a" &&
    -e "${cleanup_prefix}/include/unrelated/sentinel" ]] ||
    fail "Paimon cleanup removed another package's artifacts"

echo "PASS"
