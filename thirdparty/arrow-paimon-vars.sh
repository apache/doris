#!/bin/bash
# shellcheck disable=2034

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

# Keep the Arrow/Paimon source closure in a dedicated file so targeted CI can
# distinguish this stack from unrelated thirdparty changes.

# arrow
ARROW_VERSION="24.0.0"
ARROW_DOWNLOAD="https://github.com/apache/arrow/archive/refs/tags/apache-arrow-${ARROW_VERSION}.tar.gz"
ARROW_NAME="apache-arrow-${ARROW_VERSION}.tar.gz"
ARROW_SOURCE="arrow-apache-arrow-${ARROW_VERSION}"
ARROW_MD5SUM="66c53bd00baa79034bd2ca167beea436"

# Arrow bundled dependencies
BROTLI_DOWNLOAD="https://github.com/google/brotli/archive/v1.0.9.tar.gz"
BROTLI_NAME="brotli-1.0.9.tar.gz"
BROTLI_SOURCE="brotli-1.0.9"
BROTLI_MD5SUM="c2274f0c7af8470ad514637c35bcee7d"

XSIMD_DOWNLOAD="https://github.com/xtensor-stack/xsimd/archive/refs/tags/14.0.0.tar.gz"
XSIMD_NAME="14.0.0.tar.gz"
XSIMD_SOURCE=xsimd-14.0.0
XSIMD_MD5SUM="75c0d34cf7011924ba19978076c76dc1"

# paimon-cpp
PAIMON_CPP_DOWNLOAD="https://github.com/apache/doris-thirdparty/archive/refs/tags/paimon-cpp-0a4f4e2.tar.gz"
PAIMON_CPP_NAME="paimon-cpp-0a4f4e2.tar.gz"
PAIMON_CPP_SOURCE="doris-thirdparty-paimon-cpp-0a4f4e2"
PAIMON_CPP_MD5SUM="b8599a0421dbf1ec05e2f1a481d64e87"

ARROW_PAIMON_SHARED_PREBUILT_LINUX_X86_64_URL="${ARROW_PAIMON_SHARED_PREBUILT_LINUX_X86_64_URL:-https://github.com/apache/doris-thirdparty/releases/download/automation/doris-thirdparty-prebuilt-linux-x86_64.tar.xz}"

# Bump the corresponding schema version whenever output-affecting build options or
# helper behavior in build_arrow() or build_paimon_cpp() changes. The fingerprints
# below intentionally describe only this component stack so master and release
# branches can reuse the same shared prebuilt when their semantic inputs match.
ARROW_BUILD_SCHEMA_VERSION="1"
PAIMON_BUILD_SCHEMA_VERSION="1"

# The current shared automation prebuilt was published from master with the former
# whole-script fingerprint. Keep these exact markers during the schema transition;
# version and complete artifact validation are still mandatory before reuse.
ARROW_LEGACY_COMPATIBLE_SEMANTIC_FINGERPRINT="ab79ab0bbfbf93f9860050fb751b20fee9e40d96"
PAIMON_LEGACY_COMPATIBLE_SEMANTIC_FINGERPRINT="cb82e41ba46f534e611cdd52e66b53c227d49bf8"
ARROW_LEGACY_BUILD_FINGERPRINTS=(
    9d03645dd1cded5184a8126f5c7f4a6eb9b92b53
)
PAIMON_LEGACY_BUILD_FINGERPRINTS=(
    dbb6ca6e243cb3aa783b7a8011f97afda9e7ea28
)

# Arrow consumes xsimd and Brotli as bundled source archives, but neither is a
# build target in the focused Arrow/Paimon recovery path.
ARROW_PAIMON_BUILD_PACKAGES=(arrow paimon_cpp)
ARROW_BUNDLED_SOURCE_PACKAGES=(xsimd brotli)
ARROW_PAIMON_DOWNLOAD_PACKAGES=()

prepare_arrow_paimon_download_packages() {
    ARROW_PAIMON_DOWNLOAD_PACKAGES=("$@")

    local package
    local source_package
    local arrow_requested=false
    local source_requested
    for package in "$@"; do
        if [[ "${package}" == "arrow" ]]; then
            arrow_requested=true
            break
        fi
    done
    if [[ "${arrow_requested}" != "true" ]]; then
        return
    fi

    for source_package in "${ARROW_BUNDLED_SOURCE_PACKAGES[@]}"; do
        source_requested=false
        for package in "${ARROW_PAIMON_DOWNLOAD_PACKAGES[@]}"; do
            if [[ "${package}" == "${source_package}" ]]; then
                source_requested=true
                break
            fi
        done
        if [[ "${source_requested}" != "true" ]]; then
            ARROW_PAIMON_DOWNLOAD_PACKAGES+=("${source_package}")
        fi
    done
}

# Print stable path-and-content records for fingerprint inputs. Including the path
# makes patch selection and ordering part of the contract, not only file contents.
arrow_paimon_fingerprint_files() {
    local file
    local blob
    while IFS= read -r file; do
        blob="$(git hash-object "${file}")" || return 1
        printf 'file=%s\n' "${file}"
        printf 'blob=%s\n' "${blob}"
    done < <(printf '%s\n' "$@" | LC_ALL=C sort)
}

# Identify only the source, patch, and explicit build-schema inputs selected for
# Arrow. Arrow and Paimon publish separate installed markers so a package-only
# build cannot certify a component that it did not rebuild.
arrow_build_fingerprint() {
    local vars_dir
    vars_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    (
        set -o pipefail
        cd "${vars_dir}" || return 1
        {
            printf 'schema=%s\n' "${ARROW_BUILD_SCHEMA_VERSION}"
            printf 'ARROW_VERSION=%s\n' "${ARROW_VERSION}"
            printf 'ARROW_NAME=%s\n' "${ARROW_NAME}"
            printf 'ARROW_SOURCE=%s\n' "${ARROW_SOURCE}"
            printf 'ARROW_MD5SUM=%s\n' "${ARROW_MD5SUM}"
            printf 'BROTLI_NAME=%s\n' "${BROTLI_NAME}"
            printf 'BROTLI_SOURCE=%s\n' "${BROTLI_SOURCE}"
            printf 'BROTLI_MD5SUM=%s\n' "${BROTLI_MD5SUM}"
            printf 'XSIMD_NAME=%s\n' "${XSIMD_NAME}"
            printf 'XSIMD_SOURCE=%s\n' "${XSIMD_SOURCE}"
            printf 'XSIMD_MD5SUM=%s\n' "${XSIMD_MD5SUM}"
            arrow_paimon_fingerprint_files \
                patches/apache-arrow-"${ARROW_VERSION}"-*.patch
        } | git hash-object --stdin
    )
}

paimon_build_fingerprint() {
    local vars_dir
    vars_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    (
        set -o pipefail
        cd "${vars_dir}" || return 1
        {
            arrow_build_fingerprint
            printf 'schema=%s\n' "${PAIMON_BUILD_SCHEMA_VERSION}"
            printf 'PAIMON_CPP_NAME=%s\n' "${PAIMON_CPP_NAME}"
            printf 'PAIMON_CPP_SOURCE=%s\n' "${PAIMON_CPP_SOURCE}"
            printf 'PAIMON_CPP_MD5SUM=%s\n' "${PAIMON_CPP_MD5SUM}"
            arrow_paimon_fingerprint_files \
                paimon-cpp-cache.cmake \
                patches/paimon-cpp-*.patch
        } | git hash-object --stdin
    )
}

# Source patch markers use a combined value because either component's inputs
# may change the external-Arrow contract applied to both source trees.
arrow_paimon_build_fingerprint() {
    {
        arrow_build_fingerprint
        paimon_build_fingerprint
    } | git hash-object --stdin
}

arrow_paimon_fingerprint_matches() {
    local installed_fingerprint="$1"
    local expected_fingerprint="$2"
    local legacy_compatible_expected_fingerprint="$3"
    shift 3
    local compatible_fingerprint

    if [[ "${installed_fingerprint}" == "${expected_fingerprint}" ]]; then
        return 0
    fi
    if [[ "${expected_fingerprint}" != "${legacy_compatible_expected_fingerprint}" ]]; then
        return 1
    fi
    for compatible_fingerprint in "$@"; do
        if [[ "${installed_fingerprint}" == "${compatible_fingerprint}" ]]; then
            return 0
        fi
    done
    return 1
}

ARROW_REQUIRED_LIBRARIES=(
    libbrotlicommon.a
    libbrotlidec.a
    libbrotlienc.a
    libarrow.a
    libarrow_compute.a
    libarrow_flight.a
    libarrow_flight_sql.a
    libarrow_dataset.a
    libarrow_acero.a
    libarrow_bundled_dependencies.a
    libparquet.a
)

PAIMON_REQUIRED_LIBRARIES=(
    libpaimon.a
    libpaimon_parquet_file_format.a
    libpaimon_orc_file_format.a
    libpaimon_blob_file_format.a
    libpaimon_local_file_system.a
    libpaimon_file_index.a
    libpaimon_global_index.a
    libroaring_bitmap_paimon.a
    libxxhash_paimon.a
    libfmt_paimon.a
    libtbb_paimon.a
)

ARROW_PAIMON_REQUIRED_LIBRARIES=(
    "${ARROW_REQUIRED_LIBRARIES[@]}"
    "${PAIMON_REQUIRED_LIBRARIES[@]}"
)

arrow_artifacts_valid() {
    local install_dir="$1"
    local installed_arrow_version
    local library

    if [[ ! -f "${install_dir}/include/arrow/util/config.h" ]]; then
        echo "Missing installed Arrow version header" >&2
        return 1
    fi
    installed_arrow_version="$(
        awk '$1 == "#define" && $2 == "ARROW_VERSION_STRING" {
               gsub(/"/, "", $3); print $3; exit
             }' "${install_dir}/include/arrow/util/config.h"
    )"
    if [[ "${installed_arrow_version}" != "${ARROW_VERSION}" ]]; then
        echo "Installed Arrow version ${installed_arrow_version} does not match ${ARROW_VERSION}" >&2
        return 1
    fi

    for library in "${ARROW_REQUIRED_LIBRARIES[@]}"; do
        if [[ ! -f "${install_dir}/lib64/${library}" ]]; then
            echo "Missing Arrow library: ${library}" >&2
            return 1
        fi
    done
    return 0
}

paimon_artifacts_valid() {
    local install_dir="$1"
    local library

    for library in "${PAIMON_REQUIRED_LIBRARIES[@]}"; do
        if [[ ! -f "${install_dir}/lib64/${library}" ]]; then
            echo "Missing Paimon library: ${library}" >&2
            return 1
        fi
    done
    return 0
}

arrow_prebuilt_valid() {
    local install_dir="$1"
    local arrow_fingerprint_mark="${install_dir}/arrow-build-fingerprint.txt"
    local expected_fingerprint
    local installed_fingerprint

    if [[ ! -f "${arrow_fingerprint_mark}" ]]; then
        echo "Missing Arrow build fingerprint: ${arrow_fingerprint_mark}" >&2
        return 1
    fi
    expected_fingerprint="$(arrow_build_fingerprint)"
    installed_fingerprint="$(<"${arrow_fingerprint_mark}")"
    if ! arrow_paimon_fingerprint_matches "${installed_fingerprint}" \
        "${expected_fingerprint}" "${ARROW_LEGACY_COMPATIBLE_SEMANTIC_FINGERPRINT}" \
        "${ARROW_LEGACY_BUILD_FINGERPRINTS[@]}"; then
        echo "Arrow build fingerprint does not match selected inputs" >&2
        return 1
    fi
    arrow_artifacts_valid "${install_dir}"
}

paimon_prebuilt_valid() {
    local install_dir="$1"
    local paimon_fingerprint_mark="${install_dir}/paimon-build-fingerprint.txt"
    local expected_fingerprint
    local installed_fingerprint

    if [[ ! -f "${paimon_fingerprint_mark}" ]]; then
        echo "Missing Paimon build fingerprint: ${paimon_fingerprint_mark}" >&2
        return 1
    fi
    expected_fingerprint="$(paimon_build_fingerprint)"
    installed_fingerprint="$(<"${paimon_fingerprint_mark}")"
    if ! arrow_paimon_fingerprint_matches "${installed_fingerprint}" \
        "${expected_fingerprint}" "${PAIMON_LEGACY_COMPATIBLE_SEMANTIC_FINGERPRINT}" \
        "${PAIMON_LEGACY_BUILD_FINGERPRINTS[@]}"; then
        echo "Paimon build fingerprint does not match selected inputs" >&2
        return 1
    fi
    paimon_artifacts_valid "${install_dir}"
}

arrow_paimon_prebuilt_valid() {
    local install_dir="$1"
    arrow_prebuilt_valid "${install_dir}" && paimon_prebuilt_valid "${install_dir}"
}

# Validate an extracted candidate before replacing the selected thirdparty
# installation. This keeps an older build image usable when a download is
# incomplete or contains an incompatible Arrow/Paimon closure.
install_arrow_paimon_prebuilt_archive() {
    local archive="$1"
    local thirdparty_root="$2"
    local staging_dir
    local had_previous_installed=false

    staging_dir="$(mktemp -d "${thirdparty_root}/.arrow-paimon-install.XXXXXX")"
    mkdir -p "${staging_dir}/candidate"
    if ! tar -xf "${archive}" -C "${staging_dir}/candidate"; then
        rm -rf "${staging_dir}"
        return 1
    fi
    if ! arrow_paimon_prebuilt_valid "${staging_dir}/candidate/installed"; then
        rm -rf "${staging_dir}"
        return 1
    fi

    if [[ -e "${thirdparty_root}/installed" || -L "${thirdparty_root}/installed" ]]; then
        if ! mv "${thirdparty_root}/installed" "${staging_dir}/previous-installed"; then
            rm -rf "${staging_dir}"
            return 1
        fi
        had_previous_installed=true
    fi
    if ! mv "${staging_dir}/candidate/installed" "${thirdparty_root}/installed"; then
        if [[ "${had_previous_installed}" == "true" ]]; then
            mv "${staging_dir}/previous-installed" "${thirdparty_root}/installed"
        fi
        rm -rf "${staging_dir}"
        return 1
    fi
    rm -rf "${staging_dir}"
}

ensure_arrow_paimon_prebuilt_from_url() {
    local thirdparty_root="$1"
    local prebuilt_url="$2"
    local download_dir
    local archive

    if arrow_paimon_prebuilt_valid "${thirdparty_root}/installed"; then
        return 0
    fi

    echo "Refreshing Arrow/Paimon thirdparty libraries from ${prebuilt_url}"
    download_dir="$(mktemp -d "${thirdparty_root}/.arrow-paimon-download.XXXXXX")"
    archive="${download_dir}/doris-thirdparty-prebuilt.tar.xz"
    if ! curl --fail --location --retry 3 --show-error \
        --output "${archive}" "${prebuilt_url}"; then
        rm -rf "${download_dir}"
        return 1
    fi
    if ! install_arrow_paimon_prebuilt_archive "${archive}" "${thirdparty_root}"; then
        rm -rf "${download_dir}"
        return 1
    fi
    rm -rf "${download_dir}"
    arrow_paimon_prebuilt_valid "${thirdparty_root}/installed"
}

invalidate_arrow_prebuilt_marker() {
    local install_dir="$1"
    rm -f "${install_dir}/arrow-build-fingerprint.txt" \
        "${install_dir}/arrow-paimon-build-fingerprint.txt"
}

publish_arrow_prebuilt_marker() {
    local install_dir="$1"
    arrow_artifacts_valid "${install_dir}"
    arrow_build_fingerprint >"${install_dir}/arrow-build-fingerprint.txt"
}

invalidate_paimon_prebuilt_marker() {
    local install_dir="$1"
    rm -f "${install_dir}/paimon-build-fingerprint.txt" \
        "${install_dir}/arrow-paimon-build-fingerprint.txt"
}

publish_paimon_prebuilt_marker() {
    local install_dir="$1"
    paimon_artifacts_valid "${install_dir}"
    paimon_build_fingerprint >"${install_dir}/paimon-build-fingerprint.txt"
}

require_arrow_prebuilt_for_paimon() {
    local install_dir="$1"
    if ! arrow_prebuilt_valid "${install_dir}"; then
        echo "Paimon requires Arrow to be built from the currently selected inputs first" >&2
        return 1
    fi
}
