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

# Arrow 24 is installed in a versioned prefix. The unversioned install prefix is
# deliberately reserved for Arrow 17 so the shared thirdparty package remains
# consumable by branch-4.1.
ARROW_VERSION="24.0.0"
ARROW_DOWNLOAD="https://github.com/apache/arrow/archive/refs/tags/apache-arrow-${ARROW_VERSION}.tar.gz"
ARROW_NAME="apache-arrow-${ARROW_VERSION}.tar.gz"
ARROW_SOURCE="arrow-apache-arrow-${ARROW_VERSION}"
ARROW_MD5SUM="66c53bd00baa79034bd2ca167beea436"
ARROW_INSTALL_SUBDIR="arrow-${ARROW_VERSION}"

# Arrow 17 compatibility stack for branch-4.1. Keep these variables separate
# from ARROW_* so master can build both versions from one source bundle.
ARROW_17_VERSION="17.0.0"
ARROW_17_DOWNLOAD="https://github.com/apache/arrow/archive/refs/tags/apache-arrow-${ARROW_17_VERSION}.tar.gz"
ARROW_17_NAME="apache-arrow-${ARROW_17_VERSION}.tar.gz"
ARROW_17_SOURCE="arrow-apache-arrow-${ARROW_17_VERSION}"
ARROW_17_MD5SUM="ba18bf83e2164abd34b9ac4cb164f0f0"

# Arrow bundled dependencies
BROTLI_DOWNLOAD="https://github.com/google/brotli/archive/v1.0.9.tar.gz"
BROTLI_NAME="brotli-1.0.9.tar.gz"
BROTLI_SOURCE="brotli-1.0.9"
BROTLI_MD5SUM="c2274f0c7af8470ad514637c35bcee7d"

XSIMD_DOWNLOAD="https://github.com/xtensor-stack/xsimd/archive/refs/tags/14.0.0.tar.gz"
XSIMD_NAME="14.0.0.tar.gz"
XSIMD_SOURCE=xsimd-14.0.0
XSIMD_MD5SUM="75c0d34cf7011924ba19978076c76dc1"

XSIMD_17_DOWNLOAD="https://github.com/xtensor-stack/xsimd/archive/refs/tags/13.0.0.tar.gz"
XSIMD_17_NAME="13.0.0.tar.gz"
XSIMD_17_SOURCE=xsimd-13.0.0
XSIMD_17_MD5SUM="c661deb91836e82d3070f81032014fe6"

# paimon-cpp
PAIMON_CPP_DOWNLOAD="https://github.com/apache/doris-thirdparty/archive/refs/tags/paimon-cpp-0a4f4e2.tar.gz"
PAIMON_CPP_NAME="paimon-cpp-0a4f4e2.tar.gz"
PAIMON_CPP_SOURCE="doris-thirdparty-paimon-cpp-0a4f4e2"
PAIMON_CPP_MD5SUM="b8599a0421dbf1ec05e2f1a481d64e87"

# Both Paimon variants use the same archive, but they need independent source
# trees because only the Arrow 24 tree receives the API compatibility patches.
PAIMON_CPP_17_DOWNLOAD="${PAIMON_CPP_DOWNLOAD}"
PAIMON_CPP_17_NAME="${PAIMON_CPP_NAME}"
PAIMON_CPP_17_ARCHIVE_SOURCE="${PAIMON_CPP_SOURCE}"
PAIMON_CPP_17_SOURCE="${PAIMON_CPP_SOURCE}-arrow-17"
PAIMON_CPP_17_MD5SUM="${PAIMON_CPP_MD5SUM}"

# Arrow consumes xsimd and Brotli as bundled source archives, but neither is a
# build target in the focused Arrow/Paimon recovery path.
ARROW_PAIMON_17_BUILD_PACKAGES=(arrow_17 paimon_cpp_17)
ARROW_PAIMON_BUILD_PACKAGES=(arrow paimon_cpp)
ARROW_PAIMON_SHARED_BUILD_PACKAGES=(
    "${ARROW_PAIMON_17_BUILD_PACKAGES[@]}"
    "${ARROW_PAIMON_BUILD_PACKAGES[@]}"
)
ARROW_PAIMON_REBUILD_PACKAGES=()
ARROW_BUNDLED_SOURCE_PACKAGES=(xsimd brotli)
ARROW_17_BUNDLED_SOURCE_PACKAGES=(xsimd_17 brotli)
ARROW_PAIMON_DOWNLOAD_PACKAGES=()

prepare_arrow_paimon_download_packages() {
    ARROW_PAIMON_DOWNLOAD_PACKAGES=("$@")

    local package
    local source_package
    local arrow_requested=false
    local arrow_17_requested=false
    local source_requested
    for package in "$@"; do
        if [[ "${package}" == "arrow" ]]; then
            arrow_requested=true
        elif [[ "${package}" == "arrow_17" ]]; then
            arrow_17_requested=true
        fi
    done

    local bundled_source_packages=()
    if [[ "${arrow_requested}" == "true" ]]; then
        bundled_source_packages+=("${ARROW_BUNDLED_SOURCE_PACKAGES[@]}")
    fi
    if [[ "${arrow_17_requested}" == "true" ]]; then
        bundled_source_packages+=("${ARROW_17_BUNDLED_SOURCE_PACKAGES[@]}")
    fi

    for source_package in "${bundled_source_packages[@]}"; do
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

arrow_install_dir() {
    printf '%s/%s\n' "$1" "${ARROW_INSTALL_SUBDIR}"
}

# Identify the checked-in source, patch, and build inputs selected for Arrow.
# Arrow and Paimon publish separate installed markers so a package-only build
# cannot certify a component that it did not rebuild.
arrow_build_fingerprint() {
    local vars_dir
    vars_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    (
        cd "${vars_dir}" || return 1
        LC_ALL=C
        git hash-object \
            ../env.sh \
            arrow-paimon-vars.sh \
            vars.sh \
            download-thirdparty.sh \
            build-thirdparty.sh \
            patches/apache-arrow-"${ARROW_VERSION}"-*.patch |
            git hash-object --stdin
    )
}

paimon_build_fingerprint() {
    local vars_dir
    vars_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    (
        cd "${vars_dir}" || return 1
        LC_ALL=C
        {
            arrow_build_fingerprint
            git hash-object \
                arrow-paimon-vars.sh \
                vars.sh \
                download-thirdparty.sh \
                build-thirdparty.sh \
                paimon-cpp-cache.cmake \
                patches/paimon-cpp-buildutils-static-deps.patch \
                patches/paimon-cpp-arrow-24-compatibility.patch \
                patches/paimon-cpp-arrow-24-compute.patch
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

arrow_17_build_fingerprint() {
    local vars_dir
    vars_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    (
        cd "${vars_dir}" || return 1
        LC_ALL=C
        git hash-object \
            ../env.sh \
            arrow-paimon-vars.sh \
            vars.sh \
            download-thirdparty.sh \
            build-thirdparty.sh \
            patches/apache-arrow-"${ARROW_17_VERSION}"-*.patch |
            git hash-object --stdin
    )
}

paimon_17_build_fingerprint() {
    local vars_dir
    vars_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    (
        cd "${vars_dir}" || return 1
        LC_ALL=C
        {
            arrow_17_build_fingerprint
            git hash-object \
                arrow-paimon-vars.sh \
                vars.sh \
                download-thirdparty.sh \
                build-thirdparty.sh \
                paimon-cpp-cache.cmake \
                patches/paimon-cpp-buildutils-static-deps.patch
        } | git hash-object --stdin
    )
}

arrow_paimon_17_build_fingerprint() {
    {
        arrow_17_build_fingerprint
        paimon_17_build_fingerprint
    } | git hash-object --stdin
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

ARROW_17_REQUIRED_LIBRARIES=(
    libbrotlicommon.a
    libbrotlidec.a
    libbrotlienc.a
    libarrow.a
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

# Remove only artifacts owned by the selected Arrow/Paimon stack before an
# install. This matters for the legacy prefix: installing Arrow 17 over an
# existing Arrow 24 prefix must not leave Arrow 24-only headers or libraries
# behind and turn it into a mixed, internally inconsistent SDK.
clean_arrow_artifacts_in() {
    local install_dir="$1"
    : "${install_dir:?Arrow install directory must be set}"

    rm -rf -- \
        "${install_dir}/include/arrow" \
        "${install_dir}/include/parquet" \
        "${install_dir}/share/arrow" \
        "${install_dir}/share/doc/arrow"

    (
        shopt -s nullglob
        local generated_artifacts=(
            "${install_dir}/lib64"/libarrow*
            "${install_dir}/lib64"/libparquet*
            "${install_dir}/lib64/cmake"/Arrow*
            "${install_dir}/lib64/cmake"/Parquet
            "${install_dir}/lib64/pkgconfig"/arrow*.pc
            "${install_dir}/lib64/pkgconfig"/parquet.pc
        )
        rm -rf -- "${generated_artifacts[@]}"
    )
}

clean_paimon_artifacts_in() {
    local install_dir="$1"
    : "${install_dir:?Paimon install directory must be set}"

    rm -rf -- \
        "${install_dir}/include/paimon" \
        "${install_dir}/lib64/cmake/Paimon" \
        "${install_dir}/paimon-cpp"

    (
        shopt -s nullglob
        local generated_artifacts=(
            "${install_dir}/lib64"/libpaimon*
            "${install_dir}/lib64"/libroaring_bitmap_paimon.*
            "${install_dir}/lib64"/libxxhash_paimon.*
            "${install_dir}/lib64"/libfmt_paimon.*
            "${install_dir}/lib64"/libtbb_paimon.*
        )
        rm -rf -- "${generated_artifacts[@]}"
    )
}

ARROW_PAIMON_REQUIRED_LIBRARIES=(
    "${ARROW_REQUIRED_LIBRARIES[@]}"
    "${PAIMON_REQUIRED_LIBRARIES[@]}"
)

arrow_artifacts_valid() {
    local install_dir
    install_dir="$(arrow_install_dir "$1")"
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

arrow_17_artifacts_valid() {
    local install_dir="$1"
    local installed_arrow_version
    local library

    if [[ ! -f "${install_dir}/include/arrow/util/config.h" ]]; then
        echo "Missing installed Arrow 17 version header" >&2
        return 1
    fi
    installed_arrow_version="$(
        awk '$1 == "#define" && $2 == "ARROW_VERSION_STRING" {
               gsub(/"/, "", $3); print $3; exit
             }' "${install_dir}/include/arrow/util/config.h"
    )"
    if [[ "${installed_arrow_version}" != "${ARROW_17_VERSION}" ]]; then
        echo "Installed legacy Arrow version ${installed_arrow_version} does not match ${ARROW_17_VERSION}" >&2
        return 1
    fi

    for library in "${ARROW_17_REQUIRED_LIBRARIES[@]}"; do
        if [[ ! -f "${install_dir}/lib64/${library}" ]]; then
            echo "Missing Arrow 17 library: ${library}" >&2
            return 1
        fi
    done
    return 0
}

paimon_artifacts_valid_in() {
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

paimon_artifacts_valid() {
    paimon_artifacts_valid_in "$(arrow_install_dir "$1")"
}

arrow_prebuilt_valid() {
    local install_dir
    install_dir="$(arrow_install_dir "$1")"
    local arrow_fingerprint_mark="${install_dir}/arrow-build-fingerprint.txt"
    local expected_fingerprint
    local installed_fingerprint

    if [[ ! -f "${arrow_fingerprint_mark}" ]]; then
        echo "Missing Arrow build fingerprint: ${arrow_fingerprint_mark}" >&2
        return 1
    fi
    expected_fingerprint="$(arrow_build_fingerprint)"
    installed_fingerprint="$(<"${arrow_fingerprint_mark}")"
    if [[ "${installed_fingerprint}" != "${expected_fingerprint}" ]]; then
        echo "Arrow build fingerprint does not match selected inputs" >&2
        return 1
    fi
    arrow_artifacts_valid "$1"
}

paimon_prebuilt_valid() {
    local install_dir
    install_dir="$(arrow_install_dir "$1")"
    local paimon_fingerprint_mark="${install_dir}/paimon-build-fingerprint.txt"
    local expected_fingerprint
    local installed_fingerprint

    if [[ ! -f "${paimon_fingerprint_mark}" ]]; then
        echo "Missing Paimon build fingerprint: ${paimon_fingerprint_mark}" >&2
        return 1
    fi
    expected_fingerprint="$(paimon_build_fingerprint)"
    installed_fingerprint="$(<"${paimon_fingerprint_mark}")"
    if [[ "${installed_fingerprint}" != "${expected_fingerprint}" ]]; then
        echo "Paimon build fingerprint does not match selected inputs" >&2
        return 1
    fi
    paimon_artifacts_valid "$1"
}

arrow_paimon_prebuilt_valid() {
    local install_dir="$1"
    arrow_prebuilt_valid "${install_dir}" && paimon_prebuilt_valid "${install_dir}"
}

invalidate_arrow_prebuilt_marker() {
    local install_dir
    install_dir="$(arrow_install_dir "$1")"
    mkdir -p "${install_dir}"
    rm -f "${install_dir}/arrow-build-fingerprint.txt" \
        "${install_dir}/arrow-paimon-build-fingerprint.txt"
}

publish_arrow_prebuilt_marker() {
    local install_dir
    install_dir="$(arrow_install_dir "$1")"
    arrow_artifacts_valid "$1"
    arrow_build_fingerprint >"${install_dir}/arrow-build-fingerprint.txt"
}

invalidate_paimon_prebuilt_marker() {
    local install_dir
    install_dir="$(arrow_install_dir "$1")"
    mkdir -p "${install_dir}"
    rm -f "${install_dir}/paimon-build-fingerprint.txt" \
        "${install_dir}/arrow-paimon-build-fingerprint.txt"
}

publish_paimon_prebuilt_marker() {
    local install_dir
    install_dir="$(arrow_install_dir "$1")"
    paimon_artifacts_valid "$1"
    paimon_build_fingerprint >"${install_dir}/paimon-build-fingerprint.txt"
}

require_arrow_prebuilt_for_paimon() {
    local install_dir="$1"
    if ! arrow_prebuilt_valid "${install_dir}"; then
        echo "Paimon requires Arrow to be built from the currently selected inputs first" >&2
        return 1
    fi
}

invalidate_arrow_17_prebuilt_marker() {
    local install_dir="$1"
    rm -f "${install_dir}/arrow-17-build-fingerprint.txt" \
        "${install_dir}/arrow-paimon-17-build-fingerprint.txt"
}

publish_arrow_17_prebuilt_marker() {
    local install_dir="$1"
    arrow_17_artifacts_valid "${install_dir}"
    arrow_17_build_fingerprint >"${install_dir}/arrow-17-build-fingerprint.txt"
}

arrow_17_prebuilt_valid() {
    local install_dir="$1"
    local fingerprint_mark="${install_dir}/arrow-17-build-fingerprint.txt"
    local expected_fingerprint

    if [[ ! -f "${fingerprint_mark}" ]]; then
        echo "Missing Arrow 17 build fingerprint: ${fingerprint_mark}" >&2
        return 1
    fi
    expected_fingerprint="$(arrow_17_build_fingerprint)"
    if [[ "$(<"${fingerprint_mark}")" != "${expected_fingerprint}" ]]; then
        echo "Arrow 17 build fingerprint does not match selected inputs" >&2
        return 1
    fi
    arrow_17_artifacts_valid "${install_dir}"
}

require_arrow_17_prebuilt_for_paimon() {
    local install_dir="$1"
    if ! arrow_17_prebuilt_valid "${install_dir}"; then
        echo "Paimon for branch-4.1 requires Arrow 17 to be built first" >&2
        return 1
    fi
}

invalidate_paimon_17_prebuilt_marker() {
    local install_dir="$1"
    rm -f "${install_dir}/paimon-arrow-17-build-fingerprint.txt" \
        "${install_dir}/arrow-paimon-17-build-fingerprint.txt"
}

publish_paimon_17_prebuilt_marker() {
    local install_dir="$1"
    paimon_artifacts_valid_in "${install_dir}"
    paimon_17_build_fingerprint >"${install_dir}/paimon-arrow-17-build-fingerprint.txt"
}

paimon_17_prebuilt_valid() {
    local install_dir="$1"
    local fingerprint_mark="${install_dir}/paimon-arrow-17-build-fingerprint.txt"
    local expected_fingerprint

    if [[ ! -f "${fingerprint_mark}" ]]; then
        echo "Missing Paimon Arrow 17 build fingerprint: ${fingerprint_mark}" >&2
        return 1
    fi
    expected_fingerprint="$(paimon_17_build_fingerprint)"
    if [[ "$(<"${fingerprint_mark}")" != "${expected_fingerprint}" ]]; then
        echo "Paimon Arrow 17 build fingerprint does not match selected inputs" >&2
        return 1
    fi
    paimon_artifacts_valid_in "${install_dir}"
}

arrow_paimon_17_prebuilt_valid() {
    local install_dir="$1"
    arrow_17_prebuilt_valid "${install_dir}" &&
        paimon_17_prebuilt_valid "${install_dir}"
}

shared_arrow_paimon_prebuilt_valid() {
    local install_dir="$1"
    arrow_paimon_17_prebuilt_valid "${install_dir}" &&
        arrow_paimon_prebuilt_valid "${install_dir}"
}

select_arrow_paimon_rebuild_packages() {
    local install_dir="$1"
    ARROW_PAIMON_REBUILD_PACKAGES=()

    if ! arrow_paimon_17_prebuilt_valid "${install_dir}"; then
        ARROW_PAIMON_REBUILD_PACKAGES+=("${ARROW_PAIMON_17_BUILD_PACKAGES[@]}")
    fi
    if ! arrow_paimon_prebuilt_valid "${install_dir}"; then
        ARROW_PAIMON_REBUILD_PACKAGES+=("${ARROW_PAIMON_BUILD_PACKAGES[@]}")
    fi
}
