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

# These functions are shared by the two Rust static-library builders.
rust_archive_sha256() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | awk '{print $1}'
    else
        shasum -a 256 "$1" | awk '{print $1}'
    fi
}

check_rust_toolchain_identity() {
    local pkg="$1" cargo_bin="$2"
    shift 2
    local rustc_bin="${RUSTC:-rustc}"
    if [[ -z "${RUSTC:-}" && "${cargo_bin}" == */* && -x "${cargo_bin%/*}/rustc" ]]; then
        rustc_bin="${cargo_bin%/*}/rustc"
    fi
    local identity
    identity="$(env "$@" "${rustc_bin}" -vV)" || return 1
    local archive sidecar digest recorded_identity
    for archive in "${TP_INSTALL_DIR}/lib64/liblance_c.a" "${TP_INSTALL_DIR}/lib64/libpaimon_c.a"; do
        sidecar="${archive}.rust-id"
        if [[ ! -e "${archive}" && ! -e "${sidecar}" ]]; then
            continue
        fi
        # A legacy shared stamp cannot identify an interrupted replacement or
        # an archive installed before its stamp. Bind each identity to its bytes.
        if [[ ! -f "${archive}" || ! -f "${sidecar}" ]]; then
            echo "Incomplete Rust archive installation; rebuild lance_c and paimon_rust together." >&2
            return 1
        fi
        digest="$(head -n 1 "${sidecar}")"
        recorded_identity="$(tail -n +2 "${sidecar}")"
        if [[ "${digest}" != "$(rust_archive_sha256 "${archive}")" || "${recorded_identity}" != "${identity}" ]]; then
            echo "Rust archive content/toolchain mismatch; rebuild lance_c and paimon_rust together." >&2
            return 1
        fi
    done
    RUST_TOOLCHAIN_IDENTITY="${identity}"
    echo "${pkg}: installed Rust archive identities verified."
}

install_rust_archive() (
    set -e
    local source="$1"
    local destination="${TP_INSTALL_DIR}/lib64/${source##*/}"
    local staging
    mkdir -p "${TP_INSTALL_DIR}/lib64"
    staging="$(mktemp -d "${TP_INSTALL_DIR}/lib64/.rust-install.XXXXXX")"
    trap 'rm -rf "${staging}"' EXIT
    # Copy/strip failures must leave the previously installed archive untouched.
    cp "${source}" "${staging}/archive"
    if [[ "${STRIP_TP_LIB:-OFF}" == "ON" && "${KERNEL}" != 'Darwin' ]]; then
        strip --strip-debug --strip-unneeded "${staging}/archive"
    fi
    : "${RUST_TOOLCHAIN_IDENTITY:?Rust toolchain identity must be checked before installation}"
    rust_archive_sha256 "${staging}/archive" > "${staging}/identity"
    printf '%s\n' "${RUST_TOOLCHAIN_IDENTITY}" >> "${staging}/identity"
    # Each rename is atomic on this filesystem. If interrupted between them,
    # the missing/mismatched digest makes the next build reject the partial pair.
    mv -f "${staging}/archive" "${destination}"
    mv -f "${staging}/identity" "${destination}.rust-id"
)

verified_paimon_vindex_crate() {
    local lock="$1" cargo_bin="$2"
    shift 2
    local checksum cache candidate pass offline
    checksum="$(awk '
        $0 == "[[package]]" { name = ""; version = "" }
        $1 == "name" { gsub(/[",]/, "", $3); name = $3 }
        $1 == "version" { gsub(/[",]/, "", $3); version = $3 }
        $1 == "checksum" && name == "paimon-vindex-core" && version == "0.4.0" {
            gsub(/[",]/, "", $3); print $3; exit
        }
    ' "${lock}")"
    if [[ -z "${checksum}" ]]; then
        echo "Missing paimon-vindex-core checksum in Cargo.lock" >&2
        return 1
    fi
    cache="${CARGO_HOME:-$HOME/.cargo}/registry/cache"
    offline="$(printf '%s' "${PAIMON_RUST_CARGO_OFFLINE:-OFF}" | tr '[:lower:]' '[:upper:]')"
    for pass in 0 1; do
        # No cache directory is normal on a fresh host. Validate all candidates,
        # not just a matching filename, before deciding whether a fetch is needed.
        if [[ -d "${cache}" ]]; then
            while IFS= read -r candidate; do
                if [[ "$(rust_archive_sha256 "${candidate}")" == "${checksum}" ]]; then
                    printf '%s\n' "${candidate}"
                    return 0
                fi
            done < <(find "${cache}" -type f -name 'paimon-vindex-core-0.4.0.crate')
        fi
        if [[ "${offline}" == "ON" || "${pass}" == 1 ]]; then
            break
        fi
        # Cargo may trust a same-named cached crate. Remove only invalid copies
        # of this locked package so an online retry can download authentic bytes.
        if [[ -d "${cache}" ]]; then
            find "${cache}" -type f -name 'paimon-vindex-core-0.4.0.crate' -delete
        fi
        env "$@" "${cargo_bin}" fetch --locked >&2 || return 1
    done
    echo "No checksum-matching paimon-vindex-core crate available (offline=${offline})." >&2
    return 1
}
