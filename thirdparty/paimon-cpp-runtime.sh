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
export LC_ALL=C

# These are compiler runtime components, intentionally resolved by the selected compiler,
# not Doris-managed packages. Never use ldconfig or a host-library fallback: LDB's unversioned
# libstdc++.so/libgcc_s.so are linker scripts which select static archives.
paimon_runtime_library() {
    local compiler="$1" soname="$2" library dynamic
    library="$("${compiler}" "-print-file-name=${soname}")" || return 1
    if [[ "${library}" != /* || ! -f "${library}" ]]; then
        echo "Paimon requires ${soname} from ${compiler}; got '${library}'" >&2
        return 1
    fi
    dynamic="$(readelf -dW "${library}")" || return 1
    if [[ "${dynamic}" != *"Library soname: [${soname}]"* ]]; then
        echo "Paimon runtime is not an ELF shared library with SONAME ${soname}: ${library}" >&2
        return 1
    fi
    printf '%s\n' "${library}"
}

paimon_runtime_link_flags() {
    local stdcxx gcc
    stdcxx="$(paimon_runtime_library "$1" libstdc++.so.6)" || return 1
    gcc="$(paimon_runtime_library "$1" libgcc_s.so.1)" || return 1
    # CMAKE_CXX_STANDARD_LIBRARIES is emitted after object/dependency archives and before
    # the compiler's implicit -lstdc++/-lgcc_s. Real DSOs resolve new/delete and exceptions
    # before LDB's implicit static archives can supply private copies hidden by Paimon.
    printf '%s\n' "-Wl,--push-state,--no-as-needed \"${stdcxx}\" \"${gcc}\" -Wl,--pop-state"
}

paimon_runtime_install() {
    local compiler="$1" destination="$2" soname library
    mkdir -p "${destination}" || return 1
    for soname in libstdc++.so.6 libgcc_s.so.1; do
        library="$(paimon_runtime_library "${compiler}" "${soname}")" || return 1
        # Dereference toolchain symlinks. The installed package must be relocatable.
        cp -L "${library}" "${destination}/${soname}" || return 1
    done
}

paimon_runtime_check() {
    local directory="$1" library dynamic symbols
    for library in libstdc++.so.6 libgcc_s.so.1 libpaimon.so \
            libpaimon_parquet_file_format.so libpaimon_avro_file_format.so; do
        if [[ ! -f "${directory}/${library}" ]]; then
            echo "Missing Paimon runtime artifact: ${directory}/${library}" >&2
            return 1
        fi
    done
    for library in libstdc++.so.6 libgcc_s.so.1; do
        dynamic="$(readelf -dW "${directory}/${library}")" || return 1
        if [[ "${dynamic}" != *"Library soname: [${library}]"* ]]; then
            echo "Invalid packaged Paimon runtime: ${directory}/${library}" >&2
            return 1
        fi
    done
    for library in "${directory}"/libpaimon*.so; do
        dynamic="$(readelf -dW "${library}")" || return 1
        if [[ "${dynamic}" != *"Shared library: [libstdc++.so.6]"* ||
              "${dynamic}" != *"Shared library: [libgcc_s.so.1]"* ||
              "${dynamic}" == *"Shared library: [libunwind.so"* ]]; then
            echo "Paimon has an unexpected C++/unwind runtime dependency: ${library}" >&2
            return 1
        fi
        if [[ "${dynamic}" != *'Library runpath: [$ORIGIN]'* &&
              "${dynamic}" != *'Library rpath: [$ORIGIN]'* ]]; then
            echo "Paimon must resolve its packaged runtime using \$ORIGIN: ${library}" >&2
            return 1
        fi
        symbols="$(readelf -sW "${library}")" || return 1
        # Check local symbols too: --exclude-libs/--version-script can hide the offending
        # static allocator from nm -D. Keep Arrow isolation; do not hide a private allocator.
        # Placement new/delete are header-only helpers, not replaceable allocators.
        if awk '$7 != "UND" && ($8 ~ /^_Zn[aw][mj]($|@|\.|RKSt9nothrow_t|St11align_val_t)/ ||
                $8 ~ /^_Zd[al]Pv($|@|\.|[mj]($|@|\.|St11align_val_t)|RKSt9nothrow_t|St11align_val_t)/) { found = 1 }
                END { exit !found }' <<< "${symbols}"; then
            echo "Paimon contains a private new/delete implementation: ${library}" >&2
            return 1
        fi
    done
}

if [[ "${BASH_SOURCE[0]:-$0}" == "$0" ]]; then
    case "${1:-}" in
        link-flags) paimon_runtime_link_flags "${2:?compiler required}" ;;
        install) paimon_runtime_install "${2:?compiler required}" "${3:?destination required}" ;;
        check) paimon_runtime_check "${2:?library directory required}" ;;
        *) echo "Usage: $0 {link-flags COMPILER | install COMPILER DIRECTORY | check DIRECTORY}" >&2; exit 1 ;;
    esac
fi
