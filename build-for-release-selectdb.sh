#!/usr/bin/env bash
#
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

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
export DORIS_HOME="${ROOT}"

. "${DORIS_HOME}/build-support/selectdb-release-common.sh"

usage() {
    echo "
Usage: $0 --vendor <vendor> --version <version> <options>
  Optional options:
     --noavx2                         build without avx2
     --tar                            pack the output
     --disable-tde                    disable TDE feature modules
     --disable-tls                    disable TLS feature modules
     --disable-variant-nested-group   disable Variant NestedGroup feature module
     --disable-snapshot               disable Snapshot feature modules
     --build_only                     build only without packaging

  Eg.
    $0 --vendor selectdb --version 1.2.0
    $0 --vendor selectdb --version 1.2.0 --disable-tls
    $0 --vendor selectdb --build_only --version 1.2.0
"
    exit 1
}

if ! OPTS="$(getopt \
    -n "$0" \
    -o '' \
    -l 'noavx2' \
    -l 'tar' \
    -l 'disable-tde' \
    -l 'disable-tls' \
    -l 'disable-variant-nested-group' \
    -l 'disable-snapshot' \
    -l 'version:' \
    -l 'vendor:' \
    -l 'help' \
    -l 'build_only' \
    -- "$@")"; then
    usage
fi

eval set -- "${OPTS}"

_USE_AVX2=1
TAR=0
HELP=0
VERSION=
VENDOR=
BUILD_ONLY=0
ENABLE_TDE=1
ENABLE_TLS=1
ENABLE_VARIANT_NESTED_GROUP=1
ENABLE_SNAPSHOT=1

while true; do
    case "$1" in
    --noavx2)
        _USE_AVX2=0
        shift
        ;;
    --tar)
        TAR=1
        shift
        ;;
    --disable-tde)
        ENABLE_TDE=0
        shift
        ;;
    --disable-tls)
        ENABLE_TLS=0
        shift
        ;;
    --disable-variant-nested-group)
        ENABLE_VARIANT_NESTED_GROUP=0
        shift
        ;;
    --disable-snapshot)
        ENABLE_SNAPSHOT=0
        shift
        ;;
    --version)
        VERSION="$2"
        shift 2
        ;;
    --vendor)
        VENDOR="$2"
        shift 2
        ;;
    --build_only)
        BUILD_ONLY=1
        shift
        ;;
    --help)
        HELP=1
        shift
        ;;
    --)
        shift
        break
        ;;
    *)
        echo "Internal error"
        exit 1
        ;;
    esac
done

if [[ "${HELP}" -eq 1 ]]; then
    usage
fi
if [[ -z "${VENDOR}" ]]; then
    echo "--vendor must be specified, choose one of selectdb, velodb"
    usage
fi
if [[ -z "${VERSION}" && "${BUILD_ONLY}" -eq 0 ]]; then
    echo "Must specify version"
    usage
fi

# These env vars are used by gensrc/script/gen_build_version.sh.
export DORIS_BUILD_VERSION_PREFIX="${VENDOR}"
export DORIS_BUILD_VERSION_MAJOR="${DORIS_BUILD_VERSION_MAJOR:-4}"
export DORIS_BUILD_VERSION_MINOR="${DORIS_BUILD_VERSION_MINOR:-0}"
export DORIS_BUILD_VERSION_PATCH="${DORIS_BUILD_VERSION_PATCH:-4}"
export DORIS_BUILD_VERSION_HOTFIX="${DORIS_BUILD_VERSION_HOTFIX:-0}"
export DORIS_BUILD_VERSION_RC_VERSION="${DORIS_BUILD_VERSION_RC_VERSION:-rc01}"

selectdb_configure_extra_modules \
    "${ENABLE_TDE}" \
    "${ENABLE_TLS}" \
    "${ENABLE_VARIANT_NESTED_GROUP}" \
    "${ENABLE_SNAPSHOT}"

echo "Get params:
    VERSION                     -- ${VERSION}
    VENDOR                      -- ${VENDOR}
    USE_AVX2                    -- ${_USE_AVX2}
    TAR                         -- ${TAR}
    BUILD_ONLY                  -- ${BUILD_ONLY}
    ENABLE_TDE                  -- ${ENABLE_TDE}
    ENABLE_TLS                  -- ${ENABLE_TLS}
    ENABLE_VARIANT_NESTED_GROUP -- ${ENABLE_VARIANT_NESTED_GROUP}
    ENABLE_SNAPSHOT             -- ${ENABLE_SNAPSHOT}
    EXTRA_FE_MODULES            -- ${EXTRA_FE_MODULES}
    EXTRA_BE_MODULES            -- ${EXTRA_BE_MODULES}
    EXTRA_CLOUD_MODULES         -- ${EXTRA_CLOUD_MODULES}
"

ARCH="$(uname -m)"
if [[ "${ARCH}" == "aarch64" ]]; then
    ARCH="arm64"
elif [[ "${ARCH}" == "x86_64" ]]; then
    ARCH="x64"
else
    echo "Unknown arch: ${ARCH}"
    exit 1
fi

export USE_AVX2="${_USE_AVX2}"
ORI_OUTPUT="${ROOT}/output"

if [[ "${BUILD_ONLY}" -eq 1 ]]; then
    sh build.sh
    selectdb_copy_extra_fe_libs "${ORI_OUTPUT}"
    exit 0
fi

PACKAGE_NAME="${VENDOR}-doris-${VERSION}-bin-${ARCH}"
if [[ "${_USE_AVX2}" == "0" && "${ARCH}" == "x64" ]]; then
    PACKAGE_NAME="${PACKAGE_NAME}-noavx2"
fi
OUTPUT="${ORI_OUTPUT}/${PACKAGE_NAME}"

rm -rf "${OUTPUT}"
mkdir -p "${OUTPUT}"

sh build.sh
sh build.sh --be --meta-tool --cloud
selectdb_copy_extra_fe_libs "${ORI_OUTPUT}"

cp -r "${ORI_OUTPUT}/fe" "${OUTPUT}/fe"
cp -r "${ORI_OUTPUT}/be" "${OUTPUT}/be"
cp -r "${ORI_OUTPUT}/ms" "${OUTPUT}/ms"
cp -r "${ORI_OUTPUT}/apache_hdfs_broker" "${OUTPUT}/apache_hdfs_broker"
cp -r "${ORI_OUTPUT}/tools" "${OUTPUT}/tools"

if [[ "${TAR}" -eq 1 ]]; then
    echo "Begin to compress"
    cd "${ORI_OUTPUT}"
    tar -cf - "${PACKAGE_NAME}" | xz -T0 -z - >"${PACKAGE_NAME}.tar.xz"
    cd -
fi

echo "Output dir: ${OUTPUT}"
