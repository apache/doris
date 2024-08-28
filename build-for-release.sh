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

##############################################################
# This script is used to build for Apache Doris Release
##############################################################

set -eo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

export DORIS_HOME="${ROOT}"

# Check args
usage() {
    echo "
Usage: $0 --version version <options>
  Optional options:
     [no option]        build with avx2
     --noavx2           build without avx2
     --tar              pack the output

  Eg.
    $0 --version 1.2.0                      build with avx2
    $0 --noavx2 --version 1.2.0             build without avx2
    $0 --version 1.2.0 --tar                build with avx2 and pack the output
  "
    exit 1
}

if ! OPTS="$(getopt \
    -n "$0" \
    -o '' \
    -l 'noavx2' \
    -l 'tar' \
    -l 'version:' \
    -l 'help' \
    -- "$@")"; then
    usage
fi

eval set -- "${OPTS}"

_USE_AVX2=1
TAR=0
HELP=0
VERSION=
if [[ "$#" == 1 ]]; then
    _USE_AVX2=1
else
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
        --version)
            VERSION="$2"
            shift 2
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
fi

if [[ "${HELP}" -eq 1 ]]; then
    usage
fi

if [[ -z ${VERSION} ]]; then
    echo "Must specify version"
    usage
fi

echo "Get params:
    VERSION         -- ${VERSION}
    USE_AVX2        -- ${_USE_AVX2}
    TAR             -- ${TAR}
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

echo "ARCH: ${ARCH}"

ORI_OUTPUT="${ROOT}/output"
rm -rf "${ORI_OUTPUT}"

FE="fe"
BE="be"
CLOUD="ms"
EXT="extensions"
PACKAGE="apache-doris-${VERSION}-bin-${ARCH}"

if [[ "${_USE_AVX2}" == "0" ]]; then
    PACKAGE="${PACKAGE}-noavx2"
fi

OUTPUT="${ORI_OUTPUT}/${PACKAGE}"
OUTPUT_FE="${OUTPUT}/${FE}"
OUTPUT_EXT="${OUTPUT}/${EXT}"
OUTPUT_BE="${OUTPUT}/${BE}"
OUTPUT_CLOUD="${OUTPUT}/${CLOUD}"

echo "Package Name:"
echo "FE:    ${OUTPUT_FE}"
echo "BE:    ${OUTPUT_BE}"
echo "CLOUD: ${OUTPUT_CLOUD}"
echo "JAR:   ${OUTPUT_EXT}"

sh build.sh --clean &&
    USE_AVX2="${_USE_AVX2}" sh build.sh &&
    USE_AVX2="${_USE_AVX2}" sh build.sh --be --meta-tool --be-extension-ignore avro-scanner

echo "Begin to pack"
rm -rf "${OUTPUT}"
mkdir -p "${OUTPUT_FE}" "${OUTPUT_BE}" "${OUTPUT_EXT}" "${OUTPUT_CLOUD}"

# FE
cp -R "${ORI_OUTPUT}"/fe/* "${OUTPUT_FE}"/

# EXT
cp -R "${ORI_OUTPUT}"/apache_hdfs_broker "${OUTPUT_EXT}"/apache_hdfs_broker

# BE
cp -R "${ORI_OUTPUT}"/be/* "${OUTPUT_BE}"/

# CLOUD
if [[ "${ARCH}" == "arm64" ]]; then
    echo "WARNING: Cloud module is not supported on ARM platform, will skip building it."
else
    cp -R "${ORI_OUTPUT}"/ms/* "${OUTPUT_CLOUD}"/
fi

if [[ "${TAR}" -eq 1 ]]; then
    echo "Begin to compress"
    cd "${ORI_OUTPUT}"
    tar -cf - "${PACKAGE}" | xz -T0 -z - >"${PACKAGE}".tar.xz
    cd -
fi

echo "Output dir: ${OUTPUT}"
exit 0
