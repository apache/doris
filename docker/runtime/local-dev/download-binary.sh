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

# Download official Apache Doris release tarball (fe + be) for the current machine,
# then you can docker build with --build-arg OUTPUT_PATH=... (see printed hint).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# docker/runtime/local-dev -> Doris repo root
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"

VERSION=""
OUT_DIR=""
BASE_URL="https://apache-doris-releases.oss-accelerate.aliyuncs.com"
SUFFIX=""

usage() {
    cat <<EOF
Usage: $0 -v <version> [options]

  -v <version>          Doris release version, e.g. 3.0.5
  -o <dir>              Extract here (default: ${SCRIPT_DIR}/.prebuilt/apache-doris-<ver>-bin-<suffix>/)
  --base-url <url>      Download host (default: ${BASE_URL})
  --suffix <name>       Force package suffix: arm64 | x64 | x64-noavx2 (default: auto from OS/CPU)

Examples:
  $0 -v 3.0.5
  $0 -v 2.1.7 --suffix x64-noavx2
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        -v)
            VERSION="${2:-}"
            shift 2
            ;;
        -o)
            OUT_DIR="${2:-}"
            shift 2
            ;;
        --base-url)
            BASE_URL="${2:-}"
            shift 2
            ;;
        --suffix)
            SUFFIX="${2:-}"
            shift 2
            ;;
        -h | --help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1" >&2
            usage
            exit 1
            ;;
    esac
done

if [[ -z "${VERSION}" ]]; then
    usage
    exit 1
fi

detect_suffix() {
    local machine
    machine="$(uname -m)"
    case "${machine}" in
        arm64 | aarch64)
            echo arm64
            ;;
        x86_64 | amd64)
            if [[ "$(uname -s)" == Linux ]] && [[ -r /proc/cpuinfo ]] && ! grep -q avx2 /proc/cpuinfo; then
                echo x64-noavx2
            else
                echo x64
            fi
            ;;
        *)
            echo "Unsupported machine: ${machine}" >&2
            exit 1
            ;;
    esac
}

if [[ -z "${SUFFIX}" ]]; then
    SUFFIX="$(detect_suffix)"
fi

INNER_DIR="apache-doris-${VERSION}-bin-${SUFFIX}"
TARBALL="${INNER_DIR}.tar.gz"
URL="${BASE_URL}/${TARBALL}"

if [[ -z "${OUT_DIR}" ]]; then
    OUT_DIR="${SCRIPT_DIR}/.prebuilt/${INNER_DIR}"
fi

TMP="$(mktemp -d)"
trap 'rm -rf "${TMP}"' EXIT

echo "Downloading ${URL}"
if command -v curl &>/dev/null; then
    curl -fSL "${URL}" -o "${TMP}/${TARBALL}"
else
    wget -O "${TMP}/${TARBALL}" "${URL}"
fi

tar -xzf "${TMP}/${TARBALL}" -C "${TMP}"
rm -rf "${OUT_DIR}"
mkdir -p "$(dirname "${OUT_DIR}")"
mv "${TMP}/${INNER_DIR}" "${OUT_DIR}"

rm -rf "${TMP}"
TMP=""
trap - EXIT

OUTPUT_REL=""
if [[ "${OUT_DIR}" == "${REPO_ROOT}"/* ]]; then
    OUTPUT_REL="${OUT_DIR#"${REPO_ROOT}/"}"
fi

echo "Extracted fe/ and be/ to: ${OUT_DIR}"
if [[ -n "${OUTPUT_REL}" ]]; then
    echo "From repo root (${REPO_ROOT}), build image with:"
    echo "  docker build -f docker/runtime/local-dev/Dockerfile --target doris-local-dev-local \\"
    echo "    --build-arg OUTPUT_PATH=./${OUTPUT_REL} -t doris-local:${VERSION} ."
else
    echo "OUT_DIR is outside the Doris repo; use either:"
    echo "  docker build -f docker/runtime/local-dev/Dockerfile --target doris-local-dev-prebuilt \\"
    echo "    --build-arg DORIS_VERSION=${VERSION} -t doris-local:${VERSION} ."
    echo "or copy fe/ and be/ under the repo and pass OUTPUT_PATH relative to repo root."
fi
