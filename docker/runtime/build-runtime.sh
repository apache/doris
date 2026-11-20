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

# Sourced by the Ubuntu and UBI 9 entry points.

usage() {
    echo "Usage: $0 -v <version>"
    echo "Build FE, BE and (when present) MS with a verified release archive."
    echo "See docker/runtime/README.md for architecture and artifact overrides."
}

version=
while [[ $# -gt 0 ]]; do
    case "$1" in
        -v|--version)
            version=${2:?Missing version}
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            usage >&2
            exit 1
            ;;
    esac
done
if [[ ! "${version}" =~ ^[0-9]+\.[0-9]+\.[0-9]+([-._][a-zA-Z0-9._-]+)?$ ]]; then
    usage >&2
    exit 1
fi

runtime_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
work_dir=${DORIS_IMAGE_WORK_DIR:-"${runtime_dir}/../../output/runtime-images/${runtime_variant}"}
mkdir -p "${work_dir}"
work_dir=$(cd "${work_dir}" && pwd)
staging_dir=$(mktemp -d "${work_dir}/staging.XXXXXX")
trap 'rm -rf "$staging_dir"' EXIT

case "${DORIS_ARCH:-$(uname -m)}" in
    amd64|x86_64) arch=amd64; url_arch=x64 ;;
    arm64|aarch64) arch=arm64; url_arch=arm64 ;;
    *) echo "Unsupported architecture" >&2; exit 1 ;;
esac
if [[ "${arch}" == amd64 ]]; then
    if [[ "${DORIS_NO_AVX2:-0}" == 1 ]] || \
        { [[ "$(uname -m)" == x86_64 && -f /proc/cpuinfo ]] && ! grep -qw avx2 /proc/cpuinfo; }; then
        url_arch=x64-noavx2
    fi
fi

major_version=${version%%.*}
java_version=8
if (( 10#${major_version} >= 3 )); then
    java_version=17
fi
base_image=${DORIS_BASE_IMAGE:-"apache/doris:base-8.0-${runtime_variant}-jdk${java_version}"}
package_name="apache-doris-${version}-bin-${url_arch}"
archive_name="${package_name}.tar.gz"
download_base=${DORIS_DOWNLOAD_BASE_URL:-https://apache-doris-releases.oss-accelerate.aliyuncs.com}
expected_sha512=${DORIS_ARCHIVE_SHA512:-}

# Read only the checksum, not paths or commands from the checksum document.
if [[ -z "${expected_sha512}" ]]; then
    curl --fail --show-error --location --retry 3 --proto '=https' --proto-redir '=https' \
        --tlsv1.2 "${download_base}/${archive_name}.sha512" -o "${staging_dir}/archive.sha512"
    expected_sha512=$(awk '{
        for (i = 1; i <= NF; i++) {
            if (length($i) == 128 && $i !~ /[^0-9a-fA-F]/) {
                print tolower($i)
                count++
            }
        }
    } END { if (count != 1) exit 1 }' "${staging_dir}/archive.sha512")
fi
if [[ ! "${expected_sha512}" =~ ^[a-fA-F0-9]{128}$ ]]; then
    echo "Expected exactly one SHA512 digest for ${archive_name}" >&2
    exit 1
fi
archive=${DORIS_ARCHIVE:-"${work_dir}/${archive_name}"}
if [[ -z "${DORIS_ARCHIVE:-}" ]]; then
    curl --fail --show-error --location --retry 3 --proto '=https' --proto-redir '=https' \
        --tlsv1.2 "${download_base}/${archive_name}" -o "${archive}.part"
    mv "${archive}.part" "${archive}"
fi
if command -v sha512sum >/dev/null 2>&1; then
    actual_sha512=$(sha512sum "${archive}")
else
    actual_sha512=$(shasum -a 512 "${archive}")
fi
actual_sha512=${actual_sha512%% *}
expected_sha512=$(printf '%s' "${expected_sha512}" | tr '[:upper:]' '[:lower:]')
if [[ "${actual_sha512}" != "${expected_sha512}" ]]; then
    echo "SHA512 verification failed for ${archive}" >&2
    exit 1
fi
echo "Verified SHA512 for ${archive_name}"
tar -xzf "${archive}" -C "${staging_dir}"

base_args=(--build-arg "JAVA_VERSION=${java_version}" --build-arg "RUNTIME_DISTRO=${runtime_distro}")
for arg in UBUNTU_IMAGE UBI9_IMAGE JDK17_IMAGE JDK8_IMAGE JDK17_UBI9_IMAGE JDK8_UBI9_IMAGE GO_IMAGE; do
    if [[ -n "${!arg:-}" ]]; then
        base_args+=(--build-arg "${arg}=${!arg}")
    fi
done
docker build --pull --no-cache --platform "linux/${arch}" \
    "${base_args[@]}" \
    -t "${base_image}" -f "${runtime_dir}/base-image/Dockerfile_base" "${runtime_dir}/base-image"

# Consume the base just built. --pull here would fetch a registry tag instead.
for role in fe be ms; do
    if [[ "${role}" == ms && ! -d "${staging_dir}/${package_name}/ms" ]]; then
        continue
    fi
    context="${staging_dir}/${role}"
    mkdir -p "${context}/resource/${arch}/${package_name}"
    cp "${runtime_dir}/${role}/Dockerfile" "${context}/Dockerfile"
    cp "${runtime_dir}/${role}/resource/"*.sh "${context}/resource/"
    mv "${staging_dir}/${package_name}/${role}" "${context}/resource/${arch}/${package_name}/"
    image="${DORIS_IMAGE_PREFIX:-doris}.${role}:${version}${DORIS_IMAGE_TAG_SUFFIX-${runtime_tag_suffix}}"
    docker build --no-cache --platform "linux/${arch}" \
        --build-arg "BASE_IMAGE=${base_image}" --build-arg "JAVA_VERSION=${java_version}" \
        --build-arg "DORIS_VERSION=${version}" -t "${image}" "${context}"
    docker image inspect "${image}" > "${work_dir}/${role}-${version}-${arch}.image.json"
done
