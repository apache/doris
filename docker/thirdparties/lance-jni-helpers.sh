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

# Keep these pins in sync with lance.version in fe/pom.xml.
LANCE_JNI_VERSION="11.0.0"
LANCE_JNI_ASSET="liblance_jni-11.0.0-linux-x86_64-glibc2.17-r1.so.gz"
LANCE_JNI_URL="https://github.com/apache/doris-thirdparty/releases/download/lance-jni-11.0.0-glibc2.17-r1/${LANCE_JNI_ASSET}"
LANCE_JNI_ARCHIVE_SHA256="f9dc713269632e26c06ea2ea20637ca16ec9d76772dc20db0bead11e5cd34a6b"
LANCE_JNI_LIBRARY_SHA256="b6540edd3bdcd76b04f96a9e78fe804a2f1bffbd75eb35abe84de9091ba432ac"

lance_jni_download() (
    set -eo pipefail
    local cache_dir="$1"
    local archive="${cache_dir}/${LANCE_JNI_ASSET}"
    local work_dir url digest
    local -a urls=()

    mkdir -p "${cache_dir}"
    if [[ -f "${archive}" ]]; then
        digest=$(sha256sum "${archive}" | awk '{print $1}')
        if [[ "${digest}" == "${LANCE_JNI_ARCHIVE_SHA256}" ]]; then
            printf '%s\n' "${archive}"
            exit 0
        fi
        echo "Lance JNI cache checksum mismatch; downloading again: ${archive}" >&2
    fi

    work_dir=$(mktemp -d "${cache_dir}/.lance-jni-download.XXXXXX")
    trap 'rm -rf -- "${work_dir}"' EXIT
    if [[ -n "${REPOSITORY_URL:-}" ]]; then
        urls+=("${REPOSITORY_URL%/}/${LANCE_JNI_ASSET}")
    fi
    urls+=("${LANCE_JNI_URL}")
    for url in "${urls[@]}"; do
        echo "Downloading Lance JNI from ${url}" >&2
        if ! curl -fL --retry 3 --retry-delay 2 --connect-timeout 10 \
            -o "${work_dir}/library.gz" "${url}"; then
            continue
        fi
        digest=$(sha256sum "${work_dir}/library.gz" | awk '{print $1}')
        if [[ "${digest}" != "${LANCE_JNI_ARCHIVE_SHA256}" ]]; then
            echo "ERROR: Lance JNI archive SHA256 mismatch from ${url}: expected ${LANCE_JNI_ARCHIVE_SHA256}, got ${digest}" >&2
            continue
        fi
        # Concurrent builds only publish complete, verified cache files.
        mv -f "${work_dir}/library.gz" "${archive}"
        printf '%s\n' "${archive}"
        exit 0
    done
    echo "ERROR: failed to download and verify ${LANCE_JNI_ASSET}" >&2
    exit 1
)

lance_jni_replace() (
    set -eo pipefail
    local output_dir="$1"
    local thirdparty_dir="$2"
    local target_system="$3"
    local target_arch="$4"
    if [[ "${target_system}" != "Linux" || "${target_arch}" != "x86_64" ]]; then
        exit 0
    fi

    local entry="nativelib/linux-x86-64/liblance_jni.so"
    local target_jar="${output_dir}/fe/lib/lance-core-${LANCE_JNI_VERSION}.jar"
    local archive work_dir source_hash packaged_hash
    local -a lance_jars
    shopt -s nullglob
    lance_jars=("${output_dir}/fe/lib/"lance-core-*.jar)
    if [[ ${#lance_jars[@]} -ne 1 || "${lance_jars[0]}" != "${target_jar}" ]]; then
        echo "ERROR: expected exactly one lance-core-${LANCE_JNI_VERSION}.jar in ${output_dir}/fe/lib" >&2
        exit 1
    fi
    if ! unzip -Z1 "${target_jar}" | grep -Fx "${entry}" >/dev/null; then
        echo "ERROR: missing JNI entry ${entry} in ${target_jar}" >&2
        exit 1
    fi

    archive=$(lance_jni_download "${thirdparty_dir}/installed/lance-jni")
    work_dir=$(mktemp -d "${output_dir}/fe/lib/.lance-jni.XXXXXX")
    trap 'rm -rf -- "${work_dir}"' EXIT
    mkdir -p "${work_dir}/$(dirname "${entry}")"
    gzip -dc "${archive}" > "${work_dir}/${entry}"
    source_hash=$(sha256sum "${work_dir}/${entry}" | awk '{print $1}')
    if [[ "${source_hash}" != "${LANCE_JNI_LIBRARY_SHA256}" ]]; then
        echo "ERROR: Lance JNI library SHA256 mismatch: expected ${LANCE_JNI_LIBRARY_SHA256}, got ${source_hash}" >&2
        exit 1
    fi
    # Preserve the Maven cache and only replace the output JAR after verification.
    cp -p "${target_jar}" "${work_dir}/lance-core.jar"
    (
        cd "${work_dir}"
        zip -q lance-core.jar "${entry}"
    )
    packaged_hash=$(unzip -p "${work_dir}/lance-core.jar" "${entry}" | sha256sum | awk '{print $1}')
    if [[ "${source_hash}" != "${packaged_hash}" ]]; then
        echo "ERROR: JNI checksum mismatch in ${target_jar}" >&2
        exit 1
    fi
    mv -f "${work_dir}/lance-core.jar" "${target_jar}"
    echo "Replaced ${target_jar}!/${entry} (SHA256: ${packaged_hash})"
)
