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

# lance-core carries its JNI library (liblance_jni) inside its jar and extracts it from there at runtime,
# with no system property to load another one. Its linux-x86-64 build needs GLIBC 2.28, which hosts Doris
# still supports do not have, so the build replaces that jar entry with a glibc 2.17 build of the same
# Lance release, published on apache/doris-thirdparty and pinned here by SHA256.
#
# Keep these pins in sync with lance.version in fe/pom.xml.
LANCE_JNI_VERSION="12.0.0"
LANCE_JNI_ASSET="liblance_jni-12.0.0-linux-x86_64-glibc2.17-r1.so.gz"
LANCE_JNI_URL="https://github.com/apache/doris-thirdparty/releases/download/lance-jni-12.0.0-glibc2.17-r1/${LANCE_JNI_ASSET}"
LANCE_JNI_ARCHIVE_SHA256="678d9454ad1a24ff07a504d8c318479f6b01d095f04945248dc170eecb303801"
LANCE_JNI_LIBRARY_SHA256="72f26651b5500bf1483da1cd35130887274a42c2e8505fcfdcd846a521fc1549"

# Arrow C Data JNI is needed by Dataset.getSchema(). Build its small standalone JNI wrapper with
# the Doris toolchain and static C++ runtimes, rather than ship its newer upstream libstdc++ dependency.
# Keep this version in sync with arrow.version in fe/pom.xml. Release builds targeting glibc 2.17
# must use the same glibc 2.17 build environment as the rest of Doris.
LANCE_ARROW_VERSION="19.0.0"
LANCE_ARROW_SOURCE_ASSET="arrow-java-${LANCE_ARROW_VERSION}.tar.gz"
LANCE_ARROW_SOURCE_URL="https://codeload.github.com/apache/arrow-java/legacy.tar.gz/refs/tags/v${LANCE_ARROW_VERSION}"
LANCE_ARROW_SOURCE_SHA256="97911185dca02df15010b67737125a925417439dbc2dbf84a83c605d21d733ea"

lance_native_download() (
    set -eo pipefail
    local cache_dir="$1"
    local asset="$2"
    local download_url="$3"
    local checksum="$4"
    local archive="${cache_dir}/${asset}"
    local work_dir url digest
    local -a urls=()

    mkdir -p "${cache_dir}"
    if [[ -f "${archive}" ]]; then
        digest=$(sha256sum "${archive}" | awk '{print $1}')
        if [[ "${digest}" == "${checksum}" ]]; then
            printf '%s\n' "${archive}"
            exit 0
        fi
        echo "Lance native dependency cache checksum mismatch; downloading again: ${archive}" >&2
    fi

    work_dir=$(mktemp -d "${cache_dir}/.lance-jni-download.XXXXXX")
    trap 'rm -rf -- "${work_dir}"' EXIT
    if [[ -n "${REPOSITORY_URL:-}" ]]; then
        urls+=("${REPOSITORY_URL%/}/${asset}")
    fi
    urls+=("${download_url}")
    for url in "${urls[@]}"; do
        echo "Downloading ${asset} from ${url}" >&2
        if ! curl -fL --retry 3 --retry-delay 2 --connect-timeout 10 \
            -o "${work_dir}/library.gz" "${url}"; then
            continue
        fi
        digest=$(sha256sum "${work_dir}/library.gz" | awk '{print $1}')
        if [[ "${digest}" != "${checksum}" ]]; then
            echo "ERROR: ${asset} SHA256 mismatch from ${url}: expected ${checksum}, got ${digest}" >&2
            continue
        fi
        # Concurrent builds only publish complete, verified cache files.
        mv -f "${work_dir}/library.gz" "${archive}"
        printf '%s\n' "${archive}"
        exit 0
    done
    echo "ERROR: failed to download and verify ${asset}" >&2
    exit 1
)

lance_jni_extract_verified() (
    set -eo pipefail
    local archive="$1"
    local destination="$2"
    local digest
    gzip -dc "${archive}" > "${destination}"
    digest=$(sha256sum "${destination}" | awk '{print $1}')
    if [[ "${digest}" != "${LANCE_JNI_LIBRARY_SHA256}" ]]; then
        echo "ERROR: Lance JNI library SHA256 mismatch: expected ${LANCE_JNI_LIBRARY_SHA256}, got ${digest}" >&2
        exit 1
    fi
)

# Replaces the linux-x86-64 JNI library inside the lance-core jar of a deployed Lance connector plugin.
# Runs after build.sh unzips the plugin, against the copy in plugins/connector/lance/lib, so the Maven
# cache is never touched. Linux x86_64 only: the other platforms keep the libraries lance-core ships.
lance_jni_replace() (
    set -eo pipefail
    local plugin_lib_dir="$1"
    local thirdparty_dir="$2"
    local target_system="$3"
    local target_arch="$4"
    if [[ "${target_system}" != "Linux" || "${target_arch}" != "x86_64" ]]; then
        exit 0
    fi

    local entry="nativelib/linux-x86-64/liblance_jni.so"
    local target_jar="${plugin_lib_dir}/lance-core-${LANCE_JNI_VERSION}.jar"
    local archive work_dir source_hash packaged_hash
    local -a lance_jars
    shopt -s nullglob
    lance_jars=("${plugin_lib_dir}/"lance-core-*.jar)
    if [[ ${#lance_jars[@]} -ne 1 || "${lance_jars[0]}" != "${target_jar}" ]]; then
        echo "ERROR: expected exactly one lance-core-${LANCE_JNI_VERSION}.jar in ${plugin_lib_dir}" >&2
        exit 1
    fi
    if ! unzip -Z1 "${target_jar}" | grep -Fx "${entry}" >/dev/null; then
        echo "ERROR: missing JNI entry ${entry} in ${target_jar}" >&2
        exit 1
    fi

    archive=$(lance_native_download "${thirdparty_dir}/installed/lance-jni" "${LANCE_JNI_ASSET}" \
        "${LANCE_JNI_URL}" "${LANCE_JNI_ARCHIVE_SHA256}")
    work_dir=$(mktemp -d "${plugin_lib_dir}/.lance-jni.XXXXXX")
    trap 'rm -rf -- "${work_dir}"' EXIT
    mkdir -p "${work_dir}/$(dirname "${entry}")"
    lance_jni_extract_verified "${archive}" "${work_dir}/${entry}"
    source_hash="${LANCE_JNI_LIBRARY_SHA256}"
    # Only replace the deployed jar after the repacked entry is verified.
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

    lance_arrow_cdata_jni_replace "${plugin_lib_dir}" "${thirdparty_dir}" "${target_system}" "${target_arch}"
)

# Kept separate so packaging checks can compile and exercise the same JNI sources on the build host.
lance_arrow_cdata_jni_build() (
    set -eo pipefail
    local source_dir="$1" target_jar="$2" build_dir="$3" target_system="$4" destination="$5"
    local jdk_home="${JAVA_HOME:-}"
    if [[ -z "${jdk_home}" ]]; then
        # env.sh also accepts a JDK on PATH without setting JAVA_HOME. Ask the selected JVM,
        # rather than resolving its launcher path (which may be a symlink or a platform shim).
        jdk_home=$("${JAVA:-java}" -XshowSettings:properties -version 2>&1 | \
            sed -n 's/^[[:space:]]*java.home = //p')
    fi
    local jni_platform="linux"
    local -a runtime_flags=(-static-libstdc++ -static-libgcc '-Wl,--exclude-libs,ALL')
    if [[ "${target_system}" == "Darwin" ]]; then
        jni_platform="darwin"
        runtime_flags=()
    fi
    mkdir -p "${build_dir}/headers" "${build_dir}/classes"
    "${jdk_home}/bin/javac" -cp "${target_jar}" -h "${build_dir}/headers" -d "${build_dir}/classes" \
        "${source_dir}/c/src/main/java/org/apache/arrow/c/jni/JniWrapper.java" || exit 1
    "${CXX:-g++}" -std=c++17 -O2 -shared -fPIC "${runtime_flags[@]}" \
        -I"${jdk_home}/include" -I"${jdk_home}/include/${jni_platform}" -I"${build_dir}/headers" \
        "${source_dir}/c/src/main/cpp/jni_wrapper.cc" -o "${destination}"
)

lance_arrow_cdata_jni_replace() (
    set -eo pipefail
    local plugin_lib_dir="$1" thirdparty_dir="$2" target_system="$3" target_arch="$4"
    if [[ "${target_system}" != "Linux" || "${target_arch}" != "x86_64" ]]; then
        exit 0
    fi
    local target_jar="${plugin_lib_dir}/arrow-c-data-${LANCE_ARROW_VERSION}.jar"
    local entry="arrow_cdata_jni/x86_64/libarrow_cdata_jni.so"
    local archive work_dir digest packaged_hash
    local -a arrow_jars
    shopt -s nullglob
    arrow_jars=("${plugin_lib_dir}/"arrow-c-data-*.jar)
    if [[ ${#arrow_jars[@]} -ne 1 || "${arrow_jars[0]}" != "${target_jar}" ]]; then
        echo "ERROR: expected exactly one arrow-c-data-${LANCE_ARROW_VERSION}.jar in ${plugin_lib_dir}" >&2
        exit 1
    fi
    if ! unzip -Z1 "${target_jar}" | grep -Fx "${entry}" >/dev/null; then
        echo "ERROR: missing JNI entry ${entry} in ${target_jar}" >&2
        exit 1
    fi
    archive=$(lance_native_download "${thirdparty_dir}/installed/lance-jni" "${LANCE_ARROW_SOURCE_ASSET}" \
        "${LANCE_ARROW_SOURCE_URL}" "${LANCE_ARROW_SOURCE_SHA256}") || exit 1
    work_dir=$(mktemp -d "${plugin_lib_dir}/.arrow-cdata-jni.XXXXXX")
    trap 'rm -rf -- "${work_dir}"' EXIT
    mkdir -p "${work_dir}/source" "${work_dir}/$(dirname "${entry}")"
    tar -xzf "${archive}" -C "${work_dir}/source" --strip-components=1 || exit 1
    lance_arrow_cdata_jni_build "${work_dir}/source" "${target_jar}" "${work_dir}/build" \
        "${target_system}" "${work_dir}/${entry}" || exit 1
    # Static C++ runtimes keep the supported host's system libstdc++ out of this library's dependencies.
    readelf -d "${work_dir}/${entry}" > "${work_dir}/dynamic.txt" || exit 1
    if grep -Eq 'NEEDED.*lib(stdc\+\+|gcc_s)' "${work_dir}/dynamic.txt"; then
        echo "ERROR: rebuilt Arrow C Data JNI still depends on a shared C++ runtime" >&2
        exit 1
    fi
    digest=$(sha256sum "${work_dir}/${entry}" | awk '{print $1}')
    cp -p "${target_jar}" "${work_dir}/arrow-c-data.jar"
    (cd "${work_dir}" && zip -q arrow-c-data.jar "${entry}")
    packaged_hash=$(unzip -p "${work_dir}/arrow-c-data.jar" "${entry}" | sha256sum | awk '{print $1}')
    if [[ "${digest}" != "${packaged_hash}" ]]; then
        echo "ERROR: Arrow C Data JNI checksum mismatch in ${target_jar}" >&2
        exit 1
    fi
    mv -f "${work_dir}/arrow-c-data.jar" "${target_jar}"
    echo "Replaced ${target_jar}!/${entry} (SHA256: ${packaged_hash})"
)
