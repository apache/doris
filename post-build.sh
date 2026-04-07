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
# Post-build script for packaging third-party filesystem JARs
# (JuiceFS, JindoFS) into Doris FE/BE output directories.
#
# This script is independent of the main build and can be
# executed standalone or called from build.sh.
#
# Usage:
#   post-build.sh [--fe] [--be] [--output <dir>]
#                 [--juicefs] [--jindofs]
#
# Environment variables:
#   DISABLE_BUILD_JUICEFS=OFF  Enable JuiceFS packaging (default: ON, i.e. skip)
#   DISABLE_BUILD_JINDOFS=OFF  Enable JindoFS packaging (default: ON, i.e. skip)
##############################################################

set -eo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

DORIS_HOME="${DORIS_HOME:-${ROOT}}"
if [[ -z "${DORIS_THIRDPARTY}" ]]; then
    DORIS_THIRDPARTY="${DORIS_HOME}/thirdparty"
fi

TARGET_SYSTEM="${TARGET_SYSTEM:-$(uname -s)}"
TARGET_ARCH="${TARGET_ARCH:-$(uname -m)}"

# --- Source helper scripts ---
. "${DORIS_HOME}/docker/thirdparties/juicefs-helpers.sh"
. "${DORIS_HOME}/docker/thirdparties/jindofs-helpers.sh"

# --- JuiceFS wrapper functions (migrated from build.sh) ---

find_juicefs_hadoop_jar() {
    juicefs_find_hadoop_jar_by_globs \
        "${DORIS_THIRDPARTY}/installed/juicefs_libs/juicefs-hadoop-[0-9]*.jar" \
        "${DORIS_THIRDPARTY}/src/juicefs-hadoop-[0-9]*.jar" \
        "${DORIS_HOME}/thirdparty/installed/juicefs_libs/juicefs-hadoop-[0-9]*.jar" \
        "${DORIS_HOME}/thirdparty/src/juicefs-hadoop-[0-9]*.jar"
}

detect_juicefs_version() {
    local juicefs_jar
    juicefs_jar=$(find_juicefs_hadoop_jar || true)
    juicefs_detect_hadoop_version "${juicefs_jar}" "${JUICEFS_DEFAULT_VERSION}"
}

download_juicefs_hadoop_jar() {
    local juicefs_version="$1"
    local cache_dir="${DORIS_HOME}/thirdparty/installed/juicefs_libs"
    juicefs_download_hadoop_jar_to_cache "${juicefs_version}" "${cache_dir}"
}

copy_juicefs_hadoop_jar() {
    local target_dir="$1"
    local source_jar=""
    source_jar=$(find_juicefs_hadoop_jar || true)
    if [[ -z "${source_jar}" ]]; then
        local juicefs_version
        juicefs_version=$(detect_juicefs_version)
        source_jar=$(download_juicefs_hadoop_jar "${juicefs_version}" || true)
    fi
    if [[ -z "${source_jar}" ]]; then
        echo "WARN: skip copying juicefs-hadoop jar, not found in thirdparty and download failed"
        return 0
    fi
    cp -r -p "${source_jar}" "${target_dir}/"
    echo "Copy JuiceFS Hadoop jar to ${target_dir}: $(basename "${source_jar}")"
}

# --- Install functions ---

install_juicefs() {
    local target_type="$1"
    local output_dir="$2"

    local target_dir
    if [[ "${target_type}" == "fe" ]]; then
        target_dir="${output_dir}/fe/lib/juicefs"
    elif [[ "${target_type}" == "be" ]]; then
        target_dir="${output_dir}/be/lib/java_extensions/juicefs"
    else
        echo "ERROR: unknown target type '${target_type}' for install_juicefs"
        return 1
    fi

    install -d "${target_dir}"
    copy_juicefs_hadoop_jar "${target_dir}"
}

install_jindofs() {
    local target_type="$1"
    local output_dir="$2"

    local target_dir
    if [[ "${target_type}" == "fe" ]]; then
        target_dir="${output_dir}/fe/lib/jindofs"
    elif [[ "${target_type}" == "be" ]]; then
        target_dir="${output_dir}/be/lib/java_extensions/jindofs"
    else
        echo "ERROR: unknown target type '${target_type}' for install_jindofs"
        return 1
    fi

    install -d "${target_dir}"
    jindofs_copy_jars "${DORIS_THIRDPARTY}/installed/jindofs_libs" "${target_dir}" \
        "${TARGET_SYSTEM}" "${TARGET_ARCH}"
}

# --- CLI ---

usage() {
    echo "
Usage: $0 [--fe] [--be] [--output <dir>] [--juicefs] [--jindofs]

Package third-party filesystem JARs (JuiceFS, JindoFS) into Doris output directories.
By default, no third-party JARs are packaged. Use --juicefs/--jindofs or environment variables to opt in.

Options:
  --fe               Process FE output directory
  --be               Process BE output directory
  --output <dir>     Output directory (default: \${DORIS_HOME}/output)
  --juicefs          Enable JuiceFS packaging
  --jindofs          Enable JindoFS packaging

Environment variables:
  DISABLE_BUILD_JUICEFS=OFF  Enable JuiceFS packaging (--juicefs takes precedence)
  DISABLE_BUILD_JINDOFS=OFF  Enable JindoFS packaging (--jindofs takes precedence)
"
}

# Defaults: not installed
PROCESS_FE=0
PROCESS_BE=0
OUTPUT_DIR=""
BUILD_JUICEFS=""
BUILD_JINDOFS=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --fe)
            PROCESS_FE=1
            shift
            ;;
        --be)
            PROCESS_BE=1
            shift
            ;;
        --output)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        --juicefs)
            BUILD_JUICEFS="ON"
            shift
            ;;
        --jindofs)
            BUILD_JINDOFS="ON"
            shift
            ;;
        --help | -h)
            usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            usage
            exit 1
            ;;
    esac
done

OUTPUT_DIR="${OUTPUT_DIR:-${DORIS_HOME}/output}"

# Resolve build flags: CLI > env DISABLE_BUILD_XXX > default (OFF)
if [[ -z "${BUILD_JUICEFS}" ]]; then
    if [[ "$(echo "${DISABLE_BUILD_JUICEFS}" | tr '[:lower:]' '[:upper:]')" == "OFF" ]]; then
        BUILD_JUICEFS="ON"
    else
        BUILD_JUICEFS="OFF"
    fi
fi
if [[ -z "${BUILD_JINDOFS}" ]]; then
    if [[ "$(echo "${DISABLE_BUILD_JINDOFS}" | tr '[:lower:]' '[:upper:]')" == "OFF" ]]; then
        BUILD_JINDOFS="ON"
    else
        BUILD_JINDOFS="OFF"
    fi
fi

if [[ "${PROCESS_FE}" -eq 0 && "${PROCESS_BE}" -eq 0 ]]; then
    echo "Nothing to do. Specify --fe and/or --be."
    exit 0
fi

# --- Main ---

for target_type in fe be; do
    if [[ "${target_type}" == "fe" && "${PROCESS_FE}" -eq 0 ]]; then
        continue
    fi
    if [[ "${target_type}" == "be" && "${PROCESS_BE}" -eq 0 ]]; then
        continue
    fi

    if [[ "${BUILD_JINDOFS}" == "ON" ]]; then
        install_jindofs "${target_type}" "${OUTPUT_DIR}"
    fi

    if [[ "${BUILD_JUICEFS}" == "ON" ]]; then
        install_juicefs "${target_type}" "${OUTPUT_DIR}"
    fi
done
