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

if [[ $# -lt 2 || $# -gt 3 ]]; then
    echo "Usage: $0 <benchmark-binary> <result-dir> [benchmark-filter]" >&2
    exit 2
fi

benchmark_binary=$1
result_dir=$2
benchmark_filter=${3:-'^BM_VariantCumulativeCompaction/'}
script_path=$(realpath "${BASH_SOURCE[0]}")
repo_root=$(git -C "$(dirname "${script_path}")" rev-parse --show-toplevel)
be_config=${repo_root}/conf/be.conf
benchmark_root=${DORIS_VARIANT_BENCHMARK_ROOT:-/tmp}
thirdparty_installed_path=$(realpath "${repo_root}/thirdparty/installed")

if [[ ! -x "${benchmark_binary}" ]]; then
    echo "Benchmark binary is not executable: ${benchmark_binary}" >&2
    exit 2
fi
if [[ ! -f "${be_config}" ]]; then
    echo "Benchmark config does not exist: ${be_config}" >&2
    exit 2
fi
if [[ ! -d "${benchmark_root}" || ! -w "${benchmark_root}" ]]; then
    echo "Benchmark root must be a writable directory: ${benchmark_root}" >&2
    exit 2
fi
benchmark_binary=$(realpath "${benchmark_binary}")
benchmark_root=$(realpath "${benchmark_root}")
java_library_path=${JAVA_HOME:?JAVA_HOME must be set}/lib/server
runtime_library_path="${java_library_path}${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}"

mkdir -p "${result_dir}"

{
    date --iso-8601=seconds
    echo "repo_root=${repo_root}"
    echo "invocation_pwd=${PWD}"
    echo "git_head=$(git -C "${repo_root}" rev-parse HEAD)"
    echo "git_parent=$(git -C "${repo_root}" rev-parse HEAD^)"
    echo "git_describe=$(git -C "${repo_root}" describe --always --dirty)"
    git -C "${repo_root}" status --short --branch
    echo "benchmark_binary=${benchmark_binary}"
    sha256sum "${benchmark_binary}"
    echo "benchmark_binary_verified_against_source=no (this hash only identifies the binary; it is not independently checked against git_head/source.diff/thirdparty fingerprints below -- retain this exact binary alongside this environment.txt for a trustworthy comparison)"
    echo "JAVA_HOME=${JAVA_HOME}"
    echo "runtime_LD_LIBRARY_PATH=${runtime_library_path}"
    LD_LIBRARY_PATH="${runtime_library_path}" ldd "${benchmark_binary}"
    echo "be_config=${be_config}"
    sha256sum "${be_config}"
    echo "benchmark_source=be/benchmark/variant/benchmark_variant_segment.hpp"
    sha256sum "${repo_root}/be/benchmark/variant/benchmark_variant_segment.hpp"
    echo "thirdparty_installed=${thirdparty_installed_path}"
    while IFS= read -r fingerprint; do
        echo "$(basename "${fingerprint}")=$(< "${fingerprint}")"
    done < <(find "${thirdparty_installed_path}" -maxdepth 1 -type f \
        -name '*-build-fingerprint.txt' | sort)
    # ./build.sh --benchmark without an explicit BUILD_TYPE can name the build directory either
    # build_RELEASE (BUILD_TYPE=RELEASE, as this script's own instructions set) or build_Release
    # (build.sh's internal default casing); probe both so the documented bare invocation is still
    # covered.
    cmake_cache_found=0
    for cmake_build_dir in "${repo_root}/be/build_RELEASE" "${repo_root}/be/build_Release"; do
        if [[ -f "${cmake_build_dir}/CMakeCache.txt" ]]; then
            sha256sum "${cmake_build_dir}/CMakeCache.txt"
            grep -n '^CMAKE_BUILD_TYPE:' "${cmake_build_dir}/CMakeCache.txt"
            cmake_cache_found=1
            break
        fi
    done
    if [[ "${cmake_cache_found}" -eq 0 ]]; then
        echo "cmake_cache=not found under be/build_RELEASE or be/build_Release"
    fi
    echo "benchmark_root=${benchmark_root}"
    df -h "${benchmark_root}"
    findmnt -T "${benchmark_root}" -o TARGET,SOURCE,FSTYPE,OPTIONS
    lsblk -o NAME,TYPE,SIZE,ROTA,MODEL,MOUNTPOINTS
    free -h
    swapon --show
    uname -a
    lscpu
    echo "load_before=$(< /proc/loadavg)"
    echo "rows=${DORIS_VARIANT_BENCHMARK_ROWS:-1000000}"
    echo "filter=${benchmark_filter}"
    echo "cpu=${DORIS_BENCHMARK_CPU:-unbound}"
} >"${result_dir}/environment.txt"

git -C "${repo_root}" diff --binary HEAD -- >"${result_dir}/source.diff"
sha256sum "${result_dir}/source.diff" >>"${result_dir}/environment.txt"

command=("${benchmark_binary}"
         "--benchmark_filter=${benchmark_filter}"
         "--benchmark_counters_tabular=true"
         "--benchmark_out=${result_dir}/raw.json"
         "--benchmark_out_format=json")

if [[ -n "${DORIS_BENCHMARK_CPU:-}" ]]; then
    command=(taskset -c "${DORIS_BENCHMARK_CPU}" "${command[@]}")
fi

DORIS_HOME="${repo_root}" \
    DORIS_VARIANT_BENCHMARK_ROOT="${benchmark_root}" \
    LD_LIBRARY_PATH="${runtime_library_path}" \
    /usr/bin/time -v -o "${result_dir}/resource.txt" "${command[@]}" \
    >"${result_dir}/stdout.txt" 2>"${result_dir}/stderr.txt"

echo "load_after=$(< /proc/loadavg)" >>"${result_dir}/environment.txt"

if ! grep -q '"name": "BM_Variant' "${result_dir}/raw.json"; then
    echo "No Variant benchmark cases matched; see ${result_dir}/raw.json" >&2
    exit 1
fi

if grep -q '"error_occurred": true' "${result_dir}/raw.json"; then
    echo "One or more benchmark cases failed; see ${result_dir}/raw.json" >&2
    exit 1
fi
