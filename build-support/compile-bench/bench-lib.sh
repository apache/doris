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
# Helper library for `build.sh --compile-bench`.
# This file is sourced by build.sh, not executed.
#
# One benchmark run collects everything under
#   be/compile-bench-results/<run_id>/
#     meta.tsv           key<TAB>value facts about the run
#     phases.tsv         phase<TAB>start_ms<TAB>end_ms
#     compile_log.jsonl  one line per compiler/linker call
#                        (written by cc-timing-wrapper.py)
#     ninja_log.txt      copy of the build dir's .ninja_log
#     report.txt         human readable report (report.py)
#     summary.json       machine readable summary (report.py)
##############################################################

COMPILE_BENCH_LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

_compile_bench_now_ms() {
    python3 -c 'import time; print(int(time.time() * 1000))'
}

# compile_bench_init <doris_home>
# Prepares the run directory, disables every compile cache and installs the
# timing wrapper as the compiler launcher. Mutates:
#   CMAKE_USE_CCACHE_CXX / CMAKE_USE_CCACHE_C  (launcher slot: ccache -> wrapper)
#   COMPILE_BENCH_CMAKE_ARGS                   (extra -D args for the BE cmake call)
#   COMPILE_BENCH_BUILD_DIR                    (dedicated always-cold build dir)
#   EXTRA_CXX_FLAGS                            (+ -ftime-trace when requested)
compile_bench_init() {
    local doris_home="$1"

    if ! command -v python3 &>/dev/null; then
        echo "ERROR: --compile-bench requires python3 (timing wrapper and report generator)."
        exit 1
    fi

    COMPILE_BENCH_RUN_ID="$(date -u '+%Y%m%d_%H%M%S')"
    COMPILE_BENCH_RUN_DIR="${doris_home}/be/compile-bench-results/${COMPILE_BENCH_RUN_ID}"
    mkdir -p "${COMPILE_BENCH_RUN_DIR}"

    local cmake_build_type="${BUILD_TYPE:-Release}"
    COMPILE_BENCH_BUILD_DIR="${doris_home}/be/build_${cmake_build_type}_compile_bench"

    # Consumed by cc-timing-wrapper.py in every compiler process.
    export DORIS_COMPILE_BENCH_DIR="${COMPILE_BENCH_RUN_DIR}"

    # Kill every compile cache:
    # 1. the launcher slot below no longer contains ccache, and
    # 2. CCACHE_DISABLE turns any leftover ccache call into a pass-through.
    export CCACHE_DISABLE=1

    COMPILE_BENCH_WRAPPER="${COMPILE_BENCH_LIB_DIR}/cc-timing-wrapper.py"
    CMAKE_USE_CCACHE_CXX="-DCMAKE_CXX_COMPILER_LAUNCHER=${COMPILE_BENCH_WRAPPER}"
    CMAKE_USE_CCACHE_C="-DCMAKE_C_COMPILER_LAUNCHER=${COMPILE_BENCH_WRAPPER}"
    # Linker launcher needs CMake >= 3.21; older CMake silently ignores the
    # variables and link times are then only visible through the ninja log.
    COMPILE_BENCH_CMAKE_ARGS=(
        "-DCMAKE_CXX_LINKER_LAUNCHER=${COMPILE_BENCH_WRAPPER}"
        "-DCMAKE_C_LINKER_LAUNCHER=${COMPILE_BENCH_WRAPPER}"
    )

    COMPILE_BENCH_TIME_TRACE='OFF'
    if [[ "${COMPILE_BENCH_TRACE:-OFF}" == 'ON' ]]; then
        if [[ "${DORIS_TOOLCHAIN}" == "gcc" ]]; then
            echo "WARNING: COMPILE_BENCH_TRACE=ON needs clang (-ftime-trace); ignored with gcc."
        else
            COMPILE_BENCH_TIME_TRACE='ON'
            EXTRA_CXX_FLAGS="${EXTRA_CXX_FLAGS:+${EXTRA_CXX_FLAGS} }-ftime-trace"
        fi
    fi

    local git_commit git_branch cxx_version ncpu
    git_commit="$(git -C "${doris_home}" rev-parse --short HEAD 2>/dev/null || echo unknown)"
    git_branch="$(git -C "${doris_home}" rev-parse --abbrev-ref HEAD 2>/dev/null || echo unknown)"
    cxx_version="$("${CXX}" --version 2>/dev/null | head -n1 || echo unknown)"
    ncpu="$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo unknown)"

    {
        printf 'run_id\t%s\n' "${COMPILE_BENCH_RUN_ID}"
        printf 'doris_home\t%s\n' "${doris_home}"
        printf 'build_dir\t%s\n' "${COMPILE_BENCH_BUILD_DIR}"
        printf 'date_utc\t%s\n' "$(date -u '+%Y-%m-%d %H:%M:%S')"
        printf 'uname\t%s\n' "$(uname -sm)"
        printf 'ncpu\t%s\n' "${ncpu}"
        printf 'parallel\t%s\n' "${PARALLEL}"
        printf 'build_type\t%s\n' "${cmake_build_type}"
        printf 'generator\t%s\n' "${GENERATOR}"
        printf 'build_system\t%s\n' "${BUILD_SYSTEM}"
        printf 'toolchain\t%s\n' "${DORIS_TOOLCHAIN}"
        printf 'cxx\t%s\n' "${CXX}"
        printf 'cxx_version\t%s\n' "${cxx_version}"
        printf 'enable_pch\t%s\n' "${ENABLE_PCH}"
        printf 'use_avx2\t%s\n' "${USE_AVX2}"
        printf 'time_trace\t%s\n' "${COMPILE_BENCH_TIME_TRACE}"
        printf 'git_commit\t%s\n' "${git_commit}"
        printf 'git_branch\t%s\n' "${git_branch}"
    } >"${COMPILE_BENCH_RUN_DIR}/meta.tsv"

    COMPILE_BENCH_T0="$(_compile_bench_now_ms)"

    echo "***************************************"
    echo "* BE compile benchmark mode (--compile-bench)"
    echo "*   run dir     : ${COMPILE_BENCH_RUN_DIR}"
    echo "*   build dir   : ${COMPILE_BENCH_BUILD_DIR} (recreated from scratch)"
    echo "*   ccache      : DISABLED (timing wrapper installed as launcher)"
    echo "*   -ftime-trace: ${COMPILE_BENCH_TIME_TRACE}"
    echo "*   BE-only build: FE/cloud/java-extensions/packaging are skipped"
    echo "***************************************"
}

# compile_bench_phase_begin <phase_name>
compile_bench_phase_begin() {
    COMPILE_BENCH_PHASE_NAME="$1"
    COMPILE_BENCH_PHASE_START="$(_compile_bench_now_ms)"
    echo "Compile-bench: phase '${COMPILE_BENCH_PHASE_NAME}' started"
}

compile_bench_phase_end() {
    local end_ms
    end_ms="$(_compile_bench_now_ms)"
    printf '%s\t%s\t%s\n' \
        "${COMPILE_BENCH_PHASE_NAME}" "${COMPILE_BENCH_PHASE_START}" "${end_ms}" \
        >>"${COMPILE_BENCH_RUN_DIR}/phases.tsv"
    echo "Compile-bench: phase '${COMPILE_BENCH_PHASE_NAME}' finished in $(((end_ms - COMPILE_BENCH_PHASE_START) / 1000))s"
}

# compile_bench_finish <build_dir> <build_exit_code>
# Preserves the ninja log and generates the report. Never fails the build
# beyond the exit code that is already being propagated by the caller.
compile_bench_finish() {
    local build_dir="$1"
    local build_rc="$2"
    local end_ms
    end_ms="$(_compile_bench_now_ms)"
    printf 'total\t%s\t%s\n' "${COMPILE_BENCH_T0}" "${end_ms}" \
        >>"${COMPILE_BENCH_RUN_DIR}/phases.tsv"

    if [[ -f "${build_dir}/.ninja_log" ]]; then
        cp -f "${build_dir}/.ninja_log" "${COMPILE_BENCH_RUN_DIR}/ninja_log.txt"
    fi

    local build_status='ok'
    if [[ "${build_rc}" -ne 0 ]]; then
        build_status="failed(rc=${build_rc})"
    fi

    set +e
    python3 "${COMPILE_BENCH_LIB_DIR}/report.py" "${COMPILE_BENCH_RUN_DIR}" \
        --build-dir "${build_dir}" \
        --build-status "${build_status}"
    local report_rc=$?
    set -e
    if [[ "${report_rc}" -ne 0 ]]; then
        echo "WARNING: compile-bench report generation failed (rc=${report_rc});"
        echo "         raw timing data is still available in ${COMPILE_BENCH_RUN_DIR}"
    fi

    echo "***************************************"
    echo "* BE compile benchmark finished: ${build_status}"
    echo "*   report : ${COMPILE_BENCH_RUN_DIR}/report.txt"
    echo "*   summary: ${COMPILE_BENCH_RUN_DIR}/summary.json"
    echo "* Compare two runs with:"
    echo "*   python3 build-support/compile-bench/report.py compare <old_run_dir> <new_run_dir>"
    echo "***************************************"
}
