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

# Build Step: Command Line, placed after compile and before deploy.
: <<EOF
#!/bin/bash

export teamcity_build_checkoutDir="%teamcity.build.checkoutDir%"
if [[ -f "${teamcity_build_checkoutDir:-}"/regression-test/pipeline/performance/run-parquet-microbenchmark.sh ]]; then
    cd "${teamcity_build_checkoutDir}"/regression-test/pipeline/performance/
    bash -x run-parquet-microbenchmark.sh
else
    echo "Build Step file missing: regression-test/pipeline/performance/run-parquet-microbenchmark.sh" && exit 1
fi
EOF

#####################################################################################
## run-parquet-microbenchmark.sh content ##

set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
doris_home=$(cd "${script_dir}/../../.." && pwd)
result_dir="${PARQUET_BENCHMARK_RESULT_DIR:-${doris_home}/parquet-benchmark-results}"

if [[ "${PARQUET_MICROBENCHMARK_IN_CONTAINER:-false}" != true ]]; then
    if [[ -z "${teamcity_build_checkoutDir:-}" ]]; then
        echo "ERROR: env teamcity_build_checkoutDir not set"
        exit 1
    fi

    # shellcheck source=/dev/null
    source "$(bash "${teamcity_build_checkoutDir}"/regression-test/pipeline/common/get-or-set-tmp-env.sh 'get')"
    if ${skip_pipeline:=false}; then
        echo "INFO: skip build pipeline"
        exit 0
    fi

    parquet_microbenchmark_mode="${PARQUET_MICROBENCHMARK_MODE:-off}"
    case "${parquet_microbenchmark_mode}" in
        off)
            echo "INFO: Parquet microbenchmark is disabled"
            exit 0
            ;;
        report | gate) ;;
        *)
            echo "ERROR: PARQUET_MICROBENCHMARK_MODE must be off, report, or gate"
            exit 2
            ;;
    esac
    run_on_host() {
        if [[ ! "${parquet_benchmark_base_sha:-}" =~ ^[0-9a-f]{40}$ ]]; then
            echo "ERROR: invalid or missing Parquet benchmark base SHA"
            return 2
        fi
        if [[ -z "${performance_remote_ccache:-}" ]]; then
            echo "ERROR: performance remote ccache path is missing"
            return 2
        fi

        local docker_image="${performance_docker_image:-apache/doris:build-env-ldb-toolchain-latest}"
        local docker_name="parquet-microbenchmark-${TEAMCITY_BUILD_ID:-${commit_id_from_trigger:-manual}}"
        local git_storage_path
        git_storage_path=$(grep storage "${teamcity_build_checkoutDir}"/.git/config |
            rev | cut -d ' ' -f 1 | rev | awk -F '/lfs' '{print $1}')
        if [[ -z "${git_storage_path}" ]]; then
            echo "ERROR: TeamCity git storage path is missing"
            return 2
        fi

        local docker_environment=(
            -e PARQUET_MICROBENCHMARK_IN_CONTAINER=true
            -e "PARQUET_BENCHMARK_BASE_SHA=${parquet_benchmark_base_sha}"
            -e CCACHE_REMOTE_STORAGE=file:///root/ccache
            -e EXTRA_CXX_FLAGS=-O3
            -e USE_JEMALLOC=ON
            -e ENABLE_PCH=OFF
        )
        local variable
        for variable in \
            PARQUET_BENCHMARK_CPU \
            PARQUET_BENCHMARK_MIN_TIME \
            PARQUET_BENCHMARK_WARMUP_TIME \
            PARQUET_REGRESSION_THRESHOLD_PCT \
            PARQUET_WARNING_THRESHOLD_PCT \
            PARQUET_CONFIDENCE_MARGIN_PCT \
            PARQUET_MAX_CV_PCT; do
            if [[ -n "${!variable:-}" ]]; then
                docker_environment+=(-e "${variable}=${!variable}")
            fi
        done

        sudo docker run -i --rm \
            --name "${docker_name}" \
            "${docker_environment[@]}" \
            -v /mnt/ccache/.ccache:/root/.ccache \
            -v "${performance_remote_ccache}":/root/ccache \
            -v "${git_storage_path}":/root/git \
            -v "${teamcity_build_checkoutDir}":/root/doris \
            "${docker_image}" \
            /bin/bash -o pipefail -c "mkdir -p ${git_storage_path} \
                && cp -r /root/git/* ${git_storage_path}/ \
                && bash /root/doris/regression-test/pipeline/performance/run-parquet-microbenchmark.sh"
    }

    benchmark_status=0
    run_on_host || benchmark_status=$?

    if [[ -d "${teamcity_build_checkoutDir}/parquet-benchmark-results" ]]; then
        # The TeamCity step invokes this script with bash -x. Disable tracing before the
        # service message so the command itself is not parsed as a second artifact path.
        { set +x; } 2>/dev/null
        echo "##teamcity[publishArtifacts 'parquet-benchmark-results => parquet-microbenchmark']"
    fi
    if [[ "${benchmark_status}" -ne 0 && "${parquet_microbenchmark_mode}" == report ]]; then
        echo "##teamcity[message text='Parquet microbenchmark observation failed; see parquet-microbenchmark artifacts' status='WARNING']"
        echo "WARN: report mode ignores Parquet microbenchmark exit code ${benchmark_status}"
        exit 0
    fi
    exit "${benchmark_status}"
fi

if [[ ! "${PARQUET_BENCHMARK_BASE_SHA:-}" =~ ^[0-9a-f]{40}$ ]]; then
    echo "ERROR: invalid or missing Parquet benchmark base SHA"
    exit 2
fi

build_status=0
if bash "${script_dir}/build-parquet-microbenchmark.sh" \
    "${PARQUET_BENCHMARK_BASE_SHA}"; then
    echo "INFO: Parquet benchmark binaries built successfully"
else
    build_status=$?
fi
if [[ "${build_status}" -ne 0 ]]; then
    echo "ERROR: Parquet benchmark build failed with exit code ${build_status}"
    exit "${build_status}"
fi

if command -v python3 >/dev/null 2>&1; then
    benchmark_python=python3
elif command -v python >/dev/null 2>&1; then
    benchmark_python=python
else
    echo "ERROR: Python is required to run the Parquet performance gate"
    exit 2
fi

exec "${benchmark_python}" "${script_dir}/run-parquet-microbenchmark.py" \
    --doris-home "${doris_home}" \
    --head-binary "${PARQUET_BENCHMARK_HEAD_BINARY:-${doris_home}/parquet-benchmark-output/head/be/lib/benchmark_test}" \
    --base-binary "${PARQUET_BENCHMARK_BASE_BINARY:-${doris_home}/parquet-benchmark-output/base/be/lib/benchmark_test}" \
    --result-dir "${result_dir}" \
    --policy "${script_dir}/parquet-microbenchmark-policy.json" \
    --comparator "${script_dir}/compare-parquet-microbenchmark.py"
