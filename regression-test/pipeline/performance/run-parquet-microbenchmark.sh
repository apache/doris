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

    docker_image="${performance_docker_image:-apache/doris:build-env-ldb-toolchain-latest}"
    docker_name="parquet-microbenchmark-${TEAMCITY_BUILD_ID:-${commit_id_from_trigger:-manual}}"
    docker_environment=(-e PARQUET_MICROBENCHMARK_IN_CONTAINER=true)
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

    if sudo docker run -i --rm \
        --name "${docker_name}" \
        "${docker_environment[@]}" \
        -v "${teamcity_build_checkoutDir}":/root/doris \
        "${docker_image}" \
        /bin/bash /root/doris/regression-test/pipeline/performance/run-parquet-microbenchmark.sh; then
        benchmark_status=0
    else
        benchmark_status=$?
    fi

    if [[ -d "${teamcity_build_checkoutDir}/parquet-benchmark-results" ]]; then
        # The TeamCity step invokes this script with bash -x. Disable tracing before the
        # service message so the command itself is not parsed as a second artifact path.
        { set +x; } 2>/dev/null
        echo "##teamcity[publishArtifacts 'parquet-benchmark-results => parquet-microbenchmark']"
    fi
    exit "${benchmark_status}"
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
