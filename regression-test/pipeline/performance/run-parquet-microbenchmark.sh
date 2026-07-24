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
head_binary="${PARQUET_BENCHMARK_HEAD_BINARY:-${doris_home}/parquet-benchmark-output/head/be/lib/benchmark_test}"
base_binary="${PARQUET_BENCHMARK_BASE_BINARY:-${doris_home}/parquet-benchmark-output/base/be/lib/benchmark_test}"
result_dir="${PARQUET_BENCHMARK_RESULT_DIR:-${doris_home}/parquet-benchmark-results}"
case_list="${result_dir}/cases.txt"

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
    if sudo docker run -i --rm \
        --name "${docker_name}" \
        -e PARQUET_MICROBENCHMARK_IN_CONTAINER=true \
        -e PARQUET_BENCHMARK_CPU="${PARQUET_BENCHMARK_CPU:-8}" \
        -e PARQUET_BENCHMARK_MIN_TIME="${PARQUET_BENCHMARK_MIN_TIME:-0.5s}" \
        -e PARQUET_BENCHMARK_WARMUP_TIME="${PARQUET_BENCHMARK_WARMUP_TIME:-0.2s}" \
        -e PARQUET_REGRESSION_THRESHOLD_PCT="${PARQUET_REGRESSION_THRESHOLD_PCT:-15}" \
        -e PARQUET_WARNING_THRESHOLD_PCT="${PARQUET_WARNING_THRESHOLD_PCT:-5}" \
        -e PARQUET_MAX_CV_PCT="${PARQUET_MAX_CV_PCT:-3}" \
        -v "${teamcity_build_checkoutDir}":/root/doris \
        "${docker_image}" \
        /bin/bash /root/doris/regression-test/pipeline/performance/run-parquet-microbenchmark.sh; then
        benchmark_status=0
    else
        benchmark_status=$?
    fi

    if [[ -d "${teamcity_build_checkoutDir}/parquet-benchmark-results" ]]; then
        echo "##teamcity[publishArtifacts 'parquet-benchmark-results => parquet-microbenchmark']"
    fi
    exit "${benchmark_status}"
fi

for benchmark_binary in "${head_binary}" "${base_binary}"; do
    if [[ ! -x "${benchmark_binary}" ]]; then
        echo "ERROR: Parquet benchmark binary not found: ${benchmark_binary}"
        exit 1
    fi
done
if ! command -v jq >/dev/null 2>&1; then
    echo "ERROR: jq is required to validate benchmark JSON"
    exit 1
fi

mkdir -p "${result_dir}"
fixture_root=$(mktemp -d "${result_dir}/tmp.XXXXXX")
rm -f "${case_list}" "${result_dir}/decoder-smoke.json" "${result_dir}/reader-smoke.json"
trap 'rm -rf "${fixture_root}"' EXIT

export DORIS_HOME="${doris_home}"
export TMPDIR="${fixture_root}"

# This is an executability gate for the complete matrix. The 1 ms samples are published for
# diagnostics only and must not be used to claim a performance improvement or regression.
"${head_binary}" --benchmark_list_tests >"${case_list}"
decoder_count=$(grep -c '^ParquetDecoder/' "${case_list}" || true)
reader_count=$(grep -c '^ParquetReader/' "${case_list}" || true)
if [[ "${decoder_count}" -ne 152 || "${reader_count}" -ne 137 ]]; then
    echo "ERROR: unexpected Parquet benchmark matrix: decoder=${decoder_count}, reader=${reader_count}"
    exit 1
fi

run_smoke_and_validate() {
    local group="$1"
    local expected_count="$2"
    local output_file="$3"

    "${head_binary}" \
        --benchmark_filter="^${group}/" \
        --benchmark_min_time=0.001s \
        --benchmark_out="${output_file}" \
        --benchmark_out_format=json

    jq -e --arg prefix "${group}/" --argjson expected "${expected_count}" '
        ([.benchmarks[] | select(.name | startswith($prefix))] | length) == $expected
        and all(.benchmarks[] | select(.name | startswith($prefix));
                    (.error_occurred // false) == false)
    ' "${output_file}" >/dev/null
}

run_smoke_and_validate ParquetDecoder 152 "${result_dir}/decoder-smoke.json"
run_smoke_and_validate ParquetReader 137 "${result_dir}/reader-smoke.json"

echo "INFO: Parquet microbenchmark smoke passed: 152 decoder cases, 137 reader cases"

gate_cases=(
    "ParquetDecoder/plain/int64/sel_100/clustered"
    "ParquetDecoder/plain/int64/sel_1/alternating"
    "ParquetDecoder/plain/byte_array/sel_100/clustered"
    "ParquetDecoder/dictionary/int32/sel_1/alternating"
    "ParquetDecoder/dictionary/byte_array/sel_10/clustered"
    "ParquetDecoder/dictionary/byte_array/sel_10/alternating"
    "ParquetDecoder/byte_stream_split/double/sel_100/clustered"
    "ParquetDecoder/byte_stream_split/fixed_len_byte_array/sel_10/alternating"
    "ParquetDecoder/delta_binary_packed/int64/sel_50/clustered"
    "ParquetDecoder/delta_length_byte_array/byte_array/sel_10/alternating"
    "ParquetDecoder/delta_byte_array/byte_array/sel_10/alternating"
    "ParquetReader/open_to_first_block/plain/null_10/alternating/sel_10/predicate_projected/width_32/predicate_0"
    "ParquetReader/full_scan/plain/null_10/alternating/sel_10/predicate_projected/width_32/predicate_0"
    "ParquetReader/predicate_scan/plain/null_10/alternating/sel_10/predicate_projected/width_32/predicate_0"
    "ParquetReader/limit_1/plain/null_10/alternating/sel_10/predicate_projected/width_32/predicate_0"
    "ParquetReader/limit_1000/plain/null_10/alternating/sel_10/predicate_projected/width_32/predicate_0"
    "ParquetReader/predicate_scan/dictionary/null_10/alternating/sel_10/predicate_projected/width_32/predicate_0"
    "ParquetReader/predicate_scan/byte_stream_split/null_10/alternating/sel_10/predicate_projected/width_32/predicate_0"
    "ParquetReader/predicate_scan/delta_binary_packed/null_10/alternating/sel_10/predicate_projected/width_32/predicate_0"
    "ParquetReader/predicate_scan/plain/null_90/clustered/sel_10/predicate_projected/width_32/predicate_0"
    "ParquetReader/predicate_scan/plain/null_10/alternating/sel_90/predicate_projected/width_32/predicate_0"
    "ParquetReader/predicate_scan/plain/null_10/alternating/sel_10/predicate_projected/width_512/predicate_511"
)

base_case_list="${result_dir}/base-cases.txt"
"${base_binary}" --benchmark_list_tests >"${base_case_list}"
for case_name in "${gate_cases[@]}"; do
    if ! grep -Fxq "${case_name}" "${case_list}" || ! grep -Fxq "${case_name}" "${base_case_list}"; then
        echo "ERROR: performance gate case missing from base or PR: ${case_name}"
        exit 1
    fi
done

gate_filter=$(printf '^%s$|' "${gate_cases[@]}")
gate_filter="${gate_filter%|}"
benchmark_cpu="${PARQUET_BENCHMARK_CPU:-8}"
if ! [[ "${benchmark_cpu}" =~ ^[0-9]+$ ]] || ! command -v taskset >/dev/null 2>&1 \
        || ! taskset -c "${benchmark_cpu}" true; then
    echo "ERROR: benchmark CPU ${benchmark_cpu} is not online"
    exit 1
fi

run_gate_phase() {
    local phase="$1"
    local binary="$2"
    taskset -c "${benchmark_cpu}" "${binary}" \
        --benchmark_filter="${gate_filter}" \
        --benchmark_min_time="${PARQUET_BENCHMARK_MIN_TIME:-0.5s}" \
        --benchmark_min_warmup_time="${PARQUET_BENCHMARK_WARMUP_TIME:-0.2s}" \
        --benchmark_repetitions=5 \
        --benchmark_out="${result_dir}/${phase}.json" \
        --benchmark_out_format=json
}

# Same-agent, fixed-CPU ABBA ordering limits machine drift and order/cache bias.
run_gate_phase base-a1 "${base_binary}"
run_gate_phase head-b1 "${head_binary}"
run_gate_phase head-b2 "${head_binary}"
run_gate_phase base-a2 "${base_binary}"

compare_gate() {
    local prefix="$1"
    python3 "${script_dir}/compare-parquet-microbenchmark.py" \
        --base-a1 "${result_dir}/${prefix}base-a1.json" \
        --head-b1 "${result_dir}/${prefix}head-b1.json" \
        --head-b2 "${result_dir}/${prefix}head-b2.json" \
        --base-a2 "${result_dir}/${prefix}base-a2.json" \
        --output-json "${result_dir}/comparison.json" \
        --output-markdown "${result_dir}/comparison.md" \
        --regression-threshold-pct "${PARQUET_REGRESSION_THRESHOLD_PCT:-15}" \
        --warning-threshold-pct "${PARQUET_WARNING_THRESHOLD_PCT:-5}" \
        --max-cv-pct "${PARQUET_MAX_CV_PCT:-3}"
}

set +e
compare_gate ""
comparison_status=$?
set -e
if [[ "${comparison_status}" -eq 3 ]]; then
    echo "WARN: suspicious regression is noisy; retry the complete ABBA measurement once"
    mv "${result_dir}/comparison.json" "${result_dir}/comparison-attempt-1.json"
    mv "${result_dir}/comparison.md" "${result_dir}/comparison-attempt-1.md"
    run_gate_phase retry-base-a1 "${base_binary}"
    run_gate_phase retry-head-b1 "${head_binary}"
    run_gate_phase retry-head-b2 "${head_binary}"
    run_gate_phase retry-base-a2 "${base_binary}"
    set +e
    compare_gate "retry-"
    comparison_status=$?
    set -e
fi
if [[ "${comparison_status}" -ne 0 ]]; then
    if [[ "${comparison_status}" -eq 3 ]]; then
        echo "ERROR: performance comparison remained inconclusive after retry"
        exit 1
    fi
    exit "${comparison_status}"
fi

echo "INFO: Parquet microbenchmark performance gate passed"
