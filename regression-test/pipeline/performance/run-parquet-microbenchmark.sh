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

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
doris_home=$(cd "${script_dir}/../../.." && pwd)
benchmark_binary="${PARQUET_BENCHMARK_BINARY:-${doris_home}/parquet-benchmark-output/be/lib/benchmark_test}"
result_dir="${PARQUET_BENCHMARK_RESULT_DIR:-${doris_home}/parquet-benchmark-results}"
case_list="${result_dir}/cases.txt"

if [[ ! -x "${benchmark_binary}" ]]; then
    echo "ERROR: Parquet benchmark binary not found: ${benchmark_binary}"
    exit 1
fi
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
"${benchmark_binary}" --benchmark_list_tests >"${case_list}"
decoder_count=$(grep -c '^ParquetDecoder/' "${case_list}" || true)
reader_count=$(grep -c '^ParquetReader/' "${case_list}" || true)
if [[ "${decoder_count}" -ne 152 || "${reader_count}" -ne 137 ]]; then
    echo "ERROR: unexpected Parquet benchmark matrix: decoder=${decoder_count}, reader=${reader_count}"
    exit 1
fi

run_and_validate() {
    local group="$1"
    local expected_count="$2"
    local output_file="$3"

    "${benchmark_binary}" \
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

run_and_validate ParquetDecoder 152 "${result_dir}/decoder-smoke.json"
run_and_validate ParquetReader 137 "${result_dir}/reader-smoke.json"

echo "INFO: Parquet microbenchmark smoke passed: 152 decoder cases, 137 reader cases"
