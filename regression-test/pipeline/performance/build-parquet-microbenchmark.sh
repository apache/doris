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

if [[ "$#" -ne 1 || ! "$1" =~ ^[0-9a-f]{40}$ ]]; then
    echo "ERROR: expected the 40-character target-branch base SHA"
    exit 2
fi

base_sha="$1"
doris_home=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
result_dir="${doris_home}/parquet-benchmark-results"
output_dir="${doris_home}/parquet-benchmark-output"
base_worktree=$(mktemp -d /tmp/doris-parquet-baseline.XXXXXX)
rmdir "${base_worktree}"

cleanup() {
    git -C "${doris_home}" worktree remove --force "${base_worktree}" >/dev/null 2>&1 || true
    rm -rf "${base_worktree}"
}
trap cleanup EXIT

rm -rf "${output_dir}/head" "${output_dir}/base"
mkdir -p "${result_dir}" "${output_dir}"
printf '%s\n' "${base_sha}" >"${result_dir}/base-sha.txt"
git -C "${doris_home}" rev-parse HEAD >"${result_dir}/head-sha.txt"

echo "INFO: build Parquet benchmark for PR revision"
bash "${doris_home}/build.sh" --benchmark --output "${output_dir}/head" \
    2>&1 | tee "${result_dir}/build-head.log"

echo "INFO: build Parquet benchmark for target-branch revision ${base_sha}"
git -C "${doris_home}" worktree add --detach "${base_worktree}" "${base_sha}"
(
    cd "${base_worktree}"
    ROOT_WORKSPACE_PATH="${doris_home}" bash hooks/setup_worktree.sh
    git submodule update --init --recursive --depth 1
    bash build.sh --benchmark --output "${output_dir}/base"
) 2>&1 | tee "${result_dir}/build-base.log"
