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

# batch-utils.sh
# -----------------------------------------------------------------------------
# Thin helpers for the "batch cherry-pick admission" flow (Mergify-like batch).
# It intentionally reuses teamcity-utils.sh so that:
#   - the TeamCity URL / credentials / pipeline-name maps live in ONE place, and
#   - triggering a batch build goes through the exact same trigger_or_skip_build
#     path that a normal `run buildall` on a PR uses.
#
# The python orchestrator (tools/batch-cherry-pick.py) shells out to the two
# functions below; everything else (state, git assembly, merge) is done there.
# -----------------------------------------------------------------------------

BATCH_UTILS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "${BATCH_UTILS_DIR}/teamcity-utils.sh"

# Print, one per line, the GitHub commit-status contexts that a green PR to
# ${target_branch} is expected to carry. This is the authoritative "required
# contexts" set used both to detect batch completion and to broadcast success.
# It is derived from teamcity-utils.sh's own maps, so it can never drift from
# what the real pipelines report.
list_required_contexts() {
    local target_branch="$1"
    if [[ -z "${target_branch}" ]]; then
        echo "Usage: list_required_contexts TARGET_BRANCH" >&2
        return 1
    fi
    local pipelines="${targetBranch_to_pipelines[${target_branch}]}"
    if [[ -z "${pipelines}" ]]; then
        echo "WARNING: no pipeline list for branch ${target_branch}" >&2
        return 1
    fi
    local p ctx
    for p in ${pipelines}; do
        ctx="${conment_to_context[${p}]}"
        # some pipelines (e.g. check_coverage_fe) have no reported context -> skip
        [[ -n "${ctx}" ]] && echo "${ctx}"
    done
}

# Trigger a full "buildall" on the batch vehicle PR. Reuses trigger_or_skip_build
# with FILE_CHANGED=true (a batch always runs the full suite in v1), which also
# cancels any in-flight build on pull/<vehicle_pr> before re-triggering -- that
# is what makes `rerun batch` cheap.
#
# Triggering feut/beut/cloudut/compile mirrors the `run buildall` path exactly:
# compile has TeamCity snapshot-dependencies that chain
# p0/p1/external/cloud_p0/cloud_p1/vault_p0/nonConcurrent/check_coverage.
trigger_batch_ci() {
    local vehicle_pr="$1"
    local target_branch="$2"
    local vehicle_sha="$3"
    if [[ -z "${vehicle_pr}" || -z "${target_branch}" || -z "${vehicle_sha}" ]]; then
        echo "Usage: trigger_batch_ci VEHICLE_PR TARGET_BRANCH VEHICLE_SHA" >&2
        return 1
    fi
    local t
    for t in feut beut cloudut compile; do
        trigger_or_skip_build "true" "${vehicle_pr}" "${target_branch}" "${vehicle_sha}" "${t}" ""
    done
}
