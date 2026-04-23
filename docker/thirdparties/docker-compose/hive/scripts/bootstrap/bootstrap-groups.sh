#!/bin/bash
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

BOOTSTRAP_HELPER_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

bootstrap_normalize_groups() {
    local raw_groups="${1:-}"
    local cleaned_groups="${raw_groups// /}"
    local parsed_groups=()
    local deduped_groups=()
    local group=""
    local seen=","

    if [[ -z "${cleaned_groups}" ]]; then
        echo "all"
        return 0
    fi

    IFS=',' read -r -a parsed_groups <<< "${cleaned_groups}"
    for group in "${parsed_groups[@]}"; do
        [[ -n "${group}" ]] || continue
        case "${group}" in
        all|common|hive2_only|hive3_only)
            ;;
        *)
            echo "Unknown hive bootstrap group: ${group}" >&2
            return 1
            ;;
        esac

        if [[ "${group}" == "all" ]]; then
            echo "all"
            return 0
        fi

        if [[ "${seen}" == *",${group},"* ]]; then
            continue
        fi

        seen="${seen}${group},"
        deduped_groups+=("${group}")
    done

    if (( ${#deduped_groups[@]} == 0 )); then
        echo "all"
        return 0
    fi

    local old_ifs="${IFS}"
    IFS=','
    echo "${deduped_groups[*]}"
    IFS="${old_ifs}"
}

bootstrap_group_enabled() {
    local normalized_groups="${1:-all}"
    local group="${2}"

    if [[ "${normalized_groups}" == "all" ]]; then
        return 0
    fi

    [[ ",${normalized_groups}," == *",${group},"* ]]
}

bootstrap_merge_groups() {
    local groups_input=""
    local normalized_groups=""
    local include_common=0
    local include_hive2_only=0
    local include_hive3_only=0
    local merged_groups=()

    for groups_input in "$@"; do
        normalized_groups="$(bootstrap_normalize_groups "${groups_input}")" || return 1
        if [[ "${normalized_groups}" == "all" ]]; then
            echo "all"
            return 0
        fi

        bootstrap_group_enabled "${normalized_groups}" "common" && include_common=1
        bootstrap_group_enabled "${normalized_groups}" "hive2_only" && include_hive2_only=1
        bootstrap_group_enabled "${normalized_groups}" "hive3_only" && include_hive3_only=1
    done

    (( include_common == 1 )) && merged_groups+=("common")
    (( include_hive2_only == 1 )) && merged_groups+=("hive2_only")
    (( include_hive3_only == 1 )) && merged_groups+=("hive3_only")

    if (( ${#merged_groups[@]} == 0 )); then
        echo "all"
        return 0
    fi

    local old_ifs="${IFS}"
    IFS=','
    echo "${merged_groups[*]}"
    IFS="${old_ifs}"
}

bootstrap_list_contains() {
    local group="${1}"
    local kind="${2}"
    local relative_path="${3}"
    local list_path="${BOOTSTRAP_HELPER_DIR}/${group}.${kind}.list"

    [[ -f "${list_path}" ]] || return 1
    grep -Fxq "${relative_path}" "${list_path}"
}

bootstrap_item_group() {
    local kind="${1}"
    local relative_path="${2}"
    local matched_group=""
    local group=""

    for group in hive2_only hive3_only; do
        if bootstrap_list_contains "${group}" "${kind}" "${relative_path}"; then
            if [[ -n "${matched_group}" ]]; then
                echo "Bootstrap item ${relative_path} is mapped to multiple groups" >&2
                return 1
            fi
            matched_group="${group}"
        fi
    done

    if [[ -z "${matched_group}" ]]; then
        echo "common"
        return 0
    fi

    echo "${matched_group}"
}

bootstrap_item_selected() {
    local normalized_groups="${1:-all}"
    local kind="${2}"
    local relative_path="${3}"
    local item_group=""

    item_group="$(bootstrap_item_group "${kind}" "${relative_path}")" || return 1
    bootstrap_group_enabled "${normalized_groups}" "${item_group}"
}

bootstrap_archive_selected() {
    local normalized_groups="${1:-all}"
    local relative_archive_path="${2}"
    local relative_run_script_path

    relative_run_script_path="$(dirname "${relative_archive_path}")/run.sh"
    bootstrap_item_selected "${normalized_groups}" "run_sh" "${relative_run_script_path}"
}
