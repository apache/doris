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

set -eo pipefail

. /mnt/scripts/bootstrap/bootstrap-groups.sh
. /mnt/scripts/hive-common-lib.sh

BOOTSTRAP_GROUPS="$(bootstrap_normalize_groups "${HIVE_BOOTSTRAP_GROUPS:-}")"
DEFAULT_MODULES=(default multi_catalog partition_type statistics tvf regression test preinstalled_hql view)
LAST_REFRESH_DETAIL=""

ensure_hive_state_layout

normalize_hive_modules() {
    local raw_modules="${1:-}"
    local cleaned_modules="${raw_modules// /}"
    local module=""
    local normalized=()

    if [[ -z "${cleaned_modules}" || "${cleaned_modules}" == "all" ]]; then
        printf '%s\n' "${DEFAULT_MODULES[@]}"
        return 0
    fi

    IFS=',' read -r -a normalized <<<"${cleaned_modules}"
    for module in "${normalized[@]}"; do
        case "${module}" in
        default|multi_catalog|partition_type|statistics|tvf|regression|test|preinstalled_hql|view)
            echo "${module}"
            ;;
        *)
            echo "Unknown hive module: ${module}" >&2
            return 1
            ;;
        esac
    done
}

module_enabled() {
    local normalized_modules="${1}"
    local module="$2"

    [[ ",${normalized_modules}," == *",${module},"* ]]
}

module_state_file() {
    local module="$1"
    echo "${HIVE_STATE_DIR}/modules/${module}.sha"
}

preinstalled_hql_state_file() {
    local relative_path="$1"
    local safe_name="${relative_path//\//__}"
    echo "${HIVE_STATE_DIR}/modules/preinstalled_hql__${safe_name}.sha"
}

format_refresh_preview() {
    local limit="$1"
    shift
    local items=("$@")
    local total=${#items[@]}

    if (( total == 0 )); then
        printf 'none'
        return 0
    fi

    if (( total <= limit )); then
        printf '%s' "$(IFS=,; echo "${items[*]}")"
        return 0
    fi

    local preview=("${items[@]:0:limit}")
    printf '%s,+%d-more' "$(IFS=,; echo "${preview[*]}")" "$((total - limit))"
}

hash_files() {
    if [[ $# -eq 0 ]]; then
        printf 'empty\n'
        return 0
    fi

    sha256sum "$@" | sha256sum | awk '{print $1}'
}

calc_module_sha() {
    local module="$1"
    local files=()
    local relative_path=""

    case "${module}" in
    default|multi_catalog|partition_type|statistics|tvf|regression|test)
        while IFS= read -r -d '' file; do
            case "${file}" in
            *.sh|*.hql|*.tar.gz|*.csv|*.txt|*.json|*.parquet|*.orc|*.avro|*.gz)
                files+=("${file}")
                ;;
            esac
        done < <(find "/mnt/scripts/data/${module}" -type f -print0 | sort -z)
        ;;
    preinstalled_hql)
        while IFS= read -r -d '' file; do
            relative_path="${file#/mnt/scripts/}"
            if bootstrap_item_selected "${BOOTSTRAP_GROUPS}" "preinstalled_hql" "${relative_path}"; then
                files+=("${file}")
            fi
        done < <(find /mnt/scripts/create_preinstalled_scripts -maxdepth 1 -type f -name '*.hql' -print0 | sort -z)
        ;;
    view)
        files+=("/mnt/scripts/create_view_scripts/create_view.hql")
        ;;
    *)
        echo "Unknown module for sha: ${module}" >&2
        return 1
        ;;
    esac

    hash_files "${files[@]}"
}

calc_preinstalled_hql_sha() {
    local hql_path="$1"
    hash_files "${hql_path}"
}

module_needs_refresh() {
    local module="$1"
    local current_sha
    local recorded_sha_file
    local hql_path=""
    local relative_hql_path=""
    local current_file_sha=""
    local recorded_file_sha=""

    if [[ "${module}" == "preinstalled_hql" ]]; then
        shopt -s nullglob
        for hql_path in /mnt/scripts/create_preinstalled_scripts/*.hql; do
            relative_hql_path="${hql_path#/mnt/scripts/}"
            if ! bootstrap_item_selected "${BOOTSTRAP_GROUPS}" "preinstalled_hql" "${relative_hql_path}"; then
                continue
            fi

            current_file_sha="$(calc_preinstalled_hql_sha "${hql_path}")"
            recorded_sha_file="$(preinstalled_hql_state_file "${relative_hql_path}")"
            if [[ ! -f "${recorded_sha_file}" ]]; then
                shopt -u nullglob
                return 0
            fi
            recorded_file_sha="$(cat "${recorded_sha_file}")"
            if [[ "${recorded_file_sha}" != "${current_file_sha}" ]]; then
                shopt -u nullglob
                return 0
            fi
        done
        shopt -u nullglob
        return 1
    fi

    current_sha="$(calc_module_sha "${module}")"
    recorded_sha_file="$(module_state_file "${module}")"

    [[ ! -f "${recorded_sha_file}" ]] && return 0
    ! grep -Fxq "${current_sha}" "${recorded_sha_file}"
}

mark_module_refreshed() {
    local module="$1"
    calc_module_sha "${module}" >"$(module_state_file "${module}")"
}

copy_to_hdfs_if_selected() {
    local relative_path="$1"
    local local_path="/mnt/scripts/${relative_path}"

    if ! bootstrap_item_selected "${BOOTSTRAP_GROUPS}" "hdfs_dir" "${relative_path}"; then
        return 0
    fi

    [[ -e "${local_path}" ]]
    if [[ -d "${local_path}" ]]; then
        [[ -n "$(ls -A "${local_path}")" ]]
    fi

    hadoop fs -copyFromLocal -f "${local_path}" /user/doris/
}

refresh_run_scripts_in_dir() {
    local module_dir="$1"
    local run_scripts=()
    local run_script=""
    local relative_run_script=""

    while IFS= read -r -d '' run_script; do
        relative_run_script="${run_script#/mnt/scripts/}"
        if bootstrap_item_selected "${BOOTSTRAP_GROUPS}" "run_sh" "${relative_run_script}"; then
            run_scripts+=("${run_script}")
        fi
    done < <(find "${module_dir}" -type f -name 'run.sh' -print0 | sort -z)

    local total=${#run_scripts[@]}
    LAST_REFRESH_DETAIL="run_sh=${total}"
    if (( total > 0 )); then
        echo "  [run.sh] dir=${module_dir} count=${total} parallel=${LOAD_PARALLEL}"
        export RUN_SH_TOTAL="${total}"
        printf '%s\0' "${run_scripts[@]}" | stdbuf -oL -eL xargs -0 -P "${LOAD_PARALLEL}" -I {} stdbuf -oL -eL bash -ec '
            script="{}"
            start=$(date +%s)
            echo "  [run.sh] BEGIN ${script}"
            if ! bash -e "${script}"; then
                echo "  [run.sh] FAILED ${script}" >&2
                exit 1
            fi
            echo "  [run.sh] END   ${script} took=$(( $(date +%s) - start ))s"
        '
    fi
}

refresh_preinstalled_hql_module() {
    local preinstalled_hqls=()
    local hqls_to_refresh=()
    local refresh_rel_paths=()
    local hql_path=""
    local relative_hql_path=""
    local current_sha=""
    local state_file=""

    shopt -s nullglob
    for hql_path in /mnt/scripts/create_preinstalled_scripts/*.hql; do
        relative_hql_path="${hql_path#/mnt/scripts/}"
        if bootstrap_item_selected "${BOOTSTRAP_GROUPS}" "preinstalled_hql" "${relative_hql_path}"; then
            preinstalled_hqls+=("${hql_path}")
        fi
    done
    shopt -u nullglob

    [[ ${#preinstalled_hqls[@]} -eq 0 ]] && return 0

    IFS=$'\n' preinstalled_hqls=($(printf '%s\n' "${preinstalled_hqls[@]}" | sort))
    unset IFS

    # Phase 1 (serial): SHA check — determine what needs refresh
    for hql_path in "${preinstalled_hqls[@]}"; do
        relative_hql_path="${hql_path#/mnt/scripts/}"
        current_sha="$(calc_preinstalled_hql_sha "${hql_path}")"
        state_file="$(preinstalled_hql_state_file "${relative_hql_path}")"
        if [[ -f "${state_file}" ]] && grep -Fxq "${current_sha}" "${state_file}"; then
            echo "  [preinstalled_hql] up-to-date ${relative_hql_path}"
        else
            hqls_to_refresh+=("${hql_path}")
            refresh_rel_paths+=("${relative_hql_path}")
        fi
    done

    if (( ${#hqls_to_refresh[@]} == 0 )); then
        LAST_REFRESH_DETAIL="files=0"
        echo "  [preinstalled_hql] all selected HQL files are up-to-date"
        return 0
    fi

    LAST_REFRESH_DETAIL="files=${#hqls_to_refresh[@]}($(format_refresh_preview 5 "${refresh_rel_paths[@]}"))"

    # Phase 2 (parallel): execute changed files via xargs -P
    echo "  [preinstalled_hql] refreshing ${#hqls_to_refresh[@]} files (parallel=${LOAD_PARALLEL})"
    printf '%s\0' "${hqls_to_refresh[@]}" | stdbuf -oL -eL xargs -0 -P "${LOAD_PARALLEL}" -I {} \
        stdbuf -oL -eL bash --noprofile --norc -ec '
        hql_path="{}"
        . /mnt/scripts/hive-module-lib.sh
        relative_hql_path="${hql_path#/mnt/scripts/}"
        start=$(date +%s)
        echo "  [preinstalled_hql] BEGIN ${relative_hql_path}"
        hive -f "${hql_path}"
        calc_preinstalled_hql_sha "${hql_path}" >"$(preinstalled_hql_state_file "${relative_hql_path}")"
        echo "  [preinstalled_hql] END   ${relative_hql_path} took=$(( $(date +%s) - start ))s"
    '
}

refresh_module() {
    local module="$1"
    local _t0
    _t0=$(date +%s)
    LAST_REFRESH_DETAIL=""
    echo "[$(date '+%H:%M:%S')] [module] BEGIN ${module}"

    # Invalidate stale sha first so an interrupted refresh forces a redo next time.
    rm -f "$(module_state_file "${module}")"

    case "${module}" in
    default|multi_catalog|partition_type|statistics|tvf|regression|test)
        refresh_run_scripts_in_dir "/mnt/scripts/data/${module}"
        ;;
    preinstalled_hql)
        refresh_preinstalled_hql_module
        echo "[$(date '+%H:%M:%S')] [module] END   ${module} took=$(( $(date +%s) - _t0 ))s"
        return 0
        ;;
    view)
        LAST_REFRESH_DETAIL="create_view.hql"
        run_hive_hql /mnt/scripts/create_view_scripts/create_view.hql "create_view.hql"
        ;;
    *)
        echo "Unknown module for refresh: ${module}" >&2
        return 1
        ;;
    esac

    mark_module_refreshed "${module}"
    echo "[$(date '+%H:%M:%S')] [module] END   ${module} took=$(( $(date +%s) - _t0 ))s"
}
