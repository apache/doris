#!/usr/bin/env bash
#
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

selectdb_append_extra_module_spec() {
    local var_name="$1"
    local feature_name="$2"
    local module_path="$3"
    local current_value="${!var_name:-}"

    if [[ -n "${current_value}" ]]; then
        current_value+=","
    fi
    current_value+="${feature_name}=${module_path}"
    printf -v "${var_name}" '%s' "${current_value}"
}

selectdb_configure_extra_modules() {
    local enable_tde="$1"
    local enable_tls="$2"
    local enable_variant_nested_group="$3"
    local enable_snapshot="$4"

    EXTRA_FE_MODULES="${EXTRA_FE_MODULES:-}"
    EXTRA_BE_MODULES="${EXTRA_BE_MODULES:-}"
    EXTRA_CLOUD_MODULES="${EXTRA_CLOUD_MODULES:-}"

    if [[ "${enable_tde}" -eq 1 ]]; then
        selectdb_append_extra_module_spec EXTRA_FE_MODULES tde fe-enterprise/fe-tde
        selectdb_append_extra_module_spec EXTRA_BE_MODULES tde enterprise/tde
    fi
    if [[ "${enable_tls}" -eq 1 ]]; then
        selectdb_append_extra_module_spec EXTRA_FE_MODULES tls fe-enterprise/fe-tls
        selectdb_append_extra_module_spec EXTRA_BE_MODULES tls enterprise/tls
        selectdb_append_extra_module_spec EXTRA_CLOUD_MODULES tls enterprise/tls
    fi
    if [[ "${enable_variant_nested_group}" -eq 1 ]]; then
        selectdb_append_extra_module_spec EXTRA_BE_MODULES variant-nested-group enterprise/variant-nested-group
    fi
    if [[ "${enable_snapshot}" -eq 1 ]]; then
        selectdb_append_extra_module_spec EXTRA_FE_MODULES snapshot fe-enterprise/fe-snapshot
        selectdb_append_extra_module_spec EXTRA_CLOUD_MODULES snapshot enterprise/snapshot
    fi

    export EXTRA_FE_MODULES
    export EXTRA_BE_MODULES
    export EXTRA_CLOUD_MODULES
}

selectdb_copy_extra_fe_libs() {
    local output_root="${1:-output}"
    local extra_module module_path target_dir jar_file jar_name

    if [[ -z "${EXTRA_FE_MODULES:-}" ]]; then
        return
    fi

    mkdir -p "${output_root}/fe/lib"
    IFS=',' read -r -a extra_modules <<<"${EXTRA_FE_MODULES}"
    for extra_module in "${extra_modules[@]}"; do
        module_path="${extra_module#*=}"
        target_dir="${DORIS_HOME}/fe/${module_path}/target"

        if [[ -d "${target_dir}/lib" ]]; then
            cp -r -p "${target_dir}/lib/." "${output_root}/fe/lib/"
        fi

        shopt -s nullglob
        for jar_file in "${target_dir}"/*.jar; do
            jar_name="$(basename "${jar_file}")"
            case "${jar_name}" in
            *-sources.jar | *-javadoc.jar | *-test-sources.jar | *tests.jar | original-*.jar)
                continue
                ;;
            esac
            cp -r -p "${jar_file}" "${output_root}/fe/lib/"
        done
        shopt -u nullglob
    done
}
