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

set -e
if [[ "${HIVE_DEBUG:-0}" == "1" ]]; then
    set -x
fi

. /mnt/scripts/hive-module-lib.sh

normalized_modules="$(normalize_hive_modules "${HIVE_MODULES:-all}" | paste -sd, -)"
module=""
total=${#DEFAULT_MODULES[@]}
idx=0
overall_start=$(date +%s)
refreshed_modules=()
refresh_details=()

for module in "${DEFAULT_MODULES[@]}"; do
    idx=$((idx + 1))
    if ! module_enabled "${normalized_modules}" "${module}"; then
        echo "[hive-refresh ${idx}/${total}] skip module=${module} (not selected)"
        continue
    fi
    if module_needs_refresh "${module}"; then
        module_start=$(date +%s)
        echo "[hive-refresh ${idx}/${total}] BEGIN module=${module} ts=$(date -Is)"
        refresh_module "${module}"
        refreshed_modules+=("${module}")
        refresh_details+=("${module}:${LAST_REFRESH_DETAIL:-updated}")
        module_end=$(date +%s)
        echo "[hive-refresh ${idx}/${total}] END   module=${module} took=$((module_end - module_start))s"
    else
        echo "[hive-refresh ${idx}/${total}] up-to-date module=${module}"
    fi
done

if (( ${#refreshed_modules[@]} == 0 )); then
    echo "[hive-refresh] summary refreshed_modules=0 details=none"
else
    echo "[hive-refresh] summary refreshed_modules=${#refreshed_modules[@]} modules=$(IFS=,; echo "${refreshed_modules[*]}")"
    echo "[hive-refresh] summary details=$(IFS=';'; echo "${refresh_details[*]}")"
fi
echo "[hive-refresh] all done in $(( $(date +%s) - overall_start ))s"
