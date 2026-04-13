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

set -e -x

. /mnt/scripts/bootstrap/bootstrap-groups.sh
BOOTSTRAP_GROUPS="$(bootstrap_normalize_groups "${HIVE_BOOTSTRAP_GROUPS:-}")"
echo "Load hive data with bootstrap groups: ${BOOTSTRAP_GROUPS}"

AUX_LIB="/mnt/scripts/auxlib"
for file in "${AUX_LIB}"/*.tar.gz; do
    [ -e "$file" ] || continue
    tar -xzvf "$file" -C "$AUX_LIB"
    echo "file = ${file}"
done
ls "${AUX_LIB}/"

# Keep existing behavior for Hive metastore classpath.
cp -r "${AUX_LIB}"/* /opt/hive/lib/

# Add JuiceFS jar into Hadoop classpath for `hadoop fs jfs://...`.
shopt -s nullglob
juicefs_jars=("${AUX_LIB}"/juicefs-hadoop-*.jar)
if (( ${#juicefs_jars[@]} > 0 )); then
    for target in /opt/hadoop-3.2.1/share/hadoop/common/lib /opt/hadoop/share/hadoop/common/lib; do
        if [[ -d "${target}" ]]; then
            cp -f "${juicefs_jars[@]}" "${target}"/
        fi
    done
fi
shopt -u nullglob

# start metastore
nohup /opt/hive/bin/hive --service metastore &


# wait metastore start
while ! $(nc -z localhost "${HMS_PORT:-9083}"); do
    sleep 5s
done

if [[ ${NEED_LOAD_DATA} = "0" ]]; then
    echo "NEED_LOAD_DATA is 0, skip load data"
    touch /mnt/SUCCESS
    # Avoid container exit
    tail -f /dev/null
fi
# create paimon external table
if [[ ${enablePaimonHms} == "true" ]]; then
    START_TIME=$(date +%s)
    hive -f /mnt/scripts/create_external_paimon_scripts/create_paimon_tables.hql || (echo "Failed to executing create_paimon_table.hql" && exit 1)
    END_TIME=$(date +%s)
    EXECUTION_TIME=$((END_TIME - START_TIME))
    echo "Script: create_paimon_table.hql executed in $EXECUTION_TIME seconds"
else
    echo "enablePaimonHms is false, skip create paimon table"
fi

# create tables for other cases
# new cases should use separate dir
hadoop fs -mkdir -p /user/doris/suites/

DATA_DIR="/mnt/scripts/data/"
run_scripts=()
while IFS= read -r -d '' run_script; do
    relative_run_script="${run_script#/mnt/scripts/}"
    if bootstrap_item_selected "${BOOTSTRAP_GROUPS}" "run_sh" "${relative_run_script}"; then
        run_scripts+=("${run_script}")
    fi
done < <(find "${DATA_DIR}" -type f -name "run.sh" -print0)

if (( ${#run_scripts[@]} > 0 )); then
    printf '%s\0' "${run_scripts[@]}" | xargs -0 -P "${LOAD_PARALLEL}" -I {} bash -ec '
        START_TIME=$(date +%s)
        bash -e "{}" || (echo "Failed to executing script: {}" && exit 1)
        END_TIME=$(date +%s)
        EXECUTION_TIME=$((END_TIME - START_TIME))
        echo "Script: {} executed in $EXECUTION_TIME seconds"
    '
fi

# put data file
hadoop_put_pids=()
hadoop_put_paths=()
hadoop fs -mkdir -p /user/doris/

copy_to_hdfs_if_selected() {
    local relative_path="$1"
    local local_path="/mnt/scripts/${relative_path}"

    if ! bootstrap_item_selected "${BOOTSTRAP_GROUPS}" "hdfs_dir" "${relative_path}"; then
        return
    fi

    if [[ ! -e "${local_path}" ]]; then
        echo "${local_path} does not exist"
        exit 1
    fi

    if [[ -d "${local_path}" && -z "$(ls "${local_path}")" ]]; then
        echo "${local_path} does not exist"
        exit 1
    fi

    hadoop fs -copyFromLocal -f "${local_path}" /user/doris/ &
    hadoop_put_pids+=($!)
    hadoop_put_paths+=("${relative_path}")
}


## put tpch1
copy_to_hdfs_if_selected "tpch1.db"

## put paimon1
copy_to_hdfs_if_selected "paimon1"


## put tvf_data
copy_to_hdfs_if_selected "tvf_data"

## put other preinstalled data
copy_to_hdfs_if_selected "preinstalled_data"


# wait put finish
if (( ${#hadoop_put_pids[@]} > 0 )); then
    wait "${hadoop_put_pids[@]}"
fi

for relative_path in "${hadoop_put_paths[@]}"; do
    if ! hadoop fs -test -e "/user/doris/${relative_path}"; then
        echo "${relative_path} put failed"
        exit 1
    fi
done

# create tables
shopt -s nullglob
preinstalled_hqls=()
for hql_path in /mnt/scripts/create_preinstalled_scripts/*.hql; do
    relative_hql_path="${hql_path#/mnt/scripts/}"
    if bootstrap_item_selected "${BOOTSTRAP_GROUPS}" "preinstalled_hql" "${relative_hql_path}"; then
        preinstalled_hqls+=("${hql_path}")
    fi
done
shopt -u nullglob

if (( ${#preinstalled_hqls[@]} > 0 )); then
    printf '%s\0' "${preinstalled_hqls[@]}" | xargs -0 -P "${LOAD_PARALLEL}" -I {} bash -ec '
        START_TIME=$(date +%s)
        hive -f {} || (echo "Failed to executing hql: {}" && exit 1)
        END_TIME=$(date +%s)
        EXECUTION_TIME=$((END_TIME - START_TIME))
        echo "Script: {} executed in $EXECUTION_TIME seconds"
    '
fi

# create view
START_TIME=$(date +%s)
hive -f /mnt/scripts/create_view_scripts/create_view.hql
END_TIME=$(date +%s)
EXECUTION_TIME=$((END_TIME - START_TIME))
echo "Script: create_view.hql executed in ${EXECUTION_TIME} seconds"

touch /mnt/SUCCESS

# Avoid container exit
tail -f /dev/null
