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

export ROOT_PATH=$(pwd)
export SYS_PATH=${ROOT_PATH}/sys
export QE_PATH=${ROOT_PATH}/qe/palo2/src
export QE_BUG_PATH=${ROOT_PATH}/qe/query_regression
export REPORT_PATH=${ROOT_PATH}/result
export ENV_PATH=${ROOT_PATH}/lib
export PYTHONPATH=${PYTHONPATH}:${ROOT_PATH}:${ROOT_PATH}/sys:${ROOT_PATH}/qe/palo2/lib:${ROOT_PATH}/deploy
rm -f ${ROOT_PATH}/*.xml | tee
rm -f ${ROOT_PATH}/*.html | tee
rm -rf ${REPORT_PATH}
mkdir -p ${REPORT_PATH}

cd ${SYS_PATH}

if [[ -z ${testsuite} ]]; then
    testsuite=normal
fi

if [[ -z ${PROCESS_NUM} ]]; then
    PROCESS_NUM=30
fi

if [[ -z ${RESTART_BE_AUTO} ]]; then
    RESTART_BE_AUTO=false
fi

[[ -e /tmp/fd1 ]] || mkfifo /tmp/fd1
exec 3<>/tmp/fd1
rm -rf /tmp/fd1

for ((i=1;i<=PROCESS_NUM;i++))
do
        echo >&3
done

function check_cluster ()
{
    # check if fe or be dead and start it
    cd ${ENV_PATH}
    python node_op.py -c '''
    import Node
    Node.check_cluster(fe_check=False)
    '''
}

function pytest_execute ()
{
    case_file=$1
    ls ${case_file}
    pytest -sv --junit-xml="${REPORT_PATH}"/"${case_file%.py}".xml --html="${REPORT_PATH}"/"${case_file%.py}".html  "${case_file}" --tb=native 2>&1 | tee "${case_file%.py}".log
    sleep 1
}

function case_execute ()
{
    for case_file in "$@"
    do
        read -r -u3
        {
            echo "run ${case_file}"
            pytest_execute "${case_file}"
            if [[ "${RESTART_BE_AUTO}" == true ]]; then
                check_cluster
            fi     
            echo >&3
        }&
    done

}

# 执行时间较长，最先执行
function long_time_case ()
{
    case_files='test_sys_partition_complex_without_restart_be.py 
                test_sys_storage_medium.py test_sys_delete_load.py'
    cd ${SYS_PATH}
    case_execute "${case_files}"
}

function query_case ()
{
    # query case
    case_files='test_query_predicates.py 
                test_query_others.py
                test_query_order_group.py
                test_query_windows_quick.py
                test_query_window_basic.py
                test_query_complicate.py
                test_query_union_join.py
                test_query_join.py
                test_query_agg.py
                test_query_largeint.py
                test_query_datatype.py
                test_query_string_function.py
                test_query_subselect_1.py
                test_query_subselect_2.py
                test_query_numeric_function.py
                test_query_function_more_quick.py
                test_query_datetime_function.py
                test_query_with.py
                test_query_constant_assigned.py
                test_query_percentile.py
                test_query_spatial_function.py
                test_query_grouping_sets.py
                test_query_except.py
                test_query_intersect.py
                test_query_boolean.py
                test_query_predicate_pushdown.py
                test_query_join_explain.py
                test_query_explain.py
                test_query_lateral_view.py
                test_query_bitmap_filter.py'
    cd "${QE_PATH}"
    case_execute "${case_files}"

    # query bug case
    case_files='test_execute.py'
    cd "${QE_BUG_PATH}"
    case_execute "${case_files}"
}

function sys_case ()
{
    # sys case
    case_files='test_sys_verify.py
                test_sys_bloom_filter_a.py
                test_sys_bloom_filter_b.py
                test_sys_bloom_filter_c.py
                test_sys_precision.py
                test_sys_special_data.py
                test_sys_partition_delete.py
                test_sys_rollup_scenario.py
                test_sys_partition_basic_a.py
                test_sys_partition_basic_b.py
                test_sys_partition_load.py
                test_sys_user_property.py
                test_sys_privilege.py
                test_sys_hll_basic.py
                test_sys_null.py
                test_sys_rollup_scenario_a.py
                test_sys_duplicate_partition_a.py
                test_sys_hll_sc.py
                test_sys_hll_load.py
                test_sys_load.py
                test_sys_load_datatype_strict.py
                test_sys_load_func_strict.py
                test_sys_load_parse_from_path.py
                test_sys_time_zone.py
                test_sys_partition_multi_col.py
                test_sys_pull_load_external.py
                test_sys_alter_schema_change.py
                test_sys_load_column_func.py
                test_sys_pull_load_hdfs.py
                test_sys_alter_schema_change_modify.py
                test_sys_view.py
                test_sys_materialized_view.py
                test_sys_temp_partition_function.py
                test_sys_delete.py
                test_sys_delete_on_duplicate_value.py
                test_sys_boolean.py
                test_sys_modify_partition_property.py
                test_sys_base64.py
                test_sys_materialized_view_2.py
                test_sys_insert_value.py
                test_sys_insert_txn.py
                test_sys_bitmap_index.py
                test_sys_partition_list_basic.py
                test_sys_partition_list_alter.py
                test_sys_update_basic.py
                test_sys_dynamic_partition_alter.py
                test_sys_dynamic_partition_load.py
                test_sys_dynamic_partition_parameter.py
                test_sys_show_data_information.py
                test_sys_binlog.py'
    cd "${SYS_PATH}"
    case_execute "${case_files}"

    # sys/test_sys_routine_load routine load case
    case_files='test_routine_load_job_control.py
                test_routine_load_property.py
                test_routine_load_with_alter.py'
    cd "${SYS_PATH}"/test_sys_routine_load/
    case_execute "${case_files}"

    # sys/test_sys_broker_load broker load case
    case_files='test_broker_load_property.py
                test_broker_load_where.py
                test_broker_load_remote.py'
    cd "${SYS_PATH}"/test_sys_broker_load/
    case_execute "${case_files}"

    # sys/test_sys_backup_restore back & restore case
    case_files='test_sys_snapshot_repo.py
                test_sys_snapshot_backup.py
                test_sys_snapshot_restore.py'
    cd "${SYS_PATH}"/test_sys_backup_restore
    case_execute "${case_files}"

    # sys/test_sys_stream_load stream load case
    case_files='test_stream_insert.py
                test_stream_partition.py
                test_stream_simple.py
                test_stream_simple1.py
                test_stream_load_sc_job.py
                test_stream_load_json.py'
    cd "${SYS_PATH}"/test_sys_stream_load/
    case_execute "${case_files}"

    # sys/test_sys_alter_for_uniq uniq table alter case
    case_files='test_sys_partition_schema_change_add_a_uniq.py
                test_sys_partition_schema_change_add_b_uniq.py
                test_sys_partition_schema_change_complex_uniq.py
                test_sys_partition_schema_change_delete_uniq.py
                test_sys_partition_schema_change_drop_uniq.py
                test_sys_partition_schema_change_invalid_uniq.py
                test_sys_partition_schema_change_order_uniq.py
                test_sys_partition_schema_change_rollup_uniq.py
                test_sys_schema_change_add_a_uniq.py
                test_sys_schema_change_add_b_uniq.py
                test_sys_schema_change_complex_uniq.py
                test_sys_schema_change_drop_uniq.py
                test_sys_schema_change_order_uniq.py'
    cd "${SYS_PATH}"/test_sys_alter_for_uniq/
    case_execute "${case_files}"

    # sys/test_sys_alter_for_duplicate duplicate table alter case
    case_files='test_sys_partition_schema_change_add_a_duplicate.py
                test_sys_partition_schema_change_add_b_duplicate.py
                test_sys_partition_schema_change_complex_duplicate.py
                test_sys_partition_schema_change_delete_duplicate.py
                test_sys_partition_schema_change_drop_duplicate.py
                test_sys_partition_schema_change_invalid_duplicate.py
                test_sys_partition_schema_change_order_duplicate.py
                test_sys_partition_schema_change_rollup_duplicate.py
                test_sys_schema_change_add_a_duplicate.py
                test_sys_schema_change_add_b_duplicate.py
                test_sys_schema_change_complex_duplicate.py
                test_sys_schema_change_drop_duplicate.py
                test_sys_schema_change_order_duplicate.py'
    cd "${SYS_PATH}"/test_sys_alter_for_duplicate/
    case_execute "${case_files}"

    # sys/test_sys_alter_for_aggregate agg table alter case
    case_files='test_sys_partition_schema_change_add_a_aggregate.py
                test_sys_partition_schema_change_add_b_aggregate.py
                test_sys_partition_schema_change_complex_aggregate.py
                test_sys_partition_schema_change_delete_aggregate.py
                test_sys_partition_schema_change_drop_aggregate.py
                test_sys_partition_schema_change_invalid_aggregate.py
                test_sys_partition_schema_change_order_aggregate.py
                test_sys_partition_schema_change_rollup_aggregate.py
                test_sys_schema_change_add_a_aggregate.py
                test_sys_schema_change_add_b_aggregate.py
                test_sys_schema_change_complex_aggregate.py
                test_sys_schema_change_drop_aggregate.py
                test_sys_schema_change_order_aggregate.py'
    cd "${SYS_PATH}"/test_sys_alter_for_aggregate/
    case_execute "${case_files}"

    # sys/test_sys_bitmap
    case_files='test_sys_bitmap_basic.py
                test_sys_bitmap_function.py
                test_sys_bitmap_load.py
                test_sys_bitmap_sc.py'
    cd "${SYS_PATH}"/test_sys_bitmap/
    case_execute "${case_files}"

    # sys/test_sys_export
    case_files='test_export.py
                test_select_into_datatype.py
                test_select_into_property.py
                test_select_into_query.py'
    cd "${SYS_PATH}"/test_sys_export/
    case_execute "${case_files}"
   
    # sys/test_sys_string
    case_files='test_sys_string_basic.py'
    cd "${SYS_PATH}"/test_sys_string
    case_execute "${case_files}"

    # sys/test_sys_array
    case_files='test_array_ddl.py 
                test_array_alter.py
                test_array_load.py
                test_array_select.py'
    cd "${SYS_PATH}"/test_sys_array
    case_execute "${case_files}"
}

if [[ -z "${QUERY_ONLY}" ]]; then
    echo "default all case"
    long_time_case
    sys_case
    sleep 120
    check_cluster
    sleep 60
    query_case
else
    echo "only query case"
    query_case
fi
wait
sleep 1
echo 'FINISHED'
exec 3<&-
exec 3>&-
echo 'byebye'
