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
# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/vec")
# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/vec")

set(VEC_FILES
  aggregate_functions/aggregate_function_avg.cpp
  aggregate_functions/aggregate_function_count.cpp
  aggregate_functions/aggregate_function_distinct.cpp
  aggregate_functions/aggregate_function_sum.cpp
  aggregate_functions/aggregate_function_min_max.cpp
  aggregate_functions/aggregate_function_null.cpp
  aggregate_functions/aggregate_function_uniq.cpp
  aggregate_functions/aggregate_function_hll_union_agg.cpp
  aggregate_functions/aggregate_function_bitmap.cpp
  aggregate_functions/aggregate_function_reader.cpp
  aggregate_functions/aggregate_function_window.cpp
  aggregate_functions/aggregate_function_stddev.cpp
  aggregate_functions/aggregate_function_topn.cpp
  aggregate_functions/aggregate_function_approx_count_distinct.cpp
  aggregate_functions/aggregate_function_group_concat.cpp
  aggregate_functions/aggregate_function_percentile_approx.cpp
  aggregate_functions/aggregate_function_simple_factory.cpp
  columns/collator.cpp
  columns/column.cpp
  columns/column_array.cpp
  columns/column_const.cpp
  columns/column_decimal.cpp
  columns/column_nullable.cpp
  columns/column_string.cpp
  columns/column_vector.cpp
  columns/columns_common.cpp
  common/demangle.cpp
  common/exception.cpp
  common/pod_array.cpp
  common/string_utils/string_utils.cpp
  core/block.cpp
  core/block_info.cpp
  core/column_with_type_and_name.cpp
  core/field.cpp
  core/field.cpp
  core/sort_block.cpp
  core/materialize_block.cpp
  data_types/data_type.cpp
  data_types/data_type_array.cpp
  data_types/data_type_bitmap.cpp
  data_types/data_type_factory.cpp
  data_types/data_type_hll.cpp
  data_types/data_type_nothing.cpp
  data_types/data_type_nothing.cpp
  data_types/data_type_nullable.cpp
  data_types/data_type_nullable.cpp
  data_types/data_type_number_base.cpp
  data_types/data_type_string.cpp
  data_types/data_type_decimal.cpp
  data_types/get_least_supertype.cpp
  data_types/nested_utils.cpp
  data_types/data_type_date.cpp
  data_types/data_type_date_time.cpp
  exec/vaggregation_node.cpp
  exec/ves_http_scan_node.cpp
  exec/ves_http_scanner.cpp
  exec/volap_scan_node.cpp
  exec/vsort_node.cpp
  exec/vsort_exec_exprs.cpp
  exec/volap_scanner.cpp
  exec/vexchange_node.cpp
  exec/vset_operation_node.cpp
  exec/vunion_node.cpp
  exec/vintersect_node.cpp
  exec/vexcept_node.cpp
  exec/vselect_node.cpp
  exec/vblocking_join_node.cpp
  exec/vcross_join_node.cpp
  exec/vodbc_scan_node.cpp
  exec/vmysql_scan_node.cpp
  exec/vschema_scan_node.cpp
  exec/vempty_set_node.cpp
  exec/vanalytic_eval_node.cpp
  exec/vassert_num_rows_node.cpp
  exec/vrepeat_node.cpp
  exec/vtable_function_node.cpp
  exec/join/vhash_join_node.cpp
  exprs/vectorized_agg_fn.cpp
  exprs/vectorized_fn_call.cpp
  exprs/vexpr.cpp
  exprs/vexpr_context.cpp
  exprs/vliteral.cpp
  exprs/varray_literal.cpp
  exprs/vin_predicate.cpp
  exprs/vtuple_is_null_predicate.cpp
  exprs/vslot_ref.cpp
  exprs/vcast_expr.cpp
  exprs/vcase_expr.cpp
  exprs/vinfo_func.cpp
  exprs/table_function/vexplode_split.cpp
  exprs/table_function/vexplode_numbers.cpp
  functions/array/function_array_index.cpp
  functions/array/function_array_register.cpp
  functions/math.cpp
  functions/function_bitmap.cpp
  functions/function_bitmap_variadic.cpp
  functions/comparison.cpp
  functions/comparison_less.cpp
  functions/comparison_equals.cpp
  functions/comparison_greater.cpp
  functions/function.cpp
  functions/function_helpers.cpp
  functions/function_hash.cpp
  functions/functions_logical.cpp
  functions/function_case.cpp
  functions/function_cast.cpp
  functions/function_conv.cpp
  functions/function_encryption.cpp
  functions/function_regexp.cpp
  functions/function_hex.cpp
  functions/function_string.cpp
  functions/function_timestamp.cpp
  functions/function_utility.cpp
  functions/comparison_equal_for_null.cpp
  functions/function_json.cpp
  functions/function_datetime_floor_ceil.cpp
  functions/hll_cardinality.cpp
  functions/hll_empty.cpp
  functions/hll_hash.cpp
  functions/plus.cpp
  functions/modulo.cpp
  functions/multiply.cpp
  functions/minus.cpp
  functions/int_div.cpp
  functions/divide.cpp
  functions/function_bit.cpp
  functions/is_null.cpp
  functions/is_not_null.cpp
  functions/in.cpp
  functions/like.cpp
  functions/to_time_function.cpp
  functions/time_of_function.cpp
  functions/if.cpp
  functions/function_ifnull.cpp
  functions/nullif.cpp
  functions/random.cpp
  functions/function_coalesce.cpp
  functions/function_date_or_datetime_computation.cpp
  functions/function_date_or_datetime_to_string.cpp
  functions/function_datetime_string_to_string.cpp
  functions/function_grouping.cpp
  functions/function_java_udf.cpp
  functions/function_rpc.cpp
  functions/function_convert_tz.cpp
  functions/least_greast.cpp
  functions/function_fake.cpp
  olap/vgeneric_iterators.cpp
  olap/vcollect_iterator.cpp
  olap/block_reader.cpp
  sink/mysql_result_writer.cpp
  sink/result_sink.cpp
  sink/vdata_stream_sender.cpp
  sink/vtablet_sink.cpp
  sink/vmysql_table_writer.cpp
  sink/vmysql_table_sink.cpp
  runtime/vdatetime_value.cpp
  runtime/vdata_stream_recvr.cpp
  runtime/vdata_stream_mgr.cpp
  runtime/vpartition_info.cpp
  runtime/vsorted_run_merger.cpp)

add_library(Vec STATIC
    ${VEC_FILES}
)
