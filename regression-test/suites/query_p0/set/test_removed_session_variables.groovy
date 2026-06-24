// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

suite("test_removed_session_variables") {

    // Session variables that have been removed from SessionVariable.java.
    // For backward compatibility (BE-then-FE rolling upgrade), SET on these names must
    // silently no-op and SELECT @@<name> must return an empty string instead of throwing
    // ERR_UNKNOWN_SYSTEM_VARIABLE.
    def removedVars = [
            "use_v2_rollup",
            "rewrite_count_distinct_to_bitmap_hll",
            "enable_variant_access_in_original_planner",
            "extract_wide_range_expr",
            "auto_broadcast_join_threshold",
            "runtime_filters_max_num",
            "disable_inverted_index_v1_for_variant",
            "enable_infer_predicate",
            "limit_rows_for_single_instance",
            "nereids_star_schema_support",
            "enable_cbo_statistics",
            "enable_eliminate_sort_node",
            "drop_table_if_ctas_failed",
            "trace_nereids",
            "enable_sync_mv_cost_based_rewrite"]

    // Test 1: SET on removed variables silently no-ops (no exception), across SET syntaxes.
    for (String var : removedVars) {
        sql "set ${var} = 1"
        sql "set @@${var} = 0"
        sql "set session ${var} = 1"
        sql "set @@session.${var} = 0"
    }

    // Test 2: Case insensitivity - SET still no-ops regardless of case.
    sql "set USE_V2_ROLLUP = 1"
    sql "set Trace_Nereids = true"
    sql "set @@ENABLE_INFER_PREDICATE = 0"

    // Test 3: Combined SET statements mixing removed and normal variables.
    sql "set use_v2_rollup = 1, query_timeout = 31"
    sql "set @@trace_nereids = 0, @@enable_infer_predicate = 1"

    // Test 4: A combined SET with a removed variable must not affect the normal variable.
    def originalTimeout = sql "show variables where variable_name = 'query_timeout'"
    sql "set runtime_filters_max_num = 10, query_timeout = 12345"
    def modifiedTimeout = sql "show variables where variable_name = 'query_timeout'"
    assertTrue(modifiedTimeout[0][1] == "12345")
    sql "set query_timeout = ${originalTimeout[0][1]}"

    // Test 5: SELECT @@<removed_var> returns an empty string.
    order_qt_select_use_v2_rollup "select @@use_v2_rollup"
    order_qt_select_trace_nereids "select @@trace_nereids"
    order_qt_select_runtime_filters_max_num "select @@runtime_filters_max_num"
    order_qt_select_enable_cbo_statistics "select @@enable_cbo_statistics"

    // Test 6: Genuinely unknown variables (not in the removed set) still throw.
    test {
        sql "set this_variable_never_existed_12345 = 1"
        exception "Unknown system variable"
    }

    test {
        sql "select @@this_variable_never_existed_12345"
        exception "Unsupported system variable"
    }

    // Test 7: Normal Doris session variables still work end to end.
    sql "set query_timeout = 999"
    def afterSet = sql "show variables where variable_name = 'query_timeout'"
    assertTrue(afterSet[0][1] == "999")
    sql "set query_timeout = ${originalTimeout[0][1]}"
}
