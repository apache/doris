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

suite("test_array_functions_concat_compact_difference", "p0") {
    qt_old_sql "SELECT 'array_concat'"

    order_qt_old_sql "SELECT array_concat([]);"
    order_qt_old_sql "SELECT array_concat([], []);"
    order_qt_old_sql "SELECT array_concat([], [], []);"
    order_qt_old_sql "SELECT array_concat([Null], []);"
    order_qt_old_sql "SELECT array_concat([Null], [], [1]);"
    order_qt_old_sql "SELECT array_concat([1, 2], [-1, -2], [0.3, 0.7], [Null]);"
    order_qt_old_sql "SELECT array_concat(Null, []);"
    order_qt_old_sql "SELECT array_concat([1], [-1], Null);"
    order_qt_old_sql "SELECT array_concat([1, 2], [3, 4]);"
    order_qt_old_sql "SELECT array_concat([1], [2, 3, 4]);"
    order_qt_old_sql "SELECT array_concat([], []);"
    order_qt_old_sql " SELECT array_concat(['abc'], ['def', 'gh', 'qwe']);"
    order_qt_old_sql " SELECT array_concat([1, NULL, 2], [3, NULL, 4]);"
    order_qt_old_sql "SELECT array_concat([1, Null, 2], [3, 4]);"


    // ============= array_compact =========
    qt_old_sql "SELECT 'array_compact';"
    // this sql make be core for overflow
    // order_qt_sql "SELECT array_compact([[[]], [[], []], [[], []], [[]]]);"
    order_qt_old_sql "SELECT array_compact(array_map(x -> cast((x DIV 3) as string), array_range(number))) FROM numbers;"
    // this sql old planner make error "Lambda columnref size: is 0 but input params size is 1. the replaceExpr of columnref is  ,"
    //    nereids will make be canceled with error
    //           1105 (HY000): errCode = 2, detailMessage = [CANCELLED]const check failed, expr=VectorizedFn[VectorizedFnCall[array_compact](arguments=array_map({"vlambda_function_expr", "Array(Nullable(UInt8))"}),return=Array(Nullable(Int8)))]{
    //            VLambdaFunctionCallExpr[array_map({"vlambda_function_expr", "Array(Nullable(UInt8))"})]{
    //             type=LAMBDA_FUNCTION TYPE children=[VLiteral (name = Int8, type = Int8, value = (0))],
    //            VLiteral (name = Array(Nullable(UInt8)), type = Array(Nullable(UInt8)), value = ([NULL]))}}
    // order_qt_sql "SELECT array_compact(array_map(x -> 0, [NULL]));"
    // order_qt_sql "SELECT cast(array_compact(array_map(x->0, [NULL])) as string);"
//    order_qt_sql "SELECT array_compact(x -> x.2, groupArray((number, intDiv(number, 3) % 3))) FROM numbers limit 10;"
//    order_qt_sql "SELECT array_compact(x -> x.2, groupArray((toString(number), toString(intDiv(number, 3) % 3)))) FROM numbers limit 10;"
//    order_qt_sql "SELECT array_compact(x -> x.2, groupArray((toString(number), intDiv(number, 3) % 3))) FROM numbers limit 10;"

    // ============= array_difference =========
    //  Overflow is Ok and behaves as the CPU does it.
    qt_old_sql "SELECT 'array_difference';"
    order_qt_old_sql "SELECT array_difference([65536, -9223372036854775808]);"
    // Diff of unsigned int -> int
    order_qt_old_sql "SELECT array_difference( cast([10, 1] as Array<int>));"
    order_qt_old_sql "SELECT array_difference( cast([10, 1] as Array<BIGINT>));"
    order_qt_old_sql "SELECT array_difference( cast([10, 1] as Array<LargeInt>));"

    // now we not support array with in/comparable predict
//    order_qt_sql "SELECT [1] < [1000], ['abc'] = [NULL], ['abc'] = [toNullable('abc')], [[]] = [[]], [[], [1]] > [[], []], [[1]] < [[], []], [[], []] > [[]],[([], ([], []))] < [([], ([], ['hello']))];"

    order_qt_old_sql "SELECT array_difference(array(cast(100.0000991821289 as Decimal), -2147483647)) AS x;"

    // nereids
    sql "set enable_nereids_planner=true;"
    sql "set enable_fallback_to_original_planner=false;"
    qt_nereid_sql "SELECT 'array_concat'"

    order_qt_nereid_sql "SELECT array_concat([]);"
    order_qt_nereid_sql "SELECT array_concat([], []);"
    order_qt_nereid_sql "SELECT array_concat([], [], []);"
    order_qt_nereid_sql "SELECT array_concat([Null], []);"
    order_qt_nereid_sql "SELECT array_concat([Null], [], [1]);"
    order_qt_nereid_sql "SELECT array_concat([1, 2], [-1, -2], [0.3, 0.7], [Null]);"
    order_qt_nereid_sql "SELECT array_concat(Null, []);"
    order_qt_nereid_sql "SELECT array_concat([1], [-1], Null);"
    order_qt_nereid_sql "SELECT array_concat([1, 2], [3, 4]);"
    order_qt_nereid_sql "SELECT array_concat([1], [2, 3, 4]);"
    order_qt_nereid_sql "SELECT array_concat([], []);"
    order_qt_nereid_sql " SELECT array_concat(['abc'], ['def', 'gh', 'qwe']);"
    order_qt_nereid_sql " SELECT array_concat([1, NULL, 2], [3, NULL, 4]);"
    order_qt_nereid_sql "SELECT array_concat([1, Null, 2], [3, 4]);"


    // ============= array_compact =========
    qt_nereid_sql "SELECT 'array_compact';"
    // this sql make be core for overflow
    // order_qt_sql "SELECT array_compact([[[]], [[], []], [[], []], [[]]]);"
    order_qt_nereid_sql "SELECT array_compact(array_map(x -> cast((x DIV 3) as string), array_range(number))) FROM numbers;"
    // this sql old planner make error "Lambda columnref size: is 0 but input params size is 1. the replaceExpr of columnref is  ,"
    //    nereids will make be canceled with error
    //           1105 (HY000): errCode = 2, detailMessage = [CANCELLED]const check failed, expr=VectorizedFn[VectorizedFnCall[array_compact](arguments=array_map({"vlambda_function_expr", "Array(Nullable(UInt8))"}),return=Array(Nullable(Int8)))]{
    //            VLambdaFunctionCallExpr[array_map({"vlambda_function_expr", "Array(Nullable(UInt8))"})]{
    //             type=LAMBDA_FUNCTION TYPE children=[VLiteral (name = Int8, type = Int8, value = (0))],
    //            VLiteral (name = Array(Nullable(UInt8)), type = Array(Nullable(UInt8)), value = ([NULL]))}}
    // order_qt_sql "SELECT array_compact(array_map(x -> 0, [NULL]));"
    // order_qt_sql "SELECT cast(array_compact(array_map(x->0, [NULL])) as string);"
//    order_qt_sql "SELECT array_compact(x -> x.2, groupArray((number, intDiv(number, 3) % 3))) FROM numbers limit 10;"
//    order_qt_sql "SELECT array_compact(x -> x.2, groupArray((toString(number), toString(intDiv(number, 3) % 3)))) FROM numbers limit 10;"
//    order_qt_sql "SELECT array_compact(x -> x.2, groupArray((toString(number), intDiv(number, 3) % 3))) FROM numbers limit 10;"

    // ============= array_difference =========
    //  Overflow is Ok and behaves as the CPU does it.
    qt_nereid_sql "SELECT 'array_difference';"
    order_qt_nereid_sql "SELECT array_difference([65536, -9223372036854775808]);"
    // Diff of unsigned int -> int
    order_qt_nereid_sql "SELECT array_difference( cast([10, 1] as Array<int>));"
    order_qt_nereid_sql "SELECT array_difference( cast([10, 1] as Array<BIGINT>));"
    order_qt_nereid_sql "SELECT array_difference( cast([10, 1] as Array<LargeInt>));"

    // now we not support array with in/comparable predict
//    order_qt_sql "SELECT [1] < [1000], ['abc'] = [NULL], ['abc'] = [toNullable('abc')], [[]] = [[]], [[], [1]] > [[], []], [[1]] < [[], []], [[], []] > [[]],[([], ([], []))] < [([], ([], ['hello']))];"

    order_qt_nereid_sql "SELECT array_difference(array(cast(100.0000991821289 as Decimal), -2147483647)) AS x;"
}
