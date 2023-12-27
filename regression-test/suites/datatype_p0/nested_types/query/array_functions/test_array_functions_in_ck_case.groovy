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

suite("test_array_functions_in_ck_case", "p0") {

    sql "drop table if exists numbers;"
    sql "create table if not exists numbers (number int) ENGINE=OLAP DISTRIBUTED BY HASH(number) BUCKETS 1 PROPERTIES('replication_num' = '1');"
    sql "insert into numbers values (NULL), (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (10);"

    sql "set enable_nereids_planner=false;"
    // ============= array join =========
    qt_sql "SELECT 'array-join';"
    // only support array_join with two arguments
    // order_qt_sql "SELECT array_join(['Hello', 'World']);"
    order_qt_old_sql "SELECT array_join(['Hello', 'World'], ', ');"
    // order_qt_sql "SELECT array_join([]);"
    // order_qt_sql "SELECT array_join(array_range(number)) FROM numbers LIMIT 10;"
    order_qt_old_sql "SELECT array_join(array_range(number), '') FROM numbers LIMIT 10;"
    order_qt_old_sql "SELECT array_join(array_range(number), ',') FROM numbers LIMIT 10;"
    order_qt_old_sql "SELECT array_join(array_range(number % 4), '_') FROM numbers LIMIT 10;"
    order_qt_old_sql "SELECT array_join([Null, 'hello', Null, 'world', Null, 'xyz', 'def', Null], ';');"
    order_qt_old_sql "SELECT array_join([1, 23, 456], ';');"
    order_qt_old_sql "SELECT array_join([Null, 1, Null, 23, Null, 456, Null], ';');"
    // array with ip type make mistake
    // old planner
//    mysql> select array(cast('127.0.0.1' as ipv4), Null);
//    ERROR 1105 (HY000): errCode = 2, detailMessage = (172.21.16.12)[CANCELLED]Conversion from IPv4 to Decimal(27, 9) is not supported
    // array_join mistake
    // old planner
//    mysql> select array_join(array(cast('127.0.0.1' as ipv4), Null, cast('1.0.0.1' as ipv4)), ',');
//    ERROR 1105 (HY000): errCode = 2, detailMessage = (172.21.16.12)[CANCELLED]Conversion from IPv4 to Decimal(27, 9) is not supported
    // nereids
//    mysql> select array_join(array(cast('127.0.0.1' as ipv4), Null, cast('1.0.0.1' as ipv4)), ',');
//    ERROR 1105 (HY000): errCode = 2, detailMessage = (172.21.16.12)[CANCELLED]execute failed or unsupported types for function array_join(Array(Nullable(IPv4)),String,)

    //array_with_constant
    qt_sql "SELECT 'array_with_constant';"
    order_qt_old_sql "SELECT array_with_constant(3, number) FROM numbers limit 10;"
    order_qt_old_sql "SELECT array_with_constant(number, 'Hello') FROM numbers limit 10;"
    // not support const expression
//    order_qt_sql "SELECT array_with_constant(number % 3, number % 2 ? 'Hello' : NULL) FROM numbers limit 10;"
    // mistake:No matching function with signature: array_with_constant(INT, ARRAY<NULL_TYPE>)
//    order_qt_sql "SELECT array_with_constant(number, []) FROM numbers limit 10;"
    order_qt_old_sql "SELECT array_with_constant(2, 'qwerty'), array_with_constant(0, -1), array_with_constant(1, 1);"
    //  -- { serverError }
    try {
        sql """ 
                SELECT array_with_constant(-231.37104, -138); 
                """
    } catch (Exception ex) {
        assertTrue(ex.getMessage().contains("Array size can not be negative in function:array_with_constant"))
    }

    // ========= array_intersect ===========
    // with sort
    qt_sql "SELECT 'array_intersect-array-sort';"
    sql "drop table if exists tbl_array_intersect;"
    sql "create table tbl_array_intersect (date Date, arr Array<Int>)  ENGINE=OLAP DISTRIBUTED BY HASH(date) BUCKETS 1 PROPERTIES('replication_num' = '1');"

    sql "insert into tbl_array_intersect values ('2019-01-01', [1,2,3]);"
    sql "insert into tbl_array_intersect values ('2019-01-02', [1,2]);"
    sql "insert into tbl_array_intersect values ('2019-01-03', [1]);"
    sql "insert into tbl_array_intersect values ('2019-01-04', []);"

    order_qt_old_sql "SELECT array_sort(array_intersect(arr, [1,2])) from tbl_array_intersect order by date;"
    order_qt_old_sql "SELECT array_sort(array_intersect(arr, [])) from tbl_array_intersect order by date;"
    order_qt_old_sql "SELECT array_sort(array_intersect([], arr)) from tbl_array_intersect order by date;"
    order_qt_old_sql "SELECT array_sort(array_intersect([1,2], arr)) from tbl_array_intersect order by date;"
    order_qt_old_sql "SELECT array_sort(array_intersect([1,2], [1,2,3,4])) from tbl_array_intersect order by date;"
    order_qt_old_sql "SELECT array_sort(array_intersect([], [])) from tbl_array_intersect order by date;"


    order_qt_old_sql "SELECT array_sort(array_intersect(arr, [1,2])) from tbl_array_intersect order by date;"
    order_qt_old_sql "SELECT array_sort(array_intersect(arr, [])) from tbl_array_intersect order by date;"
    order_qt_old_sql "SELECT array_sort(array_intersect([], arr)) from tbl_array_intersect order by date;"
    order_qt_old_sql "SELECT array_sort(array_intersect([1,2], arr)) from tbl_array_intersect order by date;"
    order_qt_old_sql "SELECT array_sort(array_intersect([1,2], [1,2,3,4])) from tbl_array_intersect order by date;"
    order_qt_old_sql "SELECT array_sort(array_intersect([], [])) from tbl_array_intersect order by date;"


    order_qt_old_sql "SELECT array_sort(array_intersect([-100], [156]));"
    order_qt_old_sql "SELECT array_sort(array_intersect([1], [257]));"

    order_qt_old_sql "SELECT array_sort(array_intersect(['a', 'b', 'c'], ['a', 'a']));"
    order_qt_old_sql "SELECT array_sort(array_intersect([1, 1], [2, 2]));"
    order_qt_old_sql "SELECT array_sort(array_intersect([1, 1], [1, 2]));"
    //  nereids not support array_intersect with three argument
    order_qt_old_sql "SELECT array_sort(array_intersect([1, 1, 1], [3], [2, 2, 2]));"
    order_qt_old_sql "SELECT array_sort(array_intersect([1, 2], [1, 2], [2]));"
    order_qt_old_sql "SELECT array_sort(array_intersect([1, 1], [2, 1], [2, 2], [1]));"
    order_qt_old_sql "SELECT array_sort(array_intersect([1, 1], [2, 1], [2, 2], [2, 2, 2]));"

    // nereids not support array_intersect with one argument
    try {
        sql """
                SELECT array_sort(array_intersect([]));
                """
    } catch (Exception ex) {
        assertTrue(ex.getMessage().contains("No matching function with signature: array_intersect(ARRAY<NULL_TYPE>)"))
    }
    try {
        sql """
                SELECT array_sort(array_intersect([1, 2, 3]));
                """
    } catch (Exception ex) {
        assertTrue(ex.getMessage().contains("No matching function with signature: array_intersect(ARRAY<TINYINT>)"))
    }
//     ========== array-zip ==========
//     wrong case
    try {
        sql """
               SELECT array_zip();
                """
    } catch (Exception ex) {
        assertTrue(ex.getMessage().contains("errCode = 2, detailMessage = Unexpected exception: 0"))
    }

    try {
        sql """
               SELECT array_zip(['a', 'b', 'c'], ['d', 'e', 'f', 'd']);
                """
    } catch (Exception ex) {
        assertTrue(ex.getMessage().contains("function array_zip's 2-th argument should have same offsets with first argument"))
    }

    // ============= array element_at =========
    // ubsan test
    qt_sql "SELECT 'array_element_at';"
    order_qt_old_sql "SELECT array(number)[10000000000] FROM numbers;"
    order_qt_old_sql " SELECT array(number)[-10000000000] FROM numbers;"
    // doris can not parse -0x8000000000000000 to 9223372036854775808 && 0xFFFFFFFFFFFFFFFF to 18446744073709551615
//    order_qt_sql " SELECT array(number)[-0x8000000000000000] FROM numbers;"
//    order_qt_sql " SELECT array(number)[0xFFFFFFFFFFFFFFFF] FROM numbers;"
    // mistake detailMessage = Number out of range[18446744073709551615]. type: BIGINT
    // order_qt_sql " SELECT array(number)[18446744073709551615] FROM numbers;"


    // predict not support action
    try {
        sql """
               SELECT array_range(100) = array_range(0, 100);
                """
    } catch (Exception ex) {
        assertTrue(ex.getMessage().contains("Array type dose not support operand: array_range(100) = array_range(0, 100)"))
    }

    order_qt_old_sql "SELECT distinct size(array_range(number, number + 100, 99))=2 FROM numbers;"
    order_qt_old_sql "SELECT array_map(x -> cast(x as string), array_range(number))[2] FROM numbers LIMIT 10;"
    order_qt_old_sql "SELECT array_map(x -> cast(x as string), array_range(number))[-1] FROM numbers LIMIT 10;"
    order_qt_old_sql "SELECT array_map(x -> cast(x as string), array_range(number))[number] FROM numbers LIMIT 10;"
    order_qt_old_sql "SELECT array_map(x -> array_range(x), array_range(number))[2] FROM numbers LIMIT 10;"
    order_qt_old_sql "SELECT array_map(x -> array_range(x), array_range(number))[-1] FROM numbers LIMIT 10;"
    order_qt_old_sql "SELECT array_map(x -> array_range(x), array_range(number))[number] FROM numbers LIMIT 10;"

    // ============= array_enumerate_uniq =========
    qt_sql "SELECT 'array_enumerate_uniq';"
    order_qt_old_sql """ SELECT array_enumerate_uniq(array_enumerate_uniq(array(cast(10 as LargeInt), cast(100 as LargeInt), cast(2 as LargeInt))), array(cast(123 as LargeInt), cast(1023 as LargeInt), cast(123 as LargeInt))); """

    order_qt_old_sql """SELECT array_enumerate_uniq(
            [111111, 222222, 333333],
            [444444, 555555, 666666],
            [111111, 222222, 333333],
            [444444, 555555, 666666],
            [111111, 222222, 333333],
            [444444, 555555, 666666],
            [111111, 222222, 333333],
            [444444, 555555, 666666]);"""
    order_qt_old_sql """SELECT array_enumerate_uniq(array(STDDEV_SAMP(910947.571364)), array(NULL)) from numbers;"""
    //order_qt_sql """ SELECT max(array_join(arr)) FROM (SELECT array_enumerate_uniq(group_array(DIV(number, 54321)) AS nums, group_array(cast(DIV(number, 98765) as string))) AS arr FROM (SELECT number FROM numbers LIMIT 1000000) GROUP BY bitmap_hash(number) % 100000);"""

    qt_sql """SELECT 'array_concat_slice_push_back_push_front_pop_back_pop_front'"""
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

    // mistake:  detailMessage = No matching function with signature: array_slice(NULL_TYPE, TINYINT, TINYINT).
//    order_qt_sql "SELECT array_slice(Null, 1, 2);"
    order_qt_old_sql "SELECT array_slice([1, 2, 3, 4, 5, 6], Null, Null);"
    order_qt_old_sql "SELECT array_slice([1, 2, 3, 4, 5, 6], 2, Null);"
    order_qt_old_sql "SELECT array_slice([1, 2, 3, 4, 5, 6], Null, 4);"
    order_qt_old_sql "SELECT array_slice([1, 2, 3, 4, 5, 6], Null, -2);"
    order_qt_old_sql "SELECT array_slice([1, 2, 3, 4, 5, 6], -3, Null);"
    order_qt_old_sql "SELECT array_slice([1, 2, 3, 4, 5, 6], 2, 3);"
    order_qt_old_sql "SELECT array_slice([1, 2, 3, 4, 5, 6], 2, -2);"
    order_qt_old_sql "SELECT array_slice([1, 2, 3, 4, 5, 6], -4, 2);"
    order_qt_old_sql "SELECT array_slice([1, 2, 3, 4, 5, 6], -4, -2);"
    order_qt_old_sql "SELECT array_slice([1, 2, 3, 4, 5, 6], 2, 0);"
    order_qt_old_sql "SELECT array_slice([1, 2, 3, 4, 5, 6], -10, 15);"
    order_qt_old_sql "SELECT array_slice([1, 2, 3, 4, 5, 6], -15, 10);"
    order_qt_old_sql "SELECT array_slice([1, 2, 3, 4, 5, 6], -15, 9);"
    order_qt_old_sql "SELECT array_slice([1, 2, 3, 4, 5, 6], 10, 0);"
    order_qt_old_sql "SELECT array_slice([1, 2, 3, 4, 5, 6], 10, -1);"
    order_qt_old_sql "SELECT array_slice([1, 2, 3, 4, 5, 6], 10, 1);"
    order_qt_old_sql "SELECT array_slice([1, 2, Null, 4, 5, 6], 2, 4);"
    order_qt_old_sql "SELECT array_slice(['a', 'b', 'c', 'd', 'e'], 2, 3);"
    order_qt_old_sql "SELECT array_slice([Null, 'b', Null, 'd', 'e'], 2, 3);"
    // not support nested complex type
    // order_qt_sql "SELECT array_slice([], [NULL], NULL), 1 from numbers limit 2;"

    order_qt_old_sql "SELECT array_pushback(Null, 1);"
    order_qt_old_sql "SELECT array_pushback([1], 1);"
    order_qt_old_sql "SELECT array_pushback([Null], 1);"
    order_qt_old_sql "SELECT array_pushback([0.5, 0.7], 1);"
    order_qt_old_sql "SELECT array_pushback([1], -1);"
    order_qt_old_sql "SELECT array_pushback(['a', 'b'], 'cd');"
    order_qt_old_sql "SELECT array_pushback([], 1);"
    order_qt_old_sql "SELECT array_pushback([], -1);"

    order_qt_old_sql "SELECT array_pushfront(Null, 1);"
    order_qt_old_sql "SELECT array_pushfront([1], 1);"
    order_qt_old_sql "SELECT array_pushfront([Null], 1);"
    order_qt_old_sql "SELECT array_pushfront([0.5, 0.7], 1);"
    order_qt_old_sql "SELECT array_pushfront([1], -1);"
    order_qt_old_sql "SELECT array_pushfront(['a', 'b'], 'cd');"
    order_qt_old_sql "SELECT array_pushfront([], 1);"
    order_qt_old_sql "SELECT array_pushfront([], -1);"

    order_qt_old_sql "SELECT array_popback(Null);"
    order_qt_old_sql "SELECT array_popback([]);"
    order_qt_old_sql "SELECT array_popback([1]);"
    order_qt_old_sql "SELECT array_popback([1, 2, 3]);"
    order_qt_old_sql "SELECT array_popback([0.1, 0.2, 0.3]);"
    order_qt_old_sql "SELECT array_popback(['a', 'b', 'c']);"

    order_qt_old_sql "SELECT array_popfront(Null);"
    order_qt_old_sql "SELECT array_popfront([]);"
    order_qt_old_sql "SELECT array_popfront([1]);"
    order_qt_old_sql "SELECT array_popfront([1, 2, 3]);"
    order_qt_old_sql "SELECT array_popfront([0.1, 0.2, 0.3]);"
    order_qt_old_sql "SELECT array_popfront(['a', 'b', 'c']);"


    // ============= array_compact =========
    sql "SELECT 'array_compact';"
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
    qt_sql "SELECT 'array_difference';"
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

    // ============= array join =========
    qt_sql "SELECT 'array-join';"
    // only support array_join with two arguments
    // order_qt_sql "SELECT array_join(['Hello', 'World']);"
    order_qt_nereid_sql "SELECT array_join(['Hello', 'World'], ', ');"
    // order_qt_sql "SELECT array_join([]);"
    // order_qt_sql "SELECT array_join(array_range(number)) FROM numbers LIMIT 10;"
    order_qt_nereid_sql "SELECT array_join(array_range(number), '') FROM numbers LIMIT 10;"
    order_qt_nereid_sql "SELECT array_join(array_range(number), ',') FROM numbers LIMIT 10;"
    order_qt_nereid_sql "SELECT array_join(array_range(number % 4), '_') FROM numbers LIMIT 10;"
    order_qt_nereid_sql "SELECT array_join([Null, 'hello', Null, 'world', Null, 'xyz', 'def', Null], ';');"
    order_qt_nereid_sql "SELECT array_join([1, 23, 456], ';');"
    order_qt_nereid_sql "SELECT array_join([Null, 1, Null, 23, Null, 456, Null], ';');"
    // array with ip type make mistake
    // old planner
//    mysql> select array(cast('127.0.0.1' as ipv4), Null);
//    ERROR 1105 (HY000): errCode = 2, detailMessage = (172.21.16.12)[CANCELLED]Conversion from IPv4 to Decimal(27, 9) is not supported
    // array_join mistake
    // old planner
//    mysql> select array_join(array(cast('127.0.0.1' as ipv4), Null, cast('1.0.0.1' as ipv4)), ',');
//    ERROR 1105 (HY000): errCode = 2, detailMessage = (172.21.16.12)[CANCELLED]Conversion from IPv4 to Decimal(27, 9) is not supported
    // nereids
//    mysql> select array_join(array(cast('127.0.0.1' as ipv4), Null, cast('1.0.0.1' as ipv4)), ',');
//    ERROR 1105 (HY000): errCode = 2, detailMessage = (172.21.16.12)[CANCELLED]execute failed or unsupported types for function array_join(Array(Nullable(IPv4)),String,)

    //array_with_constant
    qt_sql "SELECT 'array_with_constant';"
    order_qt_nereid_sql "SELECT array_with_constant(3, number) FROM numbers limit 10;"
    order_qt_nereid_sql "SELECT array_with_constant(number, 'Hello') FROM numbers limit 10;"
    // not support const expression
//    order_qt_sql "SELECT array_with_constant(number % 3, number % 2 ? 'Hello' : NULL) FROM numbers limit 10;"
    order_qt_sql "SELECT array_with_constant(number, []) FROM numbers limit 10;"
    order_qt_nereid_sql "SELECT array_with_constant(2, 'qwerty'), array_with_constant(0, -1), array_with_constant(1, 1);"
    //  -- { serverError }
    try {
        sql """ 
                SELECT array_with_constant(-231.37104, -138); 
                """
    } catch (Exception ex) {
        assertTrue(ex.getMessage().contains("Array size can not be negative in function:array_with_constant"))
    }

    // ========= array_intersect ===========
    // with sort
    qt_sql "SELECT 'array_intersect-array-sort';"
    sql "drop table if exists tbl_array_intersect;"
    sql "create table tbl_array_intersect (date Date, arr Array<Int>)  ENGINE=OLAP DISTRIBUTED BY HASH(date) BUCKETS 1 PROPERTIES('replication_num' = '1');"

    sql "insert into tbl_array_intersect values ('2019-01-01', [1,2,3]);"
    sql "insert into tbl_array_intersect values ('2019-01-02', [1,2]);"
    sql "insert into tbl_array_intersect values ('2019-01-03', [1]);"
    sql "insert into tbl_array_intersect values ('2019-01-04', []);"

    order_qt_nereid_sql "SELECT array_sort(array_intersect(arr, [1,2])) from tbl_array_intersect order by date;"
    order_qt_nereid_sql "SELECT array_sort(array_intersect(arr, [])) from tbl_array_intersect order by date;"
    order_qt_nereid_sql "SELECT array_sort(array_intersect([], arr)) from tbl_array_intersect order by date;"
    order_qt_nereid_sql "SELECT array_sort(array_intersect([1,2], arr)) from tbl_array_intersect order by date;"
    order_qt_nereid_sql "SELECT array_sort(array_intersect([1,2], [1,2,3,4])) from tbl_array_intersect order by date;"
    order_qt_nereid_sql "SELECT array_sort(array_intersect([], [])) from tbl_array_intersect order by date;"


    order_qt_nereid_sql "SELECT array_sort(array_intersect(arr, [1,2])) from tbl_array_intersect order by date;"
    order_qt_nereid_sql "SELECT array_sort(array_intersect(arr, [])) from tbl_array_intersect order by date;"
    order_qt_nereid_sql "SELECT array_sort(array_intersect([], arr)) from tbl_array_intersect order by date;"
    order_qt_nereid_sql "SELECT array_sort(array_intersect([1,2], arr)) from tbl_array_intersect order by date;"
    order_qt_nereid_sql "SELECT array_sort(array_intersect([1,2], [1,2,3,4])) from tbl_array_intersect order by date;"
    order_qt_nereid_sql "SELECT array_sort(array_intersect([], [])) from tbl_array_intersect order by date;"


    order_qt_nereid_sql "SELECT array_sort(array_intersect([-100], [156]));"
    order_qt_nereid_sql "SELECT array_sort(array_intersect([1], [257]));"

    order_qt_nereid_sql "SELECT array_sort(array_intersect(['a', 'b', 'c'], ['a', 'a']));"
    order_qt_nereid_sql "SELECT array_sort(array_intersect([1, 1], [2, 2]));"
    order_qt_nereid_sql "SELECT array_sort(array_intersect([1, 1], [1, 2]));"
    //  nereids not support array_intersect with three argument
//    order_qt_nereid_sql "SELECT array_sort(array_intersect([1, 1, 1], [3], [2, 2, 2]));"
//    order_qt_nereid_sql "SELECT array_sort(array_intersect([1, 2], [1, 2], [2]));"
//    order_qt_nereid_sql "SELECT array_sort(array_intersect([1, 1], [2, 1], [2, 2], [1]));"
//    order_qt_nereid_sql "SELECT array_sort(array_intersect([1, 1], [2, 1], [2, 2], [2, 2, 2]));"
//
//    // nereids not support array_intersect with one argument
//    try {
//        sql """
//                SELECT array_sort(array_intersect([]));
//                """
//    } catch (Exception ex) {
//        assertTrue(ex.getMessage().contains("No matching function with signature: array_intersect(ARRAY<NULL_TYPE>)"))
//    }
//    try {
//        sql """
//                SELECT array_sort(array_intersect([1, 2, 3]));
//                """
//    } catch (Exception ex) {
//        assertTrue(ex.getMessage().contains("No matching function with signature: array_intersect(ARRAY<TINYINT>)"))
//    }
//     ========== array-zip ==========
//     wrong case
    try {
        sql """
               SELECT array_zip();
                """
    } catch (Exception ex) {
        assertTrue(ex.getMessage().size() > 0)
    }

    try {
        sql """
               SELECT array_zip(['a', 'b', 'c'], ['d', 'e', 'f', 'd']);
                """
    } catch (Exception ex) {
        assertTrue(ex.getMessage().contains("function array_zip's 2-th argument should have same offsets with first argument"))
    }

    // ============= array element_at =========
    // ubsan test
    qt_sql "SELECT 'array_element_at';"
    order_qt_nereid_sql "SELECT array(number)[10000000000] FROM numbers;"
    order_qt_nereid_sql " SELECT array(number)[-10000000000] FROM numbers;"
    // doris can not parse -0x8000000000000000 to 9223372036854775808 && 0xFFFFFFFFFFFFFFFF to 18446744073709551615
//    order_qt_sql " SELECT array(number)[-0x8000000000000000] FROM numbers;"
//    order_qt_sql " SELECT array(number)[0xFFFFFFFFFFFFFFFF] FROM numbers;"
     order_qt_sql " SELECT array(number)[18446744073709551615] FROM numbers;"


    // predict not support action
    try {
        sql """
               SELECT array_range(100) = array_range(0, 100);
                """
    } catch (Exception ex) {
        assertTrue(ex.getMessage().size() > 0)
    }

    order_qt_nereid_sql "SELECT distinct size(array_range(number, number + 100, 99))=2 FROM numbers;"
    order_qt_nereid_sql "SELECT array_map(x -> cast(x as string), array_range(number))[2] FROM numbers LIMIT 10;"
    order_qt_nereid_sql "SELECT array_map(x -> cast(x as string), array_range(number))[-1] FROM numbers LIMIT 10;"
    order_qt_nereid_sql "SELECT array_map(x -> cast(x as string), array_range(number))[number] FROM numbers LIMIT 10;"
    order_qt_nereid_sql "SELECT array_map(x -> array_range(x), array_range(number))[2] FROM numbers LIMIT 10;"
    order_qt_nereid_sql "SELECT array_map(x -> array_range(x), array_range(number))[-1] FROM numbers LIMIT 10;"
    order_qt_nereid_sql "SELECT array_map(x -> array_range(x), array_range(number))[number] FROM numbers LIMIT 10;"

    // nereid not support array_enumerate_uniq
    // ============= array_enumerate_uniq =========
//    qt_sql "SELECT 'array_enumerate_uniq';"
//    order_qt_nereid_sql """ SELECT array_enumerate_uniq(array_enumerate_uniq(array(cast(10 as LargeInt), cast(100 as LargeInt), cast(2 as LargeInt))), array(cast(123 as LargeInt), cast(1023 as LargeInt), cast(123 as LargeInt))); """
//
//    order_qt_nereid_sql """SELECT array_enumerate_uniq(
//            [111111, 222222, 333333],
//            [444444, 555555, 666666],
//            [111111, 222222, 333333],
//            [444444, 555555, 666666],
//            [111111, 222222, 333333],
//            [444444, 555555, 666666],
//            [111111, 222222, 333333],
//            [444444, 555555, 666666]);"""
//    order_qt_nereid_sql """SELECT array_enumerate_uniq(array(STDDEV_SAMP(910947.571364)), array(NULL)) from numbers;"""
//    //order_qt_sql """ SELECT max(array_join(arr)) FROM (SELECT array_enumerate_uniq(group_array(DIV(number, 54321)) AS nums, group_array(cast(DIV(number, 98765) as string))) AS arr FROM (SELECT number FROM numbers LIMIT 1000000) GROUP BY bitmap_hash(number) % 100000);"""

    qt_sql """SELECT 'array_concat_slice_push_back_push_front_pop_back_pop_front'"""
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

    // mistake:  detailMessage = No matching function with signature: array_slice(NULL_TYPE, TINYINT, TINYINT).
//    order_qt_sql "SELECT array_slice(Null, 1, 2);"
    order_qt_nereid_sql "SELECT array_slice([1, 2, 3, 4, 5, 6], Null, Null);"
    order_qt_nereid_sql "SELECT array_slice([1, 2, 3, 4, 5, 6], 2, Null);"
    order_qt_nereid_sql "SELECT array_slice([1, 2, 3, 4, 5, 6], Null, 4);"
    order_qt_nereid_sql "SELECT array_slice([1, 2, 3, 4, 5, 6], Null, -2);"
    order_qt_nereid_sql "SELECT array_slice([1, 2, 3, 4, 5, 6], -3, Null);"
    order_qt_nereid_sql "SELECT array_slice([1, 2, 3, 4, 5, 6], 2, 3);"
    order_qt_nereid_sql "SELECT array_slice([1, 2, 3, 4, 5, 6], 2, -2);"
    order_qt_nereid_sql "SELECT array_slice([1, 2, 3, 4, 5, 6], -4, 2);"
    order_qt_nereid_sql "SELECT array_slice([1, 2, 3, 4, 5, 6], -4, -2);"
    order_qt_nereid_sql "SELECT array_slice([1, 2, 3, 4, 5, 6], 2, 0);"
    order_qt_nereid_sql "SELECT array_slice([1, 2, 3, 4, 5, 6], -10, 15);"
    order_qt_nereid_sql "SELECT array_slice([1, 2, 3, 4, 5, 6], -15, 10);"
    order_qt_nereid_sql "SELECT array_slice([1, 2, 3, 4, 5, 6], -15, 9);"
    order_qt_nereid_sql "SELECT array_slice([1, 2, 3, 4, 5, 6], 10, 0);"
    order_qt_nereid_sql "SELECT array_slice([1, 2, 3, 4, 5, 6], 10, -1);"
    order_qt_nereid_sql "SELECT array_slice([1, 2, 3, 4, 5, 6], 10, 1);"
    order_qt_nereid_sql "SELECT array_slice([1, 2, Null, 4, 5, 6], 2, 4);"
    order_qt_nereid_sql "SELECT array_slice(['a', 'b', 'c', 'd', 'e'], 2, 3);"
    order_qt_nereid_sql "SELECT array_slice([Null, 'b', Null, 'd', 'e'], 2, 3);"
    // not support nested complex type
    // order_qt_sql "SELECT array_slice([], [NULL], NULL), 1 from numbers limit 2;"

    order_qt_nereid_sql "SELECT array_pushback(Null, 1);"
    order_qt_nereid_sql "SELECT array_pushback([1], 1);"
    order_qt_nereid_sql "SELECT array_pushback([Null], 1);"
    order_qt_nereid_sql "SELECT array_pushback([0.5, 0.7], 1);"
    order_qt_nereid_sql "SELECT array_pushback([1], -1);"
    order_qt_nereid_sql "SELECT array_pushback(['a', 'b'], 'cd');"
    order_qt_nereid_sql "SELECT array_pushback([], 1);"
    order_qt_nereid_sql "SELECT array_pushback([], -1);"

    order_qt_nereid_sql "SELECT array_pushfront(Null, 1);"
    order_qt_nereid_sql "SELECT array_pushfront([1], 1);"
    order_qt_nereid_sql "SELECT array_pushfront([Null], 1);"
    order_qt_nereid_sql "SELECT array_pushfront([0.5, 0.7], 1);"
    order_qt_nereid_sql "SELECT array_pushfront([1], -1);"
    order_qt_nereid_sql "SELECT array_pushfront(['a', 'b'], 'cd');"
    order_qt_nereid_sql "SELECT array_pushfront([], 1);"
    order_qt_nereid_sql "SELECT array_pushfront([], -1);"

    order_qt_nereid_sql "SELECT array_popback(Null);"
    order_qt_nereid_sql "SELECT array_popback([]);"
    order_qt_nereid_sql "SELECT array_popback([1]);"
    order_qt_nereid_sql "SELECT array_popback([1, 2, 3]);"
    order_qt_nereid_sql "SELECT array_popback([0.1, 0.2, 0.3]);"
    order_qt_nereid_sql "SELECT array_popback(['a', 'b', 'c']);"

    order_qt_nereid_sql "SELECT array_popfront(Null);"
    order_qt_nereid_sql "SELECT array_popfront([]);"
    order_qt_nereid_sql "SELECT array_popfront([1]);"
    order_qt_nereid_sql "SELECT array_popfront([1, 2, 3]);"
    order_qt_nereid_sql "SELECT array_popfront([0.1, 0.2, 0.3]);"
    order_qt_nereid_sql "SELECT array_popfront(['a', 'b', 'c']);"


    // ============= array_compact =========
    sql "SELECT 'array_compact';"
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
    qt_sql "SELECT 'array_difference';"
    order_qt_nereid_sql "SELECT array_difference([65536, -9223372036854775808]);"
    // Diff of unsigned int -> int
    order_qt_nereid_sql "SELECT array_difference( cast([10, 1] as Array<int>));"
    order_qt_nereid_sql "SELECT array_difference( cast([10, 1] as Array<BIGINT>));"
    order_qt_nereid_sql "SELECT array_difference( cast([10, 1] as Array<LargeInt>));"

    // now we not support array with in/comparable predict
//    order_qt_sql "SELECT [1] < [1000], ['abc'] = [NULL], ['abc'] = [toNullable('abc')], [[]] = [[]], [[], [1]] > [[], []], [[1]] < [[], []], [[], []] > [[]],[([], ([], []))] < [([], ([], ['hello']))];"

    order_qt_nereid_sql "SELECT array_difference(array(cast(100.0000991821289 as Decimal), -2147483647)) AS x;"
}
