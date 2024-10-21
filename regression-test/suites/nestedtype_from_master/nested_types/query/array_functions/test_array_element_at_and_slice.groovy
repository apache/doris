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

suite("test_array_element_at_and_slice", "p0") {
    // ============= array element_at =========
    // ubsan test
    qt_old_sql "SELECT 'array_element_at';"
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
	assertTrue(ex.getMessage().size() > 0)
    }

    order_qt_old_sql "SELECT distinct size(array_range(number, number + 100, 99))=2 FROM numbers;"
    order_qt_old_sql "SELECT array_map(x -> cast(x as string), array_range(number))[2] FROM numbers LIMIT 10;"
    order_qt_old_sql "SELECT array_map(x -> cast(x as string), array_range(number))[-1] FROM numbers LIMIT 10;"
    order_qt_old_sql "SELECT array_map(x -> cast(x as string), array_range(number))[number] FROM numbers LIMIT 10;"
    order_qt_old_sql "SELECT array_map(x -> array_range(x), array_range(number))[2] FROM numbers LIMIT 10;"
    order_qt_old_sql "SELECT array_map(x -> array_range(x), array_range(number))[-1] FROM numbers LIMIT 10;"
    order_qt_old_sql "SELECT array_map(x -> array_range(x), array_range(number))[number] FROM numbers LIMIT 10;"

    qt_old_sql "SELECT 'array_slice';"
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

    // nereids
    sql "set enable_nereids_planner=true;"
    sql "set enable_fallback_to_original_planner=false;"

    // ============= array element_at =========
    // ubsan test
    qt_nereid_sql "SELECT 'array_element_at';"
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


    qt_nereid_sql "SELECT 'array_slice';"
    // mistake:  detailMessage = No matching function with signature: array_slice(NULL_TYPE, TINYINT, TINYINT).
    order_qt_nereid_sql "SELECT array_slice(Null, 1, 2);"
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
}
