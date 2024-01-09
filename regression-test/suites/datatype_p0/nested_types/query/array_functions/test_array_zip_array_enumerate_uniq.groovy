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

suite("test_array_zip_array_enumerate_uniq", "p0") {

    sql "drop table if exists numbers;"
    sql "create table if not exists numbers (number int) ENGINE=OLAP DISTRIBUTED BY HASH(number) BUCKETS 1 PROPERTIES('replication_num' = '1');"
    sql "insert into numbers values (NULL), (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (10);"

    sql "set enable_nereids_planner=false;"
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

    // nereids
    sql "set enable_nereids_planner=true;"
    sql "set enable_fallback_to_original_planner=false;"
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


}
