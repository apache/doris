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
    sql "set enable_nereids_planner=false;"
//     ========== array-zip ==========
//     wrong case
    try {
        sql """
               SELECT array_zip();
                """
    } catch (Exception ex) {
        assertTrue(ex.getMessage().contains("errCode = 2, detailMessage ="))
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

    sql """ DROP TABLE IF EXISTS ARRAY_BIGINT_DATA;"""
    sql """ CREATE TABLE IF NOT EXISTS `ARRAY_BIGINT_DATA` (
              `id` INT NULL,
              `data` ARRAY<BIGINT> NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 10
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
            );"""
    sql """ INSERT INTO ARRAY_BIGINT_DATA VALUES (0, [-1, 0, 1, 2, -9223372036854775808, 9223372036854775807, 1]);"""
    sql """ INSERT INTO ARRAY_BIGINT_DATA VALUES (1, []);"""

    test {
     sql """ select array_enumerate_uniq((select data from ARRAY_BIGINT_DATA where id = 0), (select data from ARRAY_BIGINT_DATA where id = 1), (select data from ARRAY_BIGINT_DATA where id = 1));"""
     exception ("A subquery should not return Array/Map/Struct type")
    }


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
    qt_sql "SELECT 'array_enumerate_uniq';"
    order_qt_nereid_sql """ SELECT array_enumerate_uniq(array_enumerate_uniq(array(cast(10 as LargeInt), cast(100 as LargeInt), cast(2 as LargeInt))), array(cast(123 as LargeInt), cast(1023 as LargeInt), cast(123 as LargeInt))); """

    order_qt_nereid_sql """SELECT array_enumerate_uniq(
            [111111, 222222, 333333],
            [444444, 555555, 666666],
            [111111, 222222, 333333],
            [444444, 555555, 666666],
            [111111, 222222, 333333],
            [444444, 555555, 666666],
            [111111, 222222, 333333],
            [444444, 555555, 666666]);"""
    order_qt_nereid_sql """SELECT array_enumerate_uniq(array(STDDEV_SAMP(910947.571364)), array(NULL)) from numbers;"""
//    //order_qt_sql """ SELECT max(array_join(arr)) FROM (SELECT array_enumerate_uniq(group_array(DIV(number, 54321)) AS nums, group_array(cast(DIV(number, 98765) as string))) AS arr FROM (SELECT number FROM numbers LIMIT 1000000) GROUP BY bitmap_hash(number) % 100000);"""

    test {
     sql """ select array_enumerate_uniq((select data from ARRAY_BIGINT_DATA where id = 0), (select data from ARRAY_BIGINT_DATA where id = 1), (select data from ARRAY_BIGINT_DATA where id = 1));"""
     exception ("lengths of all arrays of function array_enumerate_uniq must be equal")
    }

    // array_shuffle
    // do not check result, since shuffle result is random
    sql "SELECT array_sum(array_shuffle([1, 2, 3, 3, null, null, 4, 4])), array_shuffle([1, 2, 3, 3, null, null, 4, 4], 0), shuffle([1, 2, 3, 3, null, null, 4, 4], 0)"
    sql "SELECT array_sum(array_shuffle([1.111, 2.222, 3.333])), array_shuffle([1.111, 2.222, 3.333], 0), shuffle([1.111, 2.222, 3.333], 0)"
    sql "SELECT array_size(array_shuffle(['aaa', null, 'bbb', 'fff'])), array_shuffle(['aaa', null, 'bbb', 'fff'], 0), shuffle(['aaa', null, 'bbb', 'fff'], 0)"
    sql """select array_size(array("2020-01-02", "2022-01-03", "2021-01-01", "1996-04-17")), array_shuffle(array("2020-01-02", "2022-01-03", "2021-01-01", "1996-04-17"), 0), shuffle(array("2020-01-02", "2022-01-03", "2021-01-01", "1996-04-17"), 0)"""

}
