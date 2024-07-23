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

suite("group_array_intersect") {
    sql "DROP TABLE IF EXISTS `group_array_intersect_test`;"
    sql """
        CREATE TABLE `group_array_intersect_test` (
            `id` int(11) NULL COMMENT ""
            , `c_array_int` ARRAY<int(11)> NULL COMMENT ""
            , `c_array_datetimev2` ARRAY<DATETIMEV2(3)> NULL COMMENT ""
            , `c_array_float` ARRAY<float> NULL COMMENT ""
            , `c_array_datev2` ARRAY<DATEV2> NULL COMMENT ""
            , `c_array_string` ARRAY<string> NULL COMMENT ""
            , `c_array_bigint` ARRAY<bigint> NULL COMMENT ""
            , `c_array_decimal` ARRAY<decimal(10, 5)> NULL COMMENT ""
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        );
     """

    sql """INSERT INTO `group_array_intersect_test`(id, c_array_int) VALUES (0, [0]),(1, [1,2,3,4,5]), (2, [6,7,8]), (3, []), (4, null), (5, [6, 7]), (6, [NULL]);"""
    sql """INSERT INTO `group_array_intersect_test`(id, c_array_int) VALUES (12, [12, null, 13]), (13, [null, null]), (14, [12, 13]);"""
    sql """INSERT INTO `group_array_intersect_test`(id, c_array_float) VALUES (7, [6.3, 7.3]), (8, [7.3, 8.3]), (9, [7.3, 9.3, 8.3]);"""
    sql """INSERT INTO `group_array_intersect_test`(id, c_array_datetimev2) VALUES (10, ['2024-03-23 00:00:00', '2024-03-24 00:00:00']), (11, ['2024-03-24 00:00:00', '2024-03-25 00:00:00']);"""
    sql """INSERT INTO `group_array_intersect_test`(id, c_array_datev2) VALUES (15, ['2024-05-23', '2024-03-29']), (16, ['2024-03-29', '2024-03-25']), (17, ['2024-05-23', null]);"""
    sql """INSERT INTO `group_array_intersect_test`(id, c_array_string) VALUES (18, ['a', 'b', 'c', 'd', 'e', 'f']), (19, ['a', 'aa', 'b', 'bb', 'c', 'cc', 'd', 'dd', 'f', 'ff']);"""
    sql """INSERT INTO `group_array_intersect_test`(id, c_array_string) VALUES (20, ['a', null]), (21, [null, null]), (22, ['x', 'y']);"""
    sql """INSERT INTO `group_array_intersect_test`(id, c_array_bigint) VALUES (23, [1234567890123456]), (24, [1234567890123456, 2333333333333333]);"""
    sql """INSERT INTO `group_array_intersect_test`(id, c_array_decimal) VALUES (25, [1.34,2.00188888888888888]), (26, [1.34,2.00123344444455555]);"""

    qt_int_1 """select array_sort(group_array_intersect(c_array_int)) from group_array_intersect_test where id in (6, 12);"""
    qt_int_2 """select array_sort(group_array_intersect(c_array_int)) from group_array_intersect_test where id in (14, 12);"""
    qt_int_3 """select array_sort(group_array_intersect(c_array_int)) from group_array_intersect_test where id in (0, 6);"""
    qt_int_4 """select array_sort(group_array_intersect(c_array_int)) from group_array_intersect_test where id in (13);"""
    qt_int_5 """select array_sort(group_array_intersect(c_array_int)) from group_array_intersect_test where id in (2, 5);"""
    qt_int_6 """select array_sort(group_array_intersect(c_array_int)) from group_array_intersect_test where id in (6, 13);"""
    qt_int_7 """select array_sort(group_array_intersect(c_array_int)) from group_array_intersect_test where id in (12);"""
    qt_int_8 """select array_sort(group_array_intersect(c_array_int)) from group_array_intersect_test where id in (6, 7);"""
    qt_int_9 """select array_sort(group_array_intersect(c_array_int)) from group_array_intersect_test where id in (9, 12);"""
    qt_float_1 """select array_sort(group_array_intersect(c_array_float)) from group_array_intersect_test where id = 7;"""
    qt_float_2 """select array_sort(group_array_intersect(c_array_float)) from group_array_intersect_test where id between 7 and 8;"""
    qt_float_3 """select array_sort(group_array_intersect(c_array_float)) from group_array_intersect_test where id in (7, 9);"""
    qt_datetimev2_1 """select array_sort(group_array_intersect(c_array_datetimev2)) from group_array_intersect_test;"""
    qt_datetimev2_2 """select array_sort(group_array_intersect(c_array_datetimev2)) from group_array_intersect_test where id in (10, 11);"""
    qt_datev2_1 """select array_sort(group_array_intersect(c_array_datev2)) from group_array_intersect_test where id in (15, 16);"""
    qt_datev2_2 """select array_sort(group_array_intersect(c_array_datev2)) from group_array_intersect_test where id in (15, 17);"""
    qt_string_1 """select array_sort(group_array_intersect(c_array_string)) from group_array_intersect_test where id in (17, 20);"""
    qt_string_2 """select array_sort(group_array_intersect(c_array_string)) from group_array_intersect_test where id in (18, 20);"""
    qt_bigint """select array_sort(group_array_intersect(c_array_bigint)) from group_array_intersect_test where id in (23, 24);"""
    qt_decimal """select array_sort(group_array_intersect(c_array_decimal)) from group_array_intersect_test where id in (25, 26);"""
    qt_groupby_1 """select id, array_sort(group_array_intersect(c_array_int)) from group_array_intersect_test where id <= 1 group by id order by id;"""
    qt_groupby_2 """select id, array_sort(group_array_intersect(c_array_string)) from group_array_intersect_test where c_array_string is not null group by id order by id;"""
    qt_groupby_3 """select id, array_sort(group_array_intersect(c_array_string)) from group_array_intersect_test where id = 18 group by id order by id;"""


    sql "DROP TABLE IF EXISTS `group_array_intersect_test_not_null`;"
    sql """
        CREATE TABLE `group_array_intersect_test_not_null` (
            `id` int(11) NULL COMMENT ""
            , `c_array_int` ARRAY<int(11)> NOT NULL COMMENT ""
            , `c_array_float` ARRAY<float> NOT NULL COMMENT ""
            , `c_array_string` ARRAY<string> NOT NULL COMMENT ""
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        );
     """

    sql """INSERT INTO `group_array_intersect_test_not_null`(id, c_array_int, c_array_float, c_array_string) VALUES (1, [1, 2, 3, 4, 5], [1.1, 2.2, 3.3, 4.4, 5.5], ['a', 'b', 'c', 'd', 'e', 'f']);"""
    sql """INSERT INTO `group_array_intersect_test_not_null`(id, c_array_int, c_array_float, c_array_string) VALUES (2, [6, 7, 8], [6.6, 7.7, 8.8], ['a', 'aa', 'b', 'bb', 'c', 'cc', 'd', 'dd', 'f', 'ff'])"""
    sql """INSERT INTO `group_array_intersect_test_not_null`(id, c_array_int, c_array_float, c_array_string) VALUES (3, [], [], []);"""
    sql """INSERT INTO `group_array_intersect_test_not_null`(id, c_array_int, c_array_float, c_array_string) VALUES (4, [6, 7], [6.6, 7.7], ['a']);"""
    sql """INSERT INTO `group_array_intersect_test_not_null`(id, c_array_int, c_array_float, c_array_string) VALUES (5, [null], [null], ['x', 'y']);"""

    qt_notnull_1 """select array_sort(group_array_intersect(c_array_float)) from group_array_intersect_test_not_null where array_size(c_array_float) between 1 and 2;"""
    qt_notnull_2 """select array_sort(group_array_intersect(c_array_int)), array_sort(group_array_intersect(c_array_float)) from group_array_intersect_test_not_null where id between 2 and 3;"""
    qt_notnull_3 """select array_sort(group_array_intersect(c_array_float)) from group_array_intersect_test_not_null where array_size(c_array_float) between 2 and 3;"""
    qt_notnull_4 """select array_sort(group_array_intersect(c_array_string)) from group_array_intersect_test_not_null where id between 1 and 2;"""
    qt_notnull_5 """select array_sort(group_array_intersect(c_array_int)), array_sort(group_array_intersect(c_array_float)), array_sort(group_array_intersect(c_array_string)) from group_array_intersect_test_not_null where id between 3 and 4;"""
    qt_notnull_6 """select array_sort(group_array_intersect(c_array_string)) from group_array_intersect_test_not_null where id between 1 and 5;"""
}
