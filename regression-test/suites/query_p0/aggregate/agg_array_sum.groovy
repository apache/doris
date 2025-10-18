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

suite('agg_array_sum') {
    sql 'DROP TABLE IF EXISTS `agg_array_sum_test`;'
    sql '''
        CREATE TABLE `agg_array_sum_test` (
            `id` int(11) NULL COMMENT ""
            , `c_array_tinyint` ARRAY<tinyint> NULL COMMENT ""
            , `c_array_smallint` ARRAY<smallint> NULL COMMENT ""
            , `c_array_int` ARRAY<int(11)> NULL COMMENT ""
            , `c_array_bigint` ARRAY<bigint> NULL COMMENT ""
            , `c_array_largeint` ARRAY<largeint> NULL COMMENT ""
            , `c_array_float` ARRAY<float> NULL COMMENT ""
            , `c_array_double` ARRAY<double> NULL COMMENT ""
            , `c_array_decimal` ARRAY<decimal(10, 5)> NULL COMMENT ""
            , `c_array_string` ARRAY<string> NULL COMMENT ""
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        );
     '''

    sql """INSERT INTO `agg_array_sum_test`(id, c_array_tinyint) VALUES (0, [0]),(1, [1,2,3,4,5]), (2, [6,7,8]), (3, []), (4, null), (5, [6, 7]);"""
    sql """INSERT INTO `agg_array_sum_test`(id, c_array_smallint) VALUES (7, [0]),(8, [1,2,3,4,5]), (9, [6,7,8]), (10, []), (11, null), (12, [6, 7]);"""
    sql """INSERT INTO `agg_array_sum_test`(id, c_array_int) VALUES (8, [0]), (8, []), (9, [1,2,3,4,5]), (9, null), (10, [6,7,8]), (10, [1,2]), (11, [4,5]), (12, null), (13, [6, 7]);"""
    sql """INSERT INTO `agg_array_sum_test`(id, c_array_bigint) VALUES (23, [1234567890123456]), (24, [1234567890123456, 2333333333333333]);"""
    sql """INSERT INTO `agg_array_sum_test`(id, c_array_largeint) VALUES (25, [1234567890123456789]), (26, [1234567890123456789, 2333333333333333333]);"""
    sql """INSERT INTO `agg_array_sum_test`(id, c_array_float) VALUES (27, [6.3, 7.3]), (28, [7.3, 8.3]), (29, [7.3, 9.3, 8.3]);"""
    sql """INSERT INTO `agg_array_sum_test`(id, c_array_double) VALUES (30, [1.34, 2.00188888888888888]), (31, [1.34,2.00123344444455555]);"""
    sql """INSERT INTO `agg_array_sum_test`(id, c_array_decimal) VALUES (41, [1.34, 2.00188888888888888]), (42, [1.34,2.00123344444455555]);"""
    sql """INSERT INTO `agg_array_sum_test`(id, c_array_string) VALUES (58, ['a', 'b', 'c', 'd', 'e', 'f']), (59, ['a', 'aa', 'b', 'bb', 'c', 'cc', 'd', 'dd', 'f', 'ff']);"""

    qt_tinyint_1 """select agg_array_sum(c_array_tinyint) from agg_array_sum_test where id between 0 and 5;"""
    qt_tinyint_2 """select agg_array_sum(c_array_tinyint) from agg_array_sum_test where id in (0, 1);"""
    qt_smallint_1 """select agg_array_sum(c_array_smallint) from agg_array_sum_test where id between 7 and 12;"""
    qt_smallint_2 """select agg_array_sum(c_array_smallint) from agg_array_sum_test where id in (7, 8);"""
    qt_int_1 """select agg_array_sum(c_array_int) from agg_array_sum_test where id in (13, 12);"""
    qt_int_2 """select agg_array_sum(c_array_int) from agg_array_sum_test where id between 8 and 13;"""
    qt_bigint_1 """select agg_array_sum(c_array_bigint) from agg_array_sum_test where id in (23, 24);"""
    qt_largeint_1 """select agg_array_sum(c_array_largeint) from agg_array_sum_test where id in (25, 26);"""
    qt_float_1 """select agg_array_sum(c_array_float) from agg_array_sum_test where id in (27, 28, 29);"""
    qt_double_1 """select agg_array_sum(c_array_double) from agg_array_sum_test where id in (30, 31);"""
    qt_decimal_1 """select agg_array_sum(c_array_decimal) from agg_array_sum_test where id in (41, 42);"""
    qt_groupby_1 """select id, agg_array_sum(c_array_int) from agg_array_sum_test where id between 8 and 13 group by id order by id;"""
    expectException({
        sql """select agg_array_sum(c_array_string) as sum_string from agg_array_sum_test where id in (58, 59);"""
        },
        "errCode = 2, detailMessage = arr_array_sum requires a array of numeric parameter: agg_array_sum(c_array_string)"
    )
    sql 'DROP TABLE IF EXISTS `agg_array_sum_test`;'

    sql 'DROP TABLE IF EXISTS `agg_array_sum_test_not_null`;'
    sql '''
        CREATE TABLE `agg_array_sum_test_not_null` (
            `id` int(11) NULL COMMENT ""
            , `c_array_tinyint` ARRAY<tinyint> NOT NULL COMMENT ""
            , `c_array_smallint` ARRAY<smallint> NOT NULL COMMENT ""
            , `c_array_int` ARRAY<int(11)> NOT NULL COMMENT ""
            , `c_array_bigint` ARRAY<bigint> NOT NULL COMMENT ""
            , `c_array_largeint` ARRAY<largeint> NOT NULL COMMENT ""
            , `c_array_float` ARRAY<float> NOT NULL COMMENT ""
            , `c_array_double` ARRAY<double> NOT NULL COMMENT ""
            , `c_array_decimal` ARRAY<decimal(10, 5)> NOT NULL COMMENT ""
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
     '''

    sql """INSERT INTO `agg_array_sum_test_not_null`(id, c_array_tinyint, c_array_smallint, c_array_int, c_array_bigint, c_array_largeint, c_array_float, c_array_double, c_array_decimal, c_array_string) 
        VALUES (1, [1, 2, 3], [1, 2, 3], [1, 2, 3], [1, 2, 3], [1, 2, 3], [1.1, 2.2, 3.3], [1.1, 2.2, 3.3], [1.1, 2.2, 3.3], ['a', 'b', 'c']);"""
    sql """INSERT INTO `agg_array_sum_test_not_null`(id, c_array_tinyint, c_array_smallint, c_array_int, c_array_bigint, c_array_largeint, c_array_float, c_array_double, c_array_decimal, c_array_string) 
        VALUES (1, [1, 2], [1, 2], [1, 2], [1, 2], [1, 2], [1.1, 2.2], [1.1, 2.2], [1.1, 2.2], ['a', 'b']);"""
    sql """INSERT INTO `agg_array_sum_test_not_null`(id, c_array_tinyint, c_array_smallint, c_array_int, c_array_bigint, c_array_largeint, c_array_float, c_array_double, c_array_decimal, c_array_string) 
        VALUES (2, [1, 2], [1, 2], [1, 2], [1, 2], [1, 2], [1.1, 2.2], [1.1, 2.2], [1.1, 2.2], ['a', 'b']);"""
    sql """INSERT INTO `agg_array_sum_test_not_null`(id, c_array_tinyint, c_array_smallint, c_array_int, c_array_bigint, c_array_largeint, c_array_float, c_array_double, c_array_decimal, c_array_string) 
        VALUES (2, [], [], [], [], [], [], [], [], []);"""
    sql """INSERT INTO `agg_array_sum_test_not_null`(id, c_array_tinyint, c_array_smallint, c_array_int, c_array_bigint, c_array_largeint, c_array_float, c_array_double, c_array_decimal, c_array_string) 
        VALUES (3, [1, 2], [1, 2], [1, 2], [1, 2], [1, 2], [1.1, 2.2], [1.1, 2.2], [1.1, 2.2], ['a', 'b']);"""
    sql """INSERT INTO `agg_array_sum_test_not_null`(id, c_array_tinyint, c_array_smallint, c_array_int, c_array_bigint, c_array_largeint, c_array_float, c_array_double, c_array_decimal, c_array_string) 
        VALUES (4, [], [], [], [], [], [], [], [], []);"""

    qt_notnull_1 """select agg_array_sum(c_array_tinyint) as sum_tinyint, agg_array_sum(c_array_smallint) as sum_smallint, agg_array_sum(c_array_int) as sum_int, agg_array_sum(c_array_bigint) as sum_bigint, 
        agg_array_sum(c_array_largeint) as sum_largeint, agg_array_sum(c_array_float) as sum_float, agg_array_sum(c_array_double) as sum_double, agg_array_sum(c_array_decimal) as sum_decimal 
        from agg_array_sum_test_not_null where id=1;"""
    qt_notnull_2 """select agg_array_sum(c_array_tinyint) as sum_tinyint, agg_array_sum(c_array_smallint) as sum_smallint, agg_array_sum(c_array_int) as sum_int, agg_array_sum(c_array_bigint) as sum_bigint, 
        agg_array_sum(c_array_largeint) as sum_largeint, agg_array_sum(c_array_float) as sum_float, agg_array_sum(c_array_double) as sum_double, agg_array_sum(c_array_decimal) as sum_decimal 
        from agg_array_sum_test_not_null where id=2;"""
    qt_notnull_3 """select agg_array_sum(c_array_tinyint) as sum_tinyint, agg_array_sum(c_array_smallint) as sum_smallint, agg_array_sum(c_array_int) as sum_int, agg_array_sum(c_array_bigint) as sum_bigint, 
        agg_array_sum(c_array_largeint) as sum_largeint, agg_array_sum(c_array_float) as sum_float, agg_array_sum(c_array_double) as sum_double, agg_array_sum(c_array_decimal) as sum_decimal 
        from agg_array_sum_test_not_null where id=3;"""
    qt_notnull_4 """select agg_array_sum(c_array_tinyint) as sum_tinyint, agg_array_sum(c_array_smallint) as sum_smallint, agg_array_sum(c_array_int) as sum_int, agg_array_sum(c_array_bigint) as sum_bigint, 
        agg_array_sum(c_array_largeint) as sum_largeint, agg_array_sum(c_array_float) as sum_float, agg_array_sum(c_array_double) as sum_double, agg_array_sum(c_array_decimal) as sum_decimal 
        from agg_array_sum_test_not_null where id=4;"""
    qt_notnull_groupby_1 """select id, agg_array_sum(c_array_tinyint) as sum_tinyint, agg_array_sum(c_array_smallint) as sum_smallint, agg_array_sum(c_array_int) as sum_int, agg_array_sum(c_array_bigint) as sum_bigint, 
        agg_array_sum(c_array_largeint) as sum_largeint, agg_array_sum(c_array_float) as sum_float, agg_array_sum(c_array_double) as sum_double, agg_array_sum(c_array_decimal) as sum_decimal 
        from agg_array_sum_test_not_null group by id order by id;"""
    
    expectException({
        sql """select agg_array_sum(c_array_string) as sum_string from agg_array_sum_test_not_null group by id order by id;"""
        },
        "errCode = 2, detailMessage = arr_array_sum requires a array of numeric parameter: agg_array_sum(c_array_string)"
    )
    sql 'DROP TABLE IF EXISTS `agg_array_sum_test_not_null`;'

    sql "DROP TABLE IF EXISTS `agg_array_sum_test_not_array`;"
    sql """
        CREATE TABLE `agg_array_sum_test_not_array` (
            `id` int(11) NULL COMMENT ""
            , `c_tinyint` tinyint NULL COMMENT ""
            , `c_smallint` smallint NULL COMMENT ""
            , `c_int` int NULL COMMENT ""
            , `c_bigint` bigint NULL COMMENT ""
            , `c_largeint` largeint NULL COMMENT ""
            , `c_float` float NULL COMMENT ""
            , `c_double` double NULL COMMENT ""
            , `c_decimal` decimal(10, 5) NULL COMMENT ""
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

    expectException({
        sql """select agg_array_sum(c_tinyint) as sum_tinyint from agg_array_sum_test_not_array group by id order by id;"""
        },
        "errCode = 2, detailMessage = arr_array_sum requires a array of numeric parameter: agg_array_sum(c_tinyint)"
    )
     expectException({
        sql """select agg_array_sum(c_smallint) as sum_smallint from agg_array_sum_test_not_array group by id order by id;"""
        },
        "errCode = 2, detailMessage = arr_array_sum requires a array of numeric parameter: agg_array_sum(c_smallint)"
    )
     expectException({
        sql """select agg_array_sum(c_int) as sum_int from agg_array_sum_test_not_array group by id order by id;"""
        },
        "errCode = 2, detailMessage = arr_array_sum requires a array of numeric parameter: agg_array_sum(c_int)"
    )
     expectException({
        sql """select agg_array_sum(c_bigint) as sum_bigint from agg_array_sum_test_not_array group by id order by id;"""
        },
        "errCode = 2, detailMessage = arr_array_sum requires a array of numeric parameter: agg_array_sum(c_bigint)"
    )
     expectException({
        sql """select agg_array_sum(c_largeint) as sum_largeint from agg_array_sum_test_not_array group by id order by id;"""
        },
        "errCode = 2, detailMessage = arr_array_sum requires a array of numeric parameter: agg_array_sum(c_largeint)"
    )
     expectException({
        sql """select agg_array_sum(c_float) as sum_float from agg_array_sum_test_not_array group by id order by id;"""
        },
        "errCode = 2, detailMessage = arr_array_sum requires a array of numeric parameter: agg_array_sum(c_float)"
    )
     expectException({
        sql """select agg_array_sum(c_double) as sum_double from agg_array_sum_test_not_array group by id order by id;"""
        },
        "errCode = 2, detailMessage = arr_array_sum requires a array of numeric parameter: agg_array_sum(c_double)"
    )
     expectException({
        sql """select agg_array_sum(c_decimal) as sum_decimal from agg_array_sum_test_not_array group by id order by id;"""
        },
        "errCode = 2, detailMessage = arr_array_sum requires a array of numeric parameter: agg_array_sum(c_decimal)"
    )
    sql "DROP TABLE IF EXISTS `agg_array_sum_test_not_array`;"

    sql 'DROP TABLE IF EXISTS `agg_array_sum_test_not_numeric`;'
    sql """
        CREATE TABLE `agg_array_sum_test_not_numeric` (
            `id` int(11) NULL COMMENT ""
            , `c_array_string` ARRAY<string> NULL COMMENT ""
            , `c_array_boolean` ARRAY<boolean> NULL COMMENT ""
            , `c_array_date` ARRAY<date> NULL COMMENT ""
            , `c_array_datetime` ARRAY<datetime> NULL COMMENT ""
            , `c_array_datev2` ARRAY<datev2> NULL COMMENT ""
            , `c_array_varchar` ARRAY<varchar(10)> NULL COMMENT ""
            , `c_array_char` ARRAY<char(10)> NULL COMMENT ""
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
     expectException({
        sql """select agg_array_sum(c_array_string) as sum_string from agg_array_sum_test_not_numeric group by id order by id;"""
        },
        "errCode = 2, detailMessage = arr_array_sum requires a array of numeric parameter: agg_array_sum(c_array_string)"
    )
     expectException({
        sql """select agg_array_sum(c_array_boolean) as sum_boolean from agg_array_sum_test_not_numeric group by id order by id;"""
        },
        "errCode = 2, detailMessage = arr_array_sum requires a array of numeric parameter: agg_array_sum(c_array_boolean)"
    )
     expectException({
        sql """select agg_array_sum(c_array_date) as sum_date from agg_array_sum_test_not_numeric group by id order by id;"""
        },
        "errCode = 2, detailMessage = arr_array_sum requires a array of numeric parameter: agg_array_sum(c_array_date)"
    )
     expectException({
        sql """select agg_array_sum(c_array_datetime) as sum_datetime from agg_array_sum_test_not_numeric group by id order by id;"""
        },
        "errCode = 2, detailMessage = arr_array_sum requires a array of numeric parameter: agg_array_sum(c_array_datetime)"
    )
     expectException({
        sql """select agg_array_sum(c_array_datev2) as sum_datev2 from agg_array_sum_test_not_numeric group by id order by id;"""
        },
        "errCode = 2, detailMessage = arr_array_sum requires a array of numeric parameter: agg_array_sum(c_array_datev2)"
    )
     expectException({
        sql """select agg_array_sum(c_array_varchar) as sum_varchar from agg_array_sum_test_not_numeric group by id order by id;"""
        },
        "errCode = 2, detailMessage = arr_array_sum requires a array of numeric parameter: agg_array_sum(c_array_varchar)"
    )
     expectException({
        sql """select agg_array_sum(c_array_char) as sum_char from agg_array_sum_test_not_numeric group by id order by id;"""
        },
        "errCode = 2, detailMessage = arr_array_sum requires a array of numeric parameter: agg_array_sum(c_array_char)"
    )
}
