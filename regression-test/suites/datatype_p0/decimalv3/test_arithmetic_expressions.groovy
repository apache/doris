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

suite("test_arithmetic_expressions") {

    def table1 = "test_arithmetic_expressions"
    sql "set enable_decimal256 = false;"

    sql "drop table if exists ${table1}"

    sql """
    CREATE TABLE IF NOT EXISTS `${table1}` (
      `k1` decimalv3(38, 18) NULL COMMENT "",
      `k2` decimalv3(38, 18) NULL COMMENT "",
      `k3` decimalv3(38, 18) NULL COMMENT ""
    ) ENGINE=OLAP
    COMMENT "OLAP"
    DISTRIBUTED BY HASH(`k1`, `k2`, `k3`) BUCKETS 8
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "in_memory" = "false",
    "storage_format" = "V2"
    )
    """

    sql """insert into ${table1} values(1.1,1.2,1.3),
            (1.2,1.2,1.3),
            (1.5,1.2,1.3)
    """
    qt_select_all "select * from ${table1} order by k1"

    qt_select "select k1 * k2 from ${table1} order by k1"
    qt_select "select * from (select k1 * k2 from ${table1} union all select k3 from ${table1}) a order by 1"

    qt_select "select k1 * k2 * k3 from ${table1} order by k1"
    qt_select "select k1 * k2 * k3 * k1 * k2 * k3 from ${table1} order by k1"
    qt_select "select k1 * k2 / k3 * k1 * k2 * k3 from ${table1} order by k1"
    sql "drop table if exists ${table1}"

    sql """
        CREATE TABLE IF NOT EXISTS ${table1} (             `a` DECIMALV3(9, 3) NOT NULL, `b` DECIMALV3(9, 3) NOT NULL, `c` DECIMALV3(9, 3) NOT NULL, `d` DECIMALV3(9, 3) NOT NULL, `e` DECIMALV3(9, 3) NOT NULL, `f` DECIMALV3(9, 3) NOT
        NULL, `g` DECIMALV3(9, 3) NOT NULL , `h` DECIMALV3(9, 3) NOT NULL, `i` DECIMALV3(9, 3) NOT NULL, `j` DECIMALV3(9, 3) NOT NULL, `k` DECIMALV3(9, 3) NOT NULL)            DISTRIBUTED BY HASH(a) PROPERTIES("replication_num" = "1");
    """

    sql """
    insert into ${table1} values(999999.999,999999.999,999999.999,999999.999,999999.999,999999.999,999999.999,999999.999,999999.999,999999.999,999999.999);
    """
    qt_select_all "select * from ${table1} order by a"

    // TODO: test result is wrong, need to fix
    qt_select_mix_calc_0 "select a + b + c from ${table1};"
    qt_select_mix_calc_1 "select (a + b + c) * d from ${table1};"
    qt_select_mix_calc_2 "select (a + b + c) / d from ${table1};"
    qt_select_mix_calc_3 "select a + b + c + d + e + f + g + h + i + j + k from ${table1};"

    sql "drop table if exists ${table1}"

    def table2 = "test_arithmetic_expressions"

    sql "drop table if exists ${table2}"
    sql """ create table ${table2} (
            id smallint,
            fz decimal(27,9),
            fzv3 decimalv3(27,9),
            fm decimalv3(38,10))
            DISTRIBUTED BY HASH(`id`) BUCKETS auto
            PROPERTIES
            (
                "replication_num" = "1"
            ); """

    sql """ insert into ${table2} values (1,92594283.129196000,92594283.129196000,147202.0000000000); """
    sql """ insert into ${table2} values (2,107684988.257976000,107684988.257976000,148981.0000000000); """
    sql """ insert into ${table2} values (3,76891560.464178000,76891560.464178000,106161.0000000000); """
    sql """ insert into ${table2} values (4,277170831.851350000,277170831.851350000,402344.0000000000); """

    // pg 16:
    // select 92594283.129196000 / 147202.0000000000
    // 629.0287029333568837
    // MySQL 8.0:
    // select 92594283.129196000 / 147202.0000000000; 
    // 629.0287029333569
    qt_select_div_mix_v2_v3 """ select id, fz/fm as dec,fzv3/fm as decv3 from ${table2} ORDER BY id; """
    sql "drop table if exists ${table2}"

    def table3 = "test_mod_expressions"

    sql "drop table if exists ${table3}"
    sql """ create table ${table3} (
            id smallint,
            v1 decimalv3(27,9),
            v2 decimalv3(9,0),
            v3 int )
            DISTRIBUTED BY HASH(`id`) BUCKETS auto
            PROPERTIES
            (
                "replication_num" = "1"
            ); """

    sql """ insert into ${table3} values (1,92594283.129196000,1,1); """
    sql """ insert into ${table3} values (2,107684988.257976000,3,3); """
    sql """ insert into ${table3} values (3,76891560.464178000,5,5); """
    sql """ insert into ${table3} values (4,277170831.851350000,7,7); """

    qt_select_mod """ select v1, v2, v1 % v2, v1 % v3 from ${table3} ORDER BY id; """
    sql "drop table if exists ${table3}"

    // decimal64
    sql "DROP TABLE IF EXISTS `test_arithmetic_expressions_64`";
    sql """
    CREATE TABLE IF NOT EXISTS `test_arithmetic_expressions_64` (
      `k1` decimalv3(18, 6) NULL COMMENT "",
      `k2` decimalv3(18, 6) NULL COMMENT "",
      `k3` decimalv3(18, 6) NULL COMMENT ""
    ) ENGINE=OLAP
    COMMENT "OLAP"
    DISTRIBUTED BY HASH(`k1`, `k2`, `k3`) BUCKETS 8
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );
    """
    sql """insert into test_arithmetic_expressions_64 values(1, 999999999999.999999, 999999999999.999999),
            (2, 499999999999.999999, 499999999999.999999),
            (3, 333333333333.333333, 333333333333.333333),
            (4, 4, 4);"""
    sql "sync"
    // TODO: fix decimal cast
    // sql "select k3, CAST(k3 AS DECIMALV3(18, 10)) from test_arithmetic_expressions_64;"
/*
mysql [test]>select k3, CAST(k3 AS DECIMALV3(18, 10)) from test_arithmetic_expressions_64;
+---------------------+-------------------------------+
| k3                  | cast(k3 as DECIMALV3(18, 10)) |
+---------------------+-------------------------------+
| 333333333333.333333 |         -552734400.8095512496 |
| 499999999999.999999 |           93235602.4711502064 |
| 999999999999.999999 |          186471204.9423014128 |
|            4.000000 |                  4.0000000000 |
+---------------------+-------------------------------+
4 rows in set (0.39 sec)
*/

    // decimal128
    sql "DROP TABLE IF EXISTS `test_arithmetic_expressions_128_1`";
    sql """
    CREATE TABLE IF NOT EXISTS `test_arithmetic_expressions_128_1` (
      `k1` decimalv3(38, 6) NULL COMMENT "",
      `k2` decimalv3(38, 6) NULL COMMENT "",
      `k3` decimalv3(38, 6) NULL COMMENT ""
    ) ENGINE=OLAP
    COMMENT "OLAP"
    DISTRIBUTED BY HASH(`k1`, `k2`, `k3`) BUCKETS 8
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );
    """
    sql """insert into test_arithmetic_expressions_128_1 values
            (1, 99999999999999999999999999999999.999999, 99999999999999999999999999999999.999999),
            (2, 49999999999999999999999999999999.999999, 49999999999999999999999999999999.999999),
            (3, 33333333333333333333333333333333.333333, 33333333333333333333333333333333.333333),
            (4.444444, 2.222222, 3.333333);"""
    sql "sync"
    qt_decimal128_select_all "select * from test_arithmetic_expressions_128_1 order by k1, k2;"
    // qt_decimal128_cast "select k3, CAST(k3 AS DECIMALV3(38, 10)) from test_arithmetic_expressions_128_1 order by 1, 2;"
    // int128 multiply overflow
    qt_decimal128_multiply_0 "select k1 * k2 a from test_arithmetic_expressions_128_1 order by 1;"
    qt_decimal128_arith_union "select * from (select k1 * k2 from test_arithmetic_expressions_128_1 union all select k3 from test_arithmetic_expressions_128_1) a order by 1"
    qt_decimal128_multiply_1 "select k1 * k2 * k3 a from test_arithmetic_expressions_128_1 order by 1;"
    qt_decimal128_multiply_2 "select k1 * k2 * k3 * k1 * k2 * k3 from test_arithmetic_expressions_128_1 order by k1"
    qt_decimal128_multiply_div "select k1 * k2 / k3 * k1 * k2 * k3 from test_arithmetic_expressions_128_1 order by k1"

    sql "DROP TABLE IF EXISTS `test_arithmetic_expressions_128_2`";
    sql """
    CREATE TABLE IF NOT EXISTS test_arithmetic_expressions_128_2 (
        `a` DECIMALV3(38, 3) NOT NULL,
        `b` DECIMALV3(38, 3) NOT NULL,
        `c` DECIMALV3(38, 3) NOT NULL,
        `d` DECIMALV3(38, 3) NOT NULL,
        `e` DECIMALV3(38, 3) NOT NULL,
        `f` DECIMALV3(38, 3) NOT NULL,
        `g` DECIMALV3(38, 3) NOT NULL,
        `h` DECIMALV3(38, 3) NOT NULL,
        `i` DECIMALV3(38, 3) NOT NULL,
        `j` DECIMALV3(38, 3) NOT NULL,
        `k` DECIMALV3(38, 3) NOT NULL
    ) DISTRIBUTED BY HASH(a) PROPERTIES("replication_num" = "1");
    """

    sql """
    insert into test_arithmetic_expressions_128_2 values(999999.999,999999.999,999999.999,999999.999,999999.999,999999.999,999999.999,999999.999,999999.999,999999.999,999999.999);
    """
    sql "sync"
    qt_decimal128_select_all_2 "select * from test_arithmetic_expressions_128_2 order by a"
    qt_decimal128_mixed_calc_0 "select a + b + c from test_arithmetic_expressions_128_2;"
    qt_decimal128_mixed_calc_1 "select (a + b + c) * d from test_arithmetic_expressions_128_2;"
    qt_decimal128_mixed_calc_2 "select (a + b + c) / d from test_arithmetic_expressions_128_2;"
    qt_decimal128_mixed_calc_3 "select a + b + c + d + e + f + g + h + i + j + k from test_arithmetic_expressions_128_2;"

    sql "set enable_nereids_planner = true;"
    sql "set enable_decimal256 = true;"

    qt_decimal128_enable_decimal256_multiply_0 "select k1 * k2 a from test_arithmetic_expressions_128_1 order by 1;"
    qt_decimal128_enable_decimal256_arith_union "select * from (select k1 * k2 from test_arithmetic_expressions_128_1 union all select k3 from test_arithmetic_expressions_128_1) a order by 1"
    qt_decimal128_enable_decimal256_multiply_1 "select k1 * k2 * k3 from test_arithmetic_expressions_128_1 order by 1;"
    qt_decimal128_enable_decimal256_multiply_2 "select k1 * k2 * k3 * k1 * k2 * k3 from test_arithmetic_expressions_128_1 order by k1"
    qt_decimal128_enable_decimal256_multiply_div "select k1 * k2 / k3 * k1 * k2 * k3 from test_arithmetic_expressions_128_1 order by k1"

    qt_decimal128_enable_decimal256_mixed_calc_0 "select a + b + c from test_arithmetic_expressions_128_2;"
    qt_decimal128_enable_decimal256_mixed_calc_1 "select (a + b + c) * d from test_arithmetic_expressions_128_2;"
    qt_decimal128_enable_decimal256_mixed_calc_2 "select (a + b + c) / d from test_arithmetic_expressions_128_2;"
    qt_decimal128_enable_decimal256_mixed_calc_3 "select a + b + c + d + e + f + g + h + i + j + k from test_arithmetic_expressions_128_2;"

    qt_decimal128_cast256_cast "select k3, CAST(k3 AS DECIMALV3(76, 10)) from test_arithmetic_expressions_128_1 order by 1, 2;"
    qt_decimal128_cast256_calc_0 "select cast(k1 as decimalv3(76, 6)) + k2 a from test_arithmetic_expressions_128_1 order by 1;"
    qt_decimal128_cast256_calc_1 "select cast(k2 as decimalv3(76, 6)) - k1 a from test_arithmetic_expressions_128_1 order by 1;"
    qt_decimal128_cast256_calc_2 "select cast(k1 as decimalv3(76, 6)) * k2 a from test_arithmetic_expressions_128_1 order by 1;"
    qt_decimal128_cast256_calc_4 "select k2, k1, cast(k2 as decimalv3(76, 6)) / k1 a from test_arithmetic_expressions_128_1 order by 1, 2;"
    qt_decimal128_cast256_calc_5 "select k2, k1, cast(k2 as decimalv3(76, 6)) % k1 a from test_arithmetic_expressions_128_1 order by 1, 2;"

    qt_decimal128_cast256_calc_6 "select * from (select cast(k1 as decimalv3(76, 6)) * k2 from test_arithmetic_expressions_128_1 union all select k3 from test_arithmetic_expressions_128_1) a order by 1"
    // overflow
    qt_decimal128_cast256_calc_7 "select cast(k1 as decimalv3(76, 6)) * k2 * k3 a from test_arithmetic_expressions_128_1 order by 1;"
    qt_decimal128_cast256_calc_8 "select cast(k1 as decimalv3(76, 6)) * k2 * k3 * k1 * k2 * k3 from test_arithmetic_expressions_128_1 order by 1"
    // qt_decimal128_cast256_calc_9 "select cast(k1 as decimalv3(76, 6)) * k2 / k3 * k1 * k2 * k3 from test_arithmetic_expressions_128_1 order by 1"

    qt_decimal128_cast256_mixed_calc_0 "select cast(a as decimalv3(39, 4)) + b + c from test_arithmetic_expressions_128_2 order by 1;"
    qt_decimal128_cast256_mixed_calc_1 "select cast((a + b + c) as decimalv3(39, 4)) * d from test_arithmetic_expressions_128_2 order by 1;"
    qt_decimal128_cast256_mixed_calc_2 "select cast((a + b + c) as decimalv3(39, 4)) / d from test_arithmetic_expressions_128_2 order by 1;"
    qt_decimal128_cast256_mixed_calc_3 "select cast(a as decimalv3(39, 4)) + b + c + d + e + f + g + h + i + j + k from test_arithmetic_expressions_128_2 order by 1;"

    qt_decimal128_cast256_mixed_calc_4 "select cast(a as decimalv3(76, 6)) + b + c from test_arithmetic_expressions_128_2 order by 1;"
    qt_decimal128_cast256_mixed_calc_5 "select cast((a + b + c) as decimalv3(76, 6)) * d from test_arithmetic_expressions_128_2 order by 1;"
    qt_decimal128_cast256_mixed_calc_6 "select cast((a + b + c) as decimalv3(76, 6)) / d from test_arithmetic_expressions_128_2 order by 1;"
    qt_decimal128_cast256_mixed_calc_7 "select cast(a as decimalv3(76, 6)) + b + c + d + e + f + g + h + i + j + k from test_arithmetic_expressions_128_2 order by 1;"

/*
mysql [test]>select k3, CAST(k3 AS DECIMALV3(38, 10)) from test_arithmetic_expressions_128_1;
+-----------------------------------------+------------------------------------------+
| k3                                      | cast(k3 as DECIMALV3(38, 10))            |
+-----------------------------------------+------------------------------------------+
| 33333333333333333333333333333333.333333 | -9999999999999999999999999999.9999999999 |
| 99999999999999999999999999999999.999999 | -9999999999999999999999999999.9999999999 |
| 49999999999999999999999999999999.999999 |  9999999999999999999999999999.9999999999 |
|                                4.000000 |                             4.0000000000 |
+-----------------------------------------+------------------------------------------+
4 rows in set (0.07 sec)
*/

    // decimal256
    /*
    mysql [regression_test_datatype_p0_decimalv3]>select CAST(k3 AS DECIMALV3(76, 19)) from test_arithmetic_expressions_256_0;
+---------------------------------------------------------------------------------+
| cast(k3 as DECIMALV3(76, 19))                                                   |
+---------------------------------------------------------------------------------+
|  3213777273360060490676974488410532053153505213067024283781.6774441511766597376 |
| -1717218125670499520334383174682575559673329346810002612127.4678374963165481728 |
| -5151654377011498561003149524047726679019988040430007836382.4035124889496445184 |
+---------------------------------------------------------------------------------+
    */
    sql "DROP TABLE IF EXISTS `test_arithmetic_expressions_256_1`"
    sql """
    CREATE TABLE IF NOT EXISTS `test_arithmetic_expressions_256_1` (
      `k1` decimalv3(76, 9) NULL COMMENT "",
      `k2` decimalv3(76, 10) NULL COMMENT "",
      `k3` decimalv3(76, 11) NULL COMMENT ""
    ) ENGINE=OLAP
    DUPLICATE KEY(`k1`)
    DISTRIBUTED BY HASH(`k1`) BUCKETS 8
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );
    """

    sql """insert into test_arithmetic_expressions_256_1 values
            (1, 999999999999999999999999999999999999999999999999999999999999999999.9999999999, 99999999999999999999999999999999999999999999999999999999999999999.99999999999),
            (2, 499999999999999999999999999999999999999999999999999999999999999999.9999999999, 49999999999999999999999999999999999999999999999999999999999999999.99999999999),
            (3, 333333333333333333333333333333333333333333333333333333333333333333.3333333333, 33333333333333333333333333333333333333333333333333333333333333333.33333333333);"""
    sql "sync"
    qt_decimal256_arith_select_all "select * from test_arithmetic_expressions_256_1 order by k1, k2, k3;"
    qt_decimal256_arith_plus "select k1 + k2 from test_arithmetic_expressions_256_1 order by 1;"
    qt_decimal256_arith_minus "select k2 - k1 from test_arithmetic_expressions_256_1 order by 1;"
    qt_decimal256_arith_multiply "select k1 * k2 from test_arithmetic_expressions_256_1 order by 1;"
    qt_decimal256_arith_div "select k2 / k1 from test_arithmetic_expressions_256_1 order by 1;"
    qt_decimal256_arith_union "select * from (select k1 * k2 from test_arithmetic_expressions_256_1 union all select k3 from test_arithmetic_expressions_256_1) a order by 1"

    qt_decimal256_multiply_1 "select k1 * k2 * k3 a from test_arithmetic_expressions_256_1 order by 1;"
    qt_decimal256_multiply_2 "select k1 * k2 * k3 * k1 * k2 * k3 from test_arithmetic_expressions_256_1 order by k1"
    qt_decimal256_multiply_div "select k1 * k2 / k3 * k1 * k2 * k3 from test_arithmetic_expressions_256_1 order by k1"

    qt_decimal256_arith_multiply_const "select k1 * 2.0 from test_arithmetic_expressions_256_1 order by 1;"

    sql "DROP TABLE IF EXISTS `test_arithmetic_expressions_256_2`";
    sql """
    CREATE TABLE IF NOT EXISTS test_arithmetic_expressions_256_2 (
        `a` DECIMALV3(76, 3) NOT NULL,
        `b` DECIMALV3(76, 3) NOT NULL,
        `c` DECIMALV3(76, 3) NOT NULL,
        `d` DECIMALV3(76, 3) NOT NULL,
        `e` DECIMALV3(76, 3) NOT NULL,
        `f` DECIMALV3(76, 3) NOT NULL,
        `g` DECIMALV3(76, 3) NOT NULL,
        `h` DECIMALV3(76, 3) NOT NULL,
        `i` DECIMALV3(76, 3) NOT NULL,
        `j` DECIMALV3(76, 3) NOT NULL,
        `k` DECIMALV3(76, 3) NOT NULL
    ) DISTRIBUTED BY HASH(a) PROPERTIES("replication_num" = "1");
    """

    sql """
    insert into test_arithmetic_expressions_256_2 values(999999.999,999999.999,999999.999,999999.999,999999.999,999999.999,999999.999,999999.999,999999.999,999999.999,999999.999);
    """
    sql "sync"
    qt_decimal256_select_all_2 "select * from test_arithmetic_expressions_256_2 order by a"

    qt_decimal256_mixed_calc_0 "select a + b + c from test_arithmetic_expressions_256_2;"
    qt_decimal256_mixed_calc_1 "select (a + b + c) * d from test_arithmetic_expressions_256_2;"
    qt_decimal256_mixed_calc_2 "select (a + b + c) / d from test_arithmetic_expressions_256_2;"
    qt_decimal256_mixed_calc_3 "select a + b + c + d + e + f + g + h + i + j + k from test_arithmetic_expressions_256_2;"

    sql "DROP TABLE IF EXISTS `test_arithmetic_expressions_256_3`"
    sql """
    CREATE TABLE IF NOT EXISTS `test_arithmetic_expressions_256_3` (
      `k1` decimalv3(76, 0) NULL COMMENT "",
      `k2` decimalv3(76, 1) NULL COMMENT "",
      `k3` decimalv3(76, 2) NULL COMMENT ""
    ) ENGINE=OLAP
    DISTRIBUTED BY HASH(`k1`, `k2`, `k3`) BUCKETS 8
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );
    """
    sql """insert into test_arithmetic_expressions_256_3 values(1, 999999999999999999999999999999999999999999999999999999999999999999999999999.9, 99999999999999999999999999999999999999999999999999999999999999999999999999.99),
            (2, 499999999999999999999999999999999999999999999999999999999999999999999999999.9, 49999999999999999999999999999999999999999999999999999999999999999999999999.99),
            (3, 333333333333333333333333333333333333333333333333333333333333333333333333333.3, 33333333333333333333333333333333333333333333333333333333333333333333333333.33);"""
    sql "sync"
    qt_decimal256_arith_3 "select k1, k2, k1 * k2 a from test_arithmetic_expressions_256_3 order by k1, k2;"

    sql "DROP TABLE IF EXISTS `test_arithmetic_expressions_256_4`"
    sql """ create table test_arithmetic_expressions_256_4 (
            id smallint,
            fz decimal(27,9),
            fzv3 decimalv3(76,9),
            fm decimalv3(76,10))
            DISTRIBUTED BY HASH(`id`) BUCKETS auto
            PROPERTIES
            (
                "replication_num" = "1"
            ); """

    sql """ insert into test_arithmetic_expressions_256_4 values (1,92594283.129196000,92594283.129196000,147202.0000000000); """
    sql """ insert into test_arithmetic_expressions_256_4 values (2,107684988.257976000,107684988.257976000,148981.0000000000); """
    sql """ insert into test_arithmetic_expressions_256_4 values (3,76891560.464178000,76891560.464178000,106161.0000000000); """
    sql """ insert into test_arithmetic_expressions_256_4 values (4,277170831.851350000,277170831.851350000,402344.0000000000); """
    sql "sync"

    qt_decimal256_div_v2_v3 """ select id, fz/fm as dec,fzv3/fm as decv3 from test_arithmetic_expressions_256_4 ORDER BY id; """

    sql "drop table if exists test_arithmetic_expressions_256_5"
    sql """ create table test_arithmetic_expressions_256_5 (
            id smallint,
            v1 decimalv3(27,9),
            v2 decimalv3(9,0),
            v3 int )
            DISTRIBUTED BY HASH(`id`) BUCKETS auto
            PROPERTIES
            (
                "replication_num" = "1"
            ); """

    sql """ insert into test_arithmetic_expressions_256_5 values (1,92594283.129196000,1,1); """
    sql """ insert into test_arithmetic_expressions_256_5 values (2,107684988.257976000,3,3); """
    sql """ insert into test_arithmetic_expressions_256_5 values (3,76891560.464178000,5,5); """
    sql """ insert into test_arithmetic_expressions_256_5 values (4,277170831.851350000,7,7); """
    sql "sync"

    qt_decimal256_mod """ select v1, v2, v1 % v2, v1 % v3 from test_arithmetic_expressions_256_5 ORDER BY id; """

}
