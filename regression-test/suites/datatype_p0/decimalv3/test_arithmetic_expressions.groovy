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

    sql "set check_overflow_for_decimal=true;"
    def table1 = "test_arithmetic_expressions"

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

    qt_select "select a + b + c from ${table1};"
    qt_select "select (a + b + c) * d from ${table1};"
    qt_select "select (a + b + c) / d from ${table1};"
    qt_select "select a + b + c + d + e + f + g + h + i + j + k from ${table1};"
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

    qt_select """ select id, fz/fm as dec,fzv3/fm as decv3 from ${table2} ORDER BY id; """
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

    qt_select """ select v1, v2, v1 % v2, v1 % v3 from ${table3} ORDER BY id; """
    sql "drop table if exists ${table3}"

    // decimal128
    sql "DROP TABLE IF EXISTS `test_arithmetic_expressions_128_1`";
    sql """
    CREATE TABLE test_arithmetic_expressions_128_1 (
      k1 decimalv3(38, 6) NULL,
      k2 decimalv3(38, 6) NULL,
      k3 decimalv3(38, 6) NULL
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

    test {
        sql """
            select k1 * k2 * k3 a from test_arithmetic_expressions_128_1 order by 1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
            select k1 * k2 * k3 * k1 * k2 * k3 from test_arithmetic_expressions_128_1 order by k1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
            select k1 * k2 / k3 * k1 * k2 * k3 from test_arithmetic_expressions_128_1 order by k1;
        """
        exception "Arithmetic overflow"
    }

    sql "DROP TABLE IF EXISTS `test_arithmetic_expressions_128_2`";
    sql """
    CREATE TABLE test_arithmetic_expressions_128_2 (
        a DECIMALV3(38, 3) NOT NULL,
        b DECIMALV3(38, 3) NOT NULL,
        c DECIMALV3(38, 3) NOT NULL,
        d DECIMALV3(38, 3) NOT NULL,
        e DECIMALV3(38, 3) NOT NULL,
        f DECIMALV3(38, 3) NOT NULL,
        g DECIMALV3(38, 3) NOT NULL,
        h DECIMALV3(38, 3) NOT NULL,
        i DECIMALV3(38, 3) NOT NULL,
        j DECIMALV3(38, 3) NOT NULL,
        k DECIMALV3(38, 3) NOT NULL
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

}
