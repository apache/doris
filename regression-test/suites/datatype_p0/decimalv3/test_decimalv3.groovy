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

suite("test_decimalv3") {
    def db = "test_decimalv3_db"
    sql "CREATE DATABASE IF NOT EXISTS ${db}"
    sql "use ${db}"
    sql "drop table if exists test5"
    sql '''CREATE  TABLE test5 (   `a` decimalv3(38,18),   `b` decimalv3(38,18) ) ENGINE=OLAP DUPLICATE KEY(`a`) COMMENT 'OLAP' DISTRIBUTED BY HASH(`a`) BUCKETS 1 PROPERTIES ( "replication_allocation" = "tag.location.default: 1" ) '''
    sql "insert into test5 values(50,2)"
    sql "drop view if exists test5_v"
    sql "create view test5_v (amout) as select cast(a*b as decimalv3(38,18)) from test5"

    qt_decimalv3 "select * from test5_v"
    qt_decimalv3 "select cast(a as decimalv3(12,10)) * cast(b as decimalv3(18,10)) from test5"
    qt_decimalv3_view1 "select count(*) from test5_v;"

    sql "drop view if exists test5_v2"
    sql "create view test5_v2 (amout) as select cast(a as decimalv3(18,6)) from test5"
    qt_decimalv3_view2 "select count(*) from test5_v2;"

    /*
    sql "drop table if exists test_decimal256;"
    sql """ create table test_decimal256(k1 decimal(76, 6), v1 decimal(76, 6))
                DUPLICATE KEY(`k1`, `v1`)
                DISTRIBUTED BY HASH(`k1`) BUCKETS 10
                properties("replication_num" = "1"); """
    sql """insert into test_decimal256 values(1, 9999999999999999999999999999999999999999999999999999999999999999999999.999999),
            (2, 4999999999999999999999999999999999999999999999999999999999999999999999.999999);"""
    qt_decimalv3_0 "select * from test_decimal256 order by k1, v1; "
    qt_decimalv3_1 "select * from test_decimal256 where v1 = 9999999999999999999999999999999999999999999999999999999999999999999999.999999 order by k1, v1; "
    qt_decimalv3_2 "select * from test_decimal256 where v1 != 9999999999999999999999999999999999999999999999999999999999999999999999.999999 order by k1, v1; "
    qt_decimalv3_3 "select * from test_decimal256 where v1 > 4999999999999999999999999999999999999999999999999999999999999999999999.999999 order by k1, v1; "
    qt_decimalv3_4 "select * from test_decimal256 where v1 >= 4999999999999999999999999999999999999999999999999999999999999999999999.999999 order by k1, v1; "
    qt_decimalv3_5 "select * from test_decimal256 where v1 < 9999999999999999999999999999999999999999999999999999999999999999999999.999999 order by k1, v1; "
    qt_decimalv3_6 "select * from test_decimal256 where v1 <= 9999999999999999999999999999999999999999999999999999999999999999999999.999999 order by k1, v1; "
	*/

    sql "set experimental_enable_nereids_planner =false;"
    qt_aEb_test1 "select 0e0;"
    qt_aEb_test2 "select 1e-1"
    qt_aEb_test3 "select -1e-2"
    qt_aEb_test4 "select 10.123456e10;"
    qt_aEb_test5 "select 123456789e-10"
    qt_aEb_test6 "select 0.123445e10;"

    sql "set enable_nereids_planner = true;"
    sql "set enable_decimal256 = true;"
    qt_decimal256_cast_0 """ select cast("999999.999999" as decimal(76,6));"""
    qt_decimal256_cast_1 """select cast("9999999999999999999999999999999999999999999999999999999999999999999999.999999" as decimal(76,6));"""

    // test const

    // nereids
    sql "set enable_nereids_planner = true;"

    sql """ set enable_fallback_to_original_planner=false """
    sql "set enable_decimal256 = true;"
    qt_decimal256_const_0 "select 1.4E-45;"
    qt_decimal256_const_1 "select 1.4E-80;"
    sql "set enable_decimal256 = false;"
    qt_decimal256_const_2 "select 1.4E-45;"
    qt_decimal256_const_3 "select 1.4E-80;"

    sql """ set enable_fallback_to_original_planner=true """
    sql "set enable_decimal256 = true;"
    qt_decimal256_const_4 "select 1.4E-45;"
    qt_decimal256_const_5 "select 1.4E-80;"
    sql "set enable_decimal256 = false;"
    qt_decimal256_const_6 "select 1.4E-45;"
    qt_decimal256_const_7 "select 1.4E-80;"

    // not nereids
    sql "set enable_nereids_planner = false;"
    sql "set enable_decimal256 = true;"
    qt_decimal256_const_8 "select 1.4E-45;"
    qt_decimal256_const_9 "select 1.4E-80;"
    sql "set enable_decimal256 = false;"
    qt_decimal256_const_10 "select 1.4E-45;"
    qt_decimal256_const_11 "select 1.4E-80;"

    sql "set enable_nereids_planner = true;"
    sql "set enable_decimal256 = true;"
    sql "drop table if exists test_decimal256_cast_str;"
    sql """ create table test_decimal256_cast_str(k1 int, v1 char(128))
                DUPLICATE KEY(`k1`, `v1`)
                DISTRIBUTED BY HASH(`k1`) BUCKETS 10
                properties("replication_num" = "1"); """
    sql """ insert into test_decimal256_cast_str values
        (1, "9999999999999999999999999999999999999999999999999999999999999999999999.999999"),
        (2, "-9999999999999999999999999999999999999999999999999999999999999999999999.999999"),
        (3, "0.999999"),
        (4, "-0.999999")
    """
    sql "sync"
    qt_decimal256_cast_from_str_0 """ select k1, v1, cast(v1 as decimalv3(76, 6)) from test_decimal256_cast_str order by 1, 2, 3; """

    sql "drop table if exists test_decimal256_cast_dec;"
    sql """ create table test_decimal256_cast_dec(k1 decimal(38, 6), v1 decimal(38, 6))
                DUPLICATE KEY(`k1`, `v1`)
                DISTRIBUTED BY HASH(`k1`) BUCKETS 10
                properties("replication_num" = "1"); """
    sql """insert into test_decimal256_cast_dec values
            (1, 99999999999999999999999999999999.999999),
            (2, -99999999999999999999999999999999.999999),
            (3, 1234567890.123456),
            (4, -1234567890.123456);
    """
    qt_decimal256_cast_dec_0 """ select k1, v1, cast(v1 as decimalv3(76, 6)) from test_decimal256_cast_dec order by 1, 2, 3; """
    qt_decimal256_cast_dec_1 """ select k1, v1, cast(v1 as decimalv3(76, 10)) from test_decimal256_cast_dec order by 1, 2, 3; """
    qt_decimal256_cast_dec_2 """ select k1, v1, cast( cast(v1 as decimalv3(76, 6)) as decimalv3(38, 6) ) from test_decimal256_cast_dec order by 1, 2, 3; """

}
