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

suite("test_sync_mv") {
    // =============case1: history data to mv===================
    multi_sql """
    drop table if exists test_decimal_mul_overflow_for_sync_mv;
    CREATE TABLE `test_decimal_mul_overflow_for_sync_mv` (
        `f1` decimal(20,5) NULL,
        `f2` decimal(21,6) NULL
    )DISTRIBUTED BY HASH(f1)
    PROPERTIES("replication_num" = "1");
    insert into test_decimal_mul_overflow_for_sync_mv values(999999999999999.12345,999999999999999.123456);"""

    sql """
    set enable_decimal256=true;
    drop materialized view if exists mv_var_sync_1 on test_decimal_mul_overflow_for_sync_mv;
    """
    createMV("""create materialized view mv_var_sync_1
            as select f1 as c1, f2 as c2, f1*f2 multi_col from test_decimal_mul_overflow_for_sync_mv;""")

    // test refresh
    sql "set enable_decimal256=true;"
    // expect scale is 11
    qt_history_data_mv "select f1, f2, f1*f2 multi_col from test_decimal_mul_overflow_for_sync_mv;"
    // test rewrite
    sql "set enable_decimal256=true;"
    explain {
        sql "select f1, f2, f1*f2 multi_col from test_decimal_mul_overflow_for_sync_mv;"
        contains "mv_var_sync_1 chose"
    }
    sql "set enable_decimal256=false;"
//    explain {
//        sql "select f1, f2, f1*f2 multi_col from test_decimal_mul_overflow_for_sync_mv;"
//        contains "mv_var_sync_1 not chose"
//    }

    // new insert data refresh
    sql "insert into test_decimal_mul_overflow_for_sync_mv values(999999999999999.12345,999999999999999.123456);"
    sql "set enable_decimal256=true;"
    // expect scale is 11
    qt_insert_refresh_mv "select f1, f2, f1*f2 multi_col from test_decimal_mul_overflow_for_sync_mv;"

    // =============case2: new insert data add to mv and mv where condition also need guard===================
    multi_sql """
    drop table if exists test_decimal_mul_overflow_for_sync_mv;
    CREATE TABLE `test_decimal_mul_overflow_for_sync_mv` (
    `f1` decimal(20,5) NULL,
    `f2` decimal(21,6) NULL
    )DISTRIBUTED BY HASH(f1)
    PROPERTIES("replication_num" = "1");
    insert into test_decimal_mul_overflow_for_sync_mv values(999999999999999.12345,999999999999999.123456);
    """

    sql """set enable_decimal256=true;
    drop materialized view if exists mv_var_sync_1 on test_decimal_mul_overflow_for_sync_mv;"""
    createMV("""create materialized view mv_var_sync_1
            as select f1 as c1, f2 as c2, f1*f2 multi_col from test_decimal_mul_overflow_for_sync_mv
            where f1*f2==999999999999998246906000000000.76833464320;""")
    // turn off 256 and insert，expect can insert this row
    // because if not guard where, this row will not be inserted into mv
    // because in 128 mode f1*f2==999999999999998246906000000000.76833464320 return false
    // in 256 mode f1*f2==999999999999998246906000000000.76833464320 return true
    sql "set enable_decimal256=false;"
    sql "insert into test_decimal_mul_overflow_for_sync_mv values(999999999999999.12345,999999999999999.123456);"

    // expect 2 rows
    sql "set enable_decimal256=true;"
    qt_where_mv "select f1 as c1, f2 as c2, f1*f2 multi_col from test_decimal_mul_overflow_for_sync_mv where f1*f2==999999999999998246906000000000.76833464320;"
    explain {
        sql "select f1 as c1, f2 as c2, f1*f2 multi_col from test_decimal_mul_overflow_for_sync_mv where f1*f2==999999999999998246906000000000.76833464320;"
        contains "mv_var_sync_1 chose"
    }

    // ===================case3: create mv with 128 mode,test history refresh mv and insert into refresh mv=====================
    sql """drop table if exists test_decimal_mul_overflow_for_sync_mv;
    CREATE TABLE `test_decimal_mul_overflow_for_sync_mv` (
    `f1` decimal(20,5) NULL,
    `f2` decimal(21,6) NULL
    )DISTRIBUTED BY HASH(f1)
    PROPERTIES("replication_num" = "1");
    insert into test_decimal_mul_overflow_for_sync_mv values(999999999999999.12345,999999999999999.123456);"""

    sql "set enable_decimal256=false;"
    sql "drop materialized view if exists mv_var_sync_1 on test_decimal_mul_overflow_for_sync_mv;"

    createMV("""create materialized view mv_var_sync_1
            as select f1 as c1, f2 as c2, f1*f2 multi_col from test_decimal_mul_overflow_for_sync_mv;""")
    sql "set enable_decimal256=true;"
    sql "insert into test_decimal_mul_overflow_for_sync_mv values(999999999999999.12345,999999999999999.123456);"

    sql "set enable_decimal256=false;"
    qt_expect_8_scale "select f1, f2, f1*f2 multi_col from test_decimal_mul_overflow_for_sync_mv;"
    explain {
        sql "select f1, f2, f1*f2 multi_col from test_decimal_mul_overflow_for_sync_mv;"
        contains "mv_var_sync_1 chose"
    }
    sql "set enable_decimal256=true;"
    explain {
        sql "select f1, f2, f1*f2 multi_col from test_decimal_mul_overflow_for_sync_mv;"
        contains "mv_var_sync_1 not chose"
    }
}