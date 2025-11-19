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
    // =============case1: 历史数据转换===================
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

    // 测试刷新
    sql "set enable_decimal256=true;"
    // 预期查询出来的有11个scale
    qt_history_data_mv "select f1, f2, f1*f2 multi_col from test_decimal_mul_overflow_for_sync_mv;"

    // 测试改写
    sql "set enable_decimal256=true;"
    explain {
        sql "select f1, f2, f1*f2 multi_col from test_decimal_mul_overflow_for_sync_mv;"
        contains "mv_var_sync_1 chose"
    }
    sql "set enable_decimal256=false;"
    explain {
        sql "select f1, f2, f1*f2 multi_col from test_decimal_mul_overflow_for_sync_mv;"
        contains "mv_var_sync_1 not chose"
    }

    // 新写入数据的转换
    sql "insert into test_decimal_mul_overflow_for_sync_mv values(999999999999999.12345,999999999999999.123456);"
    // 测试新写入数据的刷新
    sql "set enable_decimal256=true;"
    // 预期查询出来的有11个scale
    qt_insert_refresh_mv "select f1, f2, f1*f2 multi_col from test_decimal_mul_overflow_for_sync_mv;"

    // =============case2: 增量数据转换和物化视图的where条件也需要被guard===================
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
    //关闭256的时候插入数据，预期应该能把这一行插入到mv中
    //因为如果没有把where条件中的expr加guard，那么这一行不会插入到mv中
    // 因为在128的情况下f1*f2==999999999999998246906000000000.76833464320返回false，
    // 但是在256的情况下f1*f2==999999999999998246906000000000.76833464320返回true.
    sql "set enable_decimal256=false;"
    sql "insert into test_decimal_mul_overflow_for_sync_mv values(999999999999999.12345,999999999999999.123456);"

    // 查询 预期结果为2行。
    sql "set enable_decimal256=true;"
    qt_where_mv "select f1 as c1, f2 as c2, f1*f2 multi_col from test_decimal_mul_overflow_for_sync_mv where f1*f2==999999999999998246906000000000.76833464320;"
    explain {
        sql "select f1 as c1, f2 as c2, f1*f2 multi_col from test_decimal_mul_overflow_for_sync_mv where f1*f2==999999999999998246906000000000.76833464320;"
        contains "mv_var_sync_1 chose"
    }

    // ===================case3: 测试创建物化视图时为128，测试物化视图历史刷新和增量刷新（256打开进行增量刷新）=====================
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
    // 新写入数据的转换
    sql "insert into test_decimal_mul_overflow_for_sync_mv values(999999999999999.12345,999999999999999.123456);"
    sql "set enable_decimal256=false;"
    // 预期查询出来的有8个scale
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