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

suite("test_mtmv") {
    multi_sql """
    drop table if exists test_decimal_mul_overflow_for_mv;
    CREATE TABLE `test_decimal_mul_overflow_for_mv` (
        `f1` decimal(20,5) NULL,
        `f2` decimal(21,6) NULL
    )DISTRIBUTED BY HASH(f1)
    PROPERTIES("replication_num" = "1");
    insert into test_decimal_mul_overflow_for_mv values(999999999999999.12345,999999999999999.123456);"""

    def query_sql = """select f1, f2, f1*f2 multi_col from test_decimal_mul_overflow_for_mv;"""

    //打开256创建物化视图
    multi_sql """
    set enable_decimal256=true;
    drop materialized view  if exists mv_var_1;
    create materialized view mv_var_1
    BUILD IMMEDIATE
    REFRESH COMPLETE
    ON COMMIT
    PROPERTIES ('replication_num' = '1')
    as select f1, f2, f1*f2 multi_col from test_decimal_mul_overflow_for_mv;"""
    // 关闭256进行刷新
    sql """set enable_decimal256=false;
    insert into test_decimal_mul_overflow_for_mv values(1.12345,1.234567);"""

    def db = context.config.getDbNameByFile(context.file)
    def job_name = getJobName(db, "mv_var_1");
    waitingMTMVTaskFinished(job_name)
    sql """sync;"""

    // 预期multi_col的scale是11
    qt_refresh "select f1,f2,multi_col from mv_var_1 order by 1,2,3;"

    // 测试改写
    sql "set enable_decimal256=true;"
    explain {
        sql query_sql
        contains "mv_var_1 chose"
    }
    qt_rewite_open256 "$query_sql"

    sql "set enable_decimal256=false;"
    explain {
        sql query_sql
        contains "mv_var_1 not chose"
    }
    qt_rewite_open128 "$query_sql"
}