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

    def query_sql = """select f1, f2, f1*f2 multi_col from test_decimal_mul_overflow_for_mv"""

    // turn on and create
    multi_sql """
    set enable_decimal256=true;
    drop materialized view  if exists mv_var_1;
    create materialized view mv_var_1
    BUILD IMMEDIATE
    REFRESH COMPLETE
    ON COMMIT
    PROPERTIES ('replication_num' = '1')
    as select f1, f2, f1*f2 multi_col from test_decimal_mul_overflow_for_mv;"""
    // turn off and refresh
    sql """set enable_decimal256=false;
    insert into test_decimal_mul_overflow_for_mv values(1.12345,1.234567);"""
    sql """
            REFRESH MATERIALIZED VIEW mv_var_1 auto
        """
    def db = context.config.getDbNameByFile(context.file)
    def job_name = getJobName(db, "mv_var_1");
    waitingMTMVTaskFinished(job_name)
    sql """sync;"""

    // expect scale is 11
    qt_refresh "select f1,f2,multi_col from mv_var_1 order by 1,2,3;"

    sql "set enable_decimal256=true;"
    explain {
        sql query_sql
        contains "mv_var_1 chose"
    }
    qt_rewite_open256 "$query_sql order by 1,2,3;"

    sql "set enable_decimal256=false;"
    explain {
        sql query_sql
        contains "mv_var_1 not chose"
    }
    qt_rewite_open128 "$query_sql order by 1,2,3;"
    sql "drop materialized view  if exists mv_var_1;"

    // test pre mv rewrite
    sql """
    set enable_decimal256=false;
    drop view if exists v_pre_mv_rewrite;
    create view v_pre_mv_rewrite as select f1,f2,f1*f2 multi from test_decimal_mul_overflow_for_mv;
    set enable_decimal256=true;"""
    sql "drop materialized view  if exists mv_pre_mv_rewrite;"
    sql """create materialized view mv_pre_mv_rewrite
    BUILD IMMEDIATE
    REFRESH COMPLETE
    ON COMMIT
    PROPERTIES ('replication_num' = '1')
    as select * from v_pre_mv_rewrite;"""
    sql """
            REFRESH MATERIALIZED VIEW mv_pre_mv_rewrite auto
        """
    def job_name2 = getJobName(db, "mv_pre_mv_rewrite");
    waitingMTMVTaskFinished(job_name2)
    sql """sync;"""

    sql """set enable_decimal256=true;
    drop view if EXISTS v_distinct_agg_rewrite;
    create view v_distinct_agg_rewrite as
    SELECT sum(f1*f2), count(distinct f1,f2) FROM test_decimal_mul_overflow_for_mv GROUP BY f2;
    set enable_decimal256=false;"""
    qt_pre_mv_rewrite "select /*+use_mv(mv_pre_mv_rewrite)*/ * from v_distinct_agg_rewrite order by 1,2;"
    explain {
        sql "select /*+use_mv(mv_pre_mv_rewrite)*/ * from v_distinct_agg_rewrite;"
        contains "mv_pre_mv_rewrite chose"
    }

    // test rewrite
    sql """set enable_decimal256=false;
    drop view if EXISTS v_test_agg_distinct_reverse_rewrite;
    create view v_test_agg_distinct_reverse_rewrite as select f1, sum(distinct f1*f2) col_sum from test_decimal_mul_overflow_for_mv group by f1;
    set enable_decimal256=true;"""
    qt_test_normalizeagg "select  /*+use_mv(mv_pre_mv_rewrite)*/ col_sum from v_test_agg_distinct_reverse_rewrite  order by 1;"
    explain {
        sql "select /*+use_mv(mv_pre_mv_rewrite)*/  col_sum from v_test_agg_distinct_reverse_rewrite;"
        contains "mv_pre_mv_rewrite chose"
    }
    sql "drop materialized view  if exists mv_pre_mv_rewrite;"

}