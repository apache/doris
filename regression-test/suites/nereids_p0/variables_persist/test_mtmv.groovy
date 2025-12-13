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

    multi_sql """drop table if exists test_decimal_mul_overflow_for_mv2;
    CREATE TABLE `test_decimal_mul_overflow_for_mv2` (
    `f1` decimal(20,5) NULL,
    `f2` decimal(21,6) NULL
    )DISTRIBUTED BY HASH(f1)
    PROPERTIES("replication_num" = "1");
    insert into test_decimal_mul_overflow_for_mv2 values(999999999999999.12345,999999999999999.123456);"""


    def query_sql = """select f1, f2, f1*f2 multi_col from test_decimal_mul_overflow_for_mv"""

    // turn on and create
    sql "set enable_decimal256=true;"
    multi_sql """
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
    sql "analyze table test_decimal_mul_overflow_for_mv with sync;"
    sql "analyze table test_decimal_mul_overflow_for_mv2 with sync;"

    sql """
            REFRESH MATERIALIZED VIEW mv_var_1 auto
        """
    def db = context.config.getDbNameByFile(context.file)
    def job_name = getJobName(db, "mv_var_1");
    waitingMTMVTaskFinished(job_name)
    sql """sync;"""
    sql "analyze table mv_var_1 with sync"
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

    // ********************** test pre mv rewrite *****************************
    sql "set enable_decimal256=false;"
    sql """
    drop view if exists v_pre_mv_rewrite;
    create view v_pre_mv_rewrite as select f1,f2,f1*f2 multi from test_decimal_mul_overflow_for_mv;
    """
    sql "set enable_decimal256=true;"
    sql "drop materialized view  if exists mv_pre_mv_rewrite;"
    sql """create materialized view mv_pre_mv_rewrite
    BUILD IMMEDIATE
    REFRESH COMPLETE
    ON COMMIT
    PROPERTIES ('replication_num' = '1')
    as select * from v_pre_mv_rewrite;"""
    sql """REFRESH MATERIALIZED VIEW mv_pre_mv_rewrite auto"""
    def job_name2 = getJobName(db, "mv_pre_mv_rewrite");
    waitingMTMVTaskFinished(job_name2)
    sql "sync;"
    sql "analyze table mv_pre_mv_rewrite with sync;"
    sql """set enable_decimal256=true;
    drop view if EXISTS v_distinct_agg_rewrite;
    create view v_distinct_agg_rewrite as
    SELECT sum(f1*f2), count(distinct f1,f2) FROM test_decimal_mul_overflow_for_mv GROUP BY f2;
    """
    sql "set enable_decimal256=false;"
    qt_pre_mv_rewrite "select /*+use_mv(mv_pre_mv_rewrite)*/ * from v_distinct_agg_rewrite order by 1,2;"
    explain {
        sql "select /*+use_mv(mv_pre_mv_rewrite)*/ * from v_distinct_agg_rewrite;"
        contains "mv_pre_mv_rewrite chose"
    }

    // test rewrite
    sql "set enable_decimal256=false;"
    sql """
    drop view if EXISTS v_test_agg_distinct_reverse_rewrite;
    create view v_test_agg_distinct_reverse_rewrite as select f1, sum(distinct f1*f2) col_sum from test_decimal_mul_overflow_for_mv group by f1;
    """
    sql "set enable_decimal256=true;"
    qt_test_normalizeagg "select  /*+use_mv(mv_pre_mv_rewrite)*/ col_sum from v_test_agg_distinct_reverse_rewrite  order by 1;"
    mv_rewrite_success_without_check_chosen("select /*+use_mv(mv_pre_mv_rewrite)*/  col_sum from v_test_agg_distinct_reverse_rewrite;", "mv_pre_mv_rewrite")
    sql "drop materialized view  if exists mv_pre_mv_rewrite;"


    // ********************** test predicate rewrite *****************************
    sql "set enable_decimal256=true;"
    sql """
    drop materialized view  if exists where_mv;
    create materialized view where_mv
    BUILD IMMEDIATE
    REFRESH COMPLETE
    ON COMMIT
    PROPERTIES ('replication_num' = '1')
    as  select t1.f1*t1.f2, t1.f1, t1.f2 from test_decimal_mul_overflow_for_mv t1;"""

    sql """REFRESH MATERIALIZED VIEW where_mv auto"""
    job_name2 = getJobName(db, "where_mv");
    waitingMTMVTaskFinished(job_name2)
    sql "sync;"
    sql "analyze table where_mv with sync;"
    explain {
        sql """
            select t1.f1*t1.f2, t1.f1, t1.f2 from test_decimal_mul_overflow_for_mv t1 where t1.f1>1;
        """
        contains "where_mv chose"
    }
    sql "set enable_decimal256=false;"
    explain {
        sql """
            select t1.f1*t1.f2, t1.f1, t1.f2 from test_decimal_mul_overflow_for_mv t1 where t1.f1>1;
        """
        contains "where_mv not chose"
    }
    sql "drop materialized view  if exists where_mv;"

    // ********************** test agg_func rewrite *****************************
    sql "set enable_decimal256=true;"
    def var_result1 = sql "show variables"
    logger.info("show variales result: " + var_result1 )
    sql """
    drop materialized view  if exists sum_mv;
    create materialized view sum_mv
    BUILD IMMEDIATE
    REFRESH COMPLETE
    ON COMMIT
    PROPERTIES ('replication_num' = '1')
    as  select sum(f1),avg(f1*f2) from test_decimal_mul_overflow_for_mv t1;
    """
    sql """REFRESH MATERIALIZED VIEW sum_mv auto"""
    job_name2 = getJobName(db, "sum_mv");
    waitingMTMVTaskFinished(job_name2)
    sql "sync;"
    sql "analyze table sum_mv with sync;"
    def value = sql "show create materialized view sum_mv";
    logger.info("mtmv:" + value.toString())
    explain {
        sql "select sum(f1) from test_decimal_mul_overflow_for_mv t1;"
        contains "sum_mv chose"
    }
    explain {
        sql "select * from (select avg(f1*f2) c1 from test_decimal_mul_overflow_for_mv) t1 where c1>10;"
        contains "sum_mv chose"
    }
    sql """set enable_decimal256=false;"""

    def var_result = sql "show variables"
    logger.info("show variales result: " + var_result )
    def var_check = sql """show variables where variable_name = 'enable_decimal256';"""
    assertTrue(var_check.toString().contains('false'), 
        "enable_decimal256 should be false, but actual: ${var_check}")

    explain {
        sql "select * from (select avg(f1*f2) c1 from test_decimal_mul_overflow_for_mv) t1 where c1>10;"
        contains "sum_mv fail"
    }
    sql "drop materialized view  if exists sum_mv;"
    // ********************** test agg_func roll up rewrite *****************************
    sql "set enable_decimal256=true;"
    sql """
        drop materialized view  if exists sum_mv_roll_up;
        create materialized view sum_mv_roll_up
        BUILD IMMEDIATE
        REFRESH COMPLETE
        ON COMMIT
        PROPERTIES ('replication_num' = '1')
        as  select sum(f1) from test_decimal_mul_overflow_for_mv t1 group by f2;"""
    sql """REFRESH MATERIALIZED VIEW sum_mv_roll_up auto"""
    job_name2 = getJobName(db, "sum_mv_roll_up");
    waitingMTMVTaskFinished(job_name2)
    sql "analyze table sum_mv_roll_up with sync;"
    explain {
        sql "select sum(f1) from test_decimal_mul_overflow_for_mv t1;"
        contains "sum_mv_roll_up chose"
    }
    sql """set enable_decimal256=false;"""
    explain {
        sql "select sum(f1) from test_decimal_mul_overflow_for_mv t1;"
        contains "sum_mv_roll_up fail"
    }
    sql "drop materialized view  if exists sum_mv_roll_up;"

    // ********************** test join rewrite *****************************
    sql "set enable_decimal256=true;"
    sql "drop materialized view  if exists join_mv;"
    sql """create materialized view join_mv
    BUILD IMMEDIATE
    REFRESH COMPLETE
    ON COMMIT
    PROPERTIES ('replication_num' = '1')
    as select  t1.f1*t2.f1 from test_decimal_mul_overflow_for_mv t1 left outer join test_decimal_mul_overflow_for_mv2 t2
    on t1.f1=t2.f1 ;"""
    sql """REFRESH MATERIALIZED VIEW join_mv auto"""
    job_name2 = getJobName(db, "join_mv");
    waitingMTMVTaskFinished(job_name2)
    sql "analyze table join_mv with sync;"
    explain {
        sql """select t1.f1*t2.f1 from test_decimal_mul_overflow_for_mv t1 left outer join test_decimal_mul_overflow_for_mv2 t2
        on t1.f1=t2.f1;"""
        contains "join_mv chose"
    }
    sql "set enable_decimal256=false;"
    explain {
        sql """select t1.f1*t2.f1 from test_decimal_mul_overflow_for_mv t1 left outer join test_decimal_mul_overflow_for_mv2 t2
        on t1.f1=t2.f1;"""
        contains "join_mv fail"
    }
}