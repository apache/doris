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

suite("variables_up_down_test_mtmv", "restart_fe") {

    def query_sql = """select f1, f2, f1*f2 multi_col from test_decimal_mul_overflow_for_mv;"""

    def db = context.config.getDbNameByFile(context.file)
    def job_name = getJobName(db, "mv_var_1");
    waitingMTMVTaskFinished(job_name)
    sql """sync;"""

    order_qt_refresh_master_sql "select f1,f2,multi_col from mv_var_1 order by 1,2,3;"

    sql "set enable_decimal256=true;"
    explain {
        sql query_sql
        contains "mv_var_1 chose"
    }
    order_qt_rewite_open256_master_sql "$query_sql"

    sql "set enable_decimal256=false;"
    explain {
        sql query_sql
        contains "mv_var_1 chose"
    }
    order_qt_rewite_open128_master_sql "$query_sql"

    sql "set enable_decimal256=true;"
    order_qt_directe_sql256_master_sql """select * from mv_var_1"""
    sql "set enable_decimal256=false;"
    order_qt_directe_sql128_master_sql """select * from mv_var_1"""

    sql "set enable_decimal256=true;"
    sql """refresh materialized view mv_var_1 complete"""
    waitingMTMVTaskFinishedByMvName("mv_var_1")
    sql "set enable_decimal256=false;"
    sql """refresh materialized view mv_var_1 complete"""
    waitingMTMVTaskFinishedByMvName("mv_var_1")

}
