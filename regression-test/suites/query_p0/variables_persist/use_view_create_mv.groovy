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

suite("use_view_create_mv") {
    multi_sql """
        drop table if exists t_for_mv_view;
        CREATE TABLE `t_for_mv_view` (
            `f1` decimal(20,5) NULL,
            `f2` decimal(21,6) NULL
        )DISTRIBUTED BY HASH(f1)
        PROPERTIES("replication_num" = "1");
        insert into t_for_mv_view values(999999999999999.12345,999999999999999.123456);
    """
    // create view
    multi_sql """
        set enable_decimal256=false;
        drop view if exists v_for_mv_view;
        create view v_for_mv_view as select f1,f2,f1*f2 multi from t_for_mv_view;
    """

    multi_sql """
        set enable_decimal256=true;
        drop materialized view if exists mv_view;
        create materialized view mv_view
        BUILD IMMEDIATE
        REFRESH COMPLETE
        ON COMMIT
        PROPERTIES ('replication_num' = '1')
        as select * from v_for_mv_view;
        insert into t_for_mv_view values(1.12345,1.234567);
    """
    sql """set enable_decimal256=false;
    insert into t_for_mv_view values(1.12345,1.234567);"""
    sql """
            REFRESH MATERIALIZED VIEW mv_view auto
        """
    def db = context.config.getDbNameByFile(context.file)
    def job_name = getJobName(db, "mv_view");
    waitingMTMVTaskFinished(job_name)
    sql """sync;"""

    sql """set enable_decimal256=true;"""
    order_qt_open256 "select * from v_for_mv_view"
    explain {
        sql "select * from v_for_mv_view"
        contains "mv_view chose"
    }
    sql """set enable_decimal256=false;"""
    order_qt_open128 "select * from v_for_mv_view"
    explain {
        sql "memo plan select * from v_for_mv_view"
        contains "mv_view not chose"
    }
}