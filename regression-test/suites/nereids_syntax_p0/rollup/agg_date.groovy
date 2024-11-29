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
suite("agg_date", "rollup") {

    sql """set enable_nereids_planner=true"""
    sql "SET enable_fallback_to_original_planner=false"

    def tbName = "test_rollup_agg_date1"

    def getJobRollupState = { tableName ->
        def jobStateResult = sql """  SHOW ALTER TABLE ROLLUP WHERE TableName='${tableName}' ORDER BY CreateTime DESC LIMIT 1; """
        return jobStateResult[0][8]
    }
    def getJobColumnState = { tableName ->
        def jobStateResult = sql """  SHOW ALTER TABLE COLUMN WHERE TableName='${tableName}' ORDER BY CreateTime DESC LIMIT 1; """
        return jobStateResult[0][9]
    }
    sql "DROP TABLE IF EXISTS ${tbName}"
    sql """
            CREATE TABLE IF NOT EXISTS ${tbName}(
                datek1 datev2,
                datetimek1 datetimev2,
                datetimek2 datetimev2(3),
                datetimek3 datetimev2(6),
                datev1 datev2 MAX NOT NULL,
                datetimev1 datetimev2 MAX NOT NULL,
                datetimev2 datetimev2(3) MAX NOT NULL,
                datetimev3 datetimev2(6) MAX NOT NULL
            )
            AGGREGATE KEY (datek1, datetimek1, datetimek2, datetimek3)
            DISTRIBUTED BY HASH(datek1) BUCKETS 5 properties("replication_num" = "1");
        """
    sql """alter table test_rollup_agg_date1 modify column datek1 set stats ('row_count'='2');"""
    sql """ALTER TABLE ${tbName} ADD ROLLUP rollup_date(datek1,datetimek2,datetimek1,datetimek3,datev1,datetimev1,datetimev2,datetimev3);"""
    int max_try_secs = 60
    while (max_try_secs--) {
        String res = getJobRollupState(tbName)
        if (res == "FINISHED" || res == "CANCELLED") {
            assertEquals("FINISHED", res)
            sleep(3000)
            break
        } else {
            Thread.sleep(2000)
            if (max_try_secs < 1) {
                println "test timeout," + "state:" + res
                assertEquals("FINISHED",res)
            }
        }
    }
    Thread.sleep(2000)
    sql "ALTER TABLE ${tbName} ADD COLUMN datetimev4 datetimev2(3) MAX NULL;"
    max_try_secs = 60
    while (max_try_secs--) {
        String res = getJobColumnState(tbName)
        if (res == "FINISHED" || res == "CANCELLED") {
            assertEquals("FINISHED", res)
            sleep(3000)
            break
        } else {
            Thread.sleep(2000)
            if (max_try_secs < 1) {
                println "test timeout," + "state:" + res
                assertEquals("FINISHED",res)
            }
        }
    }
    sql "SHOW ALTER TABLE ROLLUP WHERE TableName='${tbName}';"
    qt_sql "DESC ${tbName} ALL;"
    sql "insert into ${tbName} values('2022-08-22', '2022-08-22 11:11:11.111111', '2022-08-22 11:11:11.111111', '2022-08-22 11:11:11.111111', '2022-08-22', '2022-08-22 11:11:11.111111', '2022-08-22 11:11:11.111111', '2022-08-22 11:11:11.111111', '2022-08-22 11:11:11.111111');"
    sql "insert into ${tbName} values('2022-08-23', '2022-08-23 11:11:11.111111', '2022-08-23 11:11:11.111111', '2022-08-23 11:11:11.111111', '2022-08-23', '2022-08-23 11:11:11.111111', '2022-08-23 11:11:11.111111', '2022-08-23 11:11:11.111111', '2022-08-23 11:11:11.111111');"

    sql "analyze table ${tbName} with sync;"
    sql """set enable_stats=false;"""

    mv_rewrite_success("SELECT datek1,datetimek1,datetimek2,datetimek3,max(datev1),max(datetimev1),max(datetimev2),max(datetimev3) FROM ${tbName} GROUP BY datek1,datetimek1,datetimek2,datetimek3",
            "rollup_date")
    sql """set enable_stats=true;"""
    mv_rewrite_success("SELECT datek1,datetimek1,datetimek2,datetimek3,max(datev1),max(datetimev1),max(datetimev2),max(datetimev3) FROM ${tbName} GROUP BY datek1,datetimek1,datetimek2,datetimek3",
            "rollup_date")

    qt_sql """ SELECT datek1,datetimek1,datetimek2,datetimek3,max(datev1),max(datetimev1),max(datetimev2),max(datetimev3) FROM ${tbName} GROUP BY datek1,datetimek1,datetimek2,datetimek3 order by datek1 desc; """
    sql "ALTER TABLE ${tbName} DROP ROLLUP rollup_date;"
    sql "DROP TABLE ${tbName} FORCE;"
}
