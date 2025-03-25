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
import org.awaitility.Awaitility
import static java.util.concurrent.TimeUnit.SECONDS

suite("test_rollup_agg_date", "rollup") {
    def tbName = "test_rollup_agg_date"

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
    sql """ALTER TABLE ${tbName} ADD ROLLUP rollup_date(datek1,datetimek2,datetimek1,datetimek3,datev1,datetimev1,datetimev2,datetimev3);"""
    int max_try_secs = 120
    // if timeout testcase will fail same behaviour as old nethod.
    Awaitility.await().atMost(max_try_secs, SECONDS).pollInterval(2, SECONDS).until{
        String res = getJobRollupState(tbName)
        if (res == "FINISHED" || res == "CANCELLED") {
            assertEquals("FINISHED", res)
            sleep(3000)
            return true;
        }
        return false;
    }

    Thread.sleep(2000)
    sql "ALTER TABLE ${tbName} ADD COLUMN datetimev4 datetimev2(3) MAX NULL;"
    // if timeout testcase will fail same behaviour as old nethod.
    Awaitility.await().atMost(max_try_secs, SECONDS).pollInterval(2, SECONDS).until{
        String res = getJobColumnState(tbName)
        if (res == "FINISHED" || res == "CANCELLED") {
            assertEquals("FINISHED", res)
            sleep(3000)
            return true;
        }
        return false;
    }

    sql "SHOW ALTER TABLE ROLLUP WHERE TableName='${tbName}';"
    qt_sql "DESC ${tbName} ALL;"
    sql "insert into ${tbName} values('2022-08-22', '2022-08-22 11:11:11.111111', '2022-08-22 11:11:11.111111', '2022-08-22 11:11:11.111111', '2022-08-22', '2022-08-22 11:11:11.111111', '2022-08-22 11:11:11.111111', '2022-08-22 11:11:11.111111', '2022-08-22 11:11:11.111111');"
    sql "insert into ${tbName} values('2022-08-23', '2022-08-23 11:11:11.111111', '2022-08-23 11:11:11.111111', '2022-08-23 11:11:11.111111', '2022-08-23', '2022-08-23 11:11:11.111111', '2022-08-23 11:11:11.111111', '2022-08-23 11:11:11.111111', '2022-08-23 11:11:11.111111');"
    explain {
        sql("SELECT datek1,datetimek1,datetimek2,datetimek3,max(datev1),max(datetimev1),max(datetimev2),max(datetimev3) FROM ${tbName} GROUP BY datek1,datetimek1,datetimek2,datetimek3")
        contains("(rollup_date)")
    }
    qt_sql """ SELECT datek1,datetimek1,datetimek2,datetimek3,max(datev1),max(datetimev1),max(datetimev2),max(datetimev3) FROM ${tbName} GROUP BY datek1,datetimek1,datetimek2,datetimek3 order by datek1 desc; """
    sql "ALTER TABLE ${tbName} DROP ROLLUP rollup_date;"
    sql "DROP TABLE ${tbName} FORCE;"
}
