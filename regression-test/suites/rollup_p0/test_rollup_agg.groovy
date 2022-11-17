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
suite("test_rollup_agg") {
    def tbName = "test_rollup_agg"

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
                siteid INT(11) NOT NULL,
                citycode SMALLINT(6) NOT NULL,
                username VARCHAR(32) NOT NULL,
                pv BIGINT(20) SUM NOT NULL DEFAULT '0',
                uv BIGINT(20) SUM NOT NULL DEFAULT '0'
            )
            AGGREGATE KEY (siteid,citycode,username)
            DISTRIBUTED BY HASH(siteid) BUCKETS 5 properties("replication_num" = "1");
        """
    sql """ALTER TABLE ${tbName} ADD ROLLUP rollup_city(citycode, pv);"""
    int max_try_secs = 60
    while (max_try_secs--) {
        String res = getJobRollupState(tbName)
        if (res == "FINISHED") {
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
    sql "ALTER TABLE ${tbName} ADD COLUMN vv BIGINT SUM NULL DEFAULT '0' TO rollup_city;"
    max_try_secs = 60
    while (max_try_secs--) {
        String res = getJobColumnState(tbName)
        if (res == "FINISHED") {
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
    sql "insert into ${tbName} values(1, 1, 'test1', 100,100,100);"
    sql "insert into ${tbName} values(2, 1, 'test2', 100,100,100);"
    explain {
        sql("SELECT citycode,SUM(pv) FROM ${tbName} GROUP BY citycode")
        contains("(rollup_city)")
    }
    qt_sql "SELECT citycode,SUM(pv) FROM ${tbName} GROUP BY citycode"
    sql "ALTER TABLE ${tbName} DROP ROLLUP rollup_city;"
    sql "DROP TABLE ${tbName} FORCE;"
}
