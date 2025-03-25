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

suite("test_rollup_add_column") {
    def tbName = "test_rollup_add_column"
    def rollupName = "test_rollup_add_column_index"

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
            CREATE TABLE ${tbName} (
                `k1` int(11) NOT NULL COMMENT "",
                `k2` int(11) NOT NULL COMMENT "",
                `v1` int(11) SUM NOT NULL COMMENT "",
                `v2` int(11) SUM NOT NULL COMMENT ""
            )
            AGGREGATE KEY(`k1`, `k2`)
            DISTRIBUTED BY HASH(`k1`) BUCKETS 1
            PROPERTIES ("replication_num" = "1");
        """

    sql """ALTER TABLE ${tbName} ADD ROLLUP ${rollupName}(k1, v1);"""
    int max_try_secs = 120
    // if timeout testcase will fail same behaviour as old method.
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

    sql "ALTER TABLE ${tbName} ADD COLUMN k3 INT KEY NOT NULL DEFAULT '3' AFTER k1 TO ${rollupName};"

    Awaitility.await().atMost(max_try_secs, SECONDS).pollInterval(2, SECONDS).until{
        String res = getJobColumnState(tbName)
        if (res == "FINISHED" || res == "CANCELLED") {
            assertEquals("FINISHED", res)
            sleep(3000)
            return true;
        }
        return false;
    }

    sql "insert into ${tbName} values(1, 2, 3, 4, 5);"

    qt_select_base " select k1,k2,k3,v1,v2 from ${tbName} order by k1,k2,k3"
    qt_select_rollup " select k1,k3,v1 from ${tbName} order by k1,k3"

    sql "ALTER TABLE ${tbName} DROP ROLLUP ${rollupName};"
    sql "DROP TABLE ${tbName} FORCE;"
}
