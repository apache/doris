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

import java.util.concurrent.TimeUnit
import org.awaitility.Awaitility

suite("test_cloud_sc_self_retry_with_stop_token", "nonConcurrent") {
    if (!isCloudMode()) {
        return
    }

    GetDebugPoint().clearDebugPointsForAllFEs()
    GetDebugPoint().clearDebugPointsForAllBEs()


    def table1 = "test_cloud_sc_self_retry_with_stop_token"
    sql "DROP TABLE IF EXISTS ${table1} FORCE;"
    sql """ CREATE TABLE IF NOT EXISTS ${table1} (
            `k1` int NOT NULL,
            `c1` int,
            `c2` int,
            `c3` int
            )UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "enable_unique_key_merge_on_write" = "true",
            "disable_auto_compaction" = "true",
            "replication_num" = "1"); """

    sql "insert into ${table1} values(1,1,1,1);"
    sql "insert into ${table1} values(2,2,2,2);"
    sql "insert into ${table1} values(3,3,3,2);"
    sql "sync;"
    qt_sql "select * from ${table1} order by k1;"

    try {
        GetDebugPoint().enableDebugPointForAllBEs("CloudSchemaChangeJob::_convert_historical_rowsets.fail.before.commit_job")

        sql "alter table ${table1} modify column c2 varchar(100);"

        def res
        Awaitility.await().atMost(600, TimeUnit.SECONDS).pollDelay(1, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS).until(() -> {
            res = sql_return_maparray """ SHOW ALTER TABLE COLUMN WHERE TableName='${table1}' ORDER BY createtime DESC LIMIT 1 """
            logger.info("res: ${res}")
            if (res[0].State == "FINISHED" || res[0].State == "CANCELLED") {
                return true;
            }
            return false;
        });

        assert res[0].State == "CANCELLED"
        assert !res[0].Msg.contains("compactions are not allowed on tablet_id") && !res[0].Msg.contains("stop token already exists")
        assert res[0].Msg.contains("DELETE_BITMAP_LOCK_ERROR")

        qt_sql "select * from ${table1} order by k1;"        
    } catch(Exception e) {
        logger.info(e.getMessage())
        throw e
    } finally {
        GetDebugPoint().clearDebugPointsForAllBEs()
    }
}
