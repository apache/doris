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

suite("test_cloud_mow_insert_with_retry", "nonConcurrent") {
    if (!isCloudMode()) {
        return
    }

    GetDebugPoint().clearDebugPointsForAllFEs()
    GetDebugPoint().clearDebugPointsForAllBEs()

    def customFeConfig = [
            calculate_delete_bitmap_task_timeout_seconds: 2
    ]
    def dbName = "regression_test_fault_injection_p0_cloud"
    def table1 = dbName + ".test_cloud_mow_insert_with_retry"
    setFeConfigTemporary(customFeConfig) {
        try {
            GetDebugPoint().enableDebugPointForAllBEs("CloudEngineCalcDeleteBitmapTask.execute.enable_wait")
            sql "DROP TABLE IF EXISTS ${table1} FORCE;"
            sql """ CREATE TABLE IF NOT EXISTS ${table1} (
                        `k1` int NOT NULL,
                        `c1` int,
                        `c2` int
                        )UNIQUE KEY(k1)
                    DISTRIBUTED BY HASH(k1) BUCKETS 1
                    PROPERTIES (
                        "enable_unique_key_merge_on_write" = "true",
                        "disable_auto_compaction" = "true",
                        "replication_num" = "1"); """
            connect(user = context.config.jdbcUser, password = context.config.jdbcPassword, url = context.config.jdbcUrl + "&useLocalSessionState=true") {
                def timeout = 2000
                def now = System.currentTimeMillis()
                sql "insert into ${table1} values(1,1,1);"
                def time_diff = System.currentTimeMillis() - now
                logger.info("time_diff:" + time_diff)
                assertTrue(time_diff > timeout, "insert or delete should take over " + timeout + " ms")

                now = System.currentTimeMillis()
                sql "insert into ${table1} values(2,2,2);"
                time_diff = System.currentTimeMillis() - now
                logger.info("time_diff:" + time_diff)
                assertTrue(time_diff > timeout, "insert or delete should take over " + timeout + " ms")
                order_qt_sql "select * from ${table1};"

                now = System.currentTimeMillis()
                sql "delete from ${table1} where k1=2;"
                time_diff = System.currentTimeMillis() - now
                logger.info("time_diff:" + time_diff)
                assertTrue(time_diff > timeout, "insert or delete should take over " + timeout + " ms")
                order_qt_sql "select * from ${table1};"
            }
        } catch (Exception e) {
            logger.info(e.getMessage())
            throw e
        } finally {
            GetDebugPoint().disableDebugPointForAllFEs("CloudEngineCalcDeleteBitmapTask.execute.enable_wait")
            sql "DROP TABLE IF EXISTS ${table1};"
            GetDebugPoint().clearDebugPointsForAllBEs()
        }
    }
}