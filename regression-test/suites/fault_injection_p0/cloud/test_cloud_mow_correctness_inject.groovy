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

import org.junit.Assert
import java.util.concurrent.TimeUnit
import org.awaitility.Awaitility

// test cases to ensure that inject points for mow correctness work as expected
suite("test_cloud_mow_correctness_inject", "nonConcurrent") {
    if (!isCloudMode()) {
        return
    }

    GetDebugPoint().clearDebugPointsForAllFEs()
    GetDebugPoint().clearDebugPointsForAllBEs()

    def table1 = "test_cloud_mow_correctness_inject"
    sql "DROP TABLE IF EXISTS ${table1} FORCE;"
    sql """ CREATE TABLE IF NOT EXISTS ${table1} (
            `k1` int NOT NULL,
            `c1` int,
            `c2` int
            )UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "enable_mow_light_delete" = "false",
            "enable_unique_key_merge_on_write" = "true",
            "disable_auto_compaction" = "true"); """

    sql "insert into ${table1} values(1,1,1);"
    sql "insert into ${table1} values(2,2,2);"
    sql "insert into ${table1} values(3,3,3);"
    sql "sync;"
    qt_sql "select * from ${table1} order by k1;"

    def waitForSC = {
        Awaitility.await().atMost(30, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS).pollInterval(1000, TimeUnit.MILLISECONDS).until(() -> {
            def res = sql_return_maparray "SHOW ALTER TABLE COLUMN WHERE TableName='${table1}' ORDER BY createtime DESC LIMIT 1"
            assert res.size() == 1
            if (res[0].State == "FINISHED" || res[0].State == "CANCELLED") {
                return true;
            }
            return false;
        });
    }

    def customFeConfig = [
        delete_bitmap_lock_expiration_seconds : 10,
        calculate_delete_bitmap_task_timeout_seconds : 2,
        mow_calculate_delete_bitmap_retry_times : 3,
        enable_schema_change_retry : false // turn off to shorten the test's time consumption
    ]

    setFeConfigTemporary(customFeConfig) {
        try {
            // 3 * 2s < 10s
            GetDebugPoint().enableDebugPointForAllBEs("CloudEngineCalcDeleteBitmapTask.handle.inject_sleep", [percent: "1.0", sleep: "10"])

            test {
                sql "insert into ${table1} values(4,4,4);"
                exception "Failed to calculate delete bitmap. Timeout."
            }

            qt_sql "select * from ${table1} order by k1;"

        } catch(Exception e) {
            logger.info(e.getMessage())
            throw e
        } finally {
            GetDebugPoint().clearDebugPointsForAllBEs()
        }


        try {
            GetDebugPoint().enableDebugPointForAllBEs("BaseTablet::calc_segment_delete_bitmap.inject_err", [percent: "1.0"])

            test {
                sql "insert into ${table1} values(5,5,5);"
                exception "injection error"
            }

            qt_sql "select * from ${table1} order by k1;"
        } catch(Exception e) {
            logger.info(e.getMessage())
            throw e
        } finally {
            GetDebugPoint().clearDebugPointsForAllBEs()
        }


        try {
            GetDebugPoint().enableDebugPointForAllBEs("get_delete_bitmap_update_lock.inject_fail", [percent: "1.0"])
            GetDebugPoint().enableDebugPointForAllBEs("CloudSchemaChangeJob.process_alter_tablet.sleep")
            sql "alter table ${table1} modify column c2 varchar(100);"
            Thread.sleep(1000)
            sql "insert into ${table1} values(10,10,10);"
            qt_sql "select * from ${table1} order by k1;"
            Thread.sleep(200)
            GetDebugPoint().disableDebugPointForAllBEs("CloudSchemaChangeJob.process_alter_tablet.sleep")

            waitForSC()

            def res = sql_return_maparray "SHOW ALTER TABLE COLUMN WHERE TableName='${table1}' ORDER BY createtime DESC LIMIT 1"
            assert res[0].State == "CANCELLED"
            assert res[0].Msg.contains("injection error when get get_delete_bitmap_update_lock")

            qt_sql "select * from ${table1} order by k1;"
        } catch(Exception e) {
            logger.info(e.getMessage())
            throw e
        } finally {
            GetDebugPoint().clearDebugPointsForAllBEs()
        }


        try {
            // sleep enough time to let sc's delete bitmap lock expired
            GetDebugPoint().enableDebugPointForAllBEs("CloudSchemaChangeJob::_process_delete_bitmap.inject_sleep", [percent: "1.0", sleep: "20"])
            GetDebugPoint().enableDebugPointForAllBEs("CloudSchemaChangeJob::_process_delete_bitmap.before_new_inc.block")
            sql "alter table ${table1} modify column c2 varchar(100);"
            Thread.sleep(3000)
            sql "insert into ${table1} values(11,11,11);"
            qt_sql "select * from ${table1} order by k1;"
            Thread.sleep(1000)
            GetDebugPoint().disableDebugPointForAllBEs("CloudSchemaChangeJob::_process_delete_bitmap.before_new_inc.block")

            // wait until sc's delete bitmap expired
            Thread.sleep(10000)
            sql "insert into ${table1} values(12,12,12);"

            waitForSC()

            def res = sql_return_maparray "SHOW ALTER TABLE COLUMN WHERE TableName='${table1}' ORDER BY createtime DESC LIMIT 1"
            assert res[0].State == "CANCELLED"
            assert res[0].Msg.contains("[DELETE_BITMAP_LOCK_ERROR]lock expired when update delete bitmap")

            qt_sql "select * from ${table1} order by k1;"
        } catch(Exception e) {
            logger.info(e.getMessage())
            throw e
        } finally {
            GetDebugPoint().clearDebugPointsForAllBEs()
        }
    }
}
