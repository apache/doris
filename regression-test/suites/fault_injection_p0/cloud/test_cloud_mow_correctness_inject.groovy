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
            "disable_auto_compaction" = "true",
            "replication_num" = "1"); """

    sql "insert into ${table1} values(1,1,1);"
    sql "insert into ${table1} values(2,2,2);"
    sql "insert into ${table1} values(3,3,3);"
    sql "sync;"
    qt_sql "select * from ${table1} order by k1;"

    def customFeConfig = [
        delete_bitmap_lock_expiration_seconds : 10,
        calculate_delete_bitmap_task_timeout_seconds : 2,
        mow_calculate_delete_bitmap_retry_times : 3
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

    }
}
