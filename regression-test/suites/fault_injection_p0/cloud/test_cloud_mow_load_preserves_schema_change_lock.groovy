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

import org.apache.doris.regression.suite.ClusterOptions

suite("test_cloud_mow_load_preserves_schema_change_lock", "docker") {
    def options = new ClusterOptions()
    options.setFeNum(1)
    options.setBeNum(1)
    options.cloudMode = true
    options.enableDebugPoints()
    options.feConfigs += [
            "delete_bitmap_lock_expiration_seconds=60",
            "enable_mow_load_force_take_ms_lock=true",
            "mow_load_force_take_ms_lock_threshold_ms=0",
            "meta_service_rpc_retry_times=2",
            "enable_schema_change_retry=false"
    ]
    options.beConfigs += [
            "delete_bitmap_lock_expiration_seconds=60"
    ]

    docker(options) {
        GetDebugPoint().clearDebugPointsForAllBEs()

        try {
            sql "DROP TABLE IF EXISTS test_cloud_mow_load_preserves_schema_change_lock"
            sql """
                CREATE TABLE test_cloud_mow_load_preserves_schema_change_lock (
                    k INT NOT NULL,
                    v INT
                )
                UNIQUE KEY(k)
                DISTRIBUTED BY HASH(k) BUCKETS 1
                PROPERTIES (
                    "enable_unique_key_merge_on_write" = "true",
                    "disable_auto_compaction" = "true",
                    "light_schema_change" = "false",
                    "replication_num" = "1"
                )
            """
            sql "INSERT INTO test_cloud_mow_load_preserves_schema_change_lock VALUES (1, 10)"

            GetDebugPoint().enableDebugPointForAllBEs(
                    "CloudSchemaChangeJob::_process_delete_bitmap.inject_sleep",
                    [percent: "1.0", sleep: "10"])
            def alterFuture = thread {
                sql """
                    ALTER TABLE test_cloud_mow_load_preserves_schema_change_lock
                    MODIFY COLUMN v BIGINT
                """
            }
            // This debug point is after the schema change acquires the delete bitmap lock.
            // The debug point runs after schema change acquires the delete bitmap lock.
            // The table has only one rowset, so three seconds is ample for reaching it.
            sleep(3000)

            test {
                sql "INSERT INTO test_cloud_mow_load_preserves_schema_change_lock VALUES (2, 20)"
                exception "Failed to get delete bitmap lock due to conflict"
            }

            alterFuture.get()
            waitForSchemaChangeDone {
                sql """
                    SHOW ALTER TABLE COLUMN
                    WHERE TableName = 'test_cloud_mow_load_preserves_schema_change_lock'
                    ORDER BY CreateTime DESC LIMIT 1
                """
                time 120
            }
            def alterResult = sql_return_maparray """
                SHOW ALTER TABLE COLUMN
                WHERE TableName = 'test_cloud_mow_load_preserves_schema_change_lock'
                ORDER BY CreateTime DESC LIMIT 1
            """
            assertEquals("FINISHED", alterResult[0].State)

            order_qt_final """
                SELECT k, v FROM test_cloud_mow_load_preserves_schema_change_lock
                ORDER BY k
            """
        } finally {
            GetDebugPoint().clearDebugPointsForAllBEs()
        }
    }
}
