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

import java.util.concurrent.TimeUnit

suite("test_insert_fail_fast_after_be_restart", "docker") {
    def options = new ClusterOptions()
    options.enableDebugPoints()
    options.setFeNum(1)
    options.setBeNum(1)
    options.cloudMode = false

    docker(options) {
        def tableName = "test_insert_fail_fast_after_be_restart"
        GetDebugPoint().clearDebugPointsForAllBEs()

        try {
            sql "DROP TABLE IF EXISTS ${tableName}"
            sql """
                CREATE TABLE ${tableName} (
                    k BIGINT NOT NULL,
                    v BIGINT NOT NULL
                )
                DUPLICATE KEY(k)
                DISTRIBUTED BY HASH(k) BUCKETS 1
                PROPERTIES (
                    "replication_num" = "1"
                )
            """

            // Hold the load fragment before it can report completion to FE. Restarting the BE
            // at this point drops that report while the replacement process quickly becomes alive.
            GetDebugPoint().enableDebugPointForAllBEs("VTabletWriter.close.sleep", [sleep_sec: 300])
            GetDebugPoint().enableDebugPointForAllBEs("VTabletWriterV2.close.sleep", [sleep_sec: 300])

            def insertFuture = thread {
                sql "SET enable_nereids_planner = true"
                sql "SET enable_fallback_to_original_planner = false"
                sql "SET insert_timeout = 300"
                try {
                    sql """
                        INSERT INTO ${tableName}
                        SELECT number, number FROM numbers("number" = "1024")
                    """
                    return null
                } catch (Throwable t) {
                    logger.info("INSERT failed after BE restart as expected: ${t.message}")
                    return t.message
                }
            }

            // The small load reaches the writer close debug point well before this wait ends.
            sleep(3000)
            cluster.restartBackends()

            // LoadProcessor checks backend health every 30 seconds. The restarted BE is alive,
            // so this only finishes before insert_timeout when FE also compares process epochs.
            def errorMessage = insertFuture.get(60, TimeUnit.SECONDS)
            assertNotNull(errorMessage, "INSERT should fail after its BE process restarts")
            assertTrue(errorMessage.contains("process epoch changed"),
                    "unexpected INSERT error after BE restart: ${errorMessage}")
            assertTrue(errorMessage.contains("backend restarted"),
                    "INSERT error should explain that the backend restarted: ${errorMessage}")

            assertEquals(0, sql("SELECT COUNT(*) FROM ${tableName}")[0][0] as int)
        } finally {
            GetDebugPoint().clearDebugPointsForAllBEs()
            try_sql "DROP TABLE IF EXISTS ${tableName}"
        }
    }
}
