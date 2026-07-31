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

// Test: schema change retry refreshes base tablet rowsets before registering a fresh V1.
//
// Timeline:
//   1. Block SC, insert data, and let new tablet compaction create a cross-V1 rowset.
//   2. Force V1=6 once to prove the cross-V1 retry path is active.
//   3. Disable the V1 override, but inject stale local base max=6 only for query_version sync.
//   4. Retry must refresh the base tablet and finish with the latest V1.
//
// The stale-local-max debug point only fires when the caller uses SyncOptions.query_version.
// Old code capped SC base sync by request.alter_version, so it keeps V1 stale and cannot finish.
// The fixed path does not set query_version, so it refreshes from meta-service and succeeds.

import org.apache.doris.regression.suite.ClusterOptions

suite('test_sc_compaction_cross_v1_stale_base_refresh', 'docker') {

    def options = new ClusterOptions()
    options.cloudMode = true
    options.enableDebugPoints()
    options.beConfigs += ["enable_java_support=false"]
    options.beConfigs += ["enable_new_tablet_do_compaction=true"]
    options.beConfigs += ["alter_tablet_worker_count=1"]
    options.beConfigs += ["cumulative_compaction_min_deltas=2"]
    options.beNum = 1
    options.feConfigs += ["http_port=8030"]
    options.feConfigs += ["rpc_port=9020"]
    options.feConfigs += ["query_port=9030"]
    options.feConfigs += ["edit_log_port=9010"]
    options.feConfigs += ["enable_schema_change_retry=true"]
    options.feConfigs += ["schema_change_max_retry_time=10"]

    docker(options) {
        def getJobState = {
            def result = sql """
                SHOW ALTER TABLE COLUMN
                WHERE IndexName='sc_cross_v1_stale_base_refresh_test'
                ORDER BY createtime DESC LIMIT 1
            """
            logger.info("getJobState: ${result}")
            return result[0][9]
        }

        sql "DROP TABLE IF EXISTS sc_cross_v1_stale_base_refresh_test"
        sql """
            CREATE TABLE sc_cross_v1_stale_base_refresh_test (
                k1 int NOT NULL,
                v1 varchar(100) NOT NULL,
                v2 int NOT NULL
            )
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1"
            )
        """

        def tablets = sql_return_maparray "SHOW TABLETS FROM sc_cross_v1_stale_base_refresh_test"
        assertEquals(1, tablets.size())
        def baseTabletId = tablets[0].TabletId.toString()
        logger.info("base tablet id for stale refresh test: ${baseTabletId}")

        for (int i = 0; i < 3; i++) {
            StringBuilder sb = new StringBuilder()
            sb.append("INSERT INTO sc_cross_v1_stale_base_refresh_test VALUES ")
            for (int j = 0; j < 20; j++) {
                if (j > 0) {
                    sb.append(", ")
                }
                def key = i * 20 + j + 1
                sb.append("(${key}, 'val_${key}', ${key * 10})")
            }
            sql sb.toString()
        }
        assertEquals(60L, (sql "SELECT count(*) FROM sc_cross_v1_stale_base_refresh_test")[0][0])

        def scBlock = 'CloudSchemaChangeJob::process_alter_tablet.block'
        def overrideDP = 'CloudSchemaChangeJob::process_alter_tablet.override_base_max_version'
        def staleMaxDP = 'CloudTablet::sync_rowsets.stale_local_max_for_query_version'

        try {
            GetDebugPoint().enableDebugPointForAllBEs(scBlock)
            try {
                sql "ALTER TABLE sc_cross_v1_stale_base_refresh_test MODIFY COLUMN v2 bigint"
                sleep(10000)
                assertEquals("RUNNING", getJobState())

                for (int i = 0; i < 6; i++) {
                    StringBuilder sb = new StringBuilder()
                    sb.append("INSERT INTO sc_cross_v1_stale_base_refresh_test VALUES ")
                    for (int j = 0; j < 10; j++) {
                        if (j > 0) {
                            sb.append(", ")
                        }
                        def key = 100 + i * 10 + j + 1
                        sb.append("(${key}, 'new_${key}', ${key * 10})")
                    }
                    sql sb.toString()
                }

                sleep(30000)
                GetDebugPoint().enableDebugPointForAllBEs(overrideDP, [version: 6])
            } finally {
                GetDebugPoint().disableDebugPointForAllBEs(scBlock)
            }

            sleep(10000)
            assertEquals("RUNNING", getJobState(),
                    "SC should still be RUNNING while V1 is forced to cross the compacted rowset")

            GetDebugPoint().enableDebugPointForAllBEs(
                    staleMaxDP, [tablet_id: baseTabletId, version: 6])
            GetDebugPoint().disableDebugPointForAllBEs(overrideDP)

            int maxTries = 180
            def finalState = ""
            while (maxTries-- > 0) {
                finalState = getJobState()
                if (finalState == "FINISHED" || finalState == "CANCELLED") {
                    break
                }
                sleep(1000)
            }

            logger.info("SC final state after stale base refresh retry: ${finalState}")
            assertEquals("FINISHED", finalState)

            assertEquals(120L,
                    (sql "SELECT count(*) FROM sc_cross_v1_stale_base_refresh_test")[0][0])

            def columns = sql "DESC sc_cross_v1_stale_base_refresh_test"
            def v2Col = columns.find { it[0] == "v2" }
            assertTrue(v2Col[1].toString().toLowerCase().contains("bigint"),
                    "v2 column should be bigint after schema change, got: ${v2Col[1]}")

            def backends = sql_return_maparray("SHOW BACKENDS")
            assertTrue(backends.every { it.Alive.toString() == "true" },
                    "BE should be alive after stale base refresh retry")
        } finally {
            GetDebugPoint().clearDebugPointsForAllBEs()
        }
    }
}
