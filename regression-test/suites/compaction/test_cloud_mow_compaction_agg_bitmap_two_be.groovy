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

// Use an existing cloud Docker image with the debug points below enabled.
// Set image and excludeDockerTest=false in regression-conf-custom.groovy, then run:
// bash run-regression-test.sh --run -d compaction \
//     -s test_cloud_mow_compaction_agg_bitmap_two_be -noKillDocker
// The final comparison fails on affected builds: 18 rows instead of 12, with
// stale (k, 0) rows for k=0,2,4,6,8,10. No generated .out file is required.
suite("test_cloud_mow_compaction_agg_bitmap_two_be", "docker") {
    def options = new ClusterOptions()
    options.cloudMode = true
    options.setFeNum(1)
    options.setBeNum(2)
    options.enableDebugPoints()
    options.beConfigs += [
        'disable_auto_compaction=true',
        'enable_parallel_cumu_compaction=false',
        'cumulative_compaction_min_deltas=2',
        'delete_bitmap_store_read_version=1',
        'delete_bitmap_store_write_version=1',
        'enable_sync_tablet_delete_bitmap_by_cache=false',
        'enable_agg_and_remove_pre_rowsets_delete_bitmap=true',
        'enable_remove_agg_pre_rowsets_delete_bitmap_by_keys=true',
        'enable_remove_pre_rowsets_delete_bitmap_by_keys=true',
    ]

    docker(options) {
        sql "DROP TABLE IF EXISTS test_cloud_mow_compaction_agg_bitmap_two_be"
        sql """
            CREATE TABLE test_cloud_mow_compaction_agg_bitmap_two_be (k BIGINT, v INT)
            UNIQUE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "true",
                "disable_auto_compaction" = "true"
            )
        """
        // R2 contains all old rows; R3, R4 and R5 overwrite disjoint sets of keys.
        sql """INSERT INTO test_cloud_mow_compaction_agg_bitmap_two_be
               SELECT number, 0 FROM numbers("number" = "12")"""
        sql """INSERT INTO test_cloud_mow_compaction_agg_bitmap_two_be
               SELECT number, 10 FROM numbers("number" = "12") WHERE number % 2 = 0"""
        sql """INSERT INTO test_cloud_mow_compaction_agg_bitmap_two_be
               SELECT number, 20 FROM numbers("number" = "12") WHERE number % 4 = 1"""
        sql """INSERT INTO test_cloud_mow_compaction_agg_bitmap_two_be
               SELECT number, 30 FROM numbers("number" = "12") WHERE number % 4 = 3"""

        def actualSql = "SELECT k, v FROM test_cloud_mow_compaction_agg_bitmap_two_be ORDER BY k, v"
        def expectedSql = """SELECT number AS k,
                CASE WHEN number % 2 = 0 THEN 10 WHEN number % 4 = 1 THEN 20 ELSE 30 END AS v
                FROM numbers("number" = "12") ORDER BY k, v"""
        quickTest("before_compaction", actualSql, expectedSql)

        def tabletId = sql_return_maparray("SHOW TABLETS FROM test_cloud_mow_compaction_agg_bitmap_two_be")[0].TabletId
        def beA = cluster.getBeByIndex(1)
        def beB = cluster.getBeByIndex(2)
        def selector = "CloudSizeBasedCumulativeCompactionPolicy::pick_input_rowsets.set_input_rowsets"
        def barrier = "CumulativeCompaction.modify_rowsets.cloud_update_delete_bitmap_without_lock.block"
        def request = { be, method, path ->
            def (code, out, err) = curl(method, "http://${be.host}:${be.httpPort}${path}")
            assertEquals(0, code, "${method} ${path}: ${err}")
            return out
        }
        def rowsets = { be ->
            parseJson(request(be, "GET", "/api/compaction/show?tablet_id=${tabletId}")).rowsets
        }
        def running = { be ->
            def status = parseJson(request(be, "GET", "/api/compaction/run_status?tablet_id=${tabletId}"))
            assertEquals("success", status.status.toLowerCase())
            return status.run_status
        }
        def submit = { be, start ->
            be.enableDebugPoint(selector, [tablet_id: tabletId, start_version: "${start}", end_version: "5"])
            def result = parseJson(request(be, "POST",
                    "/api/compaction/run?tablet_id=${tabletId}&compact_type=cumulative"))
            assertEquals("Success", result.status, result.toString())
        }
        def waitForAggregation = { be, start ->
            awaitUntil(120) {
                !running(be) && new File(be.getLogFilePath()).readLines().any {
                    it.contains("agg pre rowsets delete bitmap to ms. tablet_id=${tabletId},") &&
                            it.contains("output version=[${start}-5], cost(us)=")
                }
            }
        }

        try {
            // A publishes R45, computes D4 | D5 for R2, then pauses before the MS write.
            beA.enableDebugPoint(barrier, [timeout: "600"])
            // Cache warmup may have loaded A before all four inserts were published.
            assertTrue(request(beA, "GET", "/api/clear_cache/CloudTabletCache").contains("prune win"))
            def initialRowsets = rowsets(beA)
            assertTrue(initialRowsets.any { it.contains("[4-4]") }, initialRowsets.toString())
            assertTrue(initialRowsets.any { it.contains("[5-5]") }, initialRowsets.toString())
            submit(beA, 4)
            def beALog = new File(beA.getLogFilePath())
            awaitUntil(120) {
                beALog.text.contains("start debug block ${barrier}")
            }
            assertTrue(running(beA))
            assertTrue(rowsets(beA).any { it.contains("[4-5]") })

            // B must reload the published R45 instead of using its pre-compaction tablet.
            assertTrue(request(beB, "GET", "/api/clear_cache/CloudTabletCache").contains("prune win"))
            submit(beB, 3)
            waitForAggregation(beB, 3)
            assertTrue(rowsets(beB).any { it.contains("[3-5]") })
            assertTrue(running(beA))
            // B stores D3 | D4 | D5 at version 5 and removes source versions 3 and 4.
            quickTest("after_B_while_A_blocked", actualSql, expectedSql)

            // A's delayed D4 | D5 overwrites B's version-5 bitmap on affected builds.
            beA.disableDebugPoint(barrier)
            waitForAggregation(beA, 4)
            quickTest("after_A_released", actualSql, expectedSql)
        } finally {
            beA.disableDebugPoint(barrier)
            beA.disableDebugPoint(selector)
            beB.disableDebugPoint(selector)
        }

        // Both BEs can retain the complete bitmap in memory; restart both for a cold read.
        cluster.restartBackends()
        logger.info("Rows after restarting both BEs: ${sql(actualSql)}")
        quickTest("after_restart", actualSql, expectedSql)
    }
}
