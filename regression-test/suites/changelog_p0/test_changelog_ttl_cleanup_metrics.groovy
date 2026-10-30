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

suite("test_changelog_ttl_cleanup_metrics", "nonConcurrent") {
    def successSeries = 'doris_be_changelog_ttl_cleanup_total{result="success"}'
    def failedSeries = 'doris_be_changelog_ttl_cleanup_total{result="failed"}'
    def reclaimedSeries = 'doris_be_changelog_ttl_reclaimed_bytes_total'
    def lastSuccessSeries = 'doris_be_changelog_ttl_last_success_timestamp_seconds'
    // The cleanup of a tablet runs on the BE that holds it, or in cloud mode on any BE of the compute group,
    // so the counters are summed over the BEs.
    def readCleanupMetrics = {
        def totals = [success: 0L, failed: 0L, reclaimedBytes: 0L, lastSuccessSeconds: 0L]
        for (def backend : sql_return_maparray("SHOW BACKENDS")) {
            def (code, out, err) = curl("GET", "http://${backend.Host}:${backend.HttpPort}/metrics")
            assertEquals(0, code)
            def values = [:]
            for (def line : out.readLines()) {
                def matcher = line =~ /^(doris_be_changelog_ttl_\S+) (\d+)$/
                if (matcher.matches()) {
                    values[matcher.group(1)] = matcher.group(2) as long
                }
            }
            // Every series is there from 0, on every BE.
            assertEquals([successSeries, failedSeries, reclaimedSeries, lastSuccessSeries] as Set, values.keySet())
            totals.success += values[successSeries]
            totals.failed += values[failedSeries]
            totals.reclaimedBytes += values[reclaimedSeries]
            totals.lastSuccessSeconds = Math.max(totals.lastSuccessSeconds, values[lastSuccessSeries])
        }
        return totals
    }

    sql "DROP TABLE IF EXISTS changelog_ttl_cleanup_metrics"
    sql """
        CREATE TABLE changelog_ttl_cleanup_metrics (k INT, v INT)
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "changelog.enable" = "true",
            "changelog.ttl_seconds" = "1"
        )
    """
    def before = readCleanupMetrics()
    sql "INSERT INTO changelog_ttl_cleanup_metrics VALUES (1, 10), (2, 20)"
    // The raw changelog TVF applies no TTL: the rows go away once a cleanup drops the rowset that holds them.
    Awaitility.await().atMost(120, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS).until {
        (sql("SELECT count(*) FROM changelog('table' = 'changelog_ttl_cleanup_metrics')")[0][0] as long) == 0L
    }
    // The cleanup counts itself just after its rowsets have left the tablet, which a query can see first.
    def after = null
    Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS).until {
        after = readCleanupMetrics()
        after.success > before.success && after.reclaimedBytes > before.reclaimedBytes
    }
    logger.info("changelog TTL cleanup metrics before: ${before}, after: ${after}")
    assertTrue(after.lastSuccessSeconds > 0)
    assertTrue(after.lastSuccessSeconds >= before.lastSuccessSeconds)
}
