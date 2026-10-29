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

import org.apache.doris.regression.util.Http

/**
 * The series that the Master FE renders in /metrics from the changelog and IVM monitor.
 *
 * <p>A Master daemon samples every changelog_ivm_monitor_interval_seconds, and /metrics renders its latest
 * snapshot: when the latest round completed and how long it took, how many rounds failed, and how much changelog
 * the tables keep, which must agree with the ChangelogSize that SHOW DATA reports for the whole warehouse. Only the
 * Master exports these series, and none of them once the monitor is disabled.
 */
suite("test_changelog_ivm_monitor_metrics") {
    sql "DROP TABLE IF EXISTS test_changelog_ivm_monitor_metrics"
    sql """
        CREATE TABLE test_changelog_ivm_monitor_metrics (id INT, v INT)
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1", "changelog.enable" = "true")
    """
    sql "INSERT INTO test_changelog_ivm_monitor_metrics VALUES (1, 1), (2, 2)"
    sql "INSERT INTO test_changelog_ivm_monitor_metrics VALUES (3, 3)"

    String masterMetricsUrl = "http://${getMasterIp()}:${getMasterPort()}/metrics"
    // the value of a series, or null when /metrics does not export it
    def seriesValue = { String metrics, String series ->
        String line = metrics.readLines().find { it.startsWith(series + " ") }
        return line == null ? null : line.substring(series.length() + 1)
    }
    // the ChangelogSize that SHOW DATA reports for the whole warehouse, in bytes
    def showDataChangelogSize = {
        def total = sql("""SHOW DATA PROPERTIES ("entire_warehouse" = "true")""").find { it[0] == "total" }
        return total[3] as long
    }
    // SHOW DATA counts all the replicas, and a tablet of cloud mode keeps a single copy in the shared storage,
    // whose size is the only one exported there.
    String retainedSeries = isCloudMode() ? 'doris_fe_changelog_retained_bytes{scope="single_replica"}'
            : 'doris_fe_changelog_retained_bytes{scope="all_replicas"}'

    // A round every second keeps the latest snapshot at most about a second behind SHOW DATA, which both read from
    // the statistics of the tables. An EXPERIMENTAL config is known to SHOW FRONTEND CONFIG, and so to
    // setFeConfigTemporary, by its experimental_ name.
    setFeConfigTemporary([experimental_changelog_ivm_monitor_interval_seconds: 1]) {
        // the statistics are refreshed every tablet_stat_update_interval_second
        awaitUntil(180, 1) { showDataChangelogSize() > 0 }
        awaitUntil(120, 1) {
            String metrics = Http.GET(masterMetricsUrl, false, false)
            return seriesValue(metrics, retainedSeries) == showDataChangelogSize().toString()
        }

        String metrics = Http.GET(masterMetricsUrl, false, false)
        assertEquals("0", seriesValue(metrics, "doris_fe_changelog_ivm_monitor_sample_failures_total"))
        long now = (sql "SELECT UNIX_TIMESTAMP()")[0][0] as long
        long sampleTime = seriesValue(metrics, "doris_fe_changelog_ivm_monitor_last_sample_timestamp_seconds") as long
        assertTrue(now - sampleTime <= 60, "latest round at ${sampleTime}, now ${now}")
        assertTrue((seriesValue(metrics, "doris_fe_changelog_ivm_monitor_sample_duration_seconds") as double) >= 0)
        assertNotNull(seriesValue(metrics, 'doris_fe_changelog_retained_bytes{scope="single_replica"}'))
        assertEquals(!isCloudMode(), metrics.contains('doris_fe_changelog_retained_bytes{scope="all_replicas"}'))
    }

    // The other FEs do not sample, and export none of these series rather than 0.
    for (def fe : sql_return_maparray("SELECT Host, HttpPort, IsMaster FROM frontends()")) {
        if (fe.IsMaster == "true") {
            continue
        }
        String metrics = Http.GET("http://${fe.Host}:${fe.HttpPort}/metrics", false, false)
        assertFalse(metrics.contains("doris_fe_changelog_ivm_monitor_"), "exported by the FE at ${fe.Host}")
        assertFalse(metrics.contains("doris_fe_changelog_retained_bytes"), "exported by the FE at ${fe.Host}")
    }

    setFeConfigTemporary([experimental_enable_changelog_ivm_monitor: false]) {
        String metrics = Http.GET(masterMetricsUrl, false, false)
        assertFalse(metrics.contains("doris_fe_changelog_ivm_monitor_"))
        assertFalse(metrics.contains("doris_fe_changelog_retained_bytes"))
    }
}
