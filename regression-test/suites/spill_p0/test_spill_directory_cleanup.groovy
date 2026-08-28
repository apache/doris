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

suite("test_spill_directory_cleanup", "nonConcurrent") {
    def backends = sql_return_maparray("SHOW BACKENDS").findAll {
        it.Alive.toString().equalsIgnoreCase("true")
    }
    assertTrue(!backends.isEmpty(), "No alive backend found")

    def readSpillMetric = { String metricName ->
        def valuesByBackend = [:]
        backends.each { backend ->
            def endpoint = "http://${backend.Host}:${backend.HttpPort}/metrics"
            def metrics = Http.GET(endpoint, false, false).toString()
            def metricPattern = '(?m)^' + java.util.regex.Pattern.quote(metricName) +
                    '(?:\\{[^}]*\\})?\\s+(\\d+)$'
            def matcher = java.util.regex.Pattern.compile(metricPattern).matcher(metrics)
            long value = 0
            int matchedMetrics = 0
            while (matcher.find()) {
                value += matcher.group(1).toLong()
                matchedMetrics++
            }
            assertTrue(matchedMetrics > 0, "Metric ${metricName} not found on ${endpoint}")
            valuesByBackend[backend.BackendId.toString()] = value
        }
        return valuesByBackend
    }

    def waitForSpillDirectoryState = { boolean expectedPresent, String stage ->
        def lastPresence = [:]
        Throwable lastError = null
        long deadline = System.currentTimeMillis() + 60_000
        while (System.currentTimeMillis() < deadline) {
            try {
                lastPresence = readSpillMetric("doris_be_spill_disk_has_spill_data")
                boolean anyPresent = lastPresence.values().any { it > 0 }
                if (anyPresent == expectedPresent) {
                    logger.info("Spill directory state at ${stage}: ${lastPresence}")
                    return lastPresence
                }
                lastError = null
            } catch (Throwable t) {
                lastError = t
            }
            sleep(500)
        }
        def errorDetail = lastError == null ? "" : ", last error: ${lastError.message}"
        assertTrue(false,
                "Timed out waiting for spill directory state at ${stage}; " +
                        "expectedPresent=${expectedPresent}, lastPresence=${lastPresence}" +
                        errorDetail)
    }

    GetDebugPoint().clearDebugPointsForAllBEs()
    waitForSpillDirectoryState(false, "before query")
    def spillWriteBytesBefore =
            readSpillMetric("doris_be_spill_write_bytes").values().sum(0L)

    def deleteFailureDebugPoint =
            "fault_inject::spill_file_manager::delete_query_spill_directory"
    try {
        // Keep the otherwise short-lived empty query directory visible until the metric refreshes.
        // Disabling this point lets the spill GC retry the same deletion.
        GetDebugPoint().enableDebugPointForAllBEs(deleteFailureDebugPoint, [timeout: "120"])

        sql "SET enable_spill = true"
        sql "SET enable_force_spill = true"
        sql "SET spill_min_revocable_mem = 1048576"
        sql "SET parallel_pipeline_task_num = 1"
        sql "SET batch_size = 1024"
        sql "SET enable_reserve_memory = true"

        def result = sql """
            SELECT COUNT(*)
            FROM (
                SELECT number
                FROM numbers("number" = "200000")
                GROUP BY number
                HAVING SUM(number) >= 0
            ) t
        """
        assertEquals("200000", result[0][0].toString())

        def spillWriteBytesAfter =
                readSpillMetric("doris_be_spill_write_bytes").values().sum(0L)
        assertTrue(spillWriteBytesAfter > spillWriteBytesBefore,
                "The query did not write spill data: before=${spillWriteBytesBefore}, " +
                        "after=${spillWriteBytesAfter}")
        waitForSpillDirectoryState(true, "after forced spill")
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs(deleteFailureDebugPoint)
    }

    // The next spill GC cycle retries the failed query-directory deletion and refreshes the metric.
    waitForSpillDirectoryState(false, "after query cleanup")
}
