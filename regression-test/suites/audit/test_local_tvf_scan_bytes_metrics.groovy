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

suite("test_local_tvf_scan_bytes_metrics", "nonConcurrent") {
    setGlobalVarTemporary([enable_audit_plugin: true], {
        List<List<Object>> backends = sql """ show backends """
        assertTrue(backends.size() > 0)
        def beId = backends[0][0]
        def baseName = "test_local_tvf_scan_bytes_metrics_${System.currentTimeMillis()}_"
        def readPathPrefix = "/${baseName}"
        String beOutputRoot = null

        // Probe the BE-side local TVF root from the expected glob error message.
        try {
            sql """
                SELECT count(*)
                FROM local(
                    "file_path" = "${readPathPrefix}probe_*",
                    "backend_id" = "${beId}",
                    "format" = "csv"
                )
            """
            throw new RuntimeException("expected local() probe to fail for a missing file")
        } catch (Exception e) {
            String errorMessage = e.getMessage()
            String pathMarker = "//${baseName}probe_*"
            int markerIndex = errorMessage.indexOf(pathMarker)
            assertTrue(markerIndex > 0)
            int prefixIndex = errorMessage.lastIndexOf("failed to glob ", markerIndex)
            assertTrue(prefixIndex >= 0)
            beOutputRoot = errorMessage.substring(prefixIndex + "failed to glob ".length(), markerIndex)
        }

        // Reset audit rows to keep the later lookup deterministic.
        sql """ truncate table __internal_schema.audit_log """

        // Generate a local CSV file directly under the probed BE local TVF root.
        sql """
            INSERT INTO local(
                "file_path" = "${beOutputRoot}/${baseName}",
                "backend_id" = "${beId}",
                "format" = "csv"
            )
            SELECT number
            FROM numbers("number" = "1000")
            ORDER BY number
        """

        List<List<Object>> rowCountRows = sql """
            SELECT count(*)
            FROM local(
                "file_path" = "${readPathPrefix}*",
                "backend_id" = "${beId}",
                "format" = "csv"
            )
        """
        assertEquals("1000", rowCountRows[0][0].toString())

        String metricQuery = """
            SELECT
                scan_bytes > 0,
                scan_bytes_from_local_storage > 0,
                scan_bytes_from_remote_storage = 0
            FROM __internal_schema.audit_log
            WHERE lower(stmt) LIKE '%count(*)%'
              AND lower(stmt) LIKE '%from local(%'
              AND lower(stmt) LIKE '%${readPathPrefix}%'
              AND lower(stmt) NOT LIKE '%from __internal_schema.audit_log%'
            ORDER BY time DESC
            LIMIT 1
        """

        // Flush and poll audit_log until the local TVF query metrics are persisted.
        int retry = 120
        List<List<Object>> metricRows = []
        while (metricRows.isEmpty()) {
            if (retry-- < 0) {
                throw new RuntimeException("failed to find local tvf scan metrics in audit_log")
            }
            sql """ call flush_audit_log() """
            sleep(1000)
            metricRows = sql metricQuery
        }

        assertEquals("true", metricRows[0][0].toString())
        assertEquals("true", metricRows[0][1].toString())
        assertEquals("true", metricRows[0][2].toString())
    })
}
