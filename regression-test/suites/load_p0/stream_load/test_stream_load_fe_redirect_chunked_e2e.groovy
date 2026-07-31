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

suite("test_stream_load_fe_redirect_chunked_e2e", "p0") {
    String dbName = context.config.getDbNameByFile(context.file)
    String tableName = "test_stream_load_fe_redirect_chunked_e2e"
    String helperPath = "${context.file.parent}/scripts/stream_load_redirect_chunked_e2e.py"
    String feHttpAddress = context.config.feHttpAddress
    int feAddressSeparator = feHttpAddress.lastIndexOf(":")
    assertTrue(feAddressSeparator > 0)
    String feHost = feHttpAddress.substring(0, feAddressSeparator)
    String fePort = feHttpAddress.substring(feAddressSeparator + 1)

    sql """ CREATE DATABASE IF NOT EXISTS ${dbName} """
    sql """ DROP TABLE IF EXISTS ${dbName}.${tableName} """
    sql """
        CREATE TABLE ${dbName}.${tableName} (
            k1 BIGINT,
            v1 STRING
        )
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        )
    """

    // Read the current FE values first so the ordinary suite can restore shared settings.
    def getFrontendConfigValue = { configKey ->
        def configRows = sql """ admin show frontend config """
        def configRow = configRows.find { it[0] == configKey }
        assertTrue(configRow != null)
        return configRow[1].toString()
    }

    String originalDrainMaxBytes = getFrontendConfigValue("stream_load_redirect_bounded_drain_max_bytes")
    String originalDrainMaxIdleTimeMs = getFrontendConfigValue("stream_load_redirect_bounded_drain_max_idle_time_ms")

    // Treat common client-side disconnects as the reproduced historical failure signal.
    def reproducedExceptionTypes = ["BrokenPipeError", "ConnectionResetError"] as Set

    // Keep the helper single-shot and let the regression test control retries and assertions.
    def runChunkedStreamLoad = {
        def command = [
            "python3",
            helperPath,
            "--host", feHost,
            "--fe-http-port", fePort,
            "--user", context.config.feHttpUser,
            "--password", context.config.feHttpPassword == null ? "" : context.config.feHttpPassword,
            "--db", dbName,
            "--table", tableName,
            "--payload-mb", "8",
            "--chunk-kb", "8",
            "--sleep-ms", "10"
        ]
        def process = command.execute()
        def code = process.waitFor()
        def out = process.in.text.trim()
        def err = process.err.text.trim()
        log.info("stream load redirect helper stdout: ${out}")
        if (!err.isEmpty()) {
            log.info("stream load redirect helper stderr: ${err}")
        }
        return [code: code, result: parseJson(out)]
    }

    try {
        // First disable the drain and reproduce the historical client disconnect behavior.
        sql """ admin set frontend config("stream_load_redirect_bounded_drain_max_bytes" = "0") """
        sql """ admin set frontend config("stream_load_redirect_bounded_drain_max_idle_time_ms" = "1") """
        boolean brokenPipeObserved = false
        for (int i = 0; i < 5; i++) {
            def attempt = runChunkedStreamLoad()
            def result = attempt.result
            if (reproducedExceptionTypes.contains(result.exception_type)) {
                brokenPipeObserved = true
                break
            }
            assertEquals(307, result.status_code as Integer)
            assertTrue(result.exception_type == null)
            assertTrue(result.exception == null)
        }
        assertTrue(brokenPipeObserved)

        // Then enable the drain and verify the same request finishes with a redirect.
        sql """ admin set frontend config("stream_load_redirect_bounded_drain_max_bytes" = "16777216") """
        sql """ admin set frontend config("stream_load_redirect_bounded_drain_max_idle_time_ms" = "2000") """
        for (int i = 0; i < 3; i++) {
            def attempt = runChunkedStreamLoad()
            def result = attempt.result
            assertEquals(0, attempt.code as Integer)
            assertEquals(307, result.status_code as Integer)
            assertTrue(result.exception_type == null)
            assertTrue(result.exception == null)
            assertTrue(result.headers.Location.contains("/api/${dbName}/${tableName}/_stream_load"))
        }
    } finally {
        // Restore the shared FE settings before leaving the ordinary regression environment.
        sql """ admin set frontend config("stream_load_redirect_bounded_drain_max_bytes" = "${originalDrainMaxBytes}") """
        sql """ admin set frontend config("stream_load_redirect_bounded_drain_max_idle_time_ms" = "${originalDrainMaxIdleTimeMs}") """
    }
}
