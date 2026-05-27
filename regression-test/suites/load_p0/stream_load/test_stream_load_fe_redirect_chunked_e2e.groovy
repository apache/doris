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

suite("test_stream_load_fe_redirect_chunked_e2e", "docker") {
    def options = new ClusterOptions()
    options.feNum = 1
    options.beNum = 1
    options.cloudMode = false

    docker(options) {
        String dbName = context.config.getDbNameByFile(context.file)
        def feIp = cluster.getMasterFe().getHttpAddress()[0]
        def fePort = cluster.getMasterFe().getHttpAddress()[1]
        def helperPath = "${context.file.parent}/scripts/stream_load_redirect_chunked_e2e.py"

        sql """ CREATE DATABASE IF NOT EXISTS ${dbName} """
        sql """ DROP TABLE IF EXISTS ${dbName}.test_stream_load_fe_redirect_chunked_e2e """
        sql """
            CREATE TABLE ${dbName}.test_stream_load_fe_redirect_chunked_e2e (
                k1 BIGINT,
                v1 STRING
            )
            DUPLICATE KEY(k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1"
            )
        """

        // Keep the helper single-shot and let the regression test control retries and assertions.
        def runChunkedStreamLoad = {
            def command = [
                "python3",
                helperPath,
                "--host", feIp.toString(),
                "--fe-http-port", fePort.toString(),
                "--user", context.config.feHttpUser,
                "--password", context.config.feHttpPassword == null ? "" : context.config.feHttpPassword,
                "--db", dbName,
                "--table", "test_stream_load_fe_redirect_chunked_e2e",
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

        // First disable the drain and reproduce the historical broken-pipe behavior.
        sql """ admin set frontend config("stream_load_redirect_bounded_drain_max_bytes" = "0") """
        sql """ admin set frontend config("stream_load_redirect_bounded_drain_max_idle_time_ms" = "2000") """
        boolean brokenPipeObserved = false
        for (int i = 0; i < 5; i++) {
            def attempt = runChunkedStreamLoad()
            def result = attempt.result
            if (result.exception_type == "BrokenPipeError") {
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
            assertTrue(result.headers.Location.contains("/api/${dbName}/test_stream_load_fe_redirect_chunked_e2e/_stream_load"))
        }
    }
}
