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

suite("test_load_error_log_concurrent_write", "nonConcurrent") {
    if (!isCloudMode()) {
        return
    }

    sql "DROP TABLE IF EXISTS test_load_error_log_concurrent_write_source"
    sql "DROP TABLE IF EXISTS test_load_error_log_concurrent_write_target"
    sql """
        CREATE TABLE test_load_error_log_concurrent_write_source (
            id INT,
            value STRING
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        CREATE TABLE test_load_error_log_concurrent_write_target (
            id INT,
            value VARCHAR(4)
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO test_load_error_log_concurrent_write_source
        VALUES (1, 'value-too-long')
    """
    sql "SET enable_insert_strict = true"
    sql "SET enable_insert_value_auto_cast = false"

    setBeConfigTemporary([
            save_load_error_log_to_s3: true,
            pipeline_status_report_interval: 1
    ]) {
        try {
            GetDebugPoint().clearDebugPointsForAllBEs()
            // Keep the first writer between opening the file and writing its first row long enough
            // for periodic status reporting to request the error log URL concurrently.
            GetDebugPoint().enableDebugPointForAllBEs(
                    "RuntimeState::append_error_msg_to_file.sleep_before_write",
                    ["sleep_ms": "5000", "execute": "1"])

            test {
                sql """
                    INSERT INTO test_load_error_log_concurrent_write_target
                    SELECT id, value FROM test_load_error_log_concurrent_write_source
                """
                check { result, exception, startTime, endTime ->
                    assertNotNull(exception)
                    def errorMessage = exception.toString()
                    assertTrue(errorMessage.contains("first_error_msg"))

                    def urlMatcher = errorMessage =~ /\. url: (https?:\/\/\S+)$/
                    assertTrue(urlMatcher.find(), "No error URL found in: ${errorMessage}")
                    def errorUrl = urlMatcher.group(1)
                    def (code, out, err) = curl("GET", errorUrl)
                    assertEquals(0, code)
                    assertTrue(out.contains("Reason:"), "Empty error log from ${errorUrl}")
                }
            }
        } finally {
            GetDebugPoint().clearDebugPointsForAllBEs()
        }
    }
}
