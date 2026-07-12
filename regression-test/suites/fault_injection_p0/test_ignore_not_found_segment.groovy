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

suite("test_ignore_not_found_segment", "nonConcurrent") {
    def tableName = "test_ignore_not_found_segment_tbl"

    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort)

    def set_be_config = { key, value ->
        for (String backend_id: backendId_to_backendIP.keySet()) {
            def (code, out, err) = update_be_config(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), key, value)
            logger.info("set be config ${key}=${value}, code: ${code}, out: ${out}, err: ${err}")
        }
    }

    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE ${tableName} (
            `k1` int NOT NULL,
            `v1` string NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`)
        DISTRIBUTED BY HASH(`k1`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "disable_auto_compaction" = "true"
        );
    """

    // Insert data across multiple segments (each INSERT creates a new segment)
    sql "INSERT INTO ${tableName} VALUES (1, 'aaa'), (2, 'bbb');"
    sql "INSERT INTO ${tableName} VALUES (3, 'ccc'), (4, 'ddd');"
    sql "INSERT INTO ${tableName} VALUES (5, 'eee'), (6, 'fff');"

    // NOTE: Do NOT query the table before fault injection tests.
    // Any query would populate the segment LRU cache, and disable_segment_cache
    // only prevents new insertions — it does not block lookups from existing cache.
    // Keep disable_segment_cache=true throughout ALL fault injection tests to prevent
    // segments from being cached between test cases.

    set_be_config.call("disable_segment_cache", "true")
    try {
        // Test 1: With ignore_not_found_segment=true (default), injecting NOT_FOUND
        // should return 0 rows since all segments fail to load and are skipped.
        try {
            set_be_config.call("ignore_not_found_segment", "true")
            GetDebugPoint().enableDebugPointForAllBEs("BetaRowset::load_segment.return_not_found")

            qt_ignore_enabled "SELECT count(*) FROM ${tableName}"
        } finally {
            GetDebugPoint().disableDebugPointForAllBEs("BetaRowset::load_segment.return_not_found")
        }

        // Test 2: With ignore_not_found_segment=false, injecting NOT_FOUND should cause query failure
        try {
            set_be_config.call("ignore_not_found_segment", "false")
            GetDebugPoint().enableDebugPointForAllBEs("BetaRowset::load_segment.return_not_found")

            test {
                sql "SELECT count(*) FROM ${tableName}"
                exception "NOT_FOUND"
            }
        } finally {
            GetDebugPoint().disableDebugPointForAllBEs("BetaRowset::load_segment.return_not_found")
        }

        // Test 3: With ignore_not_found_segment=true, injecting IO_ERROR
        // should return 0 rows since all segments fail to load and are skipped.
        try {
            set_be_config.call("ignore_not_found_segment", "true")
            GetDebugPoint().enableDebugPointForAllBEs("BetaRowset::load_segment.return_io_error")

            qt_ignore_io_error "SELECT count(*) FROM ${tableName}"
        } finally {
            GetDebugPoint().disableDebugPointForAllBEs("BetaRowset::load_segment.return_io_error")
        }

        // Test 4: With ignore_not_found_segment=false, injecting IO_ERROR should cause query failure
        try {
            set_be_config.call("ignore_not_found_segment", "false")
            GetDebugPoint().enableDebugPointForAllBEs("BetaRowset::load_segment.return_io_error")

            test {
                sql "SELECT count(*) FROM ${tableName}"
                exception "IO_ERROR"
            }
        } finally {
            GetDebugPoint().disableDebugPointForAllBEs("BetaRowset::load_segment.return_io_error")
        }
    } finally {
        set_be_config.call("disable_segment_cache", "false")
        set_be_config.call("ignore_not_found_segment", "true")
    }

    // Test 5: After clearing the debug point, data should be fully accessible again
    qt_recovery "SELECT count(*) FROM ${tableName}"
}
