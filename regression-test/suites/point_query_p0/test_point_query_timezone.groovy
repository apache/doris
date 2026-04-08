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

// Test point query timezone handling (cir_19478)
// Point queries on unique key tables with row store should respect
// session timezone for functions like from_unixtime()
suite("test_point_query_timezone") {
    def tableName = "test_point_query_timezone_tbl"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE ${tableName} (
            `job_id` VARCHAR(50) NOT NULL,
            `capture_time` BIGINT NOT NULL,
            `event_time` BIGINT NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`job_id`, `capture_time`)
        DISTRIBUTED BY HASH(`job_id`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "store_row_column" = "true",
            "enable_unique_key_merge_on_write" = "true"
        )
    """

    sql """ INSERT INTO ${tableName} VALUES ('job_001', 1706745600000, 1706745600000) """
    sql """ INSERT INTO ${tableName} VALUES ('job_002', 1706832000000, 1706832000000) """

    // Verify it's a short-circuit (point) query
    explain {
        sql("SELECT * FROM ${tableName} WHERE job_id = 'job_001' AND capture_time = 1706745600000")
        contains "SHORT-CIRCUIT"
    }

    // Test with America/Mexico_City timezone (UTC-6)
    // 1706745600 = 2024-02-01 00:00:00 UTC = 2024-01-31 18:00:00 Mexico_City
    sql """ SET time_zone = 'America/Mexico_City' """

    // Full table scan - should use session timezone
    qt_full_scan """ SELECT job_id, from_unixtime(event_time/1000) AS t FROM ${tableName} ORDER BY job_id """

    // Point query - should also use session timezone (this was the bug)
    qt_point_query """ SELECT job_id, from_unixtime(event_time/1000) AS t FROM ${tableName} WHERE job_id = 'job_001' AND capture_time = 1706745600000 """

    // Test with Asia/Tokyo timezone (UTC+9)
    // 1706745600 = 2024-02-01 00:00:00 UTC = 2024-02-01 09:00:00 Tokyo
    sql """ SET time_zone = 'Asia/Tokyo' """

    qt_full_scan_tokyo """ SELECT job_id, from_unixtime(event_time/1000) AS t FROM ${tableName} ORDER BY job_id """

    qt_point_query_tokyo """ SELECT job_id, from_unixtime(event_time/1000) AS t FROM ${tableName} WHERE job_id = 'job_001' AND capture_time = 1706745600000 """

    // Reset timezone
    sql """ SET time_zone = 'Asia/Shanghai' """

    sql """ DROP TABLE IF EXISTS ${tableName} """
}
