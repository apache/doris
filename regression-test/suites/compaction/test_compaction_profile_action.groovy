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

import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_compaction_profile_action", "p0") {
    def tableName = "test_compaction_profile_action_tbl"

    // Step 1: Setup - get backend info
    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort)

    String backend_id = backendId_to_backendIP.keySet()[0]
    def beHost = backendId_to_backendIP[backend_id]
    def beHttpPort = backendId_to_backendHttpPort[backend_id]
    def baseUrl = "http://${beHost}:${beHttpPort}/api/compaction/profile"

    try {
        // Step 2: Create table, insert data, trigger compaction, wait for completion
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                `id` INT NOT NULL,
                `name` VARCHAR(128) NOT NULL,
                `value` INT NOT NULL,
                `ts` DATETIME NOT NULL
            )
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "disable_auto_compaction" = "true"
            )
        """

        // Insert several batches to create multiple rowsets
        for (int i = 0; i < 5; i++) {
            sql """ INSERT INTO ${tableName} VALUES
                (${i * 10 + 1}, 'name_${i}_1', ${i * 100 + 1}, '2025-01-01 00:00:00'),
                (${i * 10 + 2}, 'name_${i}_2', ${i * 100 + 2}, '2025-01-01 00:00:01'),
                (${i * 10 + 3}, 'name_${i}_3', ${i * 100 + 3}, '2025-01-01 00:00:02'),
                (${i * 10 + 4}, 'name_${i}_4', ${i * 100 + 4}, '2025-01-01 00:00:03'),
                (${i * 10 + 5}, 'name_${i}_5', ${i * 100 + 5}, '2025-01-01 00:00:04')
            """
        }

        // Get tablet info
        def tablets = sql_return_maparray """ show tablets from ${tableName}; """
        assertTrue(tablets.size() > 0)
        def tablet = tablets[0]
        String tablet_id = tablet.TabletId

        // Trigger compaction and wait for completion
        trigger_and_wait_compaction(tableName, "cumulative")

        // Step 3: Basic query - GET /api/compaction/profile - verify JSON response format
        def (code, out, err) = curl("GET", baseUrl)
        logger.info("Basic profile query: code=" + code + ", out=" + out + ", err=" + err)
        assertEquals(0, code)
        def json = parseJson(out.trim())
        assertEquals("Success", json.status)

        // Step 4: Verify response contains compaction_profiles array
        assertTrue(json.compaction_profiles != null, "Response should contain compaction_profiles")
        assertTrue(json.compaction_profiles instanceof List, "compaction_profiles should be a list")
        assertTrue(json.compaction_profiles.size() > 0, "compaction_profiles should not be empty after compaction")

        // Step 5: Verify a profile record has expected fields
        def profile = json.compaction_profiles[0]
        assertTrue(profile.compaction_id != null, "compaction_id should be present")
        assertTrue(profile.compaction_type != null, "compaction_type should be present")
        assertTrue(profile.tablet_id != null, "tablet_id should be present")
        assertTrue(profile.table_id != null, "table_id should be present")
        assertTrue(profile.partition_id != null, "partition_id should be present")
        assertTrue(profile.trigger_method != null, "trigger_method should be present")
        assertTrue(profile.compaction_score != null, "compaction_score should be present")
        assertTrue(profile.scheduled_time != null, "scheduled_time should be present")
        assertTrue(profile.start_time != null, "start_time should be present")
        assertTrue(profile.end_time != null, "end_time should be present")
        assertTrue(profile.cost_time_ms != null, "cost_time_ms should be present")
        assertTrue(profile.containsKey("success"), "success should be present")
        assertTrue(profile.input_rowsets_count != null, "input_rowsets_count should be present")
        assertTrue(profile.input_row_num != null, "input_row_num should be present")
        assertTrue(profile.input_data_size != null, "input_data_size should be present")
        assertTrue(profile.input_index_size != null, "input_index_size should be present")
        assertTrue(profile.input_total_size != null, "input_total_size should be present")
        assertTrue(profile.input_segments_num != null, "input_segments_num should be present")
        assertTrue(profile.input_version_range != null, "input_version_range should be present")
        assertTrue(profile.containsKey("merged_rows"), "merged_rows should be present")
        assertTrue(profile.containsKey("filtered_rows"), "filtered_rows should be present")
        assertTrue(profile.containsKey("output_rows"), "output_rows should be present")
        assertTrue(profile.output_row_num != null, "output_row_num should be present")
        assertTrue(profile.output_data_size != null, "output_data_size should be present")
        assertTrue(profile.output_index_size != null, "output_index_size should be present")
        assertTrue(profile.output_total_size != null, "output_total_size should be present")
        assertTrue(profile.output_segments_num != null, "output_segments_num should be present")
        assertTrue(profile.containsKey("merge_latency_ms"), "merge_latency_ms should be present")
        assertTrue(profile.containsKey("bytes_read_from_local"), "bytes_read_from_local should be present")
        assertTrue(profile.containsKey("bytes_read_from_remote"), "bytes_read_from_remote should be present")
        assertTrue(profile.containsKey("peak_memory_bytes"), "peak_memory_bytes should be present")
        assertTrue(profile.containsKey("is_vertical"), "is_vertical should be present")
        assertTrue(profile.containsKey("permits"), "permits should be present")
        assertTrue(profile.containsKey("vertical_total_groups"), "vertical_total_groups should be present")
        assertTrue(profile.containsKey("vertical_completed_groups"), "vertical_completed_groups should be present")

        // Verify field values are reasonable
        assertTrue(profile.compaction_id > 0, "compaction_id should be positive")
        assertTrue(profile.cost_time_ms >= 0, "cost_time_ms should be non-negative")

        // Step 6: Test top_n filter - ?top_n=1 returns exactly 1 record
        def (code2, out2, err2) = curl("GET", baseUrl + "?top_n=1")
        logger.info("top_n=1 query: code=" + code2 + ", out=" + out2)
        assertEquals(0, code2)
        def json2 = parseJson(out2.trim())
        assertEquals("Success", json2.status)
        assertEquals(1, json2.compaction_profiles.size())

        // Step 7: Test tablet_id filter - ?tablet_id=X returns records for that tablet
        def (code3, out3, err3) = curl("GET", baseUrl + "?tablet_id=" + tablet_id)
        logger.info("tablet_id filter query: code=" + code3 + ", out=" + out3)
        assertEquals(0, code3)
        def json3 = parseJson(out3.trim())
        assertEquals("Success", json3.status)
        assertTrue(json3.compaction_profiles.size() > 0, "Should have profiles for tablet " + tablet_id)
        for (def p : json3.compaction_profiles) {
            assertEquals(Long.parseLong(tablet_id), p.tablet_id,
                    "All returned profiles should match the requested tablet_id")
        }

        // Verify field values using profile from our specific tablet
        def tabletProfile = json3.compaction_profiles[0]
        assertTrue(tabletProfile.input_data_size > 0, "input_data_size should be positive")
        assertTrue(tabletProfile.input_rowsets_count > 0, "input_rowsets_count should be positive")
        assertTrue(tabletProfile.input_row_num > 0, "input_row_num should be positive")

        // Step 8: Test compact_type filter - ?compact_type=cumulative
        def (code4, out4, err4) = curl("GET", baseUrl + "?compact_type=cumulative")
        logger.info("compact_type filter query: code=" + code4 + ", out=" + out4)
        assertEquals(0, code4)
        def json4 = parseJson(out4.trim())
        assertEquals("Success", json4.status)
        assertTrue(json4.compaction_profiles.size() > 0, "Should have cumulative compaction profiles")
        for (def p : json4.compaction_profiles) {
            assertEquals("cumulative", p.compaction_type,
                    "All returned profiles should be cumulative type")
        }

        // Step 9: Test combined filters - ?tablet_id=X&top_n=1
        def (code5, out5, err5) = curl("GET", baseUrl + "?tablet_id=" + tablet_id + "&top_n=1")
        logger.info("Combined filter query: code=" + code5 + ", out=" + out5)
        assertEquals(0, code5)
        def json5 = parseJson(out5.trim())
        assertEquals("Success", json5.status)
        assertTrue(json5.compaction_profiles.size() <= 1, "top_n=1 should return at most 1 record")
        if (json5.compaction_profiles.size() > 0) {
            assertEquals(Long.parseLong(tablet_id), json5.compaction_profiles[0].tablet_id,
                    "Returned profile should match the requested tablet_id")
        }

        // Step 10: Test invalid params - ?top_n=-1 returns error
        def (code6, out6, err6) = curl("GET", baseUrl + "?top_n=-1")
        logger.info("Invalid top_n query: code=" + code6 + ", out=" + out6)
        assertEquals(0, code6)
        def json6 = parseJson(out6.trim())
        // Server should return an error status for invalid top_n
        assertTrue(json6.status != "Success" || out6.contains("top_n must be non-negative"),
                "Invalid top_n=-1 should return error response")

        // Step 11: Test non-existent tablet - returns empty array
        def (code7, out7, err7) = curl("GET", baseUrl + "?tablet_id=999999999")
        logger.info("Non-existent tablet query: code=" + code7 + ", out=" + out7)
        assertEquals(0, code7)
        def json7 = parseJson(out7.trim())
        assertEquals("Success", json7.status)
        assertEquals(0, json7.compaction_profiles.size())

    } finally {
        // Step 12: Cleanup
        try_sql("DROP TABLE IF EXISTS ${tableName} FORCE")
    }
}
