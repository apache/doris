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

suite("test_variant_compaction_empty_path_bug", "nonConcurrent") {
    def variantV2Function = "parse_to_variant"
    def tableName = "test_variant_empty_path_compaction"

    try {
        sql "DROP TABLE IF EXISTS ${tableName}"

        // Create table with variant column
        // Set variant_max_subcolumns_count to 3, so any columns beyond the top 3 will become sparse
        // This triggers the sparse column merge logic during compaction
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                k bigint,
                v variant< properties("variant_max_subcolumns_count" = "3")>
            )
            DUPLICATE KEY(`k`)
            DISTRIBUTED BY HASH(k) BUCKETS 1
            properties("replication_num" = "1", "disable_auto_compaction" = "true");
        """

        logger.info("Testing variant compaction with empty path in sparse columns")

        // Insert data with multiple different subcolumns
        // Strategy: Insert 6+ different subcolumns to exceed the limit of 3
        // The most frequently used 3 columns will be materialized, others will be sparse

        // First batch: establish column usage patterns
        sql """INSERT INTO ${tableName} VALUES
            (1, ${variantV2Function}('{"a": 1, "b": 2, "c": 3}')),
            (2, ${variantV2Function}('{"a": 10, "b": 20, "c": 30}')),
            (3, ${variantV2Function}('{"a": 100, "b": 200, "c": 300}'))
        """

        // Second batch: introduce additional columns that will become sparse
        sql """INSERT INTO ${tableName} VALUES
            (4, ${variantV2Function}('{"a": 1, "d": 4, "e": 5, "f": 6}')),
            (5, ${variantV2Function}('{"b": 2, "d": 40, "e": 50, "f": 60}')),
            (6, ${variantV2Function}('{"c": 3, "d": 400, "e": 500, "f": 600}'))
        """

        // Third batch: more sparse columns
        sql """INSERT INTO ${tableName} VALUES
            (7, ${variantV2Function}('{"a": 7, "g": 70, "h": 700}')),
            (8, ${variantV2Function}('{"b": 8, "g": 80, "h": 800}')),
            (9, ${variantV2Function}('{"c": 9, "g": 90, "h": 900}'))
        """

        // Additional inserts to create more rowsets for compaction
        sql """INSERT INTO ${tableName} VALUES
            (10, ${variantV2Function}('{"a": 13, "d": 130}')),
            (11, ${variantV2Function}('{"b": 14, "e": 140}')),
            (12, ${variantV2Function}('{"c": 15, "f": 150}'))
        """

        sql """INSERT INTO ${tableName} VALUES
            (13, ${variantV2Function}('{"d": 16, "g": 160}')),
            (14, ${variantV2Function}('{"e": 17, "h": 170}')),
            (15, ${variantV2Function}('{"f": 18, "a": 180}'))
        """

        // Verify data before compaction
        def count_before = sql "SELECT COUNT(*) FROM ${tableName}"
        logger.info("Row count before compaction: ${count_before[0][0]}")
        assertEquals(15, count_before[0][0])

        // Query to verify data integrity before compaction
        qt_before_compaction "SELECT k, cast(v as string) FROM ${tableName} ORDER BY k"

        // Test specific column access
        qt_col_a_before "SELECT k, v['a'] FROM ${tableName} WHERE v['a'] IS NOT NULL ORDER BY k"
        qt_col_d_before "SELECT k, v['d'] FROM ${tableName} WHERE v['d'] IS NOT NULL ORDER BY k"

        logger.info("Data inserted, now triggering compaction...")
        logger.info("Expected behavior: columns a,b,c materialized, d,e,f,g,h as sparse")
        logger.info("Bug scenario: if root node (empty path) is not skipped in _create_sparse_merge_reader")
        logger.info("             it will call VariantColumnReader::new_iterator with 3 params")
        logger.info("             which returns NOT_IMPLEMENTED_ERROR")

        // Trigger compaction - this may reproduce the NOT_IMPLEMENTED_ERROR bug
        def tablets = sql_return_maparray "SHOW TABLETS FROM ${tableName}"

        try {
            trigger_and_wait_compaction(tableName, "cumulative", 1800)
            logger.info("Compaction completed successfully")

            // Verify data after compaction
            def count_after = sql "SELECT COUNT(*) FROM ${tableName}"
            logger.info("Row count after compaction: ${count_after[0][0]}")
            assertEquals(15, count_after[0][0])

            // Query to verify data integrity after compaction
            qt_after_compaction "SELECT k, cast(v as string) FROM ${tableName} ORDER BY k"

            // Test specific column access after compaction
            qt_col_a_after "SELECT k, v['a'] FROM ${tableName} WHERE v['a'] IS NOT NULL ORDER BY k"
            qt_col_d_after "SELECT k, v['d'] FROM ${tableName} WHERE v['d'] IS NOT NULL ORDER BY k"

        } catch (Exception e) {
            logger.error("Compaction failed with error: ${e.getMessage()}", e)

            // Check if the error is the expected NOT_IMPLEMENTED_ERROR
            if (e.getMessage().contains("NOT_IMPLEMENTED_ERROR") ||
                e.getMessage().contains("Not implemented")) {
                logger.error("BUG REPRODUCED: Compaction failed with NOT_IMPLEMENTED_ERROR")
                throw e
            } else {
                // Different error, rethrow
                throw e
            }
        }

    } finally {
        sql "DROP TABLE IF EXISTS ${tableName}"
    }
}
