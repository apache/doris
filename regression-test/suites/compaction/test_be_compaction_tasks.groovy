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

suite("test_be_compaction_tasks", "p0") {
    def tableName = "test_be_compaction_tasks_tbl"

    // Step 1: Setup - get backend info
    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort)

    String backend_id = backendId_to_backendIP.keySet()[0]

    try {
        // Step 2: Create table with disable_auto_compaction
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

        // Step 3: Insert several batches of data to create multiple rowsets
        for (int i = 0; i < 5; i++) {
            sql """ INSERT INTO ${tableName} VALUES
                (${i * 10 + 1}, 'name_${i}_1', ${i * 100 + 1}, '2025-01-01 00:00:00'),
                (${i * 10 + 2}, 'name_${i}_2', ${i * 100 + 2}, '2025-01-01 00:00:01'),
                (${i * 10 + 3}, 'name_${i}_3', ${i * 100 + 3}, '2025-01-01 00:00:02'),
                (${i * 10 + 4}, 'name_${i}_4', ${i * 100 + 4}, '2025-01-01 00:00:03'),
                (${i * 10 + 5}, 'name_${i}_5', ${i * 100 + 5}, '2025-01-01 00:00:04')
            """
        }

        // Step 4: Basic query - verify system table returns valid results
        def basicResult = sql """ SELECT * FROM information_schema.be_compaction_tasks """
        logger.info("Basic query result rows: " + basicResult.size())
        // The table should be queryable (may or may not have rows depending on BE state)
        assertTrue(basicResult != null)

        // Step 5: DESC check - verify 38 columns
        def descResult = sql """ DESC information_schema.be_compaction_tasks """
        logger.info("DESC result rows: " + descResult.size())
        assertEquals(38, descResult.size())

        // Get tablet info for compaction trigger
        def tablets = sql_return_maparray """ show tablets from ${tableName}; """
        assertTrue(tablets.size() > 0)
        def tablet = tablets[0]
        String tablet_id = tablet.TabletId

        // Step 6: Trigger cumulative compaction via HTTP API and wait for completion
        trigger_and_wait_compaction(tableName, "cumulative")

        // Step 7: After compaction - query system table with WHERE STATUS = 'FINISHED'
        def finishedResult = sql """ SELECT * FROM information_schema.be_compaction_tasks WHERE STATUS = 'FINISHED' """
        logger.info("Finished compaction tasks: " + finishedResult.size())
        assertTrue(finishedResult.size() > 0, "Expected at least one FINISHED compaction task")

        // Step 8: Field validation - verify key fields are non-null and reasonable
        // Query specific fields for the completed compaction on our tablet
        def fieldResult = sql_return_maparray """
            SELECT BACKEND_ID, COMPACTION_ID, TABLE_ID, PARTITION_ID, TABLET_ID,
                   COMPACTION_TYPE, STATUS, TRIGGER_METHOD, COMPACTION_SCORE,
                   SCHEDULED_TIME, START_TIME, END_TIME, ELAPSED_TIME_MS,
                   INPUT_ROWSETS_COUNT, INPUT_ROW_NUM, INPUT_DATA_SIZE,
                   INPUT_INDEX_SIZE, INPUT_TOTAL_SIZE, INPUT_SEGMENTS_NUM,
                   INPUT_VERSION_RANGE,
                   OUTPUT_ROW_NUM, OUTPUT_DATA_SIZE, OUTPUT_SEGMENTS_NUM,
                   IS_VERTICAL
            FROM information_schema.be_compaction_tasks
            WHERE TABLET_ID = ${tablet_id} AND STATUS = 'FINISHED'
            ORDER BY COMPACTION_ID DESC
            LIMIT 1
        """
        logger.info("Field validation result: " + fieldResult)
        assertTrue(fieldResult.size() > 0, "Expected FINISHED record for tablet " + tablet_id)

        def record = fieldResult[0]
        // Verify key fields are non-null
        assertTrue(record.BACKEND_ID != null && Long.parseLong(record.BACKEND_ID.toString()) > 0,
                "BACKEND_ID should be positive")
        assertTrue(record.COMPACTION_ID != null && Long.parseLong(record.COMPACTION_ID.toString()) > 0,
                "COMPACTION_ID should be positive")
        assertTrue(record.TABLE_ID != null && Long.parseLong(record.TABLE_ID.toString()) > 0,
                "TABLE_ID should be positive")
        assertTrue(record.TABLET_ID != null && record.TABLET_ID.toString() == tablet_id,
                "TABLET_ID should match")
        assertTrue(record.COMPACTION_TYPE != null && record.COMPACTION_TYPE.toString() == "cumulative",
                "COMPACTION_TYPE should be cumulative")
        assertTrue(record.STATUS != null && record.STATUS.toString() == "FINISHED",
                "STATUS should be FINISHED")
        assertTrue(record.SCHEDULED_TIME != null, "SCHEDULED_TIME should not be null")
        assertTrue(record.START_TIME != null, "START_TIME should not be null")
        assertTrue(record.END_TIME != null, "END_TIME should not be null")
        assertTrue(record.INPUT_DATA_SIZE != null && Long.parseLong(record.INPUT_DATA_SIZE.toString()) > 0,
                "INPUT_DATA_SIZE should be positive")
        assertTrue(record.INPUT_ROWSETS_COUNT != null && Long.parseLong(record.INPUT_ROWSETS_COUNT.toString()) > 0,
                "INPUT_ROWSETS_COUNT should be positive")
        assertTrue(record.INPUT_ROW_NUM != null && Long.parseLong(record.INPUT_ROW_NUM.toString()) > 0,
                "INPUT_ROW_NUM should be positive")
        assertTrue(record.OUTPUT_ROW_NUM != null && Long.parseLong(record.OUTPUT_ROW_NUM.toString()) >= 0,
                "OUTPUT_ROW_NUM should be non-negative")
        assertTrue(record.OUTPUT_DATA_SIZE != null && Long.parseLong(record.OUTPUT_DATA_SIZE.toString()) >= 0,
                "OUTPUT_DATA_SIZE should be non-negative")

        // Verify fields from Finding 2/3 fix: these were previously always 0
        assertTrue(record.INPUT_INDEX_SIZE != null && Long.parseLong(record.INPUT_INDEX_SIZE.toString()) >= 0,
                "INPUT_INDEX_SIZE should be non-negative")
        assertTrue(record.INPUT_TOTAL_SIZE != null && Long.parseLong(record.INPUT_TOTAL_SIZE.toString()) > 0,
                "INPUT_TOTAL_SIZE should be positive")
        assertTrue(record.INPUT_SEGMENTS_NUM != null && Long.parseLong(record.INPUT_SEGMENTS_NUM.toString()) > 0,
                "INPUT_SEGMENTS_NUM should be positive")
        assertTrue(record.INPUT_VERSION_RANGE != null && record.INPUT_VERSION_RANGE.toString().length() > 0,
                "INPUT_VERSION_RANGE should be non-empty, got: " + record.INPUT_VERSION_RANGE)
        assertTrue(record.COMPACTION_SCORE != null && Long.parseLong(record.COMPACTION_SCORE.toString()) >= 0,
                "COMPACTION_SCORE should be non-negative")

        // Step 9: TRIGGER_METHOD check - manual triggered should show 'MANUAL'
        assertTrue(record.TRIGGER_METHOD != null && record.TRIGGER_METHOD.toString() == "MANUAL",
                "TRIGGER_METHOD should be MANUAL for manually triggered compaction, got: " + record.TRIGGER_METHOD)

        // Step 10: Filter test - WHERE tablet_id = X AND status = 'FINISHED'
        def filterResult = sql """
            SELECT COUNT(*) FROM information_schema.be_compaction_tasks
            WHERE TABLET_ID = ${tablet_id} AND STATUS = 'FINISHED'
        """
        logger.info("Filter result (tablet_id + status): " + filterResult)
        assertTrue(filterResult[0][0] > 0, "Expected at least one record matching filter")

        // Step 11: Non-existent tablet - WHERE tablet_id = 999999999 returns empty
        def emptyResult = sql """
            SELECT * FROM information_schema.be_compaction_tasks
            WHERE TABLET_ID = 999999999
        """
        logger.info("Non-existent tablet result: " + emptyResult.size())
        assertEquals(0, emptyResult.size())

    } finally {
        // Step 12: Cleanup
        try_sql("DROP TABLE IF EXISTS ${tableName} FORCE")
    }
}
