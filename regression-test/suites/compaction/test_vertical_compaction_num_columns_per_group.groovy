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

suite("test_vertical_compaction_num_columns_per_group", "nonConcurrent") {
    def tableName = "test_columns_per_group"

    // Test 1: Create table with property set to 2
    sql """ DROP TABLE IF EXISTS ${tableName}_2 """
    sql """ CREATE TABLE ${tableName}_2 (
        k1 INT,
        v1 INT, v2 INT, v3 INT, v4 INT, v5 INT,
        v6 INT, v7 INT, v8 INT, v9 INT, v10 INT
    ) DUPLICATE KEY(k1)
    DISTRIBUTED BY HASH(k1) BUCKETS 1
    PROPERTIES (
        "replication_num" = "1",
        "vertical_compaction_num_columns_per_group" = "2",
        "disable_auto_compaction" = "true"
    ); """

    // Get tablet info
    def tablets = sql_return_maparray """ SHOW TABLETS FROM ${tableName}_2; """
    def tablet_id = tablets[0].TabletId
    logger.info("Test 1 - tablet_id: ${tablet_id}")

    // Verify SHOW CREATE TABLE contains property
    def createTableResult = sql """ SHOW CREATE TABLE ${tableName}_2; """
    def createTableStr = createTableResult[0][1]
    logger.info("Test 1 - SHOW CREATE TABLE: ${createTableStr}")
    assertTrue(createTableStr.contains('"vertical_compaction_num_columns_per_group" = "2"'),
        "SHOW CREATE TABLE should contain vertical_compaction_num_columns_per_group=2 after CREATE")

    // Enable debug point with tablet_id matching - if value doesn't match, BE will LOG(FATAL)
    try {
        GetDebugPoint().enableDebugPointForAllBEs(
            "Merger.vertical_merge_rowsets.check_num_columns_per_group",
            [expected_value: "2", tablet_id: "${tablet_id}"])

        // Insert data to trigger compaction
        for (int i = 0; i < 5; i++) {
            sql """ INSERT INTO ${tableName}_2 VALUES
                    (${i}, ${i}, ${i}, ${i}, ${i}, ${i},
                     ${i}, ${i}, ${i}, ${i}, ${i}); """
        }

        // Trigger and wait for compaction
        trigger_and_wait_compaction("${tableName}_2", "full")
        logger.info("Test 1 - Compaction finished, value 2 verified for tablet ${tablet_id}")

    } finally {
        GetDebugPoint().disableDebugPointForAllBEs(
            "Merger.vertical_merge_rowsets.check_num_columns_per_group")
    }

    // Test 2: Create table without setting property (should use default value 5)
    sql """ DROP TABLE IF EXISTS ${tableName}_default """
    sql """ CREATE TABLE ${tableName}_default (
        k1 INT,
        v1 INT, v2 INT, v3 INT, v4 INT, v5 INT
    ) DUPLICATE KEY(k1)
    DISTRIBUTED BY HASH(k1) BUCKETS 1
    PROPERTIES (
        "replication_num" = "1",
        "disable_auto_compaction" = "true"
    ); """

    def tablets_default = sql_return_maparray """ SHOW TABLETS FROM ${tableName}_default; """
    def tablet_id_default = tablets_default[0].TabletId
    logger.info("Test 2 - tablet_id_default: ${tablet_id_default}")

    try {
        // Verify default value is 5
        GetDebugPoint().enableDebugPointForAllBEs(
            "Merger.vertical_merge_rowsets.check_num_columns_per_group",
            [expected_value: "5", tablet_id: "${tablet_id_default}"])

        // Insert data
        for (int i = 0; i < 5; i++) {
            sql """ INSERT INTO ${tableName}_default VALUES (${i}, ${i}, ${i}, ${i}, ${i}, ${i}); """
        }

        // Trigger and wait for compaction
        trigger_and_wait_compaction("${tableName}_default", "full")
        logger.info("Test 2 - Compaction finished, default value 5 verified for tablet ${tablet_id_default}")

    } finally {
        GetDebugPoint().disableDebugPointForAllBEs(
            "Merger.vertical_merge_rowsets.check_num_columns_per_group")
    }

    // Test 3: ALTER TABLE to modify property from 2 to 10
    sql """ ALTER TABLE ${tableName}_2 SET ("vertical_compaction_num_columns_per_group" = "10"); """
    logger.info("Test 3 - ALTER TABLE executed, changed value from 2 to 10")
    sql """sync"""
    
    Thread.sleep(1000)
    // Verify SHOW CREATE TABLE reflects the change
    def createTableResult3 = sql """ SHOW CREATE TABLE ${tableName}_2; """
    def createTableStr3 = createTableResult3[0][1]
    logger.info("Test 3 - SHOW CREATE TABLE after ALTER: ${createTableStr3}")
    assertTrue(createTableStr3.contains('"vertical_compaction_num_columns_per_group" = "10"'),
        "SHOW CREATE TABLE should contain vertical_compaction_num_columns_per_group=10 after ALTER")


    // Wait for ALTER TABLE to take effect
    // In cloud mode, BE syncs from MS which may take longer
    if (isCloudMode()) {
        return
    }
    try {
        // Verify modified value is 10
        GetDebugPoint().enableDebugPointForAllBEs(
            "Merger.vertical_merge_rowsets.check_num_columns_per_group",
            [expected_value: "10", tablet_id: "${tablet_id}"])

        // Insert more data
        for (int i = 5; i < 10; i++) {
            sql """ INSERT INTO ${tableName}_2 VALUES
                    (${i}, ${i}, ${i}, ${i}, ${i}, ${i},
                     ${i}, ${i}, ${i}, ${i}, ${i}); """
        }

        // Trigger and wait for compaction
        trigger_and_wait_compaction("${tableName}_2", "full")
        logger.info("Test 3 - Compaction finished, altered value 10 verified for tablet ${tablet_id}")

    } finally {
        GetDebugPoint().disableDebugPointForAllBEs(
            "Merger.vertical_merge_rowsets.check_num_columns_per_group")
    }

    // Verify data correctness
    def result = sql """ SELECT COUNT(*) FROM ${tableName}_2; """
    assertEquals(10, result[0][0])

    // Check tablet meta shows correct value
    def tabletMeta = sql_return_maparray """ SHOW TABLETS FROM ${tableName}_2; """
    logger.info("Test 3 - tablet meta after ALTER: ${tabletMeta[0]}")

    logger.info("=== All tests passed ===")
    logger.info("If any debug point check failed, the BE would have crashed with LOG(FATAL)")
}
