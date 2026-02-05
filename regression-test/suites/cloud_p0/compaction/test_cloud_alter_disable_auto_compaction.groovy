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

suite("test_cloud_alter_disable_auto_compaction", "p0") {
    if (!isCloudMode()) {
        logger.info("not cloud mode, skip this test")
        return
    }

    def tableName = "test_cloud_alter_disable_auto_compaction"

    sql """ DROP TABLE IF EXISTS ${tableName}; """
    sql """
        CREATE TABLE ${tableName} (
            `id` int(11) NULL,
            `name` varchar(255) NULL,
            `score` int(11) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );
    """

    // Verify initial state: disable_auto_compaction should be false (default)
    def result = sql """ SHOW CREATE TABLE ${tableName}; """
    logger.info("Initial SHOW CREATE TABLE result: ${result}")
    assertFalse(result[0][1].contains('"disable_auto_compaction" = "true"'))

    // Test 1: ALTER TABLE to set disable_auto_compaction = true
    sql """ ALTER TABLE ${tableName} SET ("disable_auto_compaction" = "true"); """

    // Verify the property is updated
    result = sql """ SHOW CREATE TABLE ${tableName}; """
    logger.info("After setting true, SHOW CREATE TABLE result: ${result}")
    assertTrue(result[0][1].contains('"disable_auto_compaction" = "true"'))

    // Test 2: ALTER TABLE to set disable_auto_compaction = false
    sql """ ALTER TABLE ${tableName} SET ("disable_auto_compaction" = "false"); """

    // Verify the property is updated back to false
    result = sql """ SHOW CREATE TABLE ${tableName}; """
    logger.info("After setting false, SHOW CREATE TABLE result: ${result}")
    // When disable_auto_compaction is false, it may or may not appear in SHOW CREATE TABLE
    // depending on whether the system shows default values
    assertFalse(result[0][1].contains('"disable_auto_compaction" = "true"'))

    // Test 3: Set it back to true and verify compaction behavior
    sql """ ALTER TABLE ${tableName} SET ("disable_auto_compaction" = "true"); """

    // Insert some data
    sql """ INSERT INTO ${tableName} VALUES (1, "test1", 100); """
    sql """ INSERT INTO ${tableName} VALUES (2, "test2", 200); """
    sql """ INSERT INTO ${tableName} VALUES (3, "test3", 300); """

    // Wait a bit for sync_meta to propagate the change
    sleep(5000)

    // Verify data is correct
    def count = sql """ SELECT count(*) FROM ${tableName}; """
    assertEquals(3, count[0][0])

    // Test 4: Toggle back to false
    sql """ ALTER TABLE ${tableName} SET ("disable_auto_compaction" = "false"); """

    result = sql """ SHOW CREATE TABLE ${tableName}; """
    logger.info("Final SHOW CREATE TABLE result: ${result}")
    assertFalse(result[0][1].contains('"disable_auto_compaction" = "true"'))

    // Clean up
    sql """ DROP TABLE IF EXISTS ${tableName}; """
}
