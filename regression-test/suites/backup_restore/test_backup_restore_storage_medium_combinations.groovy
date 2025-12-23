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

suite("test_backup_restore_storage_medium_combinations", "backup_restore") {
    String suiteName = "test_br_medium_combo"
    String repoName = "${suiteName}_repo_" + UUID.randomUUID().toString().replace("-", "")
    String dbName = "${suiteName}_db"
    String tableName = "${suiteName}_table"

    def syncer = getSyncer()
    syncer.createS3Repository(repoName)

    sql "CREATE DATABASE IF NOT EXISTS ${dbName}"

    // Test combinations of storage_medium and medium_allocation_mode
    def combinations = [
        // [storage_medium, medium_allocation_mode, testName]
        ["hdd", "strict", "hdd_strict"],
        ["hdd", "adaptive", "hdd_adaptive"],
        ["ssd", "strict", "ssd_strict"],
        ["ssd", "adaptive", "ssd_adaptive"],
        ["same_with_upstream", "strict", "same_upstream_strict"],
        ["same_with_upstream", "adaptive", "same_upstream_adaptive"]
    ]

    combinations.each { combo ->
        def storageMedium = combo[0]
        def allocationMode = combo[1]
        def testName = combo[2]
        
        logger.info("Testing combination: storage_medium=${storageMedium}, medium_allocation_mode=${allocationMode}")

        sql "DROP TABLE IF EXISTS ${dbName}.${tableName}_${testName}"
        sql """
            CREATE TABLE ${dbName}.${tableName}_${testName} (
                `id` LARGEINT NOT NULL,
                `count` LARGEINT SUM DEFAULT "0")
            AGGREGATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 2
            PROPERTIES
            (
                "replication_num" = "1"
            )
        """

        // Insert test data
        List<String> values = []
        for (int i = 1; i <= 5; ++i) {
            values.add("(${i}, ${i})")
        }
        sql "INSERT INTO ${dbName}.${tableName}_${testName} VALUES ${values.join(",")}"

        // Backup
        sql """
            BACKUP SNAPSHOT ${dbName}.snapshot_${testName}
            TO `${repoName}`
            ON (${tableName}_${testName})
        """

        syncer.waitSnapshotFinish(dbName)

        def snapshot = syncer.getSnapshotTimestamp(repoName, "snapshot_${testName}")
        assertTrue(snapshot != null, "Snapshot should be created for ${testName}")

        sql "DROP TABLE ${dbName}.${tableName}_${testName} FORCE"

        // Restore with specific storage_medium and medium_allocation_mode
        sql """
            RESTORE SNAPSHOT ${dbName}.snapshot_${testName}
            FROM `${repoName}`
            ON (`${tableName}_${testName}`)
            PROPERTIES
            (
                "backup_timestamp" = "${snapshot}",
                "reserve_replica" = "true",
                "storage_medium" = "${storageMedium}",
                "medium_allocation_mode" = "${allocationMode}"
            )
        """

        syncer.waitAllRestoreFinish(dbName)

        // Verify data
        def result = sql "SELECT * FROM ${dbName}.${tableName}_${testName}"
        assertEquals(result.size(), values.size(), "Data should be restored correctly for ${testName}")

        // Verify table properties
        def show_result = sql "SHOW CREATE TABLE ${dbName}.${tableName}_${testName}"
        def createTableStr = show_result[0][1]
        
        logger.info("Create table statement for ${testName}: ${createTableStr}")
        
        // For same_with_upstream, the actual medium depends on original table
        if (storageMedium != "same_with_upstream") {
            // For explicit medium, verify it's in the properties
            assertTrue(createTableStr.contains("storage_medium") || createTableStr.contains("STORAGE MEDIUM"),
                      "Table should have storage medium specified for ${testName}")
        }
        
        // Verify medium_allocation_mode is set
        assertTrue(createTableStr.contains("medium_allocation_mode"),
                  "Table should have medium_allocation_mode for ${testName}")

        sql "DROP TABLE ${dbName}.${tableName}_${testName} FORCE"
    }

    sql "DROP DATABASE ${dbName} FORCE"
    sql "DROP REPOSITORY `${repoName}`"
}

