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

suite("test_backup_restore_with_view", "backup_restore") {
    String suiteName = "backup_restore_with_view"
    String dbName = "${suiteName}_db"
    String dbName1 = "${suiteName}_db_1"
    String repoName = "${suiteName}_repo_" + UUID.randomUUID().toString().replace("-", "")
    String snapshotName = "${suiteName}_snapshot"
    String tableName = "${suiteName}_table"
    String viewName = "${suiteName}_view"

    def syncer = getSyncer()
    syncer.createS3Repository(repoName)
    sql "CREATE DATABASE IF NOT EXISTS ${dbName}"
    sql "CREATE DATABASE IF NOT EXISTS ${dbName1}"

    int numRows = 10;
    sql "DROP TABLE IF EXISTS ${dbName}.${tableName} FORCE"
    sql "DROP VIEW IF EXISTS ${dbName}.${viewName}"
    sql """
        CREATE TABLE ${dbName}.${tableName} (
            `id` LARGEINT NOT NULL,
            `count` LARGEINT SUM DEFAULT "0"
        )
        AGGREGATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 2
        PROPERTIES
        (
            "replication_num" = "1"
        )
        """
    List<String> values = []
    for (int j = 1; j <= numRows; ++j) {
        values.add("(${j}, ${j})")
    }
    sql "INSERT INTO ${dbName}.${tableName} VALUES ${values.join(",")}"

    sql """CREATE VIEW ${dbName}.${viewName} (id, count)
        AS
        SELECT * FROM ${dbName}.${tableName} WHERE count > 5
        """

    qt_sql "SELECT * FROM ${dbName}.${tableName} ORDER BY id ASC"
    qt_sql "SELECT * FROM ${dbName}.${viewName} ORDER BY id ASC"

    sql """
        BACKUP SNAPSHOT ${dbName}.${snapshotName}
        TO `${repoName}`
    """

    syncer.waitSnapshotFinish(dbName)

    def snapshot = syncer.getSnapshotTimestamp(repoName, snapshotName)
    assertTrue(snapshot != null)

    sql "DROP TABLE IF EXISTS ${dbName1}.${tableName} FORCE"
    sql "DROP VIEW IF EXISTS ${dbName1}.${viewName}"

    sql """
        RESTORE SNAPSHOT ${dbName1}.${snapshotName}
        FROM `${repoName}`
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "reserve_replica" = "true"
        )
    """

    syncer.waitAllRestoreFinish(dbName1)

    qt_sql "SELECT * FROM ${dbName1}.${tableName} ORDER BY id ASC"
    qt_sql "SELECT * FROM ${dbName1}.${viewName} ORDER BY id ASC"
    def show_view_result = sql_return_maparray "SHOW VIEW FROM ${tableName} FROM ${dbName1}"
    logger.info("show view result: ${show_view_result}")
    assertTrue(show_view_result.size() == 1);
    def show_view = show_view_result[0]['Create View']
    assertTrue(show_view.contains("${dbName1}"))
    assertTrue(show_view.contains("${tableName}"))

    // restore to db, test the view signature.
    sql """
        RESTORE SNAPSHOT ${dbName}.${snapshotName}
        FROM `${repoName}`
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "reserve_replica" = "true"
        )
    """

    syncer.waitAllRestoreFinish(dbName)
    def restore_result = sql_return_maparray """ SHOW RESTORE FROM ${dbName} WHERE Label ="${snapshotName}" """
    restore_result.last()
    logger.info("show restore result: ${restore_result}")
    assertTrue(restore_result.last().State == "FINISHED")

    // restore to db1, test the view signature.
    sql """
        RESTORE SNAPSHOT ${dbName1}.${snapshotName}
        FROM `${repoName}`
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "reserve_replica" = "true"
        )
    """

    syncer.waitAllRestoreFinish(dbName1)
    restore_result = sql_return_maparray """ SHOW RESTORE FROM ${dbName1} WHERE Label ="${snapshotName}" """
    restore_result.last()
    logger.info("show restore result: ${restore_result}")
    assertTrue(restore_result.last().State == "FINISHED")
    def res = sql "SHOW VIEW FROM ${dbName1}.${tableName}"
    assertTrue(res.size() > 0)

    // Test cross-database view references preservation
    logger.info("========== Testing cross-database view references ==========")
    
    String baseDbName = "${suiteName}_base_db"
    String viewDbName = "${suiteName}_view_db"
    String restoreDbName = "${suiteName}_restore_db"
    String baseTableName = "base_table"
    String localTableName = "local_table"
    String crossDbViewName = "cross_db_view"
    String mixedViewName = "mixed_view"

    try {
        // Create base database with base table
        sql "DROP DATABASE IF EXISTS ${baseDbName} FORCE"
        sql "DROP DATABASE IF EXISTS ${viewDbName} FORCE"
        sql "DROP DATABASE IF EXISTS ${restoreDbName} FORCE"
        
        sql "CREATE DATABASE ${baseDbName}"
        sql "CREATE DATABASE ${viewDbName}"

        sql """
            CREATE TABLE ${baseDbName}.${baseTableName} (
                id INT,
                name VARCHAR(100),
                value INT
            )
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 2
            PROPERTIES ("replication_num" = "1")
        """

        sql """
            CREATE TABLE ${viewDbName}.${localTableName} (
                id INT,
                category VARCHAR(100)
            )
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 2
            PROPERTIES ("replication_num" = "1")
        """

        sql """
            INSERT INTO ${baseDbName}.${baseTableName} VALUES
            (1, 'Alice', 100),
            (2, 'Bob', 200),
            (3, 'Charlie', 300)
        """

        sql """
            INSERT INTO ${viewDbName}.${localTableName} VALUES
            (1, 'TypeA'),
            (2, 'TypeB'),
            (3, 'TypeC')
        """

        // Create cross-database view (references base_db only)
        sql """
            CREATE VIEW ${viewDbName}.${crossDbViewName} AS
            SELECT id, name, value 
            FROM `internal`.`${baseDbName}`.`${baseTableName}`
            WHERE value > 100
        """

        // Create mixed view (references both base_db and view_db)
        sql """
            CREATE VIEW ${viewDbName}.${mixedViewName} AS
            SELECT 
                t1.id,
                t1.name,
                t1.value,
                t2.category
            FROM `internal`.`${baseDbName}`.`${baseTableName}` t1
            JOIN `internal`.`${viewDbName}`.`${localTableName}` t2
            ON t1.id = t2.id
        """

        // Verify original views work
        def crossDbResult = sql "SELECT * FROM ${viewDbName}.${crossDbViewName} ORDER BY id"
        assertTrue(crossDbResult.size() == 2)
        assertTrue(crossDbResult[0][0] == 2)
        assertTrue(crossDbResult[0][1] == "Bob")

        def mixedResult = sql "SELECT * FROM ${viewDbName}.${mixedViewName} ORDER BY id"
        assertTrue(mixedResult.size() == 3)

        // Backup view_db
        String crossDbSnapshot = "${suiteName}_cross_db_snapshot"
        sql """
            BACKUP SNAPSHOT ${viewDbName}.${crossDbSnapshot}
            TO `${repoName}`
        """

        syncer.waitSnapshotFinish(viewDbName)
        def crossDbSnapshotTs = syncer.getSnapshotTimestamp(repoName, crossDbSnapshot)
        assertTrue(crossDbSnapshotTs != null)
        logger.info("Cross-DB snapshot timestamp: ${crossDbSnapshotTs}")

        // Create target database before restore (FIX: prevent database not exist error)
        sql "CREATE DATABASE IF NOT EXISTS ${restoreDbName}"

        // Restore to different database
        sql """
            RESTORE SNAPSHOT ${restoreDbName}.${crossDbSnapshot}
            FROM `${repoName}`
            PROPERTIES
            (
                "backup_timestamp" = "${crossDbSnapshotTs}",
                "reserve_replica" = "true"
            )
        """

        syncer.waitAllRestoreFinish(restoreDbName)

        // Verify restore success
        def restoreResult = sql_return_maparray """ SHOW RESTORE FROM ${restoreDbName} WHERE Label = "${crossDbSnapshot}" """
        logger.info("Cross-DB restore result: ${restoreResult}")
        assertTrue(restoreResult.last().State == "FINISHED")

        // Critical verification: Check view definitions
        def crossDbViewDef = sql "SHOW CREATE VIEW ${restoreDbName}.${crossDbViewName}"
        logger.info("Cross-DB view definition after restore: ${crossDbViewDef[0][1]}")
        
        // Cross-DB view should still reference base_db, not restore_db
        assertTrue(crossDbViewDef[0][1].contains("`${baseDbName}`"), 
            "Cross-DB view should preserve base_db reference")
        
        // Mixed view should preserve base_db reference but update view_db to restore_db
        def mixedViewDef = sql "SHOW CREATE VIEW ${restoreDbName}.${mixedViewName}"
        logger.info("Mixed view definition after restore: ${mixedViewDef[0][1]}")
        
        assertTrue(mixedViewDef[0][1].contains("`${baseDbName}`"),
            "Mixed view should preserve base_db reference")
        assertTrue(mixedViewDef[0][1].contains("`${restoreDbName}`"),
            "Mixed view should reference restore_db for local tables")

        // Verify views still work after restore
        def restoredCrossDbResult = sql "SELECT * FROM ${restoreDbName}.${crossDbViewName} ORDER BY id"
        assertTrue(restoredCrossDbResult.size() == 2)
        assertTrue(restoredCrossDbResult[0][0] == 2)
        assertTrue(restoredCrossDbResult[0][1] == "Bob")

        def restoredMixedResult = sql "SELECT * FROM ${restoreDbName}.${mixedViewName} ORDER BY id"
        assertTrue(restoredMixedResult.size() == 3)
        assertTrue(restoredMixedResult[0][0] == 1)
        assertTrue(restoredMixedResult[0][1] == "Alice")
    } finally {
        // Clean up cross-DB test resources
        sql "DROP DATABASE IF EXISTS ${baseDbName} FORCE"
        sql "DROP DATABASE IF EXISTS ${viewDbName} FORCE"
        sql "DROP DATABASE IF EXISTS ${restoreDbName} FORCE"
    }

    // Clean up original test resources
    sql "DROP TABLE ${dbName}.${tableName} FORCE"
    sql "DROP VIEW ${dbName}.${viewName}"
    sql "DROP DATABASE ${dbName} FORCE"
    sql "DROP TABLE ${dbName1}.${tableName} FORCE"
    sql "DROP VIEW ${dbName1}.${viewName}"
    sql "DROP DATABASE ${dbName1} FORCE"
    sql "DROP REPOSITORY `${repoName}`"
}

