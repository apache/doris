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

suite("test_backup_restore_atomic_with_alter", "backup_restore,nonConcurrent") {
    if (!getFeConfig("enable_debug_points").equals("true")) {
        logger.info("Config.enable_debug_points=true is required")
        return
    }

    String suiteName = "test_backup_restore_atomic_with_alter"
    String dbName = "${suiteName}_db"
    String repoName = "${suiteName}_repo_" + UUID.randomUUID().toString().replace("-", "")
    String snapshotName = "snapshot_" + UUID.randomUUID().toString().replace("-", "")
    String completionSnapshotName = "completion_snapshot_" + UUID.randomUUID().toString().replace("-", "")
    String mappingSnapshotName = "mapping_snapshot_" + UUID.randomUUID().toString().replace("-", "")
    String tableNamePrefix = "${suiteName}_tables"

    def syncer = getSyncer()
    def waitRestorePaused = { label ->
        boolean restorePaused = false
        for (int k = 0; k < 60; k++) {
            def records = sql_return_maparray """ SHOW RESTORE FROM ${dbName} WHERE Label = "${label}" """
            if (records.size() == 1 && records[0].State != 'PENDING' && records[0].State != 'CREATING') {
                restorePaused = true
                break
            }
            logger.info("SHOW RESTORE result: ${records}")
            sleep(3000)
        }
        assertTrue(restorePaused)
    }
    syncer.createS3Repository(repoName)
    sql "DROP DATABASE IF EXISTS ${dbName} FORCE"
    sql "CREATE DATABASE ${dbName}"

    // during restoring, if:
    // 1. table_0 not exists, create table_0 is not allowed
    // 2. table_1 exists, alter operation is not allowed
    // 3. table_1 exists, drop table is not allowed
    // 4. table_0 not exists, rename table_2 to table_0 is not allowed
    int numTables = 3;
    List<String> tables = []
    for (int i = 0; i < numTables; ++i) {
        String tableName = "${tableNamePrefix}_${i}"
        tables.add(tableName)
        sql "DROP TABLE IF EXISTS ${dbName}.${tableName}"
        sql """
            CREATE TABLE ${dbName}.${tableName} (
                `id` LARGEINT NOT NULL,
                `count` LARGEINT SUM DEFAULT "0"
            )
            AGGREGATE KEY(`id`)
            PARTITION BY RANGE(`id`)
            (
                PARTITION p1 VALUES LESS THAN ("10"),
                PARTITION p2 VALUES LESS THAN ("20"),
                PARTITION p3 VALUES LESS THAN ("30"),
                PARTITION p4 VALUES LESS THAN ("40"),
                PARTITION p5 VALUES LESS THAN ("50"),
                PARTITION p6 VALUES LESS THAN ("60"),
                PARTITION p7 VALUES LESS THAN ("120")
            )
            DISTRIBUTED BY HASH(`id`) BUCKETS 2
            PROPERTIES
            (
                "replication_num" = "1"
            )
            """
    }

    int numRows = 10;
    List<String> values = []
    for (int j = 1; j <= numRows; ++j) {
        values.add("(${j}0, ${j}0)")
    }

    sql "INSERT INTO ${dbName}.${tableNamePrefix}_0 VALUES ${values.join(",")}"
    sql "INSERT INTO ${dbName}.${tableNamePrefix}_1 VALUES ${values.join(",")}"
    sql "INSERT INTO ${dbName}.${tableNamePrefix}_2 VALUES ${values.join(",")}"

    // only backup table 0,1
    sql """
        BACKUP SNAPSHOT ${dbName}.${snapshotName}
        TO `${repoName}`
        ON (
            ${tableNamePrefix}_0,
            ${tableNamePrefix}_1
        )
    """

    syncer.waitSnapshotFinish(dbName)

    def snapshot = syncer.getSnapshotTimestamp(repoName, snapshotName)
    assertTrue(snapshot != null)

    // Add the mapping after backup so the backup remains eligible for atomic restore while
    // the live table exercises the DDL fence during the restore.
    sql """
        ALTER TABLE ${dbName}.${tableNamePrefix}_1
        ADD CONSTRAINT atomic_restore_mapping
        COLOCATE MAPPING atomic_restore_mapping (`count`)
        DETERMINES DISTRIBUTION KEY (`id`) NOT ENFORCED
    """

    // drop table_0
    sql "DROP TABLE ${dbName}.${tableNamePrefix}_0 FORCE"

    // disable restore
 try {
    GetDebugPoint().enableDebugPointForAllFEs("FE.PAUSE_NON_PENDING_RESTORE_JOB", [value:snapshotName])

    sql """
        RESTORE SNAPSHOT ${dbName}.${snapshotName}
        FROM `${repoName}`
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "reserve_replica" = "true",
            "atomic_restore" = "true"
        )
    """
    sql "SYNC"

    waitRestorePaused(snapshotName)

    sql "SYNC"

    // 0. table_1 has in_atomic_restore property
    def show_result = master_sql """ SHOW CREATE TABLE ${dbName}.${tableNamePrefix}_1 """
    logger.info("SHOW CREATE TABLE ${tableNamePrefix}_1: ${show_result}")
    assertTrue(show_result[0][1].contains("in_atomic_restore"))

    // 1. create a restoring table (not exists before)
    expectExceptionLike({ ->
        sql """
            CREATE TABLE ${dbName}.${tableNamePrefix}_0
            (
                `id` LARGEINT NOT NULL,
                `count` LARGEINT SUM DEFAULT "0"
            )
            AGGREGATE KEY(`id`)
            PARTITION BY RANGE(`id`)
            (
                PARTITION p1 VALUES LESS THAN ("10"),
                PARTITION p2 VALUES LESS THAN ("20"),
                PARTITION p3 VALUES LESS THAN ("30"),
                PARTITION p4 VALUES LESS THAN ("40"),
                PARTITION p5 VALUES LESS THAN ("50"),
                PARTITION p6 VALUES LESS THAN ("60"),
                PARTITION p7 VALUES LESS THAN ("120")
            )
            DISTRIBUTED BY HASH(`id`) BUCKETS 2
            PROPERTIES
            (
                "replication_num" = "1"
            )
        """
    }, "is in atomic restore, please cancel the restore operation firstly")

    // 2. alter is not allowed
    expectExceptionLike({
        sql """
            ALTER TABLE ${dbName}.${tableNamePrefix}_1
            ADD PARTITION p8 VALUES LESS THAN("200")
        """
    }, "Do not allow doing ALTER ops")
    expectExceptionLike({
        sql """
            ALTER TABLE ${dbName}.${tableNamePrefix}_1
            DROP PARTITION p1
        """
    }, "Do not allow doing ALTER ops")
    expectExceptionLike({
        sql """
            ALTER TABLE ${dbName}.${tableNamePrefix}_1
            MODIFY PARTITION p1 SET ("key"="value")
        """
    }, "Do not allow doing ALTER ops")
    expectExceptionLike({
        sql """
            ALTER TABLE ${dbName}.${tableNamePrefix}_1
            ADD COLUMN new_col INT DEFAULT "0" AFTER id
        """
    }, "Do not allow doing ALTER ops")
    expectExceptionLike({
        sql """
            ALTER TABLE ${dbName}.${tableNamePrefix}_1
            DROP COLUMN count
        """
    }, "Do not allow doing ALTER ops")
    expectExceptionLike({
        sql """
            ALTER TABLE ${dbName}.${tableNamePrefix}_1
            SET ("is_being_synced"="false")
        """
    }, "Do not allow doing ALTER ops")
    expectExceptionLike({
        sql """
            ALTER TABLE ${dbName}.${tableNamePrefix}_1
            RENAME newTableName
        """
    }, "Do not allow doing ALTER ops")
    expectExceptionLike({
        sql """
            ALTER TABLE ${dbName}.${tableNamePrefix}_1
            DROP CONSTRAINT atomic_restore_mapping
        """
    }, "atomic restore state")
    // BTW, the tmp table also don't allow rename
    expectExceptionLike({
        sql """
            ALTER TABLE ${dbName}.__doris_atomic_restore_prefix__${tableNamePrefix}_1
            RENAME newTableName
        """
    }, "Do not allow doing ALTER ops")
    // 3. drop table is not allowed
    expectExceptionLike({
        sql """
            DROP TABLE ${dbName}.${tableNamePrefix}_1
        """
    }, "state is in atomic restore")
    expectExceptionLike({
        sql """
            DROP TABLE ${dbName}.__doris_atomic_restore_prefix__${tableNamePrefix}_1
        """
    }, "state is RESTORE")
    // 4. the table name is occupied
    expectExceptionLike({
        sql """
            ALTER TABLE ${dbName}.${tableNamePrefix}_2
            RENAME ${tableNamePrefix}_0
        """
    }, "is already used (in restoring)")


    sql "CANCEL RESTORE FROM ${dbName}"
    sql "SYNC"

    // 5. The restore job is cancelled, the in_atomic_restore property has been removed.
    show_result = master_sql """ SHOW CREATE TABLE ${dbName}.${tableNamePrefix}_1 """
    logger.info("SHOW CREATE TABLE ${tableNamePrefix}_1: ${show_result}")
    assertFalse(show_result[0][1].contains("in_atomic_restore"))
    sql """
        ALTER TABLE ${dbName}.${tableNamePrefix}_1
        DROP CONSTRAINT atomic_restore_mapping
    """

    // Also verify the DDL fence while a restore runs to completion, not only while it is cancelled.
    sql """
        BACKUP SNAPSHOT ${dbName}.${completionSnapshotName}
        TO `${repoName}`
        ON (${tableNamePrefix}_1)
    """
    syncer.waitSnapshotFinish(dbName)
    def completionSnapshot = syncer.getSnapshotTimestamp(repoName, completionSnapshotName)
    assertTrue(completionSnapshot != null)
    sql """
        ALTER TABLE ${dbName}.${tableNamePrefix}_1
        ADD CONSTRAINT atomic_restore_mapping
        COLOCATE MAPPING atomic_restore_mapping (`count`)
        DETERMINES DISTRIBUTION KEY (`id`) NOT ENFORCED
    """
    GetDebugPoint().enableDebugPointForAllFEs(
            "FE.PAUSE_NON_PENDING_RESTORE_JOB", [value:completionSnapshotName])
    sql """
        RESTORE SNAPSHOT ${dbName}.${completionSnapshotName}
        FROM `${repoName}`
        PROPERTIES
        (
            "backup_timestamp" = "${completionSnapshot}",
            "reserve_replica" = "true",
            "atomic_restore" = "true"
        )
    """
    waitRestorePaused(completionSnapshotName)
    expectExceptionLike({
        sql """
            ALTER TABLE ${dbName}.${tableNamePrefix}_1
            DROP CONSTRAINT atomic_restore_mapping
        """
    }, "atomic restore state")
    GetDebugPoint().disableDebugPointForAllFEs("FE.PAUSE_NON_PENDING_RESTORE_JOB")
    syncer.waitAllRestoreFinish(dbName)
    sql "SYNC"

    def completionRestore = sql_return_maparray """
        SHOW RESTORE FROM ${dbName} WHERE Label = "${completionSnapshotName}"
    """
    assertTrue(completionRestore.size() == 1)
    assertTrue(completionRestore[0].State == "FINISHED")
    show_result = master_sql """ SHOW CREATE TABLE ${dbName}.${tableNamePrefix}_1 """
    assertFalse(show_result[0][1].contains("in_atomic_restore"))
    assertTrue((sql "SHOW CONSTRAINTS FROM ${dbName}.${tableNamePrefix}_1").isEmpty())

    // An atomic restore with mappings is rejected before staging. This keeps a centralized
    // constraint with the same name from colliding with a mapping published from the backup.
    sql """
        ALTER TABLE ${dbName}.${tableNamePrefix}_1
        ADD CONSTRAINT atomic_restore_collision
        COLOCATE MAPPING atomic_restore_collision (`count`)
        DETERMINES DISTRIBUTION KEY (`id`) NOT ENFORCED
    """
    sql """
        BACKUP SNAPSHOT ${dbName}.${mappingSnapshotName}
        TO `${repoName}`
        ON (${tableNamePrefix}_1)
    """
    syncer.waitSnapshotFinish(dbName)
    def mappingSnapshot = syncer.getSnapshotTimestamp(repoName, mappingSnapshotName)
    assertTrue(mappingSnapshot != null)

    sql """
        ALTER TABLE ${dbName}.${tableNamePrefix}_1
        DROP CONSTRAINT atomic_restore_collision
    """
    sql """
        ALTER TABLE ${dbName}.${tableNamePrefix}_1
        ADD CONSTRAINT atomic_restore_collision UNIQUE (`id`)
    """
    sql """
        RESTORE SNAPSHOT ${dbName}.${mappingSnapshotName}
        FROM `${repoName}`
        PROPERTIES
        (
            "backup_timestamp" = "${mappingSnapshot}",
            "reserve_replica" = "true",
            "atomic_restore" = "true"
        )
    """
    syncer.waitRestoreError(dbName, "backup contains distribution mapping constraints")

    def mappingRestore = sql_return_maparray """
        SHOW RESTORE FROM ${dbName} WHERE Label = "${mappingSnapshotName}"
    """
    assertTrue(mappingRestore.size() == 1)
    assertTrue(mappingRestore[0].State == "CANCELLED")
    assertTrue(mappingRestore[0].Status.contains("backup contains distribution mapping constraints"))
    def constraints = sql "SHOW CONSTRAINTS FROM ${dbName}.${tableNamePrefix}_1"
    assertTrue(constraints.size() == 1)
    assertTrue(constraints[0][0] == "atomic_restore_collision")
    show_result = master_sql """ SHOW CREATE TABLE ${dbName}.${tableNamePrefix}_1 """
    assertFalse(show_result[0][1].contains("in_atomic_restore"))

    for (def tableName in tables) {
        sql "DROP TABLE IF EXISTS ${dbName}.${tableName} FORCE"
    }
    sql "DROP DATABASE ${dbName} FORCE"
    sql "DROP REPOSITORY `${repoName}`"
  } finally {
    GetDebugPoint().disableDebugPointForAllFEs("FE.PAUSE_NON_PENDING_RESTORE_JOB")
  }
}
