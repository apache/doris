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

suite("test_backup_restore_retention_count", "backup_restore") {
    String suiteName = "test_backup_restore_retention_count"
    String dbName = "${suiteName}_db"
    String snapshotName = "${suiteName}_snapshot"
    String repoName = "${suiteName}_repo_" + UUID.randomUUID().toString().replace("-", "")
    String tableName = "${suiteName}_table"

    def syncer = getSyncer()
    syncer.createS3Repository(repoName)
    sql "CREATE DATABASE IF NOT EXISTS ${dbName}"

    sql "DROP TABLE IF EXISTS ${dbName}.${tableName}"
    sql """
        CREATE TABLE ${dbName}.${tableName} (
            k0 DATETIME NOT NULL
        )
        AUTO PARTITION BY RANGE (date_trunc(k0, 'day')) ()
        DISTRIBUTED BY HASH(`k0`) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "partition.retention_count" = "5"
        )
    """

    // Insert data to create partitions
    sql """
        INSERT INTO ${dbName}.${tableName} 
        SELECT date_add('2020-01-01 00:00:00', interval number day) 
        FROM numbers("number" = "10")
    """

    sql "alter table ${dbName}.${tableName}  set ('partition.retention_count' = '4')"

    // Verify retention_count property exists
    def res = sql "SHOW CREATE TABLE ${dbName}.${tableName}"
    assertTrue(res[0][1].contains('"partition.retention_count" = "4"'))

    // Backup table
    sql """
        BACKUP SNAPSHOT ${dbName}.${snapshotName}
        TO `${repoName}`
        ON (${tableName})
    """

    syncer.waitSnapshotFinish(dbName)

    def snapshot = syncer.getSnapshotTimestamp(repoName, snapshotName)
    assertTrue(snapshot != null)

    sql "DROP TABLE ${dbName}.${tableName} FORCE"

    // Restore table
    sql """
        RESTORE SNAPSHOT ${dbName}.${snapshotName}
        FROM `${repoName}`
        PROPERTIES (
            "backup_timestamp" = "${snapshot}",
            "reserve_replica" = "true"
        )
    """

    syncer.waitAllRestoreFinish(dbName)

    // Verify retention_count property is preserved after restore
    res = sql "SHOW CREATE TABLE ${dbName}.${tableName}"
    assertTrue(res[0][1].contains('"partition.retention_count" = "4"'))

    // Trigger recycle to verify retention_count works after restore
    sql "admin set frontend config ('dynamic_partition_check_interval_seconds' = '1')"
    sleep(8000)

    res = sql "SHOW PARTITIONS FROM ${dbName}.${tableName}"
    assertEquals(4, res.size())

    qt_select "SELECT * FROM ${dbName}.${tableName} ORDER BY k0"

    // Cleanup
    sql "admin set frontend config ('dynamic_partition_check_interval_seconds' = '600')"
    sql "DROP TABLE ${dbName}.${tableName} FORCE"
    sql "DROP DATABASE ${dbName} FORCE"
    sql "DROP REPOSITORY `${repoName}`"
}
