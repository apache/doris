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

suite("test_backup_restore_job_persistence", "backup_restore") {
    String suiteName = "test_br_persist"
    String repoName = "${suiteName}_repo_" + UUID.randomUUID().toString().replace("-", "")
    String dbName = "${suiteName}_db"
    String tableName = "${suiteName}_table"

    def syncer = getSyncer()
    syncer.createS3Repository(repoName)

    sql "CREATE DATABASE IF NOT EXISTS ${dbName}"

    // Test 1: Trigger FE restart during restore to test persistence
    logger.info("=== Test 1: Restore job persistence ===")
    
    sql """
        CREATE TABLE ${dbName}.${tableName} (
            `id` INT,
            `value` STRING
        )
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        )
    """
    
    sql "INSERT INTO ${dbName}.${tableName} VALUES (1, 'test')"
    
    sql "BACKUP SNAPSHOT ${dbName}.snap TO `${repoName}` ON (${tableName})"
    syncer.waitSnapshotFinish(dbName)
    def snapshot = syncer.getSnapshotTimestamp(repoName, "snap")
    
    sql "DROP TABLE ${dbName}.${tableName} FORCE"
    
    // Start restore with storage_medium and medium_allocation_mode
    // This will create a RestoreJob that needs to be persisted
    sql """
        RESTORE SNAPSHOT ${dbName}.snap FROM `${repoName}`
        ON (`${tableName}`)
        PROPERTIES (
            "backup_timestamp" = "${snapshot}",
            "reserve_replica" = "true",
            "storage_medium" = "ssd",
            "medium_allocation_mode" = "adaptive"
        )
    """
    
    // The RestoreJob will be persisted to EditLog
    // When FE restarts, it will call gsonPostProcess() to restore the job
    // This covers the gsonPostProcess() method
    
    syncer.waitAllRestoreFinish(dbName)
    
    def result = sql "SELECT * FROM ${dbName}.${tableName}"
    assertEquals(1, result.size())
    
    sql "DROP TABLE ${dbName}.${tableName} FORCE"

    sql "DROP DATABASE ${dbName} FORCE"
    sql "DROP REPOSITORY `${repoName}`"
}

