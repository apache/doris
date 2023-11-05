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

suite("test_backup_restore_dup_without_default_keys", "backup_restore"){
    String repoName = "test_backup_restore_repo"
    String dbName = "backup_restore_db"
    String tableName = "test_backup_restore_table"

    // backup & restore for duplicate without keys by default
    def syncer = getSyncer()
    syncer.createS3Repository(repoName)
    sql "CREATE DATABASE IF NOT EXISTS ${dbName}"
    sql "DROP TABLE IF EXISTS ${dbName}.${tableName}"
    sql """
        CREATE TABLE ${dbName}.${tableName} (
            `id` LARGEINT NOT NULL,
            `count` LARGEINT SUM DEFAULT "0")
        DISTRIBUTED BY HASH(`id`) BUCKETS 2
        PROPERTIES
        (
            "replication_num" = "1",
            "enable_duplicate_without_keys_by_default" = "true"
        )
        """

    List<String> values []
    for(int i = 1;i <= 10; ++i){
      value.add("(${i},${i})")
    }
    sql "INSERT INTO ${dbName}.${tableName} VALUES ${values.join(",")}"

    def result = sql "SELECT * FROM ${dbName}.${tableName}"
    assertEquals(result.size(),values.size())

    String snapshotName = "test_backup_restore_dup_without_default_keys_snapshot"
    sql """
        BACKUP SNAPSHOT ${dbName}.${snapshotName}
        TO `${repoName}`
        ON (${tableName})
    """    
    while (!syncer.checkSnapshotFinish(dbName)) {
        Thread.sleep(3000)
    }
    def snapshot = syncer.getSnapshotTimestamp(repoName, snapshotName)
    assertTrue(snapshot != null)

    sql "TRUNCATE TABLE ${dbName}.${tableName}"

    sql"""
        RESTORE SNAPSHOT ${dbName}.${snapshotName}
        FROM `${repoName}`
        ON (`${tableName}`)
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "replication_num" = "1",
            "enable_duplicate_without_keys_by_default" = "true"
        )
    """
    while (!syncer.checkAllRestoreFinish(dbName)) {
        Thread.sleep(3000)
    }
    result = sql "SELECT * FROM ${dbName}.${tableName}"
    assertEquals(result.size(), values.size());

    sql "DROP TABLE ${dbName}.${tableName} FORCE"
    sql "DROP DATABASE ${dbName} FORCE"
    sql "DROP REPOSITORY `${repoName}`"
}
