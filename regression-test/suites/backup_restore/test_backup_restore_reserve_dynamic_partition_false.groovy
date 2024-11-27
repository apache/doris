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

suite("test_backup_restore_reserve_dynamic_partition_false", "backup_restore") {
    String repoName = "repo_" + UUID.randomUUID().toString().replace("-", "")
    String dbName = "backup_restore_dynamic_partition_reserve_false_db"
    String tableName = "dynamic_partition_reserve_false_table"

    def syncer = getSyncer()
    syncer.createS3Repository(repoName)

    sql "CREATE DATABASE IF NOT EXISTS ${dbName}"
    sql "DROP TABLE IF EXISTS ${dbName}.${tableName}"
    sql """
        CREATE TABLE ${dbName}.${tableName}  (
            `id` LARGEINT NOT NULL,
            `sdate` DATE
        )
        PARTITION BY RANGE(sdate)()
        DISTRIBUTED BY HASH(`id`) BUCKETS 2
        PROPERTIES
        (
            "dynamic_partition.enable" = "true",
            "dynamic_partition.time_unit" = "YEAR",
            "dynamic_partition.start" = "-50",
            "dynamic_partition.end" = "5",
            "dynamic_partition.prefix" = "p",
            "dynamic_partition.create_history_partition"="true",
            "replication_num" = "1"
        )
        """

    sql """
        INSERT INTO ${dbName}.${tableName} VALUES
        (1,"2023-11-01"),
        (2,"2023-11-01"),
        (3,"2023-11-01"),
        (4,"2023-11-01"),
        (5,"2023-11-01"),
        (1,"2023-11-02"),
        (2,"2023-11-02"),
        (3,"2023-11-02"),
        (4,"2023-11-02"),
        (5,"2023-11-02"),
        (1,"2024-11-01"),
        (2,"2024-11-01"),
        (3,"2024-11-01"),
        (4,"2024-11-01"),
        (5,"2024-11-01"),
        (1,"2024-11-02"),
        (2,"2024-11-02"),
        (3,"2024-11-02"),
        (4,"2024-11-02"),
        (5,"2024-11-02")
        """

    def result = sql "SELECT * FROM ${dbName}.${tableName}"
    assertEquals(result.size(),20)

    String snapshotName = "test_backup_restore_dynamic_partition_reserve_true_snapshot"
    sql """
        BACKUP SNAPSHOT ${dbName}.${snapshotName}
        TO `${repoName}`
        ON (${tableName})
    """    

    syncer.waitSnapshotFinish(dbName)
    def snapshot = syncer.getSnapshotTimestamp(repoName, snapshotName)
    assertTrue(snapshot != null)

    sql "DROP TABLE ${dbName}.${tableName} FORCE"

    sql """
        RESTORE SNAPSHOT ${dbName}.${snapshotName}
        FROM `${repoName}`
        ON (`${tableName}`)
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "reserve_dynamic_partition_enable" = "false",
            "reserve_replica" = "true"
        )
    """
    syncer.waitAllRestoreFinish(dbName)

    def restore_properties = sql "SHOW CREATE TABLE ${dbName}.${tableName}"

    assertTrue(restore_properties[0][1].contains("\"dynamic_partition.enable\" = \"false\""))

    result = sql "SELECT * FROM ${dbName}.${tableName}"
    assertEquals(result.size(),20);

    sql "DROP TABLE ${dbName}.${tableName} FORCE"
    sql "DROP DATABASE ${dbName} FORCE"
    sql "DROP REPOSITORY `${repoName}`"
}
