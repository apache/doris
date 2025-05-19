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

suite("test_backup_restore_force_replace_diff_part_val", "backup_restore") {
    String suiteName = "test_backup_restore_force_replace_diff_part_val"
    String dbName = "${suiteName}_db_0"
    String repoName = "${suiteName}_repo_" + UUID.randomUUID().toString().replace("-", "")
    String snapshotName = "${suiteName}_snapshot_" + System.currentTimeMillis()
    String tableNamePrefix = "${suiteName}_tables"
    String tableName = "${tableNamePrefix}_0"

    def syncer = getSyncer()
    syncer.createS3Repository(repoName)
    sql "CREATE DATABASE IF NOT EXISTS ${dbName}"

    sql "DROP TABLE IF EXISTS ${dbName}.${tableName}"
    sql """
        CREATE TABLE ${dbName}.${tableName} (
            `id` LARGEINT NOT NULL,
            `count` LARGEINT SUM DEFAULT "0"
        )
        AGGREGATE KEY(`id`)
        PARTITION BY RANGE(`id`)
        (
            PARTITION p0 VALUES LESS THAN ("10")
        )
        DISTRIBUTED BY HASH(`id`) BUCKETS 2
        PROPERTIES
        (
            "replication_num" = "1"
        )
        """

    sql """
        BACKUP SNAPSHOT ${dbName}.${snapshotName}
        TO `${repoName}`
        ON (
            ${tableName}
        )
    """

    syncer.waitSnapshotFinish(dbName)

    def snapshot = syncer.getSnapshotTimestamp(repoName, snapshotName)
    assertTrue(snapshot != null)

    sql "DROP TABLE ${dbName}.${tableName}"

    sql """
        CREATE TABLE ${dbName}.${tableName} (
            `id` LARGEINT NOT NULL,
            `count` LARGEINT DEFAULT "0"
        )
        AGGREGATE KEY(`id`, `count`)
        PARTITION BY RANGE(`id`, `count`)
        (
            PARTITION p0 VALUES LESS THAN ("100")
        )
        DISTRIBUTED BY HASH(`id`) BUCKETS 2
        PROPERTIES
        (
            "replication_num" = "1"
        )
        """

    sql """
        RESTORE SNAPSHOT ${dbName}.${snapshotName}
        FROM `${repoName}`
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "reserve_replica" = "true",
            "atomic_restore" = "true",
            "force_replace" = "true"
        )
    """

    syncer.waitAllRestoreFinish(dbName)

    sql "sync"
    def show_partitions = sql_return_maparray "SHOW PARTITIONS FROM ${dbName}.${tableName} where PartitionName = 'p0'"

    logger.info(show_partitions.Range)

    assertTrue(show_partitions.Range.contains("[types: [LARGEINT]; keys: [-170141183460469231731687303715884105728]; ..types: [LARGEINT]; keys: [10]; )"))
}

