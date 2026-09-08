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

suite("test_cloud_backup_snapshot_strip_replica", "p0") {
    if (!isCloudMode()) {
        logger.info("not cloud mode, skip this test")
        return
    }

    String suiteName = "test_cloud_backup_snapshot_strip_replica"
    String repoName = "${suiteName}_repo_" + UUID.randomUUID().toString().replace("-", "")
    String dbName = "test_cloud_backup_snapshot_strip_replica_db"
    String tableName = "test_cloud_backup_snapshot_strip_replica_table"
    String snapshotName = "test_cloud_backup_snapshot_strip_replica_snapshot"

    def syncer = getSyncer()
    syncer.createS3Repository(repoName)

    sql "CREATE DATABASE IF NOT EXISTS ${dbName}"
    sql "DROP TABLE IF EXISTS ${dbName}.${tableName}"
    sql """
        CREATE TABLE ${dbName}.${tableName} (
            `id` int NOT NULL,
            `value` varchar(32) NOT NULL
        )
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES
        (
            "replication_num" = "1"
        )
        """

    sql "INSERT INTO ${dbName}.${tableName} VALUES (1, 'cloud_backup')"
    def result = sql "SELECT * FROM ${dbName}.${tableName} ORDER BY id"
    assertEquals(1, result.size())

    sql """
        BACKUP SNAPSHOT ${dbName}.${snapshotName}
        TO `${repoName}`
        ON (${tableName})
        PROPERTIES ("type" = "full")
        """

    syncer.waitSnapshotFinish(dbName)

    def snapshot = syncer.getSnapshotTimestamp(repoName, snapshotName)
    assertTrue(snapshot != null)

    sql "DROP REPOSITORY `${repoName}`"
}
