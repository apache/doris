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

suite("test_backup_restore", "backup_restore") {
    String repoName = "test_backup_restore_repo"

    def syncer = getSyncer()
    syncer.createS3Repository(repoName)

    String tableName = "test_backup_restore_table"
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE ${tableName} (
            `id` LARGEINT NOT NULL,
            `count` LARGEINT SUM DEFAULT "0")
        AGGREGATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 2
        PROPERTIES
        (
            "replication_num" = "1"
        )
        """

    List<String> values = []
    for (i = 1; i <= 10; ++i) {
        values.add("(${i}, ${i})")
    }
    sql "INSERT INTO ${tableName} VALUES ${values.join(",")}"
    def result = sql "SELECT * FROM ${tableName}"
    assertEquals(result.size(), values.size());

    String snapshotName = "test_backup_restore_snapshot"
    sql """
        BACKUP SNAPSHOT ${snapshotName}
        TO `${repoName}`
        ON (${tableName})
    """

    while (!syncer.checkSnapshotFinish()) {
        Thread.sleep(3000)
    }

    snapshot = syncer.getSnapshotTimestamp(repoName, snapshotName)
    assertTrue(snapshot != null)

    sql "TRUNCATE TABLE ${tableName}"

    sql """
        RESTORE SNAPSHOT ${snapshotName}
        FROM `${repoName}`
        ON ( `${tableName}`)
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "replication_num" = "1"
        )
    """

    while (!syncer.checkAllRestoreFinish()) {
        Thread.sleep(3000)
    }

    result = sql "SELECT * FROM ${tableName}"
    assertEquals(result.size(), values.size());

    sql "DROP TABLE ${tableName} FORCE"
    sql "DROP REPOSITORY `${repoName}`"
}

