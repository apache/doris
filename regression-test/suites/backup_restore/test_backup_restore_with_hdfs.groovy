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

suite("test_backup_restore_with_hdfs", "backup_restore") {
    String suiteName = "test_backup_restore_with_hdfs"

    if (!enableHdfs()) {
        logger.info("hdfs is not eabled, skip case ${suiteName}")
        return
    }

    String repoName = "repo_" + UUID.randomUUID().toString().replace("-", "")
    String dbName = "${suiteName}_db"
    String tableName = "${suiteName}_table"
    String snapshotName = "${suiteName}_snapshot"

    def syncer = getSyncer()
    syncer.createHdfsRepository(repoName)

    sql "CREATE DATABASE IF NOT EXISTS ${dbName}"
    sql "DROP TABLE IF EXISTS ${dbName}.${tableName}"
    sql """
        CREATE TABLE ${dbName}.${tableName} (
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
    for (int i = 1; i <= 10; ++i) {
        values.add("(${i}, ${i})")
    }
    sql "INSERT INTO ${dbName}.${tableName} VALUES ${values.join(",")}"
    def result = sql "SELECT * FROM ${dbName}.${tableName}"
    assertEquals(result.size(), values.size());

    sql """
        BACKUP SNAPSHOT ${dbName}.${snapshotName}
        TO `${repoName}`
        ON (${tableName})
    """

    syncer.waitSnapshotFinish(dbName)

    def snapshot = syncer.getSnapshotTimestamp(repoName, snapshotName)
    assertTrue(snapshot != null)

    sql "TRUNCATE TABLE ${dbName}.${tableName}"

    sql """
        RESTORE SNAPSHOT ${dbName}.${snapshotName}
        FROM `${repoName}`
        ON ( `${tableName}`)
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "reserve_replica" = "true"
        )
    """

    syncer.waitAllRestoreFinish(dbName)

    result = sql "SELECT * FROM ${dbName}.${tableName}"
    assertEquals(result.size(), values.size());

    sql "DROP TABLE ${dbName}.${tableName} FORCE"
    sql "DROP DATABASE ${dbName} FORCE"
    sql "DROP REPOSITORY `${repoName}`"
}

