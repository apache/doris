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

import org.apache.doris.regression.util.ObjectStorageIamTestUtils

suite("test_backup_restore_iam") {
    if (isCloudMode()) {
        logger.info("skip ${name} case, because cloud mode not support")
        return
    }
    def config = ObjectStorageIamTestUtils.getConfig(context.config.otherConfigs)
    if (config == null) {
        logger.info("skip ${name} because objectStorageIamProvider is not configured")
        return
    }

    String suiteName = "test_backup_restore_iam"

    String dbName = "${suiteName}_db"
    String tableName = "${suiteName}_table"

    def syncer = getSyncer()

    sql "CREATE DATABASE IF NOT EXISTS ${dbName}"
    sql "DROP TABLE IF EXISTS ${dbName}.${tableName} FORCE;"
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

    config.authCases.each { authCase ->
        String suffix = UUID.randomUUID().toString().replace("-", "")
        String repoName = "obj_iam_${authCase.name}_${suffix}"
        String snapshotName = "obj_iam_${authCase.name}_${suffix.substring(0, 8)}"

        logger.info("run ${name} with ${authCase.name}")
        syncer.createObjectStorageIamRepository(repoName, config, authCase)

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
            ON (`${tableName}`)
            PROPERTIES
            (
                "backup_timestamp" = "${snapshot}",
                "reserve_replica" = "true"
            )
        """

        syncer.waitAllRestoreFinish(dbName)

        result = sql "SELECT * FROM ${dbName}.${tableName}"
        assertEquals(values.size(), result.size(), authCase.name)
        sql "DROP REPOSITORY `${repoName}`"
    }

    sql "DROP TABLE ${dbName}.${tableName} FORCE"
    sql "DROP DATABASE ${dbName} FORCE"
}
