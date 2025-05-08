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

suite("test_backup_store_with_db_properties_kv","backup_restore") {
    String dbName = "test_backup_store_with_db_properties_kv_db"
    String suiteName = "test_backup_store_with_db_properties_kv"
    String repoName = "${suiteName}_repo_" + UUID.randomUUID().toString().replace("-", "")
    String snapshotName = "${suiteName}_snapshot"
    String tableNamePrefix = "${suiteName}"

    def forceReplicaNum = getFeConfig('force_olap_table_replication_num') as int
    def replicaNum = forceReplicaNum > 0 ? forceReplicaNum : 1
    def syncer = getSyncer()
    syncer.createS3Repository(repoName)

    sql "DROP DATABASE IF EXISTS ${dbName} FORCE"

    sql """ 
        CREATE DATABASE IF NOT EXISTS ${dbName} 
        PROPERTIES
        (
            "replication_allocation" = "tag.location.default:${replicaNum}",
            "test_key" = "test_value"
        )
        """

    def result_origin = sql "SHOW CREATE DATABASE ${dbName}"
    assertTrue(result_origin.toString().containsIgnoreCase("tag.location.default:${replicaNum}"))
    assertTrue(result_origin.toString().containsIgnoreCase('"test_key" = "test_value"'))

    int numTables = 10;
    int numRows = 10;
    List<String> tables = []

    for (int i = 0; i < numTables; ++i) {
        String tableName = "${tableNamePrefix}_t${i}"
        tables.add(tableName)
        sql "DROP TABLE IF EXISTS ${dbName}.${tableName} FORCE"
        sql """
            CREATE TABLE ${dbName}.${tableName} (
                `id` LARGEINT NOT NULL
            )
            AGGREGATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 2
            PROPERTIES
            (
                "replication_allocation" = "tag.location.default:${replicaNum}"
            )
            """
        List<String> values = []
        for (int j = 1; j <= numRows; ++j) {
            values.add("(${j})")
        }
        sql "INSERT INTO ${dbName}.${tableName} VALUES ${values.join(",")}"
        def result = sql "SELECT * FROM ${dbName}.${tableName}"
        assertEquals(result.size(), numRows);
    }

    def backupTables = tables[0..numTables - 1]
    sql """
        BACKUP SNAPSHOT ${dbName}.${snapshotName}
        TO `${repoName}`
    """

    syncer.waitSnapshotFinish(dbName)

    def snapshot = syncer.getSnapshotTimestamp(repoName, snapshotName)
    assertTrue(snapshot != null)

    for (def tableName in backupTables) {
        sql "TRUNCATE TABLE ${dbName}.${tableName}"
    }

    sql """
        RESTORE SNAPSHOT ${dbName}.${snapshotName}
        FROM `${repoName}`
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "reserve_replica" = "true"
        )
    """

    syncer.waitAllRestoreFinish(dbName)

    def result_restore = sql "SHOW CREATE DATABASE ${dbName}"
    assertEquals(result_restore, result_origin);

    for (def tableName in tables) {
        def result = sql "SELECT * FROM ${dbName}.${tableName}"
        assertEquals(result.size(), numRows);
        sql "DROP TABLE ${dbName}.${tableName} FORCE"
    }

    sql "DROP REPOSITORY `${repoName}`"
}

