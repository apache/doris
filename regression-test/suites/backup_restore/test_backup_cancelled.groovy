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

suite("test_backup_cancelled", "backup_cancelled") {
    String suiteName = "test_backup_cancelled"
    String repoName = "repo_" + UUID.randomUUID().toString().replace("-", "")
    String dbName = "${suiteName}_db"
    String tableName = "${suiteName}_table"
    String snapshotName = "${suiteName}_snapshot"
    String snapshotName_1 = "${suiteName}_snapshot1"

    def syncer = getSyncer()
    syncer.createS3Repository(repoName)

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

    result = sql_return_maparray """show tablets from ${dbName}.${tableName}"""
    assertNotNull(result)
    def tabletId = null
    for (def res : result) {
        tabletId = res.TabletId
        break
    }

    // test failed to get tablet when truncate or drop table

    GetDebugPoint().enableDebugPointForAllBEs("SnapshotManager::make_snapshot.inject_failure", [tablet_id:"${tabletId}", execute:3]);


    sql """
        BACKUP SNAPSHOT ${dbName}.${snapshotName}
        TO `${repoName}`
        ON (${tableName})
    """

    syncer.waitSnapshotFinish(dbName)


    GetDebugPoint().disableDebugPointForAllBEs("SnapshotManager::make_snapshot.inject_failure")




    // test missing versions when compaction or balance

    GetDebugPoint().enableDebugPointForAllBEs("Tablet::capture_consistent_versions.inject_failure", [tablet_id:"${tabletId}", execute:1]);

    sql """
        BACKUP SNAPSHOT ${dbName}.${snapshotName_1}
        TO `${repoName}`
        ON (${tableName})
    """

    syncer.waitSnapshotFinish(dbName)

    GetDebugPoint().disableDebugPointForAllBEs("Tablet::capture_consistent_versions.inject_failure");


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


suite("test_backup_cooldown_cancelled", "backup_cooldown_cancelled") {

    String suiteName = "test_backup_cooldown_cancelled"
    String resource_name = "resource_${suiteName}"
    String policy_name= "policy_${suiteName}"
    String dbName = "${suiteName}_db"
    String tableName = "${suiteName}_table"
    String snapshotName = "${suiteName}_snapshot"
    String repoName = "repo_" + UUID.randomUUID().toString().replace("-", "")

    def syncer = getSyncer()
    syncer.createS3Repository(repoName)



    sql """
      CREATE RESOURCE IF NOT EXISTS "${resource_name}"
      PROPERTIES(
          "type"="s3",
          "AWS_ENDPOINT" = "${getS3Endpoint()}",
          "AWS_REGION" = "${getS3Region()}",
          "AWS_ROOT_PATH" = "regression/cooldown",
          "AWS_ACCESS_KEY" = "${getS3AK()}",
          "AWS_SECRET_KEY" = "${getS3SK()}",
          "AWS_MAX_CONNECTIONS" = "50",
          "AWS_REQUEST_TIMEOUT_MS" = "3000",
          "AWS_CONNECTION_TIMEOUT_MS" = "1000",
          "AWS_BUCKET" = "${getS3BucketName()}",
          "s3_validity_check" = "true"
      );
    """

    sql """
      CREATE STORAGE POLICY IF NOT EXISTS ${policy_name}
      PROPERTIES(
          "storage_resource" = "${resource_name}",
          "cooldown_ttl" = "300"
      )
    """

    sql "CREATE DATABASE IF NOT EXISTS ${dbName}"
    sql "DROP TABLE IF EXISTS ${dbName}.${tableName}"

    sql """
     CREATE TABLE ${dbName}.${tableName} 
     (
         k1 BIGINT,
         v1 VARCHAR(48)
     )
     DUPLICATE KEY(k1)
     DISTRIBUTED BY HASH (k1) BUCKETS 3
     PROPERTIES(
         "storage_policy" = "${policy_name}",
         "replication_allocation" = "tag.location.default: 1"
     );
     """


    // test backup cooldown table and should be cancelled
    sql """
        BACKUP SNAPSHOT ${dbName}.${snapshotName}
        TO `${repoName}`
        ON (${tableName})
    """

    syncer.waitSnapshotFinish(dbName)

    //cleanup
    sql "DROP TABLE ${dbName}.${tableName} FORCE"
    sql "DROP DATABASE ${dbName} FORCE"
    sql "DROP REPOSITORY `${repoName}`"

    sql """
    drop storage policy ${policy_name};
    """

    sql """
    drop resource ${resource_name};
    """
}
