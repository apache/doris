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

suite("test_backup_cooldown", "backup_cooldown_data") {

    String suiteName = "test_backup_cooldown"
    String resource_name1 = "resource_${suiteName}_1"
    String policy_name1 = "policy_${suiteName}_1"
    String resource_name2 = "resource_${suiteName}_2"
    String policy_name2 = "policy_${suiteName}_2"
    String dbName = "${suiteName}_db"
    String tableName = "${suiteName}_table"
    String snapshotName = "${suiteName}_snapshot"
    String repoName = "${suiteName}_repo"

    def syncer = getSyncer()
    syncer.createS3Repository(repoName)



    sql """
      CREATE RESOURCE IF NOT EXISTS "${resource_name1}"
      PROPERTIES(
          "type"="s3",
          "AWS_ENDPOINT" = "${getS3Endpoint()}",
          "AWS_REGION" = "${getS3Region()}",
          "AWS_ROOT_PATH" = "regression/cooldown1",
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
      CREATE RESOURCE IF NOT EXISTS "${resource_name2}"
      PROPERTIES(
          "type"="s3",
          "AWS_ENDPOINT" = "${getS3Endpoint()}",
          "AWS_REGION" = "${getS3Region()}",
          "AWS_ROOT_PATH" = "regression/cooldown2",
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
      CREATE STORAGE POLICY IF NOT EXISTS ${policy_name1}
      PROPERTIES(
          "storage_resource" = "${resource_name1}",
          "cooldown_ttl" = "10"
      )
    """

    sql """
      CREATE STORAGE POLICY IF NOT EXISTS ${policy_name2}
      PROPERTIES(
          "storage_resource" = "${resource_name2}",
          "cooldown_ttl" = "10"
      )
    """

    //generate_cooldown_task_interval_sec default is 20

    sql "CREATE DATABASE IF NOT EXISTS ${dbName}"
    sql "DROP TABLE IF EXISTS ${dbName}.${tableName}"

    sql """
     CREATE TABLE ${dbName}.${tableName}
     (
         k1 BIGINT,
         v1 VARCHAR(48),
         INDEX idx1 (v1) USING INVERTED PROPERTIES("parser" = "english")
     )
     DUPLICATE KEY(k1)
     PARTITION BY RANGE(`k1`)
     (
                PARTITION p201701 VALUES [(0), (3)) ("storage_policy" = "${policy_name1}"),
                PARTITION `p201702` VALUES LESS THAN (6)("storage_policy" = "${policy_name2}"),
                PARTITION `p2018` VALUES [(6),(100))
    )
     DISTRIBUTED BY HASH (k1) BUCKETS 3
     PROPERTIES(
         "replication_allocation" = "tag.location.default: 1"
     );
     """

    List<String> values = []
    for (int i = 1; i <= 10; ++i) {
        values.add("(${i}, ${i})")
    }
    sql "INSERT INTO ${dbName}.${tableName} VALUES ${values.join(",")}"
    def result = sql "SELECT * FROM ${dbName}.${tableName}"
    assertEquals(result.size(), values.size());


    // wait cooldown
    result = sql "show data FROM ${dbName}.${tableName}"
    sqlResult = result[0][5].toString();
    int count = 0;
    while (sqlResult.contains("0.00")) {
        if (++count >= 120) {  // 10min
            logger.error('cooldown task is timeouted')
            throw new Exception("cooldown task is timeouted after 10 mins")
        }
        Thread.sleep(5000)

        result = sql "show data FROM ${dbName}.${tableName}"
        sqlResult = result[0][5].toString();


    }

    assertNotEquals('0.000 ', result[0][5].toString())

    sql """
        BACKUP SNAPSHOT ${dbName}.${snapshotName}
        TO `${repoName}`
        ON (${tableName})
    """

    syncer.waitSnapshotFinish(dbName)

    def snapshot = syncer.getSnapshotTimestamp(repoName, snapshotName)
    assertTrue(snapshot != null)

    sql "DROP TABLE ${dbName}.${tableName}"

    sql """
    drop storage policy ${policy_name1};
    """

    sql """
    drop resource ${resource_name1};
    """

    sql """
    drop storage policy ${policy_name2};
    """

    sql """
    drop resource ${resource_name2};
    """

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

    // wait cooldown
    result = sql "show data FROM ${dbName}.${tableName}"
    sqlResult = result[0][5].toString();
    count = 0;
    while (sqlResult.contains("0.00")) {
        if (++count >= 120) {  // 10min
            logger.error('cooldown task is timeouted')
            throw new Exception("cooldown task is timeouted after 10 mins")
        }
        Thread.sleep(5000)

        result = sql "show data FROM ${dbName}.${tableName}"
        sqlResult = result[0][5].toString();


    }

    //cleanup
    sql "DROP TABLE ${dbName}.${tableName} FORCE"
    sql "DROP DATABASE ${dbName} FORCE"
    sql "DROP REPOSITORY `${repoName}`"

    sql """
    drop storage policy ${policy_name1};
    """

    sql """
    drop resource ${resource_name1};
    """

    sql """
    drop storage policy ${policy_name2};
    """

    sql """
    drop resource ${resource_name2};
    """
}