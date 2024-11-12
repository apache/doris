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

// test restore back to old instance
suite("test_backup_cooldown_1", "backup_cooldown_data") {

    String suiteName = "test_backup_cooldown_1"
    String resource_name1 = "resource_${suiteName}_1"
    String policy_name1 = "policy_${suiteName}_1"
    String resource_name2 = "resource_${suiteName}_2"
    String policy_name2 = "policy_${suiteName}_2"
    String dbName = "${suiteName}_db"
    String tableName = "${suiteName}_table"
    String snapshotName = "${suiteName}_snapshot"
    String repoName = "${suiteName}_repo"
    def found = 0
    def records
    def result
    def row

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
    result = sql "SELECT * FROM ${dbName}.${tableName}"
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

    // 1 老表存在的情况
    // 1.1 restore 不指定。预期失败, 不支持将冷热属性的表恢复到已存在的表中。
    // 1.2 restore 指定 （"reserve_storage_policy"="true"),  预期失败, 不支持将冷热属性的表恢复到已存在的表中。
    // 1.3 restore 指定 （"reserve_storage_policy"="false"), 预期成功，且不落冷

    // 2 删除老表
    // 1.1 restore 不指定 预期成功，且落冷
    // 1.2 restore 指定 （"reserve_storage_policy"="true"）预期成功，且落冷
    // 1.3 restore 指定 （"reserve_storage_policy"="false"）预期成功，且不落冷


    // 3 删除resource 和 policy
    // 1.1 restore 不指定 预期成功，且落冷
    // 1.2 restore 指定 （"reserve_storage_policy"="true"）预期成功，且落冷
    // 1.3 restore 指定 （"reserve_storage_policy"="false"）预期成功，且不落冷


    // 1. old table exist
    // 1.1 restore normal fail 
    // 1.2 restore with（"reserve_storage_policy"="true") fail
    // 1.3 restore with（"reserve_storage_policy"="false") success and don't cooldown
    logger.info(" ====================================== 1.1 ==================================== ")
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

    // restore failed 
    records = sql_return_maparray "SHOW restore FROM ${dbName}"
    row = records[records.size() - 1] 
    assertTrue(row.Status.contains("Can't restore remote partition"))
         
    result = sql "SELECT * FROM ${dbName}.${tableName}"
    assertEquals(result.size(), values.size());


    logger.info(" ====================================== 1.2 ==================================== ")
    sql """
        RESTORE SNAPSHOT ${dbName}.${snapshotName}
        FROM `${repoName}`
        ON ( `${tableName}`)
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "reserve_replica" = "true",
            "reserve_storage_policy"="true"
        )
    """

    syncer.waitAllRestoreFinish(dbName)

    // restore failed 
    records = sql_return_maparray "SHOW restore FROM ${dbName}"
    row = records[records.size() - 1] 
    assertTrue(row.Status.contains("Can't restore remote partition"))
         
    result = sql "SELECT * FROM ${dbName}.${tableName}"
    assertEquals(result.size(), values.size());


    logger.info(" ====================================== 1.3 ==================================== ")
    sql """
        RESTORE SNAPSHOT ${dbName}.${snapshotName}
        FROM `${repoName}`
        ON ( `${tableName}`)
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "reserve_replica" = "true",
            "reserve_storage_policy"="false"
        )
    """

    syncer.waitAllRestoreFinish(dbName)

    // restore failed 
    records = sql_return_maparray "SHOW restore FROM ${dbName}"
    row = records[records.size() - 1] 
    assertTrue(row.Status.contains("Can't restore remote partition"))
         
    result = sql "SELECT * FROM ${dbName}.${tableName}"
    assertEquals(result.size(), values.size());


    // 2. drop old table 
    // 2.1 restore normal success and cooldown
    // 2.2 restore with （"reserve_storage_policy"="true"）success and cooldown
    // 2.3 restore with （"reserve_storage_policy"="false"）success and don't cooldown
    logger.info(" ====================================== 2.1 ==================================== ")
    sql "DROP TABLE IF EXISTS ${dbName}.${tableName}"
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
    assertNotEquals('0.000 ', result[0][5].toString())


    logger.info(" ====================================== 2.2 ==================================== ")
    sql "DROP TABLE IF EXISTS ${dbName}.${tableName}"
    sql """
        RESTORE SNAPSHOT ${dbName}.${snapshotName}
        FROM `${repoName}`
        ON ( `${tableName}`)
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "reserve_replica" = "true",
            "reserve_storage_policy"="true"
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
    assertNotEquals('0.000 ', result[0][5].toString())


    logger.info(" ====================================== 2.3 ==================================== ")
    sql "DROP TABLE IF EXISTS ${dbName}.${tableName}"
    sql """
        RESTORE SNAPSHOT ${dbName}.${snapshotName}
        FROM `${repoName}`
        ON ( `${tableName}`)
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "reserve_replica" = "true",
            "reserve_storage_policy"="false"
        )
    """

    syncer.waitAllRestoreFinish(dbName)

    result = sql "SELECT * FROM ${dbName}.${tableName}"
    assertEquals(result.size(), values.size());

    // check table don't have storage_policy
    records = sql_return_maparray "show storage policy using"
    found = 0
    for (def res2 : records) {
        if (res2.Database.equals(dbName) && res2.Table.equals(tableName)) {
            found = 1
            break
        }
    }    
    assertEquals(found, 0)


    // 3. drop old table and resource and policy
    // 3.1 restore normal success and cooldown
    // 3.2 restore with（"reserve_storage_policy"="true"）success and cooldown
    // 3.3 restore with（"reserve_storage_policy"="false"）success and don't cooldown
    logger.info(" ====================================== 3.1 ==================================== ")
    sql "DROP TABLE IF EXISTS ${dbName}.${tableName}"
    try_sql """
    drop storage policy ${policy_name1};
    """
    try_sql """
    drop resource ${resource_name1};
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
    assertNotEquals('0.000 ', result[0][5].toString())


    logger.info(" ====================================== 3.2 ==================================== ")
    sql "DROP TABLE IF EXISTS ${dbName}.${tableName}"
    try_sql """
    drop storage policy ${policy_name1};
    """
    try_sql """
    drop resource ${resource_name1};
    """
    sql """
        RESTORE SNAPSHOT ${dbName}.${snapshotName}
        FROM `${repoName}`
        ON ( `${tableName}`)
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "reserve_replica" = "true",
            "reserve_storage_policy"="true"
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
    assertNotEquals('0.000 ', result[0][5].toString())


    logger.info(" ====================================== 3.3 ==================================== ")
    sql "DROP TABLE IF EXISTS ${dbName}.${tableName}"
    try_sql """
    drop storage policy ${policy_name1};
    """
    try_sql """
    drop resource ${resource_name1};
    """
    sql """
        RESTORE SNAPSHOT ${dbName}.${snapshotName}
        FROM `${repoName}`
        ON ( `${tableName}`)
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "reserve_replica" = "true",
            "reserve_storage_policy"="false"
        )
    """

    syncer.waitAllRestoreFinish(dbName)

    result = sql "SELECT * FROM ${dbName}.${tableName}"
    assertEquals(result.size(), values.size());

    // check table don't have storage_policy
    records = sql_return_maparray "show storage policy using"
    found = 0
    for (def res2 : records) {
        if (res2.Database.equals(dbName) && res2.Table.equals(tableName)) {
            found = 1
            break
        }
    }    
    assertEquals(found, 0)

    // check storage policy ${policy_name1} not exist
    records = sql_return_maparray "show storage policy"
    found = 0
    for (def res2 : records) {
        if (res2.PolicyName.equals(policy_name1)) {
            found = 1
            break
        }
    }    
    assertEquals(found, 0)

    // check resource ${resource_name1} not exist
    records = sql_return_maparray "show storage policy"
    found = 0
    for (def res2 : records) {
        if (res2.Name.equals(resource_name1)) {
            found = 1
            break
        }
    }    
    assertEquals(found, 0)


    // 4. alter policy and success
    logger.info(" ====================================== 4.1 ==================================== ")
    sql "DROP TABLE IF EXISTS ${dbName}.${tableName}"
    sql """
    ALTER STORAGE POLICY ${policy_name2} PROPERTIES ("cooldown_ttl" = "11");
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
    assertNotEquals('0.000 ', result[0][5].toString())

    // check storage policy ${policy_name2}  exist
    records = sql_return_maparray "show storage policy"
    found = 0
    for (def res2 : records) {
        if (res2.PolicyName.equals(policy_name2) && res2.CooldownTtl.equals("11")) {
            found = 1
            break
        }
    }    
    assertEquals(found, 1)



    //cleanup
    sql "DROP TABLE IF EXISTS ${dbName}.${tableName} FORCE"
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





// test restore back to a new instance
suite("test_backup_cooldown_2", "backup_cooldown_data") {

    String suiteName = "test_backup_cooldown_2"
    String resource_name1 = "resource_${suiteName}_1"
    String policy_name1 = "policy_${suiteName}_1"
    String resource_name2 = "resource_${suiteName}_2"
    String resource_new_name = "resource_${suiteName}_new"
    String policy_name2 = "policy_${suiteName}_2"
    String dbName = "${suiteName}_db"
    String tableName = "${suiteName}_table"
    String snapshotName = "${suiteName}_snapshot"
    String repoName = "${suiteName}_repo"
    def found = 0
    def records
    def syncer = getSyncer()
    def result
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
      CREATE RESOURCE IF NOT EXISTS "${resource_new_name}"
      PROPERTIES(
          "type"="s3",
          "AWS_ENDPOINT" = "${getS3Endpoint()}",
          "AWS_REGION" = "${getS3Region()}",
          "AWS_ROOT_PATH" = "regression/cooldown3",
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
    result = sql "SELECT * FROM ${dbName}.${tableName}"
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

    // 1 老表存在的情况
    // 1.1 restore 指定 （"storage_resource"="resource_name_exist"),  预期失败，不支持将冷热属性的表恢复到已存在的表中。
    // 1.2 restore 指定 （"storage_resource"="resource_name_not_exist"), 预期失败，resource不存在
    // 1.3 restore 指定 （"storage_resource"="resource_new_name"),  预期失败，不支持将冷热属性的表恢复到已存在的表中。


    // 2 删除表
    // 2.1 restore 指定 （"storage_resource"="resource_name_exist"),  预期失败，resource路径需不一致
    // 2.2 restore 指定 （"storage_resource"="resource_name_not_exist"), 预期失败，resource不存在
    // 2.3 restore 指定 （"storage_resource"="resource_new_name"),  storage policy 存在失败


    // 3 删除表和policy
    // 3.1 restore 指定 （"storage_resource"="resource_name_not_exist"), 预期失败，resource不存在
    // 3.2 restore 指定 （"storage_resource"="resource_new_name"),  成功
    


    // 4 删除表和policy 同时指定storage_resource和reserve_storage_policy
    // 4.1 restore 指定 （"storage_resource"="resource_name_not_exist", "reserve_storage_policy"="true"), 预期失败，resource不存在
    // 4.2 restore 指定 （"storage_resource"="resource_name_not_exist", "reserve_storage_policy"="false"), 预期失败，resource不存在
    // 4.3 restore 指定 （"storage_resource"="resource_new_name", "reserve_storage_policy"="true"), 预期成功,且落冷
    // 4.4 restore 指定 （"storage_resource"="resource_new_name", "reserve_storage_policy"="false"), 预期成功，且不落冷





    // 1 old table exist
    // 1.1 restore with （"storage_resource"="resource_name1") fail
    // 1.2 restore with （"storage_resource"="resource_name_not_exist") fail
    // 1.3 restore with （"storage_resource"="resource_new_name") fail
    logger.info(" ====================================== 1.1 ==================================== ")
    sql """
        RESTORE SNAPSHOT ${dbName}.${snapshotName}
        FROM `${repoName}`
        ON ( `${tableName}`)
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "reserve_replica" = "true",
            "storage_resource"="${resource_name1}"
        )
    """

    syncer.waitAllRestoreFinish(dbName)

    result = sql "SELECT * FROM ${dbName}.${tableName}"
    assertEquals(result.size(), values.size());

    // restore failed 
    records = sql_return_maparray "SHOW restore FROM ${dbName}"
    row = records[records.size() - 1] 
    assertTrue(row.Status.contains("Can't restore remote partition"))




    logger.info(" ====================================== 1.2 ==================================== ")
    def fail_restore_1 = try_sql """
        RESTORE SNAPSHOT ${dbName}.${snapshotName}
        FROM `${repoName}`
        ON ( `${tableName}`)
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "reserve_replica" = "true",
            "storage_resource"="resource_name_not_exist"
        )
    """

    logger.info("fail_restore_1: ${fail_restore_1}")

    assertEquals(fail_restore_1, null)

    logger.info(" ====================================== 1.3 ==================================== ")
    sql """
        RESTORE SNAPSHOT ${dbName}.${snapshotName}
        FROM `${repoName}`
        ON ( `${tableName}`)
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "reserve_replica" = "true",
            "storage_resource"="${resource_new_name}"
        )
    """

    syncer.waitAllRestoreFinish(dbName)

    result = sql "SELECT * FROM ${dbName}.${tableName}"
    assertEquals(result.size(), values.size());

    // restore failed 
    records = sql_return_maparray "SHOW restore FROM ${dbName}"
    row = records[records.size() - 1] 
    assertTrue(row.Status.contains("Can't restore remote partition"))




    // 2 drop old table
    // 2.1 restore with （"storage_resource"="resource_name_exist"）fail
    // 2.2 restore with （"storage_resource"="resource_name_not_exist"） fail
    // 2.3 restore with （"storage_resource"="resource_new_name"）fail
    logger.info(" ====================================== 2.1 ==================================== ")
    sql "DROP TABLE IF EXISTS ${dbName}.${tableName}"
    sql """
        RESTORE SNAPSHOT ${dbName}.${snapshotName}
        FROM `${repoName}`
        ON ( `${tableName}`)
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "reserve_replica" = "true",
            "storage_resource"="${resource_name1}"
        )
    """

    syncer.waitAllRestoreFinish(dbName)

    // restore failed 
    records = sql_return_maparray "SHOW restore FROM ${dbName}"
    row = records[records.size() - 1] 
    assertTrue(row.Status.contains("should not same as restored resource root path"))




    logger.info(" ====================================== 2.2 ==================================== ")
    sql "DROP TABLE IF EXISTS ${dbName}.${tableName}"
    def fail_restore_2 = try_sql """
        RESTORE SNAPSHOT ${dbName}.${snapshotName}
        FROM `${repoName}`
        ON ( `${tableName}`)
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "reserve_replica" = "true",
            "storage_resource"="resource_name_not_exist"
        )
    """

    assertEquals(fail_restore_2, null)


    logger.info(" ====================================== 2.3 ==================================== ")
    sql "DROP TABLE IF EXISTS ${dbName}.${tableName}"
    sql """
        RESTORE SNAPSHOT ${dbName}.${snapshotName}
        FROM `${repoName}`
        ON ( `${tableName}`)
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "reserve_replica" = "true",
            "storage_resource"="${resource_new_name}"
        )
    """

    syncer.waitAllRestoreFinish(dbName)

    // restore failed 
    records = sql_return_maparray "SHOW restore FROM ${dbName}"
    row = records[records.size() - 1] 
    assertTrue(row.Status.contains("already exist but with different properties"))

    // 3 drop table and resource and policy
    // 3.1 restore with （"storage_resource"="resource_name_not_exist") fail
    // 3.2 restore with （"storage_resource"="resource_new_name") success and cooldown
    logger.info(" ====================================== 3.1 ==================================== ")
    sql "DROP TABLE IF EXISTS ${dbName}.${tableName}"
    try_sql """
    drop storage policy ${policy_name1};
    """
    try_sql """
    drop storage policy ${policy_name2};
    """
    def fail_restore_3 = try_sql """
        RESTORE SNAPSHOT ${dbName}.${snapshotName}
        FROM `${repoName}`
        ON ( `${tableName}`)
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "reserve_replica" = "true",
            "storage_resource"="resource_name_not_exist"
        )
    """

    assertEquals(fail_restore_3, null)

    logger.info(" ====================================== 3.2 ==================================== ")
    sql "DROP TABLE IF EXISTS ${dbName}.${tableName}"
    try_sql """
    drop storage policy ${policy_name1};
    """
    try_sql """
    drop storage policy ${policy_name2};
    """
 
    sql """
        RESTORE SNAPSHOT ${dbName}.${snapshotName}
        FROM `${repoName}`
        ON ( `${tableName}`)
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "reserve_replica" = "true",
            "storage_resource"="${resource_new_name}"
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
    assertNotEquals('0.000 ', result[0][5].toString())

    // check plocy_name1 storage_resource change to resource_new_name
    records = sql_return_maparray "show storage policy"
    found = 0
    for (def res2 : records) {
        if (res2.StorageResource.equals(resource_new_name) && res2.PolicyName.equals(policy_name1)) {
            found = 1
            break
        }
    }    
    assertEquals(found, 1)

    // check plocy_name2 storage_resource change to resource_new_name
    records = sql_return_maparray "show storage policy"
    found = 0
    for (def res2 : records) {
        if (res2.StorageResource.equals(resource_new_name) && res2.PolicyName.equals(policy_name2)) {
            found = 1
            break
        }
    }    
    assertEquals(found, 1)

    

    // 4 drop table/resource/policy,  set both storage_resource and reserve_storage_policy
    // 4.1 restore with （"storage_resource"="resource_name_not_exist", "reserve_storage_policy"="true") fail
    // 4.2 restore with （"storage_resource"="resource_name_not_exist", "reserve_storage_policy"="false") fail
    // 4.3 restore with （"storage_resource"="resource_new_name", "reserve_storage_policy"="true") success and cooldown
    // 4.4 restore with （"storage_resource"="resource_new_name", "reserve_storage_policy"="false") success and don't cooldown
    logger.info(" ====================================== 4.1 ==================================== ")
    sql "DROP TABLE if exists ${dbName}.${tableName}"
    try_sql """
    drop storage policy ${policy_name1};
    """
    try_sql """
    drop storage policy ${policy_name2};
    """
    def fail_restore_4 = try_sql """
        RESTORE SNAPSHOT ${dbName}.${snapshotName}
        FROM `${repoName}`
        ON ( `${tableName}`)
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "reserve_replica" = "true",
            "storage_resource"="resource_name_not_exist",
            "reserve_storage_policy"="true"
        )
    """

    assertEquals(fail_restore_4, null)


    logger.info(" ====================================== 4.2 ==================================== ")
    sql "DROP TABLE IF EXISTS ${dbName}.${tableName}"
    try_sql """
    drop storage policy ${policy_name1};
    """
    try_sql """
    drop storage policy ${policy_name2};
    """

    def fail_restore_5 = try_sql """
        RESTORE SNAPSHOT ${dbName}.${snapshotName}
        FROM `${repoName}`
        ON ( `${tableName}`)
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "reserve_replica" = "true",
            "storage_resource"="resource_name_not_exist",
            "reserve_storage_policy"="false"
        )
    """

    assertEquals(fail_restore_5, null)


    logger.info(" ====================================== 4.3 ==================================== ")
    sql "DROP TABLE IF EXISTS ${dbName}.${tableName}"
    try_sql """
    drop storage policy ${policy_name1};
    """
    try_sql """
    drop storage policy ${policy_name2};
    """
 
    sql """
        RESTORE SNAPSHOT ${dbName}.${snapshotName}
        FROM `${repoName}`
        ON ( `${tableName}`)
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "reserve_replica" = "true",
            "storage_resource"="${resource_new_name}",
            "reserve_storage_policy"="true"
        )
    """

    syncer.waitAllRestoreFinish(dbName)

    result = sql "SELECT * FROM ${dbName}.${tableName}"
    assertEquals(result.size(), values.size());

 // check plocy_name1 storage_resource change to resource_new_name
    records = sql_return_maparray "show storage policy"
    found = 0
    for (def res2 : records) {
        if (res2.StorageResource.equals(resource_new_name) && res2.PolicyName.equals(policy_name1)) {
            found = 1
            break
        }
    }    
    assertEquals(found, 1)

    // check plocy_name2 storage_resource change to resource_new_name
    records = sql_return_maparray "show storage policy"
    found = 0
    for (def res2 : records) {
        if (res2.StorageResource.equals(resource_new_name) && res2.PolicyName.equals(policy_name2)) {
            found = 1
            break
        }
    }    
    assertEquals(found, 1)

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
    assertNotEquals('0.000 ', result[0][5].toString())



    logger.info(" ====================================== 4.4 ==================================== ")
    sql "DROP TABLE IF EXISTS ${dbName}.${tableName}"
    try_sql """
    drop storage policy ${policy_name1};
    """
    try_sql """
    drop storage policy ${policy_name2};
    """
 
    sql """
        RESTORE SNAPSHOT ${dbName}.${snapshotName}
        FROM `${repoName}`
        ON ( `${tableName}`)
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "reserve_replica" = "true",
            "storage_resource"="${resource_new_name}",
            "reserve_storage_policy"="false"        
        )
    """

    syncer.waitAllRestoreFinish(dbName)

    result = sql "SELECT * FROM ${dbName}.${tableName}"
    assertEquals(result.size(), values.size());


    // check table don't have storage_policy
    records = sql_return_maparray "show storage policy using"
    found = 0
    for (def res2 : records) {
        if (res2.Database.equals(dbName) && res2.Table.equals(tableName)) {
            found = 1
            break
        }
    }    
    assertEquals(found, 0)
  

    //cleanup
    sql "DROP TABLE IF EXISTS ${dbName}.${tableName} FORCE"
    sql "DROP DATABASE ${dbName} FORCE"
    sql "DROP REPOSITORY `${repoName}`"

    try_sql """
    drop storage policy ${policy_name1};
    """

    try_sql """
    drop resource ${resource_name1};
    """

    try_sql """
    drop storage policy ${policy_name2};
    """

    try_sql """
    drop resource ${resource_name2};
    """
}