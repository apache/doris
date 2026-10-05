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

suite("test_is_being_synced") {
    def syncer = getSyncer()
    if (!syncer.checkEnableFeatureBinlog()) {
        logger.info("fe enable_feature_binlog is false, skip case test_is_being_synced")
        return
    }

    String suiteName = "test_is_being_synced"
    String repoName = "repo_" + UUID.randomUUID().toString().replace("-", "")
    String dbName = "${suiteName}_db"
    String tableName = "${suiteName}_table"
    String resourceName = "${suiteName}_resource"
    String policyName = "${suiteName}_policy"

    sql """
        CREATE RESOURCE IF NOT EXISTS "${resourceName}"
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
        CREATE STORAGE POLICY IF NOT EXISTS ${policyName}
        PROPERTIES(
            "storage_resource" = "${resourceName}",
            "cooldown_ttl" = "300"
        )
    """

    sql "DROP DATABASE IF EXISTS ${dbName} FORCE"
    sql "CREATE DATABASE ${dbName}"

    // 1. Create a table with is_being_synced property
    sql """
        CREATE TABLE ${dbName}.${tableName}
        (
            k1 INT,
            v1 INT
        )
        ENGINE=OLAP
        DUPLICATE KEY(k1)
        PARTITION BY RANGE(k1)
        (
            PARTITION p1 VALUES LESS THAN (100),
            PARTITION p2 VALUES LESS THAN (200),
            PARTITION p3 VALUES LESS THAN (300) ("storage_policy" = "${policyName}")
        )
        DISTRIBUTED BY HASH(k1) BUCKETS 3
        PROPERTIES (
            "replication_num" = "1",
            "is_being_synced" = "true",
            "storage_policy" = "${policyName}"
        )
    """

    def show_result = sql "SHOW CREATE TABLE ${dbName}.${tableName}"
    def create_table_sql = show_result[0][1]
    logger.info("${create_table_sql}")

    assertFalse(create_table_sql.containsIgnoreCase("${policyName}"))
    assertTrue(create_table_sql.containsIgnoreCase('"is_being_synced" = "true"'))

    // 2. Create table with is_being_synced property, storage policy is not exists
    sql """ DROP TABLE ${dbName}.${tableName} FORCE """
    sql """
        CREATE TABLE ${dbName}.${tableName}
        (
            k1 INT,
            v1 INT
        )
        ENGINE=OLAP
        DUPLICATE KEY(k1)
        PARTITION BY RANGE(k1)
        (
            PARTITION p1 VALUES LESS THAN (100),
            PARTITION p2 VALUES LESS THAN (200),
            PARTITION p3 VALUES LESS THAN (300) ("storage_policy" = "unknown_partition_storage_policy")
        )
        DISTRIBUTED BY HASH(k1) BUCKETS 3
        PROPERTIES (
            "replication_num" = "1",
            "is_being_synced" = "true",
            "storage_policy" = "unknown_table_storage_policy"
        )
    """

    show_result = sql "SHOW CREATE TABLE ${dbName}.${tableName}"
    create_table_sql = show_result[0][1]
    logger.info("${create_table_sql}")

    assertFalse(create_table_sql.containsIgnoreCase('unknown_partition_storage_policy'))
    assertFalse(create_table_sql.containsIgnoreCase('unknown_table_storage_policy'))

    // 3. For list partition
    sql """ DROP TABLE ${dbName}.${tableName} FORCE """
    sql """
        CREATE TABLE ${dbName}.${tableName}
        (
            k1 INT,
            v1 INT
        )
        ENGINE=OLAP
        DUPLICATE KEY(k1)
        PARTITION BY LIST(k1)
        (
            PARTITION p1 VALUES IN ((100), (200), (300)),
            PARTITION p2 VALUES IN ((400), (500), (600)),
            PARTITION p3 VALUES IN ((700), (800)) ("storage_policy" = "unknown_partition_storage_policy")
        )
        DISTRIBUTED BY HASH(k1) BUCKETS 3
        PROPERTIES (
            "replication_num" = "1",
            "is_being_synced" = "true",
            "storage_policy" = "unknown_table_storage_policy"
        )
    """

    show_result = sql "SHOW CREATE TABLE ${dbName}.${tableName}"
    create_table_sql = show_result[0][1]
    logger.info("${create_table_sql}")

    assertFalse(create_table_sql.containsIgnoreCase('unknown_partition_storage_policy'))
    assertFalse(create_table_sql.containsIgnoreCase('unknown_table_storage_policy'))

    // 4. For single partition
    sql """ DROP TABLE ${dbName}.${tableName} FORCE """
    sql """
        CREATE TABLE ${dbName}.${tableName}
        (
            k1 INT,
            v1 INT
        )
        ENGINE=OLAP
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 3
        PROPERTIES (
            "replication_num" = "1",
            "is_being_synced" = "true",
            "storage_policy" = "unknown_table_storage_policy"
        )
    """

    show_result = sql "SHOW CREATE TABLE ${dbName}.${tableName}"
    create_table_sql = show_result[0][1]
    logger.info("${create_table_sql}")

    assertFalse(create_table_sql.containsIgnoreCase('unknown_table_storage_policy'))

    // 5. For auto partition
    sql """ DROP TABLE ${dbName}.${tableName} FORCE """
    sql """
        CREATE TABLE ${dbName}.${tableName}
        (
            k1 INT,
            v1 INT,
            `TIME_STAMP` datev2 NOT NULL COMMENT '采集日期'
        )
        ENGINE=OLAP
        DUPLICATE KEY(k1)
        AUTO PARTITION BY RANGE (date_trunc(`TIME_STAMP`, 'month'))
        (
        )
        DISTRIBUTED BY HASH(k1) BUCKETS 3
        PROPERTIES (
            "replication_num" = "1",
            "is_being_synced" = "true",
            "storage_policy" = "unknown_table_storage_policy"
        )
    """

    show_result = sql "SHOW CREATE TABLE ${dbName}.${tableName}"
    create_table_sql = show_result[0][1]
    logger.info("${create_table_sql}")

    assertFalse(create_table_sql.containsIgnoreCase('unknown_table_storage_policy'))
}

