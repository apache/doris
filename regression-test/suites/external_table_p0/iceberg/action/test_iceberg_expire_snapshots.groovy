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

suite("test_iceberg_expire_snapshots", "p0,external,doris,external_docker,external_docker_doris") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String catalog_name = "test_iceberg_expire_snapshots"
    String db_name = "test_db"
    String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    sql """drop catalog if exists ${catalog_name}"""
    sql """
    CREATE CATALOG ${catalog_name} PROPERTIES (
        'type'='iceberg',
        'iceberg.catalog.type'='rest',
        'uri' = 'http://${externalEnvIp}:${rest_port}',
        "s3.access_key" = "admin",
        "s3.secret_key" = "password",
        "s3.endpoint" = "http://${externalEnvIp}:${minio_port}",
        "s3.region" = "us-east-1"
    );"""

    sql """switch ${catalog_name}"""
    sql """CREATE DATABASE IF NOT EXISTS ${db_name} """
    sql """use ${db_name}"""

    // =====================================================================================
    // Test Case 1: expire_snapshots action with retain_last parameter
    // Tests the ability to expire old snapshots from Iceberg tables
    // =====================================================================================
    logger.info("Starting expire_snapshots test case")

    // Create test table for expire_snapshots
    sql """DROP TABLE IF EXISTS ${db_name}.test_expire_snapshots"""
    sql """
        CREATE TABLE ${db_name}.test_expire_snapshots (
            id BIGINT,
            data STRING
        ) ENGINE=iceberg
    """

    // Insert data to create multiple snapshots
    sql """INSERT INTO ${db_name}.test_expire_snapshots VALUES (1, 'data1')"""
    sql """INSERT INTO ${db_name}.test_expire_snapshots VALUES (2, 'data2')"""
    sql """INSERT INTO ${db_name}.test_expire_snapshots VALUES (3, 'data3')"""
    sql """INSERT INTO ${db_name}.test_expire_snapshots VALUES (4, 'data4')"""

    // Verify 4 snapshots exist
    List<List<Object>> snapshotsBefore = sql """
        SELECT snapshot_id FROM test_expire_snapshots\$snapshots ORDER BY committed_at
    """
    assertTrue(snapshotsBefore.size() == 4, "Expected 4 snapshots before expiration")
    logger.info("Snapshots before expire: ${snapshotsBefore}")

    // Test 1: expire_snapshots with retain_last=2
    qt_expire_snapshots_result """
        ALTER TABLE ${catalog_name}.${db_name}.test_expire_snapshots
        EXECUTE expire_snapshots("retain_last" = "2")
    """

    // Verify only 2 snapshots remain
    List<List<Object>> snapshotsAfter = sql """
        SELECT snapshot_id FROM test_expire_snapshots\$snapshots ORDER BY committed_at
    """
    assertTrue(snapshotsAfter.size() == 2, "Expected 2 snapshots after expiration with retain_last=2")
    logger.info("Snapshots after expire: ${snapshotsAfter}")

    // Test 2: Verify data is still accessible
    qt_after_expire_snapshots """SELECT * FROM test_expire_snapshots ORDER BY id"""

    logger.info("expire_snapshots test case completed successfully")

    // =====================================================================================
    // Test Case 2: expire_snapshots with older_than parameter
    // =====================================================================================
    // Test expire_snapshots with older_than (should work, but not expire snapshots that are recent)
    List<List<Object>> expireOlderThanResult = sql """
        ALTER TABLE ${catalog_name}.${db_name}.test_expire_snapshots
        EXECUTE expire_snapshots("older_than" = "2024-01-01T00:00:00")
    """
    logger.info("Expire older_than result: ${expireOlderThanResult}")

    // =====================================================================================
    // Negative Test Cases for expire_snapshots
    // =====================================================================================

    // Test validation - missing required property (neither older_than, retain_last, nor snapshot_ids)
    test {
        sql """
            ALTER TABLE ${catalog_name}.${db_name}.test_expire_snapshots EXECUTE expire_snapshots
            ()
        """
        exception "At least one of 'older_than', 'retain_last', or 'snapshot_ids' must be specified"
    }

    // Test expire_snapshots with invalid older_than timestamp
    test {
        sql """
            ALTER TABLE ${catalog_name}.${db_name}.test_expire_snapshots EXECUTE expire_snapshots
            ("older_than" = "not-a-timestamp")
        """
        exception "Invalid older_than format"
    }

    // Test expire_snapshots with negative timestamp
    test {
        sql """
            ALTER TABLE ${catalog_name}.${db_name}.test_expire_snapshots EXECUTE expire_snapshots
            ("older_than" = "-1000")
        """
        exception "older_than timestamp must be non-negative"
    }

    // Test validation - retain_last must be at least 1
    test {
        sql """
            ALTER TABLE ${catalog_name}.${db_name}.test_expire_snapshots EXECUTE expire_snapshots
            ("retain_last" = "0")
        """
        exception "retain_last must be positive, got: 0"
    }

    // Test expire_snapshots with invalid retain_last format
    test {
        sql """
            ALTER TABLE ${catalog_name}.${db_name}.test_expire_snapshots EXECUTE expire_snapshots
            ("retain_last" = "not-a-number")
        """
        exception "Invalid retain_last format: not-a-number"
    }

    // Test expire_snapshots with negative retain_last
    test {
        sql """
            ALTER TABLE ${catalog_name}.${db_name}.test_expire_snapshots EXECUTE expire_snapshots
            ("retain_last" = "-5")
        """
        exception "retain_last must be positive, got: -5"
    }

    // Test expire_snapshots with invalid snapshot_ids format
    test {
        sql """
            ALTER TABLE ${catalog_name}.${db_name}.test_expire_snapshots EXECUTE expire_snapshots
            ("snapshot_ids" = "not-a-number")
        """
        exception "Invalid snapshot_id format: not-a-number"
    }

    // Test expire_snapshots with partition specification (should fail)
    test {
        sql """
            ALTER TABLE ${catalog_name}.${db_name}.test_expire_snapshots EXECUTE expire_snapshots
            ("older_than" = "2024-01-01T00:00:00") PARTITIONS (p1, p2, p3)
        """
        exception "Action 'expire_snapshots' does not support partition specification"
    }
}
