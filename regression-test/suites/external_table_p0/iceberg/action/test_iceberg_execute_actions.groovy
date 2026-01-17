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

import java.time.format.DateTimeFormatter
import java.time.format.DateTimeFormatterBuilder
import java.time.temporal.ChronoField
import java.time.LocalDateTime
import java.time.ZoneId

suite("test_iceberg_optimize_actions_ddl", "p0,external,doris,external_docker,external_docker_doris") {
    DateTimeFormatter unifiedFormatter = new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd")
            .optionalStart()
            .appendLiteral('T')
            .optionalEnd()
            .optionalStart()
            .appendLiteral(' ')
            .optionalEnd()
            .appendPattern("HH:mm:ss")
            .optionalStart()
            .appendFraction(ChronoField.MILLI_OF_SECOND, 0, 3, true)
            .optionalEnd()
            .toFormatter()

    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String catalog_name = "test_iceberg_execute_actions_ddl"
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
    def table_name = "test_iceberg_systable_partitioned"

    sql """drop table if exists ${db_name}.test_fast_forward"""
    sql """
        CREATE TABLE ${db_name}.test_fast_forward (
        id BIGINT,
        name STRING,
        value INT
    ) ENGINE=iceberg;
    """
    sql """
    INSERT INTO ${db_name}.test_fast_forward VALUES
    (1, 'record1', 100);
    """
    sql """
    ALTER TABLE ${db_name}.test_fast_forward CREATE BRANCH feature_branch;
    """
    sql """INSERT INTO ${db_name}.test_fast_forward VALUES
    (2, 'record2', 200);"""
    sql """ALTER TABLE ${db_name}.test_fast_forward CREATE TAG feature_tag;"""
    sql """
    INSERT INTO ${db_name}.test_fast_forward VALUES
    (3, 'record3', 300);
    """

    sql """DROP TABLE IF EXISTS ${db_name}.test_cherrypick"""

    sql """
        CREATE TABLE ${db_name}.test_cherrypick (
            id BIGINT,
            data STRING,
            status INT
        ) ENGINE=iceberg
    """
    sql """
        INSERT INTO ${db_name}.test_cherrypick VALUES
        (1, 'data1', 1)
    """
    sql """
        INSERT INTO ${db_name}.test_cherrypick VALUES
        (2, 'data2', 2)
    """
    sql """
        INSERT INTO ${db_name}.test_cherrypick VALUES
        (3, 'data3', 3)
    """
    logger.info("test_cherrypick table setup completed with 3 incremental snapshots")

    logger.info("Creating test_rollback table for rollback_to_snapshot testing")

    sql """DROP TABLE IF EXISTS ${db_name}.test_rollback"""
    sql """
        CREATE TABLE ${db_name}.test_rollback (
            id BIGINT,
            version STRING,
            timestamp datetime
        ) ENGINE=iceberg
    """
    sql """
        INSERT INTO ${db_name}.test_rollback VALUES
        (1, 'v1.0', '2024-01-01 10:00:00')
    """
    sql """
        INSERT INTO ${db_name}.test_rollback VALUES
        (2, 'v1.1', '2024-01-02 11:00:00')
    """
    sql """
        INSERT INTO ${db_name}.test_rollback VALUES
        (3, 'v1.2', '2024-01-03 12:00:00')
    """

    logger.info("test_rollback table setup completed with 3 version snapshots")

    logger.info("Creating test_rollback_timestamp table for timestamp-based rollback testing")

    sql """DROP TABLE IF EXISTS ${db_name}.test_rollback_timestamp"""

    sql """
        CREATE TABLE ${db_name}.test_rollback_timestamp (
            id BIGINT,
            version STRING,
            timestamp datetime
        ) ENGINE=iceberg
    """

    sql """
        INSERT INTO ${db_name}.test_rollback_timestamp VALUES
        (1, 'v1.0', '2025-01-01 10:00:00')
    """
    sql """
        INSERT INTO ${db_name}.test_rollback_timestamp VALUES
        (2, 'v1.1', '2025-01-02 11:00:00')
    """
    sql """
        INSERT INTO ${db_name}.test_rollback_timestamp VALUES
        (3, 'v1.2', '2025-01-03 12:00:00')
    """
    logger.info("test_rollback_timestamp table setup completed with future-dated snapshots")

    logger.info("Creating test_current_snapshot table for set_current_snapshot testing")

    sql """DROP TABLE IF EXISTS ${db_name}.test_current_snapshot"""

    sql """
        CREATE TABLE ${db_name}.test_current_snapshot (
            id BIGINT,
            content STRING
        ) ENGINE=iceberg
    """

    sql """
        INSERT INTO ${db_name}.test_current_snapshot VALUES
        (1, 'content1')
    """
    sql """
        INSERT INTO ${db_name}.test_current_snapshot VALUES
        (2, 'content2')
    """
    sql """
        ALTER TABLE ${db_name}.test_current_snapshot CREATE BRANCH dev_branch
    """
    sql """
        INSERT INTO ${db_name}.test_current_snapshot VALUES
        (3, 'content3')
    """
    sql """
        ALTER TABLE ${db_name}.test_current_snapshot CREATE TAG dev_tag
    """
    // Insert final content record - latest main state
    sql """
        INSERT INTO ${db_name}.test_current_snapshot VALUES
        (4, 'content4')
    """

    logger.info("test_current_snapshot table setup completed with 4 snapshots, 1 branch, and 1 tag")

    // =====================================================================================
    // Test Case 1: rollback_to_snapshot action
    // Tests the ability to rollback a table to a specific historical snapshot
    // =====================================================================================
    logger.info("Starting rollback_to_snapshot test case")

    // Capture table state before rollback operation
    qt_before_rollback_to_snapshot """SELECT * FROM test_rollback ORDER BY id"""

    // Retrieve all available snapshots for the rollback test table
    List<List<Object>> rollbackSnapshotList = sql """
        SELECT committed_at, snapshot_id FROM test_rollback\$snapshots ORDER BY committed_at
    """
    logger.info("Available snapshots for rollback test: ${rollbackSnapshotList}")

    // Validate snapshot data structure and count
    assertTrue(rollbackSnapshotList.size() == 3, "Expected exactly 3 snapshots for rollback test")
    assertTrue(rollbackSnapshotList[0].size() == 2, "Invalid snapshot metadata structure")

    // Extract snapshot IDs for test operations
    String rollbackEarliestSnapshotId = rollbackSnapshotList[0][1]  // First/oldest snapshot
    String rollbackLatestSnapshotId = rollbackSnapshotList[2][1]    // Last/newest snapshot

    // Execute rollback to the earliest snapshot
    List<List<Object>> rollbackResult = sql """
        ALTER TABLE ${catalog_name}.${db_name}.test_rollback
        EXECUTE rollback_to_snapshot("snapshot_id" = "${rollbackEarliestSnapshotId}")
    """
    logger.info("Rollback result: ${rollbackResult}")
    qt_after_rollback_to_snapshot """SELECT * FROM test_rollback ORDER BY id"""

    // =====================================================================================
    // Test Case 2: rollback_to_timestamp action
    // Tests the ability to rollback a table to a specific point in time using timestamps
    // =====================================================================================
    logger.info("Starting rollback_to_timestamp test case")

    // Capture table state before timestamp-based rollback
    qt_before_rollback_to_timestamp """SELECT * FROM test_rollback_timestamp ORDER BY id"""

    // Retrieve snapshots ordered by timestamp (newest first) for timestamp rollback test
    List<List<Object>> timestampSnapshotList = sql """
        SELECT committed_at, snapshot_id FROM test_rollback_timestamp\$snapshots ORDER BY committed_at DESC
    """
    logger.info("Snapshot timeline for timestamp rollback: ${timestampSnapshotList}")

    // Validate snapshot availability for timestamp test
    assertTrue(timestampSnapshotList.size() == 3, "Expected exactly 3 snapshots for timestamp test")
    assertTrue(timestampSnapshotList[0].size() == 2, "Invalid timestamp snapshot structure")

    // Extract and format timestamp for rollback operation
    String latestCommittedTime = timestampSnapshotList[0][0]

    // Convert timestamp to required format for rollback operation
    DateTimeFormatter outputFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
    LocalDateTime dateTime = LocalDateTime.parse(latestCommittedTime, unifiedFormatter)
    String formattedSnapshotTime = dateTime.atZone(ZoneId.systemDefault()).format(outputFormatter)

    // Execute timestamp-based rollback
    List<List<Object>> rollbackTimestampResult = sql """
        ALTER TABLE ${catalog_name}.${db_name}.test_rollback_timestamp
        EXECUTE rollback_to_timestamp("timestamp" = "${formattedSnapshotTime}")
    """
    logger.info("Rollback timestamp result: ${rollbackTimestampResult}")
    qt_after_rollback_to_timestamp """SELECT * FROM test_rollback_timestamp ORDER BY id"""


    // =====================================================================================
    // Test Case 3: set_current_snapshot action
    // Tests setting the current snapshot using snapshot ID and reference-based approaches
    // =====================================================================================
    logger.info("Starting set_current_snapshot test case")

    // Test setting current snapshot by snapshot ID
    qt_before_set_current_snapshot_by_snapshotid """SELECT * FROM test_current_snapshot ORDER BY id"""

    // Retrieve available snapshots for current snapshot test
    List<List<Object>> currentSnapshotList = sql """
        SELECT committed_at, snapshot_id FROM test_current_snapshot\$snapshots ORDER BY committed_at
    """
    logger.info("Available snapshots for current snapshot test: ${currentSnapshotList}")

    // Validate snapshot data for current snapshot test
    assertTrue(currentSnapshotList.size() == 4, "Expected exactly 4 snapshots for current snapshot test")
    assertTrue(currentSnapshotList[0].size() == 2, "Invalid current snapshot metadata structure")

    String targetCurrentSnapshotId = currentSnapshotList[0][1]  // Select first snapshot as target

    // Execute set current snapshot by snapshot ID
    List<List<Object>> setCurrentSnapshotByIdResult = sql """
        ALTER TABLE ${catalog_name}.${db_name}.test_current_snapshot
        EXECUTE set_current_snapshot("snapshot_id" = "${targetCurrentSnapshotId}")
    """
    logger.info("Set current snapshot by snapshot ID result: ${setCurrentSnapshotByIdResult}")
    qt_after_set_current_snapshot_by_snapshotid """SELECT * FROM test_current_snapshot ORDER BY id"""

    // Verify reference structure after snapshot change
    List<List<Object>> currentSnapshotRefs = sql """
        SELECT name, type FROM test_current_snapshot\$refs ORDER BY snapshot_id
    """
    logger.info("References after current snapshot change: ${currentSnapshotRefs}")
    assertTrue(currentSnapshotRefs.size() == 3, "Expected exactly 3 references after snapshot change")

    // Test setting current snapshot by branch reference
    qt_before_set_current_snapshot_by_branch """SELECT * FROM test_current_snapshot ORDER BY id"""
    List<List<Object>> setCurrentSnapshotByBranchResult = sql """
        ALTER TABLE ${catalog_name}.${db_name}.test_current_snapshot
        EXECUTE set_current_snapshot("ref" = "dev_branch")
    """
    logger.info("Set current snapshot by branch result: ${setCurrentSnapshotByBranchResult}")
    qt_after_set_current_snapshot_by_branch """SELECT * FROM test_current_snapshot ORDER BY id"""

    // Test setting current snapshot by tag reference
    qt_before_set_current_snapshot_by_tag """SELECT * FROM test_current_snapshot ORDER BY id"""
    List<List<Object>> setCurrentSnapshotByTagResult = sql """
        ALTER TABLE ${catalog_name}.${db_name}.test_current_snapshot
        EXECUTE set_current_snapshot("ref" = "dev_tag")
    """
    logger.info("Set current snapshot by tag result: ${setCurrentSnapshotByTagResult}")
    qt_after_set_current_snapshot_by_tag """SELECT * FROM test_current_snapshot ORDER BY id"""

    // =====================================================================================
    // Test Case 4: cherrypick_snapshot action
    // Tests selective application of changes from a specific snapshot (cherrypick operation)
    // =====================================================================================
    logger.info("Starting cherrypick_snapshot test case")

    // Capture initial state before cherrypick operations
    qt_before_cherrypick_snapshot """SELECT * FROM test_cherrypick ORDER BY id"""

    // Retrieve snapshots for cherrypick test scenario
    List<List<Object>> cherrypickSnapshotList = sql """
        SELECT committed_at, snapshot_id FROM test_cherrypick\$snapshots ORDER BY committed_at
    """
    logger.info("Available snapshots for cherrypick test: ${cherrypickSnapshotList}")

    // Validate cherrypick test data structure
    assertTrue(cherrypickSnapshotList.size() == 3, "Expected exactly 3 snapshots for cherrypick test")
    assertTrue(cherrypickSnapshotList[0].size() == 2, "Invalid cherrypick snapshot structure")

    String cherrypickEarliestSnapshotId = cherrypickSnapshotList[0][1]  // First snapshot for rollback
    String cherrypickLatestSnapshotId = cherrypickSnapshotList[2][1]    // Last snapshot for cherrypick

    // Step 1: Rollback to earliest snapshot to create test scenario
    List<List<Object>> cherrypickRollbackResult = sql """
        ALTER TABLE ${catalog_name}.${db_name}.test_cherrypick
        EXECUTE rollback_to_snapshot("snapshot_id" = "${cherrypickEarliestSnapshotId}")
    """
    logger.info("Cherrypick rollback result: ${cherrypickRollbackResult}")
    qt_rollback_snapshot """SELECT * FROM test_cherrypick ORDER BY id"""

    // Step 2: Cherrypick changes from the latest snapshot
    List<List<Object>> cherrypickResult = sql """
        ALTER TABLE ${catalog_name}.${db_name}.test_cherrypick
        EXECUTE cherrypick_snapshot("snapshot_id" = "${cherrypickLatestSnapshotId}")
    """
    logger.info("Cherrypick snapshot result: ${cherrypickResult}")
    qt_after_cherrypick_snapshot """SELECT * FROM test_cherrypick ORDER BY id"""


    // =====================================================================================
    // Test Case 5: fast_forward action
    // Tests fast-forward operations for branch synchronization in Iceberg tables
    // =====================================================================================
    logger.info("Starting fast_forward action test case")

    // Capture state before fast-forward operations
    qt_before_test_fast_forward """SELECT * FROM test_fast_forward ORDER BY id"""

    // Retrieve snapshot timeline for fast-forward test (newest first)
    List<List<Object>> fastForwardSnapshotList = sql """
        SELECT committed_at, snapshot_id FROM test_fast_forward\$snapshots ORDER BY committed_at DESC
    """
    logger.info("Snapshot timeline for fast-forward test: ${fastForwardSnapshotList}")

    // Validate fast-forward test data
    assertTrue(fastForwardSnapshotList.size() == 3, "Expected exactly 3 snapshots for fast-forward test")
    assertTrue(fastForwardSnapshotList[0].size() == 2, "Invalid fast-forward snapshot structure")

    // Verify available references for fast-forward operations
    List<List<Object>> fastForwardRefs = sql """
        SELECT name, type FROM test_fast_forward\$refs ORDER BY snapshot_id
    """
    logger.info("Available references for fast-forward: ${fastForwardRefs}")
    assertTrue(fastForwardRefs.size() == 3, "Expected exactly 3 references for fast-forward test")

    // Test fast-forward from feature branch to main branch
    qt_before_fast_forword_branch """SELECT * FROM test_fast_forward@branch(feature_branch) ORDER BY id"""

    List<List<Object>> fastForwardResult = sql """
        ALTER TABLE ${catalog_name}.${db_name}.test_fast_forward
        EXECUTE fast_forward("branch" = "feature_branch", "to" = "main")
    """
    logger.info("Fast forward result: ${fastForwardResult}")
    qt_after_fast_forword_branch """SELECT * FROM test_fast_forward@branch(feature_branch) ORDER BY id"""


    // Test validation - missing required property
    test {
        sql """
            ALTER TABLE ${catalog_name}.${db_name}.${table_name} EXECUTE rollback_to_snapshot
            ()
        """
        exception "Missing required argument: snapshot_id"
    }

    // Test validation - negative snapshot_id
    test {
        sql """
            ALTER TABLE ${catalog_name}.${db_name}.${table_name} EXECUTE rollback_to_snapshot
            ("snapshot_id" = "-123")
        """
        exception "snapshot_id must be positive, got: -123"
    }

    // Test validation - zero snapshot_id
    test {
        sql """
            ALTER TABLE ${catalog_name}.${db_name}.${table_name} EXECUTE cherrypick_snapshot
            ("snapshot_id" = "0")
        """
        exception "snapshot_id must be positive, got: 0"
    }

    // Test validation - empty snapshot_id
    test {
        sql """
            ALTER TABLE ${catalog_name}.${db_name}.${table_name} EXECUTE set_current_snapshot
            ("snapshot_id" = "")
        """
        exception "Invalid snapshot_id format:"
    }

    // Test validation - missing timestamp for rollback_to_timestamp
    test {
        sql """
            ALTER TABLE ${catalog_name}.${db_name}.${table_name} EXECUTE rollback_to_timestamp
            ()
        """
        exception "Missing required argument: timestamp"
    }

    // Test unknown action
    test {
        sql """
            ALTER TABLE ${catalog_name}.${db_name}.${table_name} EXECUTE unknown_action
            ()
        """
        exception "Unsupported Iceberg procedure: unknown_action."
    }

    // Test missing action property
    test {
        sql """
            ALTER TABLE ${catalog_name}.${db_name}.${table_name} EXECUTE
        """
        exception "mismatched input '<EOF>'"
    }

    // Test unknown property for specific action
    test {
        sql """
            ALTER TABLE ${catalog_name}.${db_name}.${table_name} EXECUTE rollback_to_snapshot
            ("snapshot_id" = "123", "unknown_param" = "value")
        """
        exception "Unknown argument: unknown_param"
    }

    // Test rewrite_data_files with invalid target-file-size-bytes
    test {
        sql """
            ALTER TABLE ${catalog_name}.${db_name}.${table_name} EXECUTE rewrite_data_files
            ("target-file-size-bytes" = "0")
        """
        exception "target-file-size-bytes must be positive, got: 0"
    }

    // Test rewrite_data_files with invalid file size format
    test {
        sql """
            ALTER TABLE ${catalog_name}.${db_name}.${table_name} EXECUTE rewrite_data_files
            ("target-file-size-bytes" = "not-a-number")
        """
        exception "Invalid target-file-size-bytes format: not-a-number"
    }

    // Test set_current_snapshot with both snapshot_id and ref
    test {
        sql """
            ALTER TABLE ${catalog_name}.${db_name}.${table_name} EXECUTE set_current_snapshot
            ("snapshot_id" = "123", "ref" = "main")
        """
        exception "snapshot_id and ref are mutually exclusive, only one can be provided"
    }

    // Test set_current_snapshot with neither snapshot_id nor ref
    test {
        sql """
            ALTER TABLE ${catalog_name}.${db_name}.${table_name} EXECUTE set_current_snapshot
            ()
        """
        exception "Either snapshot_id or ref must be provided"
    }

    // Test very large snapshot_id (within Long range)
    test {
        sql """
            ALTER TABLE ${catalog_name}.${db_name}.${table_name} EXECUTE rollback_to_snapshot
            ("snapshot_id" = "9223372036854775807")
        """
        exception "Snapshot 9223372036854775807 not found in table"
    }

    // Test snapshot_id exceeding Long.MAX_VALUE
    test {
        sql """
            ALTER TABLE ${catalog_name}.${db_name}.${table_name} EXECUTE rollback_to_snapshot
            ("snapshot_id" = "99999999999999999999")
        """
        exception "Invalid snapshot_id format: 99999999999999999999"
    }

    // Test whitespace handling in parameters
    test {
        sql """
            ALTER TABLE ${catalog_name}.${db_name}.${table_name} EXECUTE rollback_to_snapshot
            ("snapshot_id" = "  123456789  ")
        """
        exception "Snapshot 123456789 not found in table"
    }

    // Test case sensitivity in action names
    test {
        sql """
            ALTER TABLE ${catalog_name}.${db_name}.${table_name} EXECUTE ROLLBACK_TO_SNAPSHOT
            ("snapshot_id" = "123456789")
        """
        exception "Snapshot 123456789 not found in table"
    }

    // =====================================================================================
// Test Case 6: publish_changes action with WAP (Write-Audit-Publish) pattern
// Simplified workflow:
//
//   - Main branch is initially empty (0 rows)
//   - A WAP snapshot exists with wap.id = "test_wap_001" and 2 rows
//   - publish_changes should cherry-pick the WAP snapshot into the main branch
// =====================================================================================

logger.info("Starting simplified WAP (Write-Audit-Publish) workflow verification test")

// WAP test database and table
String wap_db = "wap_test"
String wap_table = "orders_wap"

// Step 1: Verify no data is visible before publish_changes
logger.info("Step 1: Verifying table is empty before publish_changes")
qt_wap_before_publish """
    SELECT order_id, customer_id, amount, order_date
    FROM ${catalog_name}.${wap_db}.${wap_table}
    ORDER BY order_id
"""

// Step 2: Publish the WAP changes with wap_id = "test_wap_001"
logger.info("Step 2: Publishing WAP changes with wap_id=test_wap_001")
sql """
    ALTER TABLE ${catalog_name}.${wap_db}.${wap_table}
    EXECUTE publish_changes("wap_id" = "test_wap_001")
"""
logger.info("Publish changes executed successfully")

// Step 3: Verify WAP data is visible after publish_changes
logger.info("Step 3: Verifying WAP data is visible after publish_changes")
qt_wap_after_publish """
    SELECT order_id, customer_id, amount, order_date
    FROM ${catalog_name}.${wap_db}.${wap_table}
    ORDER BY order_id
"""

logger.info("Simplified WAP (Write-Audit-Publish) workflow verification completed successfully")

// Negative tests for publish_changes

// publish_changes on table without write.wap.enabled = true (should fail)
test {
    String nonWapDb = "wap_test"
    String nonWapTable = "orders_non_wap"

    sql """
    ALTER TABLE ${catalog_name}.${nonWapDb}.${nonWapTable}
    EXECUTE publish_changes("wap_id" = "test_wap_001")
    """
    exception "Cannot find snapshot with wap.id = test_wap_001"
}


// publish_changes with missing wap_id (should fail)
test {
    sql """
        ALTER TABLE ${catalog_name}.${db_name}.${table_name}
        EXECUTE publish_changes ()
    """
    exception "Missing required argument: wap_id"
}

// publish_changes with invalid wap_id (should fail)
test {
    sql """
    ALTER TABLE ${catalog_name}.${wap_db}.${wap_table}
    EXECUTE publish_changes("wap_id" = "non_existing_wap_id")
    """
    exception "Cannot find snapshot with wap.id = non_existing_wap_id"
}

// publish_changes with partition specification (should fail)
test {
    sql """
        ALTER TABLE ${catalog_name}.${db_name}.${table_name}
        EXECUTE publish_changes ("wap_id" = "test_wap_001") PARTITIONS (part1)
    """
    exception "Action 'publish_changes' does not support partition specification"
}

// publish_changes with WHERE condition (should fail)
test {
    sql """
        ALTER TABLE ${catalog_name}.${db_name}.${table_name}
        EXECUTE publish_changes ("wap_id" = "test_wap_001") WHERE id > 0
    """
    exception "Action 'publish_changes' does not support WHERE condition"
}

  
}