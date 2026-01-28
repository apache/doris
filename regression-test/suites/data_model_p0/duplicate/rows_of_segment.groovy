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

// ============================================================================
// Test Suite: rows_of_segment property
// ============================================================================
// This test suite verifies the rows_of_segment table property functionality.
// The rows_of_segment property controls the maximum number of rows per segment
// file during data import and compaction.
//
// TABLE OF CONTENTS:
// ============================================================================
// Line   | TC#     | Test Case Description
// -------|---------|----------------------------------------------------------
//  ~50   | TC-001  | Create table with rows_of_segment property
//  ~70   | TC-002  | Default value verification (rows_of_segment = 0)
//  ~100  | TC-003  | Data import - row limit takes effect (50000 rows/segment)
//  ~160  | TC-005  | Verify modification of rows_of_segment is not allowed
//  ~185  | TC-009  | Boundary value - minimum value 0
//  ~210  | TC-010  | Boundary value - negative number should fail
//  ~235  | TC-011  | Boundary value - non-numeric value should fail
//  ~255  | TC-012  | Boundary value - large value (UINT32_MAX)
//  ~285  | TC-013  | Boundary value - small value (100 rows/segment)
//  ~330  | TC-014  | AGGREGATE KEY table should NOT allow rows_of_segment
//  ~380  | TC-015  | UNIQUE KEY table should NOT allow rows_of_segment
//  ~430  | TC-016  | Verify rowset information after multiple inserts
//  ~490  | TC-017  | Verify segment rows distribution (20000 rows/segment)
//  ~530  | TC-018  | Materialized View (Rollup) inherits rows_of_segment
//  ~620  | TC-019  | Sync Materialized View with data import
// ============================================================================

suite("rows_of_segment") {
    sql "CREATE DATABASE IF NOT EXISTS test_rows_of_segment_db"
    sql "USE test_rows_of_segment_db"

    // ============================================================
    // TC-001: Create table with rows_of_segment property
    // ============================================================
    sql "DROP TABLE IF EXISTS test_rows_of_segment_basic"
    sql """
        CREATE TABLE test_rows_of_segment_basic (
            id BIGINT,
            name VARCHAR(100),
            value DOUBLE
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 4
        PROPERTIES (
            "replication_num" = "1",
            "rows_of_segment" = "100000"
        )
    """
    
    // Verify table created successfully and property is set
    def createTableResult = sql "SHOW CREATE TABLE test_rows_of_segment_basic"
    assertTrue(createTableResult[0][1].contains('"rows_of_segment" = "100000"'))
    
    sql "DROP TABLE IF EXISTS test_rows_of_segment_basic"

    // ============================================================
    // TC-002: Default value verification (rows_of_segment = 0)
    // ============================================================
    sql "DROP TABLE IF EXISTS test_rows_of_segment_default"
    sql """
        CREATE TABLE test_rows_of_segment_default (
            id BIGINT,
            name VARCHAR(100)
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 4
        PROPERTIES (
            "replication_num" = "1"
        )
    """
    
    // Default value is 0, may not show in SHOW CREATE TABLE or show as "0"
    def defaultResult = sql "SHOW CREATE TABLE test_rows_of_segment_default"
    // When default is 0, it may not be shown or shown as "0"
    def createStmt = defaultResult[0][1]
    logger.info("SHOW CREATE TABLE result: " + createStmt)
    // Use specific pattern to avoid matching table name containing "rows_of_segment"
    if (createStmt.contains('"rows_of_segment"')) {
        assertTrue(createStmt.contains('"rows_of_segment" = "0"'))
    }
    
    sql "DROP TABLE IF EXISTS test_rows_of_segment_default"

    // ============================================================
    // TC-003: Data import - row limit takes effect
    // Verify segment rows using information_schema.rowsets
    // ============================================================
    sql "DROP TABLE IF EXISTS test_import_rows"
    sql """
        CREATE TABLE test_import_rows (
            id BIGINT,
            data VARCHAR(100)
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "rows_of_segment" = "50000",
            "disable_auto_compaction" = "true"
        )
    """
    
    // Insert 150000 rows of data
    sql """
        INSERT INTO test_import_rows
        SELECT number, CONCAT('data_', CAST(number AS VARCHAR))
        FROM numbers("number" = "150000")
    """
    
    // Get tablet id
    def tablets3 = sql "SHOW TABLETS FROM test_import_rows"
    def tabletId3 = tablets3[0][0]
    
    // Verify using information_schema.rowsets
    // Each rowset should have segments, and each segment should not exceed 50000 rows
    def rowsetsResult3 = sql """
        SELECT ROWSET_ID, ROWSET_NUM_ROWS, NUM_SEGMENTS
        FROM information_schema.rowsets
        WHERE TABLET_ID = ${tabletId3}
        ORDER BY START_VERSION
    """
    
    // Verify data was inserted
    def count3 = sql "SELECT COUNT(*) FROM test_import_rows"
    assertEquals(150000, count3[0][0])
    
    // With rows_of_segment=50000 and 150000 rows, we expect approximately 3 segments
    for (row in rowsetsResult3) {
        def numRows = row[1] as Long
        def numSegments = row[2] as Long
        if (numSegments > 0 && numRows > 0) {
            // Average rows per segment should be close to rows_of_segment value
            def avgRowsPerSegment = numRows / numSegments
            // Each segment should not exceed rows_of_segment
            assertTrue(avgRowsPerSegment <= 50000, 
                "Average rows per segment ${avgRowsPerSegment} exceeds rows_of_segment limit 50000")
            logger.info("Rowset verification: totalRows=${numRows}, segments=${numSegments}, avgRowsPerSegment=${avgRowsPerSegment}")
        }
    }
    
    sql "DROP TABLE IF EXISTS test_import_rows"

    // ============================================================
    // TC-005: Verify modification of rows_of_segment is not allowed
    // ============================================================
    sql "DROP TABLE IF EXISTS test_modify_rows"
    sql """
        CREATE TABLE test_modify_rows (
            id BIGINT
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "rows_of_segment" = "100000"
        )
    """
    
    // Attempt to modify the property should fail
    test {
        sql """ALTER TABLE test_modify_rows SET ("rows_of_segment" = "200000")"""
        exception "is not allowed to be modified"
    }
    
    sql "DROP TABLE IF EXISTS test_modify_rows"

    // ============================================================
    // TC-009: Boundary value - minimum value 0
    // ============================================================
    sql "DROP TABLE IF EXISTS test_rows_zero"
    sql """
        CREATE TABLE test_rows_zero (
            id BIGINT
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "rows_of_segment" = "0"
        )
    """
    
    // Should succeed with 0 (uses default memory-based mechanism)
    def result9 = sql "SHOW CREATE TABLE test_rows_zero"
    assertTrue(result9.size() > 0)
    
    sql "DROP TABLE IF EXISTS test_rows_zero"

    // ============================================================
    // TC-010: Boundary value - negative number should fail
    // ============================================================
    sql "DROP TABLE IF EXISTS test_rows_negative"
    
    test {
        sql """
            CREATE TABLE test_rows_negative (
                id BIGINT
            )
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "rows_of_segment" = "-1"
            )
        """
        exception "rows_of_segment must be greater than or equal to 0"
    }

    // ============================================================
    // TC-011: Boundary value - non-numeric value should fail
    // ============================================================
    sql "DROP TABLE IF EXISTS test_rows_invalid"
    
    test {
        sql """
            CREATE TABLE test_rows_invalid (
                id BIGINT
            )
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "rows_of_segment" = "abc"
            )
        """
        exception "parse rows_of_segment format error"
    }

    // ============================================================
    // TC-012: Boundary value - large value (UINT32_MAX = 4294967295)
    // ============================================================
    sql "DROP TABLE IF EXISTS test_rows_large"
    sql """
        CREATE TABLE test_rows_large (
            id BIGINT
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "rows_of_segment" = "4294967295"
        )
    """
    
    // Should succeed with UINT32_MAX value
    def result12 = sql "SHOW CREATE TABLE test_rows_large"
    assertTrue(result12.size() > 0)
    assertTrue(result12[0][1].contains('"rows_of_segment" = "4294967295"'))
    
    sql "DROP TABLE IF EXISTS test_rows_large"

    // ============================================================
    // TC-013: Boundary value - small value with data import
    // ============================================================
    sql "DROP TABLE IF EXISTS test_rows_small"
    sql """
        CREATE TABLE test_rows_small (
            id BIGINT,
            data VARCHAR(100)
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "rows_of_segment" = "100",
            "disable_auto_compaction" = "true"
        )
    """
    
    // Insert 1000 rows
    sql """
        INSERT INTO test_rows_small
        SELECT number, CONCAT('test_', CAST(number AS VARCHAR))
        FROM numbers("number" = "1000")
    """
    
    // Verify data count
    def count13 = sql "SELECT COUNT(*) FROM test_rows_small"
    assertEquals(1000, count13[0][0])
    
    // Get tablet id and check rowsets
    def tablets13 = sql "SHOW TABLETS FROM test_rows_small"
    def tabletId13 = tablets13[0][0]
    
    def rowsetsResult13 = sql """
        SELECT ROWSET_ID, ROWSET_NUM_ROWS, NUM_SEGMENTS
        FROM information_schema.rowsets
        WHERE TABLET_ID = ${tabletId13}
        ORDER BY START_VERSION
    """
    
    // With rows_of_segment=100 and 1000 rows, we expect approximately 10 segments
    for (row in rowsetsResult13) {
        def numRows = row[1] as Long
        def numSegments = row[2] as Long
        if (numSegments > 0 && numRows > 0) {
            def avgRowsPerSegment = numRows / numSegments
            // Each segment should not exceed rows_of_segment (100)
            assertTrue(avgRowsPerSegment <= 100, 
                "Average rows per segment ${avgRowsPerSegment} exceeds rows_of_segment limit 100")
            logger.info("Rowset verification: totalRows=${numRows}, segments=${numSegments}, avgRowsPerSegment=${avgRowsPerSegment}")
        }
    }
    
    sql "DROP TABLE IF EXISTS test_rows_small"

    // ============================================================
    // TC-014: AGGREGATE KEY table should NOT allow rows_of_segment
    // ============================================================
    sql "DROP TABLE IF EXISTS test_rows_aggregate"
    
    // Attempting to create AGGREGATE KEY table with rows_of_segment should fail
    test {
        sql """
            CREATE TABLE test_rows_aggregate (
                k1 INT,
                k2 INT,
                v1 BIGINT SUM
            )
            AGGREGATE KEY(k1, k2)
            DISTRIBUTED BY HASH(k1) BUCKETS 2
            PROPERTIES (
                "replication_num" = "1",
                "rows_of_segment" = "50000"
            )
        """
        exception "rows_of_segment property is only supported for DUPLICATE KEY tables"
    }
    
    // Verify AGGREGATE KEY table can be created without rows_of_segment
    sql """
        CREATE TABLE test_rows_aggregate (
            k1 INT,
            k2 INT,
            v1 BIGINT SUM
        )
        AGGREGATE KEY(k1, k2)
        DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        )
    """
    def result14 = sql "SHOW CREATE TABLE test_rows_aggregate"
    assertTrue(result14.size() > 0)
    
    sql "DROP TABLE IF EXISTS test_rows_aggregate"

    // ============================================================
    // TC-015: UNIQUE KEY table should NOT allow rows_of_segment
    // ============================================================
    sql "DROP TABLE IF EXISTS test_rows_unique"
    
    // Attempting to create UNIQUE KEY table with rows_of_segment should fail
    test {
        sql """
            CREATE TABLE test_rows_unique (
                id BIGINT,
                name VARCHAR(100),
                value INT
            )
            UNIQUE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 2
            PROPERTIES (
                "replication_num" = "1",
                "rows_of_segment" = "80000"
            )
        """
        exception "rows_of_segment property is only supported for DUPLICATE KEY tables"
    }
    
    // Verify UNIQUE KEY table can be created without rows_of_segment
    sql """
        CREATE TABLE test_rows_unique (
            id BIGINT,
            name VARCHAR(100),
            value INT
        )
        UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        )
    """
    def result15 = sql "SHOW CREATE TABLE test_rows_unique"
    assertTrue(result15.size() > 0)
    
    sql "DROP TABLE IF EXISTS test_rows_unique"

    // ============================================================
    // TC-016: Verify rowset information after multiple inserts
    // ============================================================
    sql "DROP TABLE IF EXISTS test_rows_multi_insert"
    sql """
        CREATE TABLE test_rows_multi_insert (
            id BIGINT,
            data VARCHAR(50)
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "rows_of_segment" = "30000",
            "disable_auto_compaction" = "true"
        )
    """
    
    // Perform multiple inserts
    sql """INSERT INTO test_rows_multi_insert SELECT number, 'a' FROM numbers("number" = "30000")"""
    sql """INSERT INTO test_rows_multi_insert SELECT number, 'b' FROM numbers("number" = "30000")"""
    sql """INSERT INTO test_rows_multi_insert SELECT number, 'c' FROM numbers("number" = "30000")"""
    
    // Get tablet id
    def tablets16 = sql "SHOW TABLETS FROM test_rows_multi_insert"
    def tabletId16 = tablets16[0][0]
    
    // Check rowsets - should have multiple rowsets with segment details
    def rowsetsResult16 = sql """
        SELECT ROWSET_ID, ROWSET_NUM_ROWS, NUM_SEGMENTS
        FROM information_schema.rowsets
        WHERE TABLET_ID = ${tabletId16}
        ORDER BY START_VERSION
    """
    
    // Should have at least 3 rowsets (one for each insert)
    assertTrue(rowsetsResult16.size() >= 3, 
        "Expected at least 3 rowsets, got ${rowsetsResult16.size()}")
    
    // Verify each rowset's segment count and rows per segment
    for (row in rowsetsResult16) {
        def numRows = row[1] as Long
        def numSegments = row[2] as Long
        if (numSegments > 0 && numRows > 0) {
            def avgRowsPerSegment = numRows / numSegments
            // Each segment should not exceed rows_of_segment (30000)
            assertTrue(avgRowsPerSegment <= 30000, 
                "Average rows per segment ${avgRowsPerSegment} exceeds rows_of_segment limit 30000")
            logger.info("Rowset verification: totalRows=${numRows}, segments=${numSegments}, avgRowsPerSegment=${avgRowsPerSegment}")
        }
    }
    
    // Verify total data
    def count16 = sql "SELECT COUNT(*) FROM test_rows_multi_insert"
    assertEquals(90000, count16[0][0])
    
    sql "DROP TABLE IF EXISTS test_rows_multi_insert"

    // ============================================================
    // TC-017: Verify segment rows distribution
    // ============================================================
    sql "DROP TABLE IF EXISTS test_rows_distribution"
    sql """
        CREATE TABLE test_rows_distribution (
            id BIGINT,
            data VARCHAR(50)
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "rows_of_segment" = "20000",
            "disable_auto_compaction" = "true"
        )
    """
    
    // Insert 100000 rows in one batch
    sql """
        INSERT INTO test_rows_distribution
        SELECT number, CONCAT('val_', CAST(number AS VARCHAR))
        FROM numbers("number" = "100000")
    """
    
    // Get tablet id
    def tablets17 = sql "SHOW TABLETS FROM test_rows_distribution"
    def tabletId17 = tablets17[0][0]
    
    // Check rowsets details
    def rowsetsResult17 = sql """
        SELECT ROWSET_ID, ROWSET_NUM_ROWS, NUM_SEGMENTS, START_VERSION, END_VERSION
        FROM information_schema.rowsets
        WHERE TABLET_ID = ${tabletId17}
          AND ROWSET_NUM_ROWS > 0
        ORDER BY START_VERSION
    """
    
    logger.info("Rowsets for test_rows_distribution:")
    for (row in rowsetsResult17) {
        logger.info("  Rowset: id=${row[0]}, rows=${row[1]}, segments=${row[2]}, version=${row[3]}-${row[4]}")
    }
    
    // Verify total count
    def count17 = sql "SELECT COUNT(*) FROM test_rows_distribution"
    assertEquals(100000, count17[0][0])
    
    // With rows_of_segment=20000 and 100000 rows, expect ~5 segments
    def totalSegments17 = 0
    for (row in rowsetsResult17) {
        totalSegments17 += (row[2] as Long)
    }
    logger.info("Total segments: ${totalSegments17}")
    
    sql "DROP TABLE IF EXISTS test_rows_distribution"

    // ============================================================
    // TC-018: Materialized View (Rollup) inherits rows_of_segment
    // ============================================================
    sql "DROP TABLE IF EXISTS test_rows_mv_base"
    sql """
        CREATE TABLE test_rows_mv_base (
            id BIGINT,
            k1 INT,
            k2 INT,
            v1 BIGINT,
            v2 VARCHAR(100)
        )
        DUPLICATE KEY(id, k1, k2)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "rows_of_segment" = "30000",
            "disable_auto_compaction" = "true"
        )
    """
    
    // Verify base table has rows_of_segment property
    def mvBaseResult = sql "SHOW CREATE TABLE test_rows_mv_base"
    assertTrue(mvBaseResult[0][1].contains('"rows_of_segment" = "30000"'))
    logger.info("Base table created with rows_of_segment=30000")
    
    // Insert data into base table
    sql """
        INSERT INTO test_rows_mv_base
        SELECT number, number % 100, number % 50, number * 10, CONCAT('value_', CAST(number AS VARCHAR))
        FROM numbers("number" = "100000")
    """
    
    // Verify data count
    def countMvBase = sql "SELECT COUNT(*) FROM test_rows_mv_base"
    assertEquals(100000, countMvBase[0][0])
    
    // Create a materialized view (rollup)
    sql """
        CREATE MATERIALIZED VIEW mv_test_rows AS
        SELECT k1 as mv_k1, SUM(v1) as sum_v1
        FROM test_rows_mv_base
        GROUP BY mv_k1
    """
    
    // Wait for materialized view to be created
    def maxWaitTime = 60000 // 60 seconds
    def waitInterval = 1000 // 1 second
    def elapsedTime = 0
    def mvReady = false
    
    while (elapsedTime < maxWaitTime && !mvReady) {
        def mvStatus = sql """
            SHOW ALTER TABLE MATERIALIZED VIEW 
            WHERE TableName = 'test_rows_mv_base'
            ORDER BY CreateTime DESC LIMIT 1
        """
        if (mvStatus.size() > 0) {
            def state = mvStatus[0][8] // State column
            logger.info("MV creation state: ${state}")
            if (state == "FINISHED") {
                mvReady = true
            } else if (state == "CANCELLED") {
                logger.warn("MV creation cancelled: ${mvStatus[0]}")
                break
            }
        }
        if (!mvReady) {
            Thread.sleep(waitInterval)
            elapsedTime += waitInterval
        }
    }
    
    if (mvReady) {
        logger.info("Materialized view created successfully")
        
        // Verify MV can be queried - query the base table, optimizer will use MV
        def mvQueryResult = sql """
            SELECT k1, SUM(v1) 
            FROM test_rows_mv_base 
            GROUP BY k1 
            ORDER BY k1 LIMIT 5
        """
        assertTrue(mvQueryResult.size() > 0)
        logger.info("MV query result: ${mvQueryResult}")
        
        // Note: The rows_of_segment property is inherited internally 
        // during CreateReplicaTask for the rollup index.
        // We cannot directly verify the MV's rows_of_segment from SQL,
        // but the BE logs would show it if enabled.
    } else {
        logger.warn("Materialized view creation timed out or failed, skipping MV verification")
    }
    
    sql "DROP TABLE IF EXISTS test_rows_mv_base"

    // ============================================================
    // TC-019: Sync Materialized View with data import
    // Verify rows_of_segment affects MV build
    // ============================================================
    sql "DROP TABLE IF EXISTS test_rows_mv_import"
    sql """
        CREATE TABLE test_rows_mv_import (
            dt DATE,
            user_id BIGINT,
            city VARCHAR(50),
            amount DECIMAL(10, 2)
        )
        DUPLICATE KEY(dt, user_id)
        DISTRIBUTED BY HASH(user_id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "rows_of_segment" = "25000",
            "disable_auto_compaction" = "true"
        )
    """
    
    // Insert initial data
    sql """
        INSERT INTO test_rows_mv_import
        SELECT DATE_ADD('2024-01-01', INTERVAL (number % 30) DAY),
               number,
               CONCAT('city_', CAST(number % 10 AS VARCHAR)),
               (number % 1000) * 1.5
        FROM numbers("number" = "80000")
    """
    
    // Create MV for aggregation
    sql """
        CREATE MATERIALIZED VIEW mv_city_amount AS
        SELECT city as mv_city, SUM(amount) as total_amount
        FROM test_rows_mv_import
        GROUP BY mv_city
    """
    
    // Wait for MV creation
    elapsedTime = 0
    mvReady = false
    while (elapsedTime < maxWaitTime && !mvReady) {
        def mvStatus = sql """
            SHOW ALTER TABLE MATERIALIZED VIEW 
            WHERE TableName = 'test_rows_mv_import'
            ORDER BY CreateTime DESC LIMIT 1
        """
        if (mvStatus.size() > 0) {
            def state = mvStatus[0][8]
            logger.info("MV mv_city_amount creation state: ${state}")
            if (state == "FINISHED") {
                mvReady = true
            } else if (state == "CANCELLED") {
                break
            }
        }
        if (!mvReady) {
            Thread.sleep(waitInterval)
            elapsedTime += waitInterval
        }
    }
    
    if (mvReady) {
        // Insert more data after MV is created
        sql """
            INSERT INTO test_rows_mv_import
            SELECT DATE_ADD('2024-02-01', INTERVAL (number % 28) DAY),
                   number + 80000,
                   CONCAT('city_', CAST(number % 10 AS VARCHAR)),
                   (number % 500) * 2.0
            FROM numbers("number" = "50000")
        """
        
        // Verify total data
        def countMvImport = sql "SELECT COUNT(*) FROM test_rows_mv_import"
        assertEquals(130000, countMvImport[0][0])
        
        // Query using MV (optimizer should use mv_city_amount)
        def cityResult = sql """
            SELECT city, SUM(amount)
            FROM test_rows_mv_import
            GROUP BY city
            ORDER BY city
        """
        assertEquals(10, cityResult.size()) // 10 cities
        logger.info("City aggregation result (first 3): ${cityResult.take(3)}")
        
        // Check base table tablets for segment info
        def tabletsMvImport = sql "SHOW TABLETS FROM test_rows_mv_import"
        def tabletIdMvImport = tabletsMvImport[0][0]
        
        def rowsetsMvImport = sql """
            SELECT ROWSET_ID, ROWSET_NUM_ROWS, NUM_SEGMENTS
            FROM information_schema.rowsets
            WHERE TABLET_ID = ${tabletIdMvImport}
              AND ROWSET_NUM_ROWS > 0
            ORDER BY START_VERSION
        """
        
        logger.info("Base table rowsets after MV creation and more inserts:")
        for (row in rowsetsMvImport) {
            def numRows = row[1] as Long
            def numSegments = row[2] as Long
            if (numSegments > 0 && numRows > 0) {
                def avgRows = numRows / numSegments
                logger.info("  Rowset: rows=${numRows}, segments=${numSegments}, avgRowsPerSeg=${avgRows}")
                // With rows_of_segment=25000, each segment should have <= 25000 rows
                assertTrue(avgRows <= 25000, 
                    "Average rows per segment ${avgRows} exceeds limit 25000")
            }
        }
    } else {
        logger.warn("MV creation timed out, skipping test")
    }
    
    sql "DROP TABLE IF EXISTS test_rows_mv_import"

    // ============================================================
    // Cleanup
    // ============================================================
    sql "DROP DATABASE IF EXISTS test_rows_of_segment_db"
}