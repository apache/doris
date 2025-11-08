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

suite('test_temp_table_case_sensitivity', 'p0') {
    def dbName = "test_temp_table_case_db"
    
    // Setup: Create test database
    sql "DROP DATABASE IF EXISTS ${dbName}"
    sql "CREATE DATABASE ${dbName}"
    sql "USE ${dbName}"
    
    try {
        // Test 1: Create temporary table and verify display name is case-insensitive
        sql """
            CREATE TEMPORARY TABLE test_case_temp (
                id INT,
                name VARCHAR(50)
            ) DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES ('replication_num' = '1')
        """
        
        // Insert data
        sql "INSERT INTO test_case_temp VALUES (1, 'Alice'), (2, 'Bob')"
        
        // Verify data can be queried
        def result = sql "SELECT * FROM test_case_temp ORDER BY id"
        assertEquals(2, result.size())
        assertEquals(1, result[0][0])
        assertEquals("Alice", result[0][1])
        
        // Test 2: SHOW TABLES should display temp table with correct name (without temp sign)
        result = sql "SHOW TABLES"
        logger.info("SHOW TABLES result: ${result}")
        def foundTempTable = false
        for (row in result) {
            def tableName = row[0]
            // The display name should not contain the temporary table sign in any case
            assertFalse(tableName.toLowerCase().contains("__doris_temp__"),
                "Table name '${tableName}' should not contain temporary table sign")
            if (tableName == "test_case_temp") {
                foundTempTable = true
            }
        }
        assertTrue(foundTempTable, "Temporary table 'test_case_temp' should be visible in SHOW TABLES")
        
        // Test 3: SHOW TABLE STATUS should display temp table correctly
        result = sql "SHOW TABLE STATUS"
        logger.info("SHOW TABLE STATUS result: ${result}")
        foundTempTable = false
        for (row in result) {
            def tableName = row[0]
            // The Name column should not contain the temporary table sign
            assertFalse(tableName.toLowerCase().contains("__doris_temp__"),
                "Table name '${tableName}' in SHOW TABLE STATUS should not contain temporary table sign")
            if (tableName == "test_case_temp") {
                foundTempTable = true
            }
        }
        assertTrue(foundTempTable, "Temporary table 'test_case_temp' should be visible in SHOW TABLE STATUS")
        
        // Test 4: SHOW TABLE STATUS LIKE pattern matching should work case-insensitively
        result = sql "SHOW TABLE STATUS LIKE '%temp%'"
        assertTrue(result.size() > 0, "SHOW TABLE STATUS LIKE should find temp table")
        
        // Test 5: DESC should work with temp table
        result = sql "DESC test_case_temp"
        assertEquals(2, result.size())
        
        // Test 6: Try to create a regular table with temp sign in name (should fail)
        test {
            sql """
                CREATE TABLE __doris_temp__bad_table (
                    id INT
                ) DISTRIBUTED BY HASH(id) BUCKETS 1
                PROPERTIES ('replication_num' = '1')
            """
            exception "Invalid table name"
        }
        
        // Test 7: Try to create a regular table with temp sign in mixed case (should also fail)
        test {
            sql """
                CREATE TABLE __DORIS_TEMP__bad_table (
                    id INT
                ) DISTRIBUTED BY HASH(id) BUCKETS 1
                PROPERTIES ('replication_num' = '1')
            """
            exception "Invalid table name"
        }
        
        test {
            sql """
                CREATE TABLE __Doris_Temp__bad_table (
                    id INT
                ) DISTRIBUTED BY HASH(id) BUCKETS 1
                PROPERTIES ('replication_num' = '1')
            """
            exception "Invalid table name"
        }
        
        // Test 8: Create temp table with partition and verify operations work
        sql """
            CREATE TEMPORARY TABLE test_temp_partition (
                id INT,
                dt DATE,
                value VARCHAR(50)
            ) 
            PARTITION BY RANGE(dt) (
                PARTITION p202401 VALUES [('2024-01-01'), ('2024-02-01')),
                PARTITION p202402 VALUES [('2024-02-01'), ('2024-03-01'))
            )
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES ('replication_num' = '1')
        """
        
        // Insert into partitioned temp table
        sql """
            INSERT INTO test_temp_partition VALUES 
            (1, '2024-01-15', 'Jan'),
            (2, '2024-02-15', 'Feb')
        """
        
        // Verify partition operations work
        result = sql "SELECT * FROM test_temp_partition ORDER BY id"
        assertEquals(2, result.size())
        
        // Test 9: SHOW PARTITIONS should work for temp table
        result = sql "SHOW PARTITIONS FROM test_temp_partition"
        logger.info("SHOW PARTITIONS result: ${result}")
        assertTrue(result.size() >= 2, "Should show at least 2 partitions")
        
        // Test 10: ALTER TABLE operations on temp table
        sql "ALTER TABLE test_temp_partition ADD PARTITION p202403 VALUES [('2024-03-01'), ('2024-04-01'))"
        
        result = sql "SHOW PARTITIONS FROM test_temp_partition"
        assertTrue(result.size() >= 3, "Should show at least 3 partitions after adding")
        
        // Test 11: Verify temp table is isolated per session
        // Create another temp table with same name in current session (should replace)
        sql """
            CREATE TEMPORARY TABLE test_isolation (
                id INT,
                name VARCHAR(50)
            ) DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES ('replication_num' = '1')
        """
        sql "INSERT INTO test_isolation VALUES (1, 'Session1')"
        
        result = sql "SELECT * FROM test_isolation"
        assertEquals(1, result.size())
        assertEquals("Session1", result[0][1])
        
        // Test 12: DROP temp table should work
        sql "DROP TABLE test_case_temp"
        
        // Verify it's dropped
        result = sql "SHOW TABLES"
        foundTempTable = false
        for (row in result) {
            if (row[0] == "test_case_temp") {
                foundTempTable = true
            }
        }
        assertFalse(foundTempTable, "Temporary table should be dropped")
        
        // Test 13: Create regular table and temp table with same base name
        sql """
            CREATE TABLE regular_table (
                id INT,
                name VARCHAR(50)
            ) DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES ('replication_num' = '1')
        """
        
        sql """
            CREATE TEMPORARY TABLE regular_table (
                id INT,
                name VARCHAR(50),
                extra VARCHAR(50)
            ) DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES ('replication_num' = '1')
        """
        
        // Verify temp table shadows regular table
        sql "INSERT INTO regular_table VALUES (1, 'Temp', 'Extra')"
        result = sql "SELECT * FROM regular_table"
        assertEquals(3, result[0].size(), "Should query temp table with 3 columns")
        
        // Drop temp table
        sql "DROP TABLE regular_table"
        
        // Now should query regular table
        result = sql "DESC regular_table"
        assertEquals(2, result.size(), "Regular table should have 2 columns")
        
        // Test 14: Verify information_schema queries work correctly
        result = sql """
            SELECT TABLE_NAME 
            FROM information_schema.tables 
            WHERE TABLE_SCHEMA = '${dbName}' 
            AND TABLE_TYPE = 'BASE TABLE'
        """
        logger.info("information_schema.tables result: ${result}")
        
        // All table names should not contain the temp sign
        for (row in result) {
            def tableName = row[0]
            assertFalse(tableName.toLowerCase().contains("__doris_temp__"),
                "Table name '${tableName}' in information_schema should not contain temporary table sign")
        }
        
    } finally {
        // Cleanup
        sql "DROP DATABASE IF EXISTS ${dbName}"
    }
}
