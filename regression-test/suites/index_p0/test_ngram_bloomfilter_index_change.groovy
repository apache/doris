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
import groovy.json.JsonSlurper

suite("test_ngram_bloomfilter_index_change") {
    def tableName = 'test_ngram_bloomfilter_index_change'
    def timeout = 60000
    def delta_time = 1000
    def alter_res = "null"
    def useTime = 0

    def wait_for_latest_op_on_table_finish = { table_name, OpTimeout ->
        for(int t = delta_time; t <= OpTimeout; t += delta_time){
            alter_res = sql """SHOW ALTER TABLE COLUMN WHERE TableName = "${table_name}" ORDER BY CreateTime DESC LIMIT 1;"""
            alter_res = alter_res.toString()
            if(alter_res.contains("FINISHED")) {
                sleep(3000) // wait change table state to normal
                logger.info(table_name + " latest alter job finished, detail: " + alter_res)
                break
            }
            useTime = t
            sleep(delta_time)
        }
        assertTrue(useTime <= OpTimeout, "wait_for_latest_op_on_table_finish timeout")
    }

    // Function to insert test data batch
    def insertTestData = { ->
        // insert 10 records
        sql "INSERT INTO ${tableName} VALUES (1001, '2023-10-06 15:00:00', 'Laptop', 'John Smith', 199.99, 'North');"
        sql "INSERT INTO ${tableName} VALUES (1002, '2023-10-09 17:05:00', 'Smartphone', 'Emily Johnson', 299.99, 'South');"
        sql "INSERT INTO ${tableName} VALUES (1003, '2023-10-12 19:10:00', 'Headphones', 'Michael Brown', 399.99, 'East');"
        sql "INSERT INTO ${tableName} VALUES (1004, '2023-10-15 21:15:00', 'Monitor', 'Jessica Davis', 499.99, 'West');"
        sql "INSERT INTO ${tableName} VALUES (1005, '2023-10-18 23:20:00', 'Keyboard', 'David Wilson', 89.99, 'North');"
        sql "INSERT INTO ${tableName} VALUES (1006, '2023-10-21 07:25:00', 'Mouse', 'Sarah Taylor', 699.99, 'South');"
        sql "INSERT INTO ${tableName} VALUES (1007, '2023-10-24 09:30:00', 'Printer', 'Thomas Anderson', 799.99, 'East');"
        sql "INSERT INTO ${tableName} VALUES (1008, '2023-10-27 11:35:00', 'Speaker', 'Jennifer Martin', 899.99, 'West');"
        sql "INSERT INTO ${tableName} VALUES (1009, '2023-10-02 13:40:00', 'External SSD', 'Robert Clark', 999.99, 'North');"
        sql "INSERT INTO ${tableName} VALUES (1010, '2023-10-05 15:45:00', 'Webcam', 'Amanda Lewis', 89.99, 'South');"
        sql "sync"
    }

    // Test settings
    sql "set enable_function_pushdown=true"
    sql "set enable_profile=true"
    sql "set profile_level=2"

    // Define test query
    def query = "SELECT /*+SET_VAR(enable_function_pushdown = true, enable_profile = true, profile_level = 2)*/  * FROM ${tableName} WHERE customer_name LIKE '%xxxx%' ORDER BY sale_id"
    // Test Case 1: Test with enable_add_index_for_new_data = true
    logger.info("=== Test Case 1: enable_add_index_for_new_data = true ===")
    // Create table
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
    CREATE TABLE ${tableName} (
    `sale_id` int NULL,
    `sale_date` datetime NULL,
    `product_name` varchar(100) NULL,
    `customer_name` varchar(100) NULL,
    `amount` decimal(10,2) NULL,
    `region` char(50) NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`sale_id`)
    PARTITION BY RANGE(`sale_date`) (
    PARTITION p202310 VALUES [('2023-10-01 00:00:00'), ('2023-11-01 00:00:00')),
    PARTITION p202311 VALUES [('2023-11-01 00:00:00'), ('2023-12-01 00:00:00')),
    PARTITION p202312 VALUES [('2023-12-01 00:00:00'), ('2024-01-01 00:00:00'))
    )
    DISTRIBUTED BY HASH(`sale_id`) BUCKETS 1
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "storage_format" = "V2",
    "light_schema_change" = "true",
    "disable_auto_compaction" = "true"
    );
    """
    
    // Insert test data
    insertTestData()
    // Verify data loaded correctly
    qt_select_light_mode_init "SELECT * FROM ${tableName} ORDER BY sale_id"

    // Test without NGRAM Bloom Filter index
    profile("sql_select_like_without_ngram_index_light_mode") {
        run {
            sql "/* sql_select_like_without_ngram_index_light_mode */ ${query}"
            sleep(1000)
        }

        check { profileString, exception ->
            log.info(profileString)
            assertTrue(profileString.contains("RowsBloomFilterFiltered:  0"))
        }
    }
    sql "set enable_add_index_for_new_data = true"

    // Add NGRAM Bloom Filter index (should be immediate in light mode)
    sql "ALTER TABLE ${tableName} ADD INDEX idx_ngram_customer_name(customer_name) USING NGRAM_BF PROPERTIES('bf_size' = '1024', 'gram_size' = '3');"

    // In light mode, the index should be effective immediately, no need to wait for alter job
    // But let's give it a moment to ensure metadata is updated
    sleep(2000)

    // Insert more data after index added
    insertTestData()
    // Verify more data loaded correctly
    qt_select_light_mode_more_data "SELECT * FROM ${tableName} ORDER BY sale_id"

    // Test with more data (should still filter correctly)
    profile("sql_select_like_with_ngram_index_light_mode_more_data") {
        run {
            sql "/* sql_select_like_with_ngram_index_light_mode_more_data */ ${query}"
            sleep(1000)
        }

        check { profileString, exception ->
            log.info(profileString)
            assertTrue(profileString.contains("RowsBloomFilterFiltered:  10"))
        }
    }

    // Drop index
    sql "DROP INDEX idx_ngram_customer_name ON ${tableName};"
    wait_for_latest_op_on_table_finish(tableName, timeout)

    // Test after dropping index
    profile("sql_select_like_with_ngram_index_light_mode_dropped") {
        run {
            sql "/* sql_select_like_with_ngram_index_light_mode_dropped */ ${query}"
            sleep(1000)
        }

        check { profileString, exception ->
            log.info(profileString)
            assertTrue(profileString.contains("RowsBloomFilterFiltered:  0"))
        }
    }

    // Test Case 2: Test with enable_add_index_for_new_data = false (schema change mode)
    logger.info("=== Test Case 2: enable_add_index_for_new_data = false ===")
    // Set enable_add_index_for_new_data = false
    sql "set enable_add_index_for_new_data = false"
    // Create new table
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
    CREATE TABLE ${tableName} (
    `sale_id` int NULL,
    `sale_date` datetime NULL,
    `product_name` varchar(100) NULL,
    `customer_name` varchar(100) NULL,
    `amount` decimal(10,2) NULL,
    `region` char(50) NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`sale_id`)
    PARTITION BY RANGE(`sale_date`) (
    PARTITION p202310 VALUES [('2023-10-01 00:00:00'), ('2023-11-01 00:00:00')),
    PARTITION p202311 VALUES [('2023-11-01 00:00:00'), ('2023-12-01 00:00:00')),
    PARTITION p202312 VALUES [('2023-12-01 00:00:00'), ('2024-01-01 00:00:00'))
    )
    DISTRIBUTED BY HASH(`sale_id`) BUCKETS 1
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "storage_format" = "V2",
    "light_schema_change" = "true",
    "disable_auto_compaction" = "true"
    );
    """
    // Insert test data
    insertTestData()
    // Verify data loaded correctly
    qt_select_schema_change_mode_init "SELECT * FROM ${tableName} ORDER BY sale_id"

    // Test without NGRAM Bloom Filter index
    profile("sql_select_like_without_ngram_index_schema_change_mode") {
        run {
            sql "/* sql_select_like_without_ngram_index_schema_change_mode */ ${query}"
            sleep(1000)
        }

        check { profileString, exception ->
            log.info(profileString)
            assertTrue(profileString.contains("RowsBloomFilterFiltered:  0"))
        }
    }

    // Add NGRAM Bloom Filter index (will trigger schema change in this mode)
    sql "ALTER TABLE ${tableName} ADD INDEX idx_ngram_customer_name(customer_name) USING NGRAM_BF PROPERTIES('bf_size' = '1024', 'gram_size' = '3');"
    wait_for_latest_op_on_table_finish(tableName, timeout)

    // Test after adding NGRAM Bloom Filter index (should filter existing data)
    profile("sql_select_like_with_ngram_index_schema_change_mode_added") {
        run {
            sql "/* sql_select_like_with_ngram_index_schema_change_mode_added */ ${query}"
            sleep(1000)
        }

        check { profileString, exception ->
            log.info(profileString)
            assertTrue(profileString.contains("RowsBloomFilterFiltered:  10"))
        }
    }

    // Insert more data after index is built
    insertTestData()
    // Verify more data loaded correctly
    qt_select_schema_change_mode_more_data "SELECT * FROM ${tableName} ORDER BY sale_id"

    // Test with more data (should filter all data)
    profile("sql_select_like_with_ngram_index_schema_change_mode_more_data") {
        run {
            sql "/* sql_select_like_with_ngram_index_schema_change_mode_more_data */ ${query}"
            sleep(1000)
        }

        check { profileString, exception ->
            log.info(profileString)
            assertTrue(profileString.contains("RowsBloomFilterFiltered:  20"))
        }
    }

    // Drop index
    sql "DROP INDEX idx_ngram_customer_name ON ${tableName};"
    wait_for_latest_op_on_table_finish(tableName, timeout)

    // Test after dropping index
    profile("sql_select_like_with_ngram_index_schema_change_mode_dropped") {
        run {
            sql "/* sql_select_like_with_ngram_index_schema_change_mode_dropped */ ${query}"
            sleep(1000)
        }

        check { profileString, exception ->
            log.info(profileString)
            assertTrue(profileString.contains("RowsBloomFilterFiltered:  0"))
        }
    }

    // Test Case 3: Test different scenarios for index lifecycle
    logger.info("=== Test Case 3: Index lifecycle with enable_add_index_for_new_data = true ===")
    // Set enable_add_index_for_new_data = true
    sql "set enable_add_index_for_new_data = true"
    // Create table and add index before inserting data
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
    CREATE TABLE ${tableName} (
    `sale_id` int NULL,
    `sale_date` datetime NULL,
    `product_name` varchar(100) NULL,
    `customer_name` varchar(100) NULL,
    `amount` decimal(10,2) NULL,
    `region` char(50) NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`sale_id`)
    PARTITION BY RANGE(`sale_date`) (
    PARTITION p202310 VALUES [('2023-10-01 00:00:00'), ('2023-11-01 00:00:00')),
    PARTITION p202311 VALUES [('2023-11-01 00:00:00'), ('2023-12-01 00:00:00')),
    PARTITION p202312 VALUES [('2023-12-01 00:00:00'), ('2024-01-01 00:00:00'))
    )
    DISTRIBUTED BY HASH(`sale_id`) BUCKETS 1
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "storage_format" = "V2",
    "light_schema_change" = "true",
    "disable_auto_compaction" = "true"
    );
    """

    // Add ngram bf index before data insertion
    sql "ALTER TABLE ${tableName} ADD INDEX idx_ngram_customer_name(customer_name) USING NGRAM_BF PROPERTIES('bf_size' = '1024', 'gram_size' = '3');"
    wait_for_latest_op_on_table_finish(tableName, timeout)

    // Insert data after index creation
    insertTestData()
    // Verify data loaded correctly
    qt_select_lifecycle_after_data "SELECT * FROM ${tableName} ORDER BY sale_id"

    // Test filtering with index added before data insertion
    profile("sql_select_like_with_ngram_index_lifecycle_test") {
        run {
            sql "/* sql_select_like_with_ngram_index_lifecycle_test */ ${query}"
            sleep(1000)
        }

        check { profileString, exception ->
            log.info(profileString)
            assertTrue(profileString.contains("RowsBloomFilterFiltered:  10"))
        }
    }

    // Insert more data
    insertTestData()
    // Verify more data loaded correctly
    qt_select_lifecycle_final "SELECT * FROM ${tableName} ORDER BY sale_id"

    // Test filtering with more data
    profile("sql_select_like_with_ngram_index_lifecycle_final") {
        run {
            sql "/* sql_select_like_with_ngram_index_lifecycle_final */ ${query}"
            sleep(1000)
        }

        check { profileString, exception ->
            log.info(profileString)
            assertTrue(profileString.contains("RowsBloomFilterFiltered:  20"))
        }
    }

    // Final cleanup
    sql "DROP INDEX idx_ngram_customer_name ON ${tableName};"
    sleep(2000)
}