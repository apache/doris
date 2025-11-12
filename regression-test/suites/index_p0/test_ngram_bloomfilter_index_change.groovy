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

    // Helper functions to fetch profile via HTTP API
    def getProfileList = {
        def dst = 'http://' + context.config.feHttpAddress
        def conn = new URL(dst + "/rest/v1/query_profile").openConnection()
        conn.setRequestMethod("GET")
        def encoding = Base64.getEncoder().encodeToString((context.config.feHttpUser + ":" + 
                (context.config.feHttpPassword == null ? "" : context.config.feHttpPassword)).getBytes("UTF-8"))
        conn.setRequestProperty("Authorization", "Basic ${encoding}")
        return conn.getInputStream().getText()
    }

    def getProfile = { id ->
        def dst = 'http://' + context.config.feHttpAddress
        def conn = new URL(dst + "/api/profile/text/?query_id=$id").openConnection()
        conn.setRequestMethod("GET")
        def encoding = Base64.getEncoder().encodeToString((context.config.feHttpUser + ":" + 
                (context.config.feHttpPassword == null ? "" : context.config.feHttpPassword)).getBytes("UTF-8"))
        conn.setRequestProperty("Authorization", "Basic ${encoding}")
        return conn.getInputStream().getText()
    }

    // Fetch profile text by token with retries for robustness
    def getProfileWithToken = { token ->
        String profileId = ""
        int attempts = 0
        while (attempts < 10 && (profileId == null || profileId == "")) {
            List profileData = new JsonSlurper().parseText(getProfileList()).data.rows
            for (def profileItem in profileData) {
                if (profileItem["Sql Statement"].toString().contains(token)) {
                    profileId = profileItem["Profile ID"].toString()
                    break
                }
            }
            if (profileId == null || profileId == "") {
                Thread.sleep(300)
            }
            attempts++
        }
        assertTrue(profileId != null && profileId != "", "Failed to find profile for token: ${token}")
        // ensure profile text is fully ready
        Thread.sleep(800)
        return getProfile(profileId).toString()
    }

    // Helper to extract RowsBloomFilterFiltered value from profile
    def extractRowsBloomFilterFiltered = { String profileText ->
        def lines = profileText.split("\n")
        for (def line : lines) {
            if (line.contains("RowsBloomFilterFiltered:")) {
                def m = (line =~ /RowsBloomFilterFiltered:\s*([0-9]+)/)
                if (m.find()) {
                    return m.group(1).toInteger()
                }
            }
        }
        return null
    }

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
    sql "set enable_condition_cache=false"
    sql "set enable_profile=true"
    sql "set profile_level=2"

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
    def token1 = UUID.randomUUID().toString()
    sql "SELECT /*+SET_VAR(enable_function_pushdown = true)*/ '${token1}', * FROM ${tableName} WHERE customer_name LIKE '%xxxx%' ORDER BY sale_id"
    def profile1 = getProfileWithToken(token1)
    def filtered1 = extractRowsBloomFilterFiltered(profile1)
    logger.info("sql_select_like_without_ngram_index_light_mode: RowsBloomFilterFiltered = ${filtered1}")
    assertTrue(filtered1 != null && filtered1 == 0, "Expected RowsBloomFilterFiltered = 0, but got ${filtered1}")
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
    def token2 = UUID.randomUUID().toString()
    sql "SELECT /*+SET_VAR(enable_function_pushdown = true)*/ '${token2}', * FROM ${tableName} WHERE customer_name LIKE '%xxxx%' ORDER BY sale_id"
    def profile2 = getProfileWithToken(token2)
    def filtered2 = extractRowsBloomFilterFiltered(profile2)
    logger.info("sql_select_like_with_ngram_index_light_mode_more_data: RowsBloomFilterFiltered = ${filtered2}")
    assertTrue(filtered2 != null && filtered2 == 10, "Expected RowsBloomFilterFiltered = 10, but got ${filtered2}")

    // Drop index
    sql "DROP INDEX idx_ngram_customer_name ON ${tableName};"
    wait_for_latest_op_on_table_finish(tableName, timeout)
    wait_for_last_build_index_finish(tableName, timeout)

    // Test after dropping index
    def token3 = UUID.randomUUID().toString()
    sql "SELECT /*+SET_VAR(enable_function_pushdown = true)*/ '${token3}', * FROM ${tableName} WHERE customer_name LIKE '%xxxx%' ORDER BY sale_id"
    def profile3 = getProfileWithToken(token3)
    def filtered3 = extractRowsBloomFilterFiltered(profile3)
    logger.info("sql_select_like_with_ngram_index_light_mode_dropped: RowsBloomFilterFiltered = ${filtered3}")
    assertTrue(filtered3 != null && filtered3 == 0, "Expected RowsBloomFilterFiltered = 0, but got ${filtered3}")

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
    def token4 = UUID.randomUUID().toString()
    sql "SELECT /*+SET_VAR(enable_function_pushdown = true)*/ '${token4}', * FROM ${tableName} WHERE customer_name LIKE '%xxxx%' ORDER BY sale_id"
    def profile4 = getProfileWithToken(token4)
    def filtered4 = extractRowsBloomFilterFiltered(profile4)
    logger.info("sql_select_like_without_ngram_index_schema_change_mode: RowsBloomFilterFiltered = ${filtered4}")
    assertTrue(filtered4 != null && filtered4 == 0, "Expected RowsBloomFilterFiltered = 0, but got ${filtered4}")

    // Add NGRAM Bloom Filter index (will trigger schema change in this mode)
    sql "ALTER TABLE ${tableName} ADD INDEX idx_ngram_customer_name(customer_name) USING NGRAM_BF PROPERTIES('bf_size' = '1024', 'gram_size' = '3');"
    wait_for_latest_op_on_table_finish(tableName, timeout)
    wait_for_last_build_index_finish(tableName, timeout)

    // Test after adding NGRAM Bloom Filter index (should filter existing data)
    def token5 = UUID.randomUUID().toString()
    sql "SELECT /*+SET_VAR(enable_function_pushdown = true)*/ '${token5}', * FROM ${tableName} WHERE customer_name LIKE '%xxxx%' ORDER BY sale_id"
    def profile5 = getProfileWithToken(token5)
    def filtered5 = extractRowsBloomFilterFiltered(profile5)
    logger.info("sql_select_like_with_ngram_index_schema_change_mode_added: RowsBloomFilterFiltered = ${filtered5}")
    assertTrue(filtered5 != null && filtered5 == 10, "Expected RowsBloomFilterFiltered = 10, but got ${filtered5}")

    // Insert more data after index is built
    insertTestData()
    // Verify more data loaded correctly
    qt_select_schema_change_mode_more_data "SELECT * FROM ${tableName} ORDER BY sale_id"

    // Test with more data (should filter all data)
    def token6 = UUID.randomUUID().toString()
    sql "SELECT /*+SET_VAR(enable_function_pushdown = true)*/ '${token6}', * FROM ${tableName} WHERE customer_name LIKE '%xxxx%' ORDER BY sale_id"
    def profile6 = getProfileWithToken(token6)
    def filtered6 = extractRowsBloomFilterFiltered(profile6)
    logger.info("sql_select_like_with_ngram_index_schema_change_mode_more_data: RowsBloomFilterFiltered = ${filtered6}")
    assertTrue(filtered6 != null && filtered6 == 20, "Expected RowsBloomFilterFiltered = 20, but got ${filtered6}")

    // Drop index
    sql "DROP INDEX idx_ngram_customer_name ON ${tableName};"
    wait_for_latest_op_on_table_finish(tableName, timeout)
    wait_for_last_build_index_finish(tableName, timeout)

    // Test after dropping index
    def token7 = UUID.randomUUID().toString()
    sql "SELECT /*+SET_VAR(enable_function_pushdown = true)*/ '${token7}', * FROM ${tableName} WHERE customer_name LIKE '%xxxx%' ORDER BY sale_id"
    def profile7 = getProfileWithToken(token7)
    def filtered7 = extractRowsBloomFilterFiltered(profile7)
    logger.info("sql_select_like_with_ngram_index_schema_change_mode_dropped: RowsBloomFilterFiltered = ${filtered7}")
    assertTrue(filtered7 != null && filtered7 == 0, "Expected RowsBloomFilterFiltered = 0, but got ${filtered7}")

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
    wait_for_last_build_index_finish(tableName, timeout)

    // Insert data after index creation
    insertTestData()
    // Verify data loaded correctly
    qt_select_lifecycle_after_data "SELECT * FROM ${tableName} ORDER BY sale_id"

    // Test filtering with index added before data insertion
    def token8 = UUID.randomUUID().toString()
    sql "SELECT /*+SET_VAR(enable_function_pushdown = true)*/ '${token8}', * FROM ${tableName} WHERE customer_name LIKE '%xxxx%' ORDER BY sale_id"
    def profile8 = getProfileWithToken(token8)
    def filtered8 = extractRowsBloomFilterFiltered(profile8)
    logger.info("sql_select_like_with_ngram_index_lifecycle_test: RowsBloomFilterFiltered = ${filtered8}")
    assertTrue(filtered8 != null && filtered8 == 10, "Expected RowsBloomFilterFiltered = 10, but got ${filtered8}")

    // Insert more data
    insertTestData()
    // Verify more data loaded correctly
    qt_select_lifecycle_final "SELECT * FROM ${tableName} ORDER BY sale_id"

    // Test filtering with more data
    def token9 = UUID.randomUUID().toString()
    sql "SELECT /*+SET_VAR(enable_function_pushdown = true)*/ '${token9}', * FROM ${tableName} WHERE customer_name LIKE '%xxxx%' ORDER BY sale_id"
    def profile9 = getProfileWithToken(token9)
    def filtered9 = extractRowsBloomFilterFiltered(profile9)
    logger.info("sql_select_like_with_ngram_index_lifecycle_final: RowsBloomFilterFiltered = ${filtered9}")
    assertTrue(filtered9 != null && filtered9 == 20, "Expected RowsBloomFilterFiltered = 20, but got ${filtered9}")

    // Final cleanup
    sql "DROP INDEX idx_ngram_customer_name ON ${tableName};"
    sleep(2000)
}
