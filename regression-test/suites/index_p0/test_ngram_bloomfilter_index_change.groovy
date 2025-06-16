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

    // Test setup
    // 1. Create table
    // 2. Insert test data
    // 3. Add NGRAM Bloom Filter index
    // 4. Build index
    // 5. Insert more data
    // 6. Drop index
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
    "disable_auto_compaction" = "false"
    );
    """
    
    // Insert first batch of data
    insertTestData()

    // Test settings
    sql "set enable_function_pushdown=true"
    sql "set enable_profile=true"
    sql "set profile_level=2"

    // Verify data loaded correctly
    qt_select "SELECT * FROM ${tableName} ORDER BY sale_id"

    // Define test query
    def query = "SELECT /*+SET_VAR(enable_function_pushdown = true, enable_profile = true, profile_level = 2)*/  * FROM ${tableName} WHERE customer_name LIKE '%xxxx%' ORDER BY sale_id"
    
    // Test 1: without NGRAM Bloom Filter index
    profile("sql_select_like_without_ngram_index") {
        run {
            sql "/* sql_select_like_without_ngram_index */ ${query}"
            sleep(1000) // sleep 1s wait for the profile collection to be completed
        }

        check { profileString, exception ->
            log.info(profileString)
            assertTrue(profileString.contains("RowsBloomFilterFiltered:  0"))
        }
    }

    // Test 2: After adding NGRAM Bloom Filter index
    sql "ALTER TABLE ${tableName} ADD INDEX idx_ngram_customer_name(customer_name) USING NGRAM_BF PROPERTIES('bf_size' = '1024', 'gram_size' = '3');"
    wait_for_latest_op_on_table_finish(tableName, timeout)
    profile("sql_select_like_with_ngram_index_added") {
        run {
            sql "/* sql_select_like_with_ngram_index_added */ ${query}"
            sleep(1000)
        }

        check { profileString, exception ->
            log.info(profileString)
            assertTrue(profileString.contains("RowsBloomFilterFiltered:  0"))
        }
    }

    // Test 3: After building the index
    sql "BUILD INDEX idx_ngram_customer_name ON ${tableName};"
    wait_for_latest_op_on_table_finish(tableName, timeout)
    profile("sql_select_like_with_ngram_index_built") {
        run {
            sql "/* sql_select_like_with_ngram_index_built */ ${query}"
            sleep(1000)
        }

        check { profileString, exception ->
            log.info(profileString)
            assertTrue(profileString.contains("RowsBloomFilterFiltered:  10"))
        }
    }

    // Insert second batch of data 
    insertTestData()
    // Verify data loaded correctly
    qt_select "SELECT * FROM ${tableName} ORDER BY sale_id"

    // Test 4: Verify filtering with more data
    profile("sql_select_like_with_ngram_index_more_data") {
        run {
            sql "/* sql_select_like_with_ngram_index_more_data */ ${query}"
            sleep(1000)
        }

        check { profileString, exception ->
            log.info(profileString)
            assertTrue(profileString.contains("RowsBloomFilterFiltered:  20"))
        }
    }

    // Test 5: After dropping the index
    sql "DROP INDEX idx_ngram_customer_name ON ${tableName};"
    wait_for_latest_op_on_table_finish(tableName, timeout)
    profile("sql_select_like_with_ngram_index_dropped") {
        run {
            sql "/* sql_select_like_with_ngram_index_dropped */ ${query}"
            sleep(1000)
        }

        check { profileString, exception ->
            log.info(profileString)
            assertTrue(profileString.contains("RowsBloomFilterFiltered:  0"))
        }
    }

    // recreate table
    // 1. Create table
    // 2. Add NGRAM Bloom Filter index
    // 3. Insert data
    // 4. Insert more data
    // 5. Build index
    // 6. Drop index
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
    "disable_auto_compaction" = "false"
    );
    """

    // add ngram bf index 
    sql "ALTER TABLE ${tableName} ADD INDEX idx_ngram_customer_name(customer_name) USING NGRAM_BF PROPERTIES('bf_size' = '1024', 'gram_size' = '3');"
    wait_for_latest_op_on_table_finish(tableName, timeout)

    // insert data
    insertTestData()

    // Verify data loaded correctly
    qt_select "SELECT * FROM ${tableName} ORDER BY sale_id"

    // Test 6: Verify filtering with index added
    profile("sql_select_like_with_ngram_index_recreated") {
        run {
            sql "/* sql_select_like_with_ngram_index_recreated */ ${query}"
            sleep(1000)
        }

        check { profileString, exception ->
            log.info(profileString)
            assertTrue(profileString.contains("RowsBloomFilterFiltered:  10"))
        }
    }

    // insert more data
    insertTestData()
    
    // Verify data loaded correctly
    qt_select "SELECT * FROM ${tableName} ORDER BY sale_id"

    // Test 7: Verify filtering with more data
    profile("sql_select_like_with_ngram_index_recreated_more_data") {
        run {
            sql "/* sql_select_like_with_ngram_index_recreated_more_data */ ${query}"
            sleep(1000)
        }

        check { profileString, exception ->
            log.info(profileString)
            assertTrue(profileString.contains("RowsBloomFilterFiltered:  20"))
        }
    }

    // build index
    sql "BUILD INDEX idx_ngram_customer_name ON ${tableName};"
    wait_for_latest_op_on_table_finish(tableName, timeout)

    // Test 8: Verify filtering with index built
    profile("sql_select_like_with_ngram_index_recreated_built") {
        run {
            sql "/* sql_select_like_with_ngram_index_recreated_built */ ${query}"
            sleep(1000)
        }

        check { profileString, exception ->
            log.info(profileString)
            assertTrue(profileString.contains("RowsBloomFilterFiltered:  20"))
        }
    }

    // drop index
    sql "DROP INDEX idx_ngram_customer_name ON ${tableName};"
    wait_for_latest_op_on_table_finish(tableName, timeout)

    // Test 9: Verify filtering with index dropped
    profile("sql_select_like_with_ngram_index_recreated_dropped") {
        run {
            sql "/* sql_select_like_with_ngram_index_recreated_dropped */ ${query}"
            sleep(1000)
        }

        check { profileString, exception ->
            log.info(profileString)
            assertTrue(profileString.contains("RowsBloomFilterFiltered:  0"))
        }
    }
}