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

suite("test_inverted_index_storage_format_upgrade", "nonConcurrent") {
    def tableName = "test_inverted_index_format_upgrade"
    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort)
    
    def show_nested_index_file_on_tablet = { ip, port, tablet ->
        return http_client("GET", String.format("http://%s:%s/api/show_nested_index_file?tablet_id=%s", ip, port, tablet))
    }
    
    def get_tablet_inverted_index_format = { tblName, partitionName = null ->
        def targetTablet = null
        if (partitionName != null) {
            def tablets = sql_return_maparray """ show tablets from ${tblName} partition ${partitionName}; """
            logger.info("tablets: ${tablets}")
            if (tablets.size() >= 0) {
                targetTablet = tablets[0]
            }
        } else {
            def tablets = sql_return_maparray """ show tablets from ${tblName}; """
            logger.info("tablets: ${tablets}")
            if (tablets.size() >= 0) {
                targetTablet = tablets[0]
            }
        }
        
        if (targetTablet == null) {
            logger.error("No tablet found for table: ${tblName}, partition: ${partitionName}")
            return null
        }
        
        String tablet_id = targetTablet.TabletId
        String backend_id = targetTablet.BackendId
        String ip = backendId_to_backendIP.get(backend_id)
        String port = backendId_to_backendHttpPort.get(backend_id)
        
        def (code, out, err) = show_nested_index_file_on_tablet(ip, port, tablet_id)
        logger.info("Get tablet inverted index format: tablet_id=${tablet_id}, partition=${partitionName}, code=" + code)
        
        if (code == 0 && out != null) {
            def jsonResponse = parseJson(out.trim())
            if (jsonResponse.rowsets != null && jsonResponse.rowsets.size() > 0) {
                // Return the format from the first rowset
                def format = jsonResponse.rowsets[0].index_storage_format
                logger.info("Tablet ${tablet_id} in partition ${partitionName} has format: ${format}")
                return format
            }
        }
        
        logger.warn("Could not determine format for tablet ${tablet_id}")
        return null
    }
    
    def get_fe_config = { key ->
        def result = sql "SHOW FRONTEND CONFIG LIKE '${key}'"
        if (result.size() > 0) {
            return result[0][1]
        }
        return null
    }

    def set_fe_config = { key, value ->
        sql "ADMIN SET FRONTEND CONFIG ('${key}' = '${value}')"
        // Wait a bit for config to take effect
        sleep(2000)
    }

    def getJobState = { tblName ->
        def jobStateResult = sql """  SHOW ALTER TABLE COLUMN WHERE TableName='${tblName}' ORDER BY createtime DESC LIMIT 1 """
        return jobStateResult.size() > 0 ? jobStateResult[0][9] : "FINISHED"
    }

    def waitForJob = { tblName ->
        int max_try_secs = 60
        while (max_try_secs--) {
            String res = getJobState(tblName)
            if (res == "FINISHED" || res == "CANCELLED") {
                assertEquals("FINISHED", res)
                sleep(3000)
                break
            } else {
                Thread.sleep(1000)
                if (max_try_secs < 1) {
                    println "test timeout," + "state:" + res
                    assertEquals("FINISHED", res)
                }
            }
        }
    }

    def originalConfigValue
    try {
        // Get original config value to restore later
        originalConfigValue = get_fe_config("enable_new_partition_inverted_index_v2_format")
        logger.info("Original enable_new_partition_inverted_index_v2_format value: ${originalConfigValue}")

        // Test 1: Mixed format partitions - V1 table with config disabled/enabled
        logger.info("=== Test 1: Mixed Format Partitions ===")
        
        // Step 1: Disable config, create table with V1 format
        set_fe_config("enable_new_partition_inverted_index_v2_format", "false")
        sql "DROP TABLE IF EXISTS ${tableName}"
        
        sql """
            CREATE TABLE ${tableName} (
                id int(11) NOT NULL,
                name varchar(255) NOT NULL,
                description text,
                score int(11),
                INDEX idx_name (name) USING INVERTED,
                INDEX idx_description (description) USING INVERTED PROPERTIES("parser"="english"),
                INDEX idx_score (score) USING INVERTED
            )
            DUPLICATE KEY(id)
            PARTITION BY RANGE(id) (
                PARTITION p1 VALUES [("1"), ("100"))
            )
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "inverted_index_storage_format" = "V1"
            );
        """
        
        // Verify table was created with V1 format
        def tableInfo = sql "SHOW CREATE TABLE ${tableName}"
        assertTrue(tableInfo[0][1].contains("inverted_index_storage_format") && tableInfo[0][1].contains("V1"))
        logger.info("Table created with V1 format successfully")
        
        // Insert some test data
        sql "INSERT INTO ${tableName} VALUES (1, 'alice', 'alice loves programming', 95)"
        sql "INSERT INTO ${tableName} VALUES (2, 'bob', 'bob enjoys coding', 88)"
        sql "INSERT INTO ${tableName} VALUES (50, 'charlie', 'charlie studies algorithms', 92)"
        
        // Sync data to ensure tablets are created
        sql "SELECT * FROM ${tableName};"
        
        // Verify inverted index works with V1 format
        qt_sql_v1_original "SELECT * FROM ${tableName} WHERE name MATCH 'alice' ORDER BY id"
        qt_sql_v1_original_text "SELECT * FROM ${tableName} WHERE description MATCH 'programming' ORDER BY id"
        qt_sql_v1_original_numeric "SELECT * FROM ${tableName} WHERE score = 95 ORDER BY id"
        
        // Verify p1 partition uses V1 format through API
        def p1_format = get_tablet_inverted_index_format(tableName, "p1")
        assertEquals("V1", p1_format)
        
        // Step 2: Enable config for V2 format upgrade  
        set_fe_config("enable_new_partition_inverted_index_v2_format", "true")
        
        // Add new partition - should use V2 format due to config upgrade
        sql "ALTER TABLE ${tableName} ADD PARTITION p2 VALUES [('100'), ('200'))"
        waitForJob(tableName)
        
        // Insert data into new partition
        sql "INSERT INTO ${tableName} VALUES (150, 'david', 'david develops applications', 89)"
        sql "INSERT INTO ${tableName} VALUES (180, 'eve', 'eve explores databases', 94)"
        
        // Sync data 
        sql "SELECT * FROM ${tableName};"
        
        // Verify both partitions work correctly
        qt_sql_mixed_all "SELECT * FROM ${tableName} ORDER BY id"
        qt_sql_mixed_p1 "SELECT * FROM ${tableName} WHERE id < 100 AND name MATCH 'alice' ORDER BY id"
        qt_sql_mixed_p2 "SELECT * FROM ${tableName} WHERE id >= 100 AND name MATCH 'david' ORDER BY id"
        qt_sql_mixed_text "SELECT * FROM ${tableName} WHERE description MATCH 'programming OR databases' ORDER BY id"
        
        // Verify formats through API - this is the key validation
        def p1_format_after = get_tablet_inverted_index_format(tableName, "p1")
        def p2_format = get_tablet_inverted_index_format(tableName, "p2")
        
        assertEquals("V1", p1_format_after) // p1 should still be V1
        assertEquals("V2", p2_format)       // p2 should be upgraded to V2
        
        logger.info("Mixed partition format test completed successfully - p1: ${p1_format_after}, p2: ${p2_format}")
        
        // Test 2: V1 format preservation when config is disabled
        logger.info("=== Test 2: V1 Format Preservation ===")
        
        def tableName2 = "${tableName}_v1_preserve"
        set_fe_config("enable_new_partition_inverted_index_v2_format", "false")
        
        sql "DROP TABLE IF EXISTS ${tableName2}"
        sql """
            CREATE TABLE ${tableName2} (
                id int(11) NOT NULL,
                content varchar(255),
                INDEX idx_content (content) USING INVERTED
            )
            DUPLICATE KEY(id)
            PARTITION BY RANGE(id) (
                PARTITION p1 VALUES [("1"), ("100"))
            )
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "inverted_index_storage_format" = "V1"
            );
        """
        
        sql "INSERT INTO ${tableName2} VALUES (1, 'test content for V1 format')"
        
        // Sync data
        sql "SELECT * FROM ${tableName2};"
        
        qt_sql_v1_preserve "SELECT * FROM ${tableName2} WHERE content MATCH 'test' ORDER BY id"
        
        // Verify initial partition format
        def initial_format = get_tablet_inverted_index_format(tableName2, "p1")
        assertEquals("V1", initial_format)
        
        // Verify V1 format is preserved when adding partitions with config disabled
        sql "ALTER TABLE ${tableName2} ADD PARTITION p2 VALUES [('100'), ('200'))"
        waitForJob(tableName2)
        
        sql "INSERT INTO ${tableName2} VALUES (150, 'new content in second partition')"
        
        // Sync data
        sql "SELECT * FROM ${tableName2};"
        
        qt_sql_v1_preserve_new "SELECT * FROM ${tableName2} WHERE content MATCH 'content' ORDER BY id"
        
        // Verify both partitions use V1 format when config is disabled  
        def p1_v1_format = get_tablet_inverted_index_format(tableName2, "p1")  // initial partition
        def p2_v1_format = get_tablet_inverted_index_format(tableName2, "p2")
        
        // Both should be V1 since config is disabled
        assertEquals("V1", p1_v1_format)
        assertEquals("V1", p2_v1_format)
        
        logger.info("V1 format preservation test completed successfully")
        
        // Test 3: V2 format behavior - remains V2 regardless of config
        logger.info("=== Test 3: V2 Format Behavior ===")
        
        def tableName3 = "${tableName}_v2_behavior"
        set_fe_config("enable_new_partition_inverted_index_v2_format", "true")
        
        sql "DROP TABLE IF EXISTS ${tableName3}"
        sql """
            CREATE TABLE ${tableName3} (
                id int(11) NOT NULL,
                content varchar(255),
                INDEX idx_content (content) USING INVERTED
            )
            DUPLICATE KEY(id)
            PARTITION BY RANGE(id) (
                PARTITION p1 VALUES [("1"), ("100"))
            )
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "inverted_index_storage_format" = "V2"
            );
        """
        
        sql "INSERT INTO ${tableName3} VALUES (1, 'test content for V2 format')"
        
        // Sync data
        sql "SELECT * FROM ${tableName3};"
        
        qt_sql_v2_enabled "SELECT * FROM ${tableName3} WHERE content MATCH 'test' ORDER BY id"
        
        // Verify initial partition uses V2 format
        def p1_v2_format = get_tablet_inverted_index_format(tableName3, "p1")
        assertEquals("V2", p1_v2_format)
        
        // Disable config and verify V2 format is still preserved
        set_fe_config("enable_new_partition_inverted_index_v2_format", "false")
        
        sql "ALTER TABLE ${tableName3} ADD PARTITION p2 VALUES [('100'), ('200'))"
        waitForJob(tableName3)
        
        sql "INSERT INTO ${tableName3} VALUES (150, 'new content should still use V2')"
        
        // Sync data
        sql "SELECT * FROM ${tableName3};"
        
        qt_sql_v2_disabled_config "SELECT * FROM ${tableName3} WHERE content MATCH 'content' ORDER BY id"
        
        // Verify both partitions still use V2 format even when config is disabled
        def p1_v2_after = get_tablet_inverted_index_format(tableName3, "p1")  
        def p2_v2_format = get_tablet_inverted_index_format(tableName3, "p2")
        
        assertEquals("V2", p1_v2_after) // p1 should remain V2
        assertEquals("V2", p2_v2_format) // p2 should also be V2 (V2 table format preserved)
        
        // Verify table info still shows V2 format
        def tableInfo3 = sql "SHOW CREATE TABLE ${tableName3}"
        assertTrue(tableInfo3[0][1].contains("inverted_index_storage_format") && tableInfo3[0][1].contains("V2"))
        
        logger.info("V2 format behavior test completed successfully")
        
        // Test 4: Performance comparison between formats (basic functionality test)
        logger.info("=== Test 4: Format Functionality Verification ===")
        
        def testData = [
            [1, "apple", "red apple is sweet"],
            [2, "banana", "yellow banana is nutritious"],
            [3, "cherry", "red cherry is sour"],
            [4, "date", "brown date is sweet"],
            [5, "elderberry", "purple elderberry is rare"]
        ]
        
        // Test with V1 format
        set_fe_config("enable_new_partition_inverted_index_v2_format", "false")
        def tableName4V1 = "${tableName}_func_v1"
        sql "DROP TABLE IF EXISTS ${tableName4V1}"
        sql """
            CREATE TABLE ${tableName4V1} (
                id int(11) NOT NULL,
                name varchar(255),
                description text,
                INDEX idx_name (name) USING INVERTED,
                INDEX idx_description (description) USING INVERTED PROPERTIES("parser"="english")
            )
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "inverted_index_storage_format" = "V1"
            );
        """
        
        // Insert test data for V1
        for (data in testData) {
            sql "INSERT INTO ${tableName4V1} VALUES (${data[0]}, '${data[1]}', '${data[2]}')"
        }
        
        // Sync data
        sql "SELECT * FROM ${tableName4V1};"
        
        // Test with V2 format
        set_fe_config("enable_new_partition_inverted_index_v2_format", "true")
        def tableName4V2 = "${tableName}_func_v2"
        sql "DROP TABLE IF EXISTS ${tableName4V2}"
        sql """
            CREATE TABLE ${tableName4V2} (
                id int(11) NOT NULL,
                name varchar(255),
                description text,
                INDEX idx_name (name) USING INVERTED,
                INDEX idx_description (description) USING INVERTED PROPERTIES("parser"="english")
            )
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "inverted_index_storage_format" = "V2"
            );
        """
        
        // Insert test data for V2
        for (data in testData) {
            sql "INSERT INTO ${tableName4V2} VALUES (${data[0]}, '${data[1]}', '${data[2]}')"
        }
        
        // Sync data
        sql "SELECT * FROM ${tableName4V2};"
        
        // Verify formats through API
        def v1_format = get_tablet_inverted_index_format(tableName4V1)
        def v2_format = get_tablet_inverted_index_format(tableName4V2)
        
        assertEquals("V1", v1_format)
        assertEquals("V2", v2_format)
        
        // Compare functionality between V1 and V2
        qt_sql_v1_func "SELECT * FROM ${tableName4V1} WHERE name MATCH 'apple' ORDER BY id"
        qt_sql_v2_func "SELECT * FROM ${tableName4V2} WHERE name MATCH 'apple' ORDER BY id"
        
        qt_sql_v1_text_func "SELECT * FROM ${tableName4V1} WHERE description MATCH 'sweet' ORDER BY id"
        qt_sql_v2_text_func "SELECT * FROM ${tableName4V2} WHERE description MATCH 'sweet' ORDER BY id"
        
        logger.info("Format functionality verification test completed successfully")
        
        // Clean up test tables
        sql "DROP TABLE IF EXISTS ${tableName}"
        sql "DROP TABLE IF EXISTS ${tableName}_v1_preserve"
        sql "DROP TABLE IF EXISTS ${tableName}_v2_behavior"  
        sql "DROP TABLE IF EXISTS ${tableName4V1}"
        sql "DROP TABLE IF EXISTS ${tableName4V2}"
        
        logger.info("All inverted index storage format upgrade tests completed successfully")
        
    } finally {
        // Restore original config
        if (originalConfigValue != null) {
            set_fe_config("enable_new_partition_inverted_index_v2_format", originalConfigValue)
            logger.info("Restored enable_new_partition_inverted_index_v2_format to: ${originalConfigValue}")
        }
    }
}