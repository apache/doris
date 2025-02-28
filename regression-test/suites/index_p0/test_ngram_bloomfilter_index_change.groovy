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

    def httpGet = { url ->
        def dst = 'http://' + context.config.feHttpAddress
        def conn = new URL(dst + url).openConnection()
        conn.setRequestMethod("GET")
        def encoding = Base64.getEncoder().encodeToString((context.config.feHttpUser + ":" + 
                (context.config.feHttpPassword == null ? "" : context.config.feHttpPassword)).getBytes("UTF-8"))
        conn.setRequestProperty("Authorization", "Basic ${encoding}")
        conn.setRequestProperty("Cache-Control", "no-cache")
        conn.setRequestProperty("Pragma", "no-cache")
        conn.setConnectTimeout(10000) // 10 seconds
        conn.setReadTimeout(10000) // 10 seconds

        int responseCode = conn.getResponseCode()
        log.info("HTTP response status: " + responseCode)

        if (responseCode == 200) {
            InputStream inputStream = conn.getInputStream()
            String response = inputStream.text
            inputStream.close()
            return response
        } else {
            log.error("HTTP request failed with response code: " + responseCode)
            return null
        }
    }

    def getProfileIdWithRetry = { query, maxRetries, waitSeconds ->
        def profileUrl = '/rest/v1/query_profile/'
        def profiles = null
        def profileId = null
        int attempt = 0

        while (attempt < maxRetries) {
            sql "sync"
            sql """ ${query} """
            profiles = httpGet(profileUrl)
            log.info("profiles attempt ${attempt + 1}: {}", profiles)
            if (profiles == null) {
                log.warn("Failed to fetch profiles on attempt ${attempt + 1}")
            } else {
                def jsonProfiles = new JsonSlurper().parseText(profiles)
                if (jsonProfiles.code == 0) {
                    for (def profile in jsonProfiles["data"]["rows"]) {
                        if (profile["Sql Statement"].contains(query)) {
                            profileId = profile["Profile ID"]
                            break
                        }
                    }
                } else {
                    log.warn("Profile response code is not 0 on attempt ${attempt + 1}")
                }
            }

            if (profileId != null) {
                break
            } else {
                attempt++
                if (attempt < maxRetries) {
                    log.info("profileId is null, retrying after ${waitSeconds} second(s)... (Attempt ${attempt + 1}/${maxRetries})")
                    sleep(waitSeconds * 1000)
                }
            }
        }

        assertTrue(profileId != null, "Failed to retrieve profileId after ${maxRetries} attempts")
        return profileId
    }
    
    // Function to execute query and verify bloom filter count
    def executeQueryAndCheckFilter = { query, expectedFilteredCount ->
        def profileId = getProfileIdWithRetry(query, 3, 30)
        log.info("profileId:{}", profileId)
        def profileDetail = httpGet("/rest/v1/query_profile/" + profileId)
        log.info("profileDetail:{}", profileDetail)
        assertTrue(profileDetail.contains("BloomFilterFiltered:&nbsp;&nbsp;" + expectedFilteredCount))
    }
    
    // Function to insert test data batch
    def insertTestData = { startId, datePeriod, nameOverride = null ->
        def customerData = [
            ["John Smith", "Laptop", "North"],
            ["Emily Johnson", "Smartphone", "South"],
            ["Michael Brown", "Headphones", "East"],
            ["Jessica Davis", "Monitor", "West"],
            ["David Wilson", "Keyboard", "North"],
            ["Sarah Taylor", "Mouse", "South"],
            ["Thomas Anderson", "Printer", "East"],
            ["Jennifer Martin", "Speaker", "West"],
            ["Robert Clark", "External SSD", "North"],
            [(nameOverride != null && nameOverride.length > 0) ? nameOverride : "Amanda Lewis", "Webcam", "South"]
        ]
        
        customerData.eachWithIndex { data, index ->
            def id = startId + index
            def date = "${datePeriod}-${String.format("%02d", (index * 3 + 5) % 28 + 1)} ${String.format("%02d", (index * 2 + 8) % 18 + 7)}:${String.format("%02d", (index * 5) % 60)}:00"
            sql "INSERT INTO ${tableName} VALUES(${id}, '${date}', '${data[1]}', '${data[0]}', ${id % 5 == 0 ? 89.99 : 199.99 + index * 100}, '${data[2]}');"
        }
    }

    // Test setup
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
    
    // Insert first batch of data (2023)
    insertTestData(1001, "2023-10")

    // Test settings
    sql "SET global enable_function_pushdown = true"
    sql "SET global enable_profile = true"
    sql "SET global profile_level = 2"

    // Verify data loaded correctly
    qt_select "SELECT * FROM ${tableName} ORDER BY sale_id"

    // Define test query
    def query = "SELECT /*+SET_VAR(enable_function_pushdown = true, enable_profile = true, profile_level = 2)*/  * FROM ${tableName} WHERE customer_name LIKE '%xxxx%' ORDER BY sale_id"
    
    // Test 1: without NGRAM Bloom Filter index
    executeQueryAndCheckFilter(query, 0)

    // Test 2: After adding NGRAM Bloom Filter index
    sql "ALTER TABLE ${tableName} ADD INDEX idx_ngram_customer_name(customer_name) USING NGRAM_BF PROPERTIES('bf_size' = '1024', 'gram_size' = '3');"
    wait_for_latest_op_on_table_finish(tableName, timeout)
    executeQueryAndCheckFilter(query, 0)

    // Test 3: After building the index
    sql "BUILD INDEX idx_ngram_customer_name ON ${tableName};"
    wait_for_latest_op_on_table_finish(tableName, timeout)
    executeQueryAndCheckFilter(query, 10)

    // Insert second batch of data (2024)
    insertTestData(1011, "2024-01", "Cindy Lewis")

    // Test 4: Verify filtering with more data
    executeQueryAndCheckFilter(query, 20)

    // Test 5: After dropping the index
    sql "DROP INDEX idx_ngram_customer_name ON ${tableName};"
    wait_for_latest_op_on_table_finish(tableName, timeout)
    executeQueryAndCheckFilter(query, 0)
}