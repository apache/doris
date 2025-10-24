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

suite('test_profile_archive', 'nonConcurrent') {        
    def tableName = "test_profile_archive_table"
    def user = context.config.feHttpUser
    def password = context.config.feHttpPassword == null ? "" : context.config.feHttpPassword


    // Helper function to get profile list from FE HTTP API
    def getProfileList = { ->
        def dst = 'http://' + context.config.feHttpAddress + '/rest/v1/query_profile'
        def conn = new URL(dst).openConnection()
        conn.setRequestMethod("GET")
        def encoding = Base64.getEncoder().encodeToString((user + ":" + password).getBytes("UTF-8"))
        conn.setRequestProperty("Authorization", "Basic ${encoding}")
        return conn.getInputStream().getText()
    }

    // Helper function to extract query IDs from profile list
    def getQueryIds = { ->
        def profileListJson = getProfileList()
        def profiles = new groovy.json.JsonSlurper().parseText(profileListJson)
        return profiles.data.rows.collect { it['Profile ID'] } as Set
    }

    // Helper function to check if a query ID exists in current profiles
    def containsQueryId = { queryId ->
        return getQueryIds().contains(queryId)
    }

    // Helper function to get specific profile by ID
    def getProfile = { id ->
        def dst = 'http://' + context.config.feHttpAddress + '/rest/v1/query_profile/' + id
        def conn = new URL(dst).openConnection()
        conn.setRequestMethod("GET")
        def encoding = Base64.getEncoder().encodeToString((user + ":" + password).getBytes("UTF-8"))
        conn.setRequestProperty("Authorization", "Basic ${encoding}")
        return conn.getInputStream().getText()
    }

    // Helper function to wait for profile archiving
    def waitForArchiving = { queryId, maxWaitSeconds = 30 ->
        def startTime = System.currentTimeMillis()
        def timeout = maxWaitSeconds * 1000

        while (System.currentTimeMillis() - startTime < timeout) {
            try {
                // Check if profile is no longer in memory (archived)
                def profileListJson = getProfileList()
                def profiles = new groovy.json.JsonSlurper().parseText(profileListJson)

                boolean foundInMemory = false
                for (def row : profiles.data.rows) {
                    if (row['Profile ID'] == queryId) {
                        foundInMemory = true
                        break
                    }
                }

                // If not found in memory, it's archived
                if (!foundInMemory) {
                    return true
                }
            } catch (Exception e) {
                // Profile might be in transition, continue waiting
            }
            Thread.sleep(1000)
        }
        return false
    }

    // Helper function to extract query ID from profile list
    def extractQueryId = { profileListJson ->
        def profiles = new groovy.json.JsonSlurper().parseText(profileListJson)
        if (profiles.data && profiles.data.rows && profiles.data.rows.size() > 0) {
            return profiles.data.rows[0]['Profile ID']
        }
        return null
    }

    // Helper to compute archived IDs among a candidate set
    def getArchivedProfileIds = { ids ->
        def current = getQueryIds()
        def archived = [] as Set
        logger.info("currentProfileIds ${current.size()}  ${current}")
        logger.info("ids ${ids.size()}  ${ids}")
        ids.each { id ->
            if (!current.contains(id)) {
                try {
                    def content = getProfile(id)
                    if (content != null && !content.contains("does not exist")) {
                        archived.add(id)
                    }
                } catch (Exception e) {
                    // ignore transient errors
                }
            }
        }
        logger.info("archivedProfileIds ${archived.size()}  ${archived}")
        return archived
    }

    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE ${tableName} (
            id INT,
            name VARCHAR(50),
            age INT
        ) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """

    // Insert test data
    sql "INSERT INTO ${tableName} VALUES (1, 'Alice', 25), (2, 'Bob', 30), (3, 'Charlie', 35)"

    // Test 1: Verify profile archiving is enabled (moved out of test closure)
    sql "ADMIN SET FRONTEND CONFIG ('enable_profile_archive' = 'true')"
    def result = sql "ADMIN SHOW FRONTEND CONFIG LIKE 'enable_profile_archive'"
    assertTrue(result.size() > 0)
    assertEquals("true", result[0][1])


    // Test 2: Force profile archiving by reducing memory limits
    // Set a very low profile memory limit to force archiving
    // Enable Debugpoint Because max_query_profile_num is too small, we should write profile to storage before archiving
    GetDebugPoint().enableDebugPointForAllFE("FE.ProfileArchiveManager.archiveProfile.writeBeforeArchive")
    sql "ADMIN SET FRONTEND CONFIG ('max_query_profile_num' = '2')"
    // Enable profiling
    sql "SET enable_profile = true"
    sql "SET profile_level = 2"

    // Get initial profile list to establish baseline
    def initialProfileIds = getQueryIds()
    def queryIds = [] as Set
    logger.info("Found ${initialProfileIds.size()} initial query IDs: ${initialProfileIds}")

    // Generate profiles one by one and collect queryIds with polling
    for (int i = 0; i < 5; i++) {
        sql "SELECT COUNT(*) FROM ${tableName} WHERE age > ${20 + i}"
        long deadline = System.currentTimeMillis() + 5000
        Set newQueryIds = [] as Set
        Set currentProfileIds = [] as Set
        while (System.currentTimeMillis() < deadline) {
            currentProfileIds = getQueryIds()
            newQueryIds = currentProfileIds.findAll { !(initialProfileIds.contains(it) || queryIds.contains(it)) } as Set
            if (!newQueryIds.isEmpty()) {
                break
            }
            Thread.sleep(200)
        }

        queryIds.addAll(newQueryIds)
    }
    logger.info("Found ${queryIds.size()} new query IDs: ${queryIds}")
    assert queryIds.size() > 0
    // Verify the oldest query profile is archived
    def archived = waitForArchiving(queryIds[0],  5)
    assertTrue(archived, "Oldest query profile should be archived")

    // Verify that only 2 profiles remain in active memory (not archived)
    assertEquals(2, getQueryIds().size(), "Should have exactly 2 profiles remaining in active memory")

    // Verify all generated profiles are still accessible (both active and archived)
    logger.info("Verifying accessibility of all ${queryIds.size()} generated profiles...")

    for (def queryId : queryIds) {
        try {
            def profile = getProfile(queryId)
            assertNotNull(profile, "Profile ${queryId} should be accessible")
        } catch (Exception e) {
            fail("Profile ${queryId} should be accessible but got error: ${e.message}")
        }
    }
    sql "ADMIN SET FRONTEND CONFIG ('max_archived_profile_num' = '1')"
    sql "ADMIN SET FRONTEND CONFIG ('profile_archive_cleanup_interval_seconds' = '1')"
    Thread.sleep(2000)
    // Verify cleaned up, 2 remain in active memory, 1 remain in archived
    assertEquals(2, getQueryIds().size(), "Should have exactly 2 profiles remaining in active memory")

    def archivedProfileIds = getArchivedProfileIds(queryIds)
    assertEquals(1, archivedProfileIds.size(), "Should have exactly 1 profile remaining in archived memory")


    // Cleanup
    sql "DROP TABLE IF EXISTS ${tableName}"
    // Disable Debugpoint
    GetDebugPoint().disableDebugPointForAllFE("FE.ProfileArchiveManager.archiveProfile.writeBeforeArchive")

    // Reset configs to defaults
    sql "ADMIN SET FRONTEND CONFIG ('enable_profile_archive' = 'true')"
    sql "ADMIN SET FRONTEND CONFIG ('max_query_profile_num' = '100')"
    sql "ADMIN SET FRONTEND CONFIG ('profile_archive_retention_seconds' = '604800')" // 7 days in seconds
    sql "ADMIN SET FRONTEND CONFIG ('profile_archive_max_size_bytes' = '10737418240')" // 10GB
    sql "ADMIN SET FRONTEND CONFIG ('max_archived_profile_num' = '100000')"
    sql "ADMIN SET FRONTEND CONFIG ('profile_archive_cleanup_interval_seconds' = '21600')" // 6 hours in seconds

}