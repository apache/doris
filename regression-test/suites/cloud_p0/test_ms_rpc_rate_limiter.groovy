import groovy.json.JsonSlurper

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

suite("test_ms_rpc_rate_limiter") {
    if (!isCloudMode()) {
        return
    }
    def tableName = "test_ms_rpc_rate_limiter"

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

    def profileId = ""
    def getProfileWithToken = { token ->
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
        assertTrue(profileId != null && profileId != "")
        Thread.sleep(800)
        return getProfile(profileId).toString()
    }

    sql "set cloud_partition_version_cache_ttl_ms = 0;"
    sql "set cloud_table_version_cache_ttl_ms = 0;"
    sql "set enable_profile = true;"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                `k1` int(11) NULL, 
                `k2` int(11) NULL, 
                `v3` int(11) NULL,
                `v4` int(11) NULL
            ) unique KEY(`k1`) 
            DISTRIBUTED BY HASH(`k1`) BUCKETS 1
            PROPERTIES (
                "function_column.sequence_col" = "v4",
                "replication_num" = "1",
                "disable_auto_compaction" = "true"
            );
    """
    sql """ INSERT INTO ${tableName} VALUES (10, 20, 39, 40),(11, 20, 38, 40),(12, 20, 37, 39),(13, 20, 36, 40),(14, 20, 35, 40); """
    def t1 = UUID.randomUUID().toString()
    sql """ select *, "${t1}" from ${tableName}; """

    // check profile
    def profile = getProfileWithToken("${t1}")
    logger.info("Profile_id: ${profileId}")
    logger.info("Profile: ${profile}")
    assertTrue(profile.contains("Wait MS RPC Rate Limiter Time"))
    assertTrue(profile.contains("Wait MS RPC Rate Limiter Count"))
    assertFalse(profile.contains("Wait MS RPC Rate Limiter Count: 0"))

    // check audit log
    def audit_log
    for (int i = 0; i < 20; i++) {
        audit_log = sql "select query_id, get_meta_times_ms, time from __internal_schema.audit_log where query_id = '${profileId}' order by time desc limit 1"
        if (audit_log.size() == 0) {
            sleep(2000)
            continue
        }
        logger.info("Audit log: ${audit_log}")
        break
    }
    if (audit_log.size() == 0) {
        logger.warn("No audit log found for query_id: ${profileId}")
        return
    }
    assertEquals(audit_log.size(), 1)
    def getMetaTimesInfo = audit_log[0][1]
    assertTrue(getMetaTimesInfo.contains("wait_meta_service_rpc_rate_limit_time_ms"))
    assertTrue(getMetaTimesInfo.contains("wait_meta_service_rpc_rate_limit_count"))
}