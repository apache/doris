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

suite("ann_index_topn_cache") {
    sql "unset variable all;"
    sql "set enable_common_expr_pushdown=true;"
    sql "set profile_level=2;"
    sql "set enable_profile=true;"
    sql "set parallel_pipeline_task_num=1;"
    sql "set enable_sql_cache=false;"

    String[][] backends = sql """ show backends """
    def backendIdToBackendIP = [:]
    def backendIdToBackendHttpPort = [:]
    for (String[] backend in backends) {
        // backend[9] is Alive in current SHOW BACKENDS format
        if (backend[9].equalsIgnoreCase("true")) {
            backendIdToBackendIP.put(backend[0], backend[1])
            backendIdToBackendHttpPort.put(backend[0], backend[4])
        }
    }
    assertTrue(backendIdToBackendIP.size() > 0)

    sql "drop table if exists ann_index_topn_cache"
    sql """
        CREATE TABLE ann_index_topn_cache (
            id INT NOT NULL,
            embedding ARRAY<FLOAT> NOT NULL,
            INDEX idx_emb (`embedding`) USING ANN PROPERTIES(
                "index_type"="hnsw",
                "metric_type"="l2_distance",
                "dim"="3"
            )
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1");
    """

    sql """
        INSERT INTO ann_index_topn_cache VALUES
        (1, [1.0, 2.0, 3.0]),
        (2, [1.1, 2.1, 3.1]),
        (3, [1.2, 2.2, 3.2]),
        (4, [1.3, 2.3, 3.3]),
        (5, [1.4, 2.4, 3.4]),
        (6, [1.5, 2.5, 3.5]),
        (7, [5.0, 5.0, 5.0]),
        (8, [8.0, 8.0, 8.0]),
        (9, [9.0, 9.0, 9.0]);
    """

    def timeout = 60000
    def deltaTime = 1000
    def wait_for_latest_op_on_table_finish = { tableName, opTimeout ->
        int useTime = 0
        for (int t = deltaTime; t <= opTimeout; t += deltaTime) {
            def alterRes = sql """SHOW ALTER TABLE COLUMN WHERE TableName = "${tableName}" ORDER BY CreateTime DESC LIMIT 1;"""
            def alterResStr = alterRes.toString()
            if (alterResStr.contains("FINISHED")) {
                sleep(3000)
                logger.info("${tableName} latest alter job finished, detail: ${alterResStr}")
                break
            }
            useTime = t
            sleep(deltaTime)
        }
        assertTrue(useTime <= opTimeout, "wait_for_latest_op_on_table_finish timeout")
    }

    def getProfileWithToken = { token ->
        String profileId = ""
        int attempts = 0
        while (attempts < 12 && (profileId == null || profileId == "")) {
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

    def extractCounterValue = { String profileText, String counterName ->
        // Common format: AnnIndexTopNCacheHitCnt: 0
        def m1 = (profileText =~ /${counterName}:\s*(\d+)/)
        if (m1.find()) {
            return Integer.parseInt(m1.group(1))
        }
        // Alternative format: AnnIndexTopNCacheHitCnt: xx (0)
        def m2 = (profileText =~ /${counterName}:\s*[^\(]*\((\d+)\)/)
        if (m2.find()) {
            return Integer.parseInt(m2.group(1))
        }
        return 0
    }

    def clearAnnTopnResultCache = {
        for (def backendId : backendIdToBackendIP.keySet()) {
            def url = backendIdToBackendIP.get(backendId) + ":" +
                    backendIdToBackendHttpPort.get(backendId) + "/api/clear_cache/AnnIndexTopnResultCache"
            httpTest {
                endpoint ""
                uri url
                op "get"
                body ""
                check { respCode, body ->
                    assertEquals("${respCode}".toString(), "200")
                    assertTrue("${body}".toString().contains("prune win"))
                }
            }
        }
    }

    // 1) No prefilter: second identical ANN TopN query should hit cache.
    def tokenNoFilter1 = UUID.randomUUID().toString()
    qt_no_prefilter_1 """
        select id, "${tokenNoFilter1}"
        from ann_index_topn_cache
        order by l2_distance_approximate(embedding, [1.0,2.0,3.0])
        limit 3;
    """

    def tokenNoFilter2 = UUID.randomUUID().toString()
    qt_no_prefilter_2 """
        select id, "${tokenNoFilter2}"
        from ann_index_topn_cache
        order by l2_distance_approximate(embedding, [1.0,2.0,3.0])
        limit 3;
    """

    def profileNoFilter2 = getProfileWithToken(tokenNoFilter2)
    def noFilterHit = extractCounterValue(profileNoFilter2, "AnnIndexTopNCacheHitCnt")
    assertTrue(noFilterHit > 0)

    // 1.1) Manual clear cache API: after clear, next same query should be cache miss.
    clearAnnTopnResultCache()
    def tokenAfterClear = UUID.randomUUID().toString()
    qt_no_prefilter_after_clear """
        select id, "${tokenAfterClear}"
        from ann_index_topn_cache
        order by l2_distance_approximate(embedding, [1.0,2.0,3.0])
        limit 3;
    """
    def profileAfterClear = getProfileWithToken(tokenAfterClear)
    def hitAfterClear = extractCounterValue(profileAfterClear, "AnnIndexTopNCacheHitCnt")
    assertEquals(0, hitAfterClear)

    // 2) With prefilter (id < 3): ANN TopN result cache should be bypassed.
    def tokenFilter1 = UUID.randomUUID().toString()
    qt_prefilter_1 """
        select id, "${tokenFilter1}"
        from ann_index_topn_cache
        where id < 3
        order by l2_distance_approximate(embedding, [1.0,2.0,3.0])
        limit 2;
    """

    def tokenFilter2 = UUID.randomUUID().toString()
    qt_prefilter_2 """
        select id, "${tokenFilter2}"
        from ann_index_topn_cache
        where id < 3
        order by l2_distance_approximate(embedding, [1.0,2.0,3.0])
        limit 2;
    """

    def profileFilter2 = getProfileWithToken(tokenFilter2)
    def filterHit = extractCounterValue(profileFilter2, "AnnIndexTopNCacheHitCnt")
    assertEquals(0, filterHit)

    // 3) After dropping ANN index, query should still run (fallback path), and TopN cache hit must stay 0.
    sql "drop index idx_emb on ann_index_topn_cache"
    wait_for_latest_op_on_table_finish("ann_index_topn_cache", timeout)

    def tokenNoIndex1 = UUID.randomUUID().toString()
    qt_no_index_1 """
        select id, "${tokenNoIndex1}"
        from ann_index_topn_cache
        order by l2_distance_approximate(embedding, [1.0,2.0,3.0])
        limit 3;
    """

    def tokenNoIndex2 = UUID.randomUUID().toString()
    qt_no_index_2 """
        select id, "${tokenNoIndex2}"
        from ann_index_topn_cache
        order by l2_distance_approximate(embedding, [1.0,2.0,3.0])
        limit 3;
    """

    def profileNoIndex2 = getProfileWithToken(tokenNoIndex2)
    def noIndexHit = extractCounterValue(profileNoIndex2, "AnnIndexTopNCacheHitCnt")
    assertEquals(0, noIndexHit)
}
