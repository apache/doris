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

/**
 * Verifies that SummaryProfile.queryFinished() writes the
 * "Is Profile Collection Completed: true" field into the profile text.
 *
 * This field is written after Coordinator.waitForFragmentsDone() returns,
 * guaranteeing all BE pipeline task profiles are merged. It allows pollers
 * to detect profile readiness without fixed sleeps or heuristics.
 */
suite("test_profile_collection_completed") {

    def getProfileText = { String queryId ->
        def addr = context.config.feHttpAddress
        def user = context.config.feHttpUser ?: "root"
        def pass = context.config.feHttpPassword ?: ""
        def encoding = Base64.getEncoder().encodeToString("${user}:${pass}".getBytes("UTF-8"))
        def conn = new URL("http://${addr}/api/profile/text?query_id=${queryId}").openConnection()
        conn.setRequestMethod("GET")
        conn.setRequestProperty("Authorization", "Basic ${encoding}")
        conn.setConnectTimeout(5000)
        conn.setReadTimeout(10000)
        try {
            return conn.getInputStream().getText()
        } catch (Exception e) {
            return ""
        }
    }

    // Poll until the completion marker appears, or timeout after 10 s.
    def waitForCompletionMarker = { String queryId ->
        def marker = "Is Profile Collection Completed: true"
        def maxAttempts = 33   // 33 × 300 ms ≈ 10 s
        for (int i = 0; i < maxAttempts; i++) {
            Thread.sleep(300)
            def text = getProfileText(queryId)
            if (text.contains(marker)) {
                return text
            }
        }
        return getProfileText(queryId)
    }

    sql "SET enable_profile = true"
    sql "SET enable_sql_cache = false"

    // Use a multi-instance query so the fragment pipeline is exercised
    // and a proper execution profile is written (simple SET statements are skipped).
    sql "DROP TABLE IF EXISTS test_profile_complete_t"
    sql """
        CREATE TABLE test_profile_complete_t (
            k1 INT NOT NULL,
            v1 INT
        ) ENGINE=OLAP
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 4
        PROPERTIES ("replication_num" = "1")
    """

    sql "INSERT INTO test_profile_complete_t VALUES (1, 10), (2, 20), (3, 30)"

    sql "SELECT count(*) FROM test_profile_complete_t"

    def queryIdResult = sql "SELECT last_query_id()"
    def queryId = queryIdResult[0][0].toString()
    logger.info("query_id for completion marker test: ${queryId}")

    def profileText = waitForCompletionMarker(queryId)

    assertTrue(
        profileText.contains("Is Profile Collection Completed: true"),
        "Expected 'Is Profile Collection Completed: true' in profile for query ${queryId}. " +
        "Profile length=${profileText.length()}, first 500 chars: ${profileText.take(500)}"
    )

    logger.info("'Is Profile Collection Completed: true' verified in profile for query ${queryId}")

    sql "DROP TABLE IF EXISTS test_profile_complete_t"
}
