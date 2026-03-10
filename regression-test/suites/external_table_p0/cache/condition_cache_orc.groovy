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

import java.util.regex.Pattern
import groovy.json.JsonSlurper

suite("condition_cache_orc", "tvf,external,external_docker") {
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
        assertTrue(profileId != null && profileId != "")
        Thread.sleep(800)
        return getProfile(profileId).toString()
    }

    def extractProfileBlockMetrics = {String profileText, String blockName ->
        List<String> lines = profileText.readLines()

        Map<String, String> metrics = [:]
        boolean inBlock = false
        int blockIndent = -1

        lines.each { line ->
            if (!inBlock) {
                def m = line =~ /^(\s*)\s+${Pattern.quote(blockName)}:/
                if (m.find()) {
                    inBlock = true
                    blockIndent = m.group(1).length()
                }
            } else {
                // 当前行缩进
                def indent = (line =~ /^(\s*)/)[0][1].length()

                if (indent > blockIndent) {
                    def kv = line =~ /^\s*-\s*([^:]+):\s*(.+)$/
                    if (kv.matches()) {
                        metrics[kv[0][1].trim()] = kv[0][2].trim()
                    }
                } else {
                    // 缩进回退，block 结束
                    inBlock = false
                }
            }
        }

        return metrics
    }

    def extractProfileValue =  { String profileText, String keyName -> 
        def matcher = profileText =~ /(?m)^\s*-\s*${keyName}:\s*(.+)$/
        return matcher.find() ? matcher.group(1).trim() : null
    }

    List<List<Object>> backends = sql """ show backends """
    assertTrue(backends.size() > 0)
    def be_id = backends[0][0]
    def be_host = backends[0][1]
    def basePath = "/tmp/test_condition_cache_orc"

    sshExec("root", be_host, "rm -rf ${basePath}", false)
    sshExec("root", be_host, "mkdir -p ${basePath}")
    sshExec("root", be_host, "chmod 777 ${basePath}")

    // ============ Source tables ============

    def srcTable = "cc_ext_src_orc"
    def joinSrcTable = "cc_ext_join_src_orc"
    def largeSrcTable = "cc_ext_large_src_orc"

    sql """ DROP TABLE IF EXISTS ${srcTable} """
    sql """
        CREATE TABLE ${srcTable} (
            `id` int NULL,
            `name` varchar(50) NULL,
            `age` int NULL,
            `score` int NULL
        ) DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """

    sql """
        INSERT INTO ${srcTable}(id, name, age, score) VALUES
            (1, 'Alice', 25, 85.5),
            (2, 'Bob', 30, 90.0),
            (3, 'Charlie', 22, 75.5),
            (4, 'David', 28, 92.0),
            (5, 'Eve', 26, 88.0)
    """

    sql """ DROP TABLE IF EXISTS ${joinSrcTable} """
    sql """
        CREATE TABLE ${joinSrcTable} (
            `id` int NULL,
            `department` varchar(50) NULL,
            `position` varchar(50) NULL,
            `salary` int NULL
        ) DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """

    sql """
        INSERT INTO ${joinSrcTable}(id, department, position, salary) VALUES
            (1, 'Engineering', 'Developer', 100000),
            (2, 'Marketing', 'Manager', 120000),
            (3, 'HR', 'Specialist', 80000),
            (4, 'Engineering', 'Senior Developer', 140000),
            (5, 'Finance', 'Analyst', 95000)
    """

    // Large table for LIMIT test: 5000 rows spanning multiple granules (GRANULE_SIZE=2048)
    sql """ DROP TABLE IF EXISTS ${largeSrcTable} """
    sql """
        CREATE TABLE ${largeSrcTable} (
            `id` int NULL,
            `name` varchar(50) NULL,
            `age` int NULL,
            `score` int NULL
        ) DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """

    sql """
        INSERT INTO ${largeSrcTable}
        SELECT
            cast(number AS INT),
            concat('name_', cast(number AS VARCHAR(20))),
            cast(20 + (number % 30) AS INT),
            cast(50 + (number % 50) AS INT)
        FROM numbers("number" = "5000")
    """

    def fmt = "orc"
    // Export data to format
    sshExec("root", be_host, "rm -f ${basePath}/${fmt}_main_*")
    sql """
        INSERT INTO local(
            "file_path" = "${basePath}/${fmt}_main_",
            "backend_id" = "${be_id}",
            "format" = "${fmt}"
        ) SELECT id, name, age, score FROM ${srcTable} ORDER BY id
    """

    sshExec("root", be_host, "rm -f ${basePath}/${fmt}_join_*")
    sql """
        INSERT INTO local(
            "file_path" = "${basePath}/${fmt}_join_",
            "backend_id" = "${be_id}",
            "format" = "${fmt}"
        ) SELECT id, department, position, salary FROM ${joinSrcTable} ORDER BY id
    """

    sshExec("root", be_host, "rm -f ${basePath}/${fmt}_large_*")
    sql """
        INSERT INTO local(
            "file_path" = "${basePath}/${fmt}_large_",
            "backend_id" = "${be_id}",
            "format" = "${fmt}"
        ) SELECT id, name, age, score FROM ${largeSrcTable} ORDER BY id
    """

    def mainTvf = """local(
        "file_path" = "${basePath}/${fmt}_main_*",
        "backend_id" = "${be_id}",
        "format" = "${fmt}")"""

    def joinTvf = """local(
        "file_path" = "${basePath}/${fmt}_join_*",
        "backend_id" = "${be_id}",
        "format" = "${fmt}")"""

    def largeTvf = """local(
        "file_path" = "${basePath}/${fmt}_large_*",
        "backend_id" = "${be_id}",
        "format" = "${fmt}")"""

    sql "unset variable all;"
    sql "set enable_condition_cache=true;"
    sql "set enable_profile=true;"
    sql "set profile_level=2;"
    sql " set parallel_pipeline_task_num = 1;"
    sql """set max_file_scanners_concurrency =  1; """

    def uuid = UUID.randomUUID().toString()
    qt_condition_cache_verify_hit0 """
        SELECT id, name, age, score FROM (
            SELECT id, name, age, score, "${uuid}" FROM ${largeTvf}
            WHERE id > 2048 and age > 47 and score > 80
        ) tmpa
        ORDER BY 1, 2, 3, 4;
    """
    def profileText = getProfileWithToken(uuid)
    assertTrue(profileText.contains("Scanner"), "Profile does not contain Scanner")
    assertTrue(profileText.contains("ConditionCacheFileHit"), "Profile does not contain ConditionCacheFileHit")
    assertTrue(profileText.contains("ConditionCacheFilteredRows"), "Profile does not contain ConditionCacheFilteredRows")
    def metrics = extractProfileBlockMetrics(profileText, "Scanner")
    logger.info("metrics = ${metrics}")
    assertEquals("0", metrics["ConditionCacheFileHit"])
    assertEquals("0", metrics["ConditionCacheFilteredRows"])

    uuid = UUID.randomUUID().toString()
    qt_condition_cache_verify_hit0_1 """
        SELECT id, name, age, score FROM (
            SELECT id, name, age, score, "${uuid}" FROM ${largeTvf}
            WHERE id > 2048 and age > 47 and score > 80
        ) tmpa
        ORDER BY 1, 2, 3, 4;
    """
    profileText = getProfileWithToken(uuid)
    assertTrue(profileText.contains("Scanner"), "Profile does not contain Scanner")
    assertTrue(profileText.contains("ConditionCacheFileHit"), "Profile does not contain ConditionCacheFileHit")
    assertTrue(profileText.contains("ConditionCacheFilteredRows"), "Profile does not contain ConditionCacheFilteredRows")
    metrics = extractProfileBlockMetrics(profileText, "Scanner")
    logger.info("metrics = ${metrics}")
    assertEquals("1", metrics["ConditionCacheFileHit"])
    assertEquals("2.048K (2048)", metrics["ConditionCacheFilteredRows"])

    qt_condition_cache_verify_hit1 """
        SELECT id, name, age, score FROM ${largeTvf}
            WHERE id > 4980
        ORDER BY 1, 2, 3, 4;
    """

    uuid = UUID.randomUUID().toString()
    qt_condition_cache_verify_hit1_1 """
        SELECT id, name, age, score FROM (
            SELECT id, name, age, score, "${uuid}" FROM ${largeTvf}
            WHERE id > 4980
        ) tmpa
        ORDER BY 1, 2, 3, 4; """
    profileText = getProfileWithToken(uuid)
    assertTrue(profileText.contains("ConditionCacheFileHit"), "Profile does not contain ConditionCacheFileHit")
    assertTrue(profileText.contains("ConditionCacheFilteredRows"), "Profile does not contain ConditionCacheFilteredRows")
    metrics = extractProfileBlockMetrics(profileText, "Scanner")
    logger.info("metrics = ${metrics}")
    assertEquals("1", metrics["ConditionCacheFileHit"])
    assertEquals("4.096K (4096)", metrics["ConditionCacheFilteredRows"])

    // small split size to force more splits
    sql "set MAX_INITIAL_FILE_SPLIT_SIZE = 4 * 1024;"
    qt_condition_cache_verify_hit2 """
        SELECT id, name, age, score FROM ${largeTvf}
        WHERE id > 4981
        ORDER BY 1, 2, 3, 4;
    """
    uuid = UUID.randomUUID().toString()
    qt_condition_cache_verify_hit2_1 """
        SELECT id, name, age, score FROM (
            SELECT id, name, age, score, "${uuid}" FROM ${largeTvf}
            WHERE id > 4981
        ) tmpa
        ORDER BY 1, 2, 3, 4; """
    profileText = getProfileWithToken(uuid)
    assertTrue(profileText.contains("ConditionCacheFileHit"), "Profile does not contain ConditionCacheFileHit")
    assertTrue(profileText.contains("ConditionCacheFilteredRows"), "Profile does not contain ConditionCacheFilteredRows")
    metrics = extractProfileBlockMetrics(profileText, "Scanner")
    logger.info("metrics = ${metrics}")
    /*
|   0:VTVF_SCAN_NODE(52)                                                                                                                        |
|      table: null.null.LocalTableValuedFunction                                                                                                |
|      predicates: (_tvf_local.id[#0] > 2048)                                                                                                   |
|      inputSplitNum=3, totalFileSize=12682, scanRanges=3                                                                                       |
|      partition=0/0                                                                                                                            |
|      backends:                                                                                                                                |
|        1763953575995                                                                                                                          |
|          /tmp/test_condition_cache_orc/orc_large_6e4d2f6b6c524666-81f0d7ac43cf1110_0.orc start: 0 length: 4096                                |
|          /tmp/test_condition_cache_orc/orc_large_6e4d2f6b6c524666-81f0d7ac43cf1110_0.orc start: 4096 length: 4096                             |
|          /tmp/test_condition_cache_orc/orc_large_6e4d2f6b6c524666-81f0d7ac43cf1110_0.orc start: 8192 length: 4490                             |
|          dataFileNum=1, deleteFileNum=0, deleteSplitNum=0 
    */
    assertEquals("3", metrics["ConditionCacheFileHit"])
    assertEquals("4.096K (4096)", metrics["ConditionCacheFilteredRows"])

    // ---- Test 1: Basic predicate, no cache (baseline) ----
    sql "set enable_condition_cache=false"
    sql "set runtime_filter_type=0"

    qt_condition_cache0 """
        SELECT id, name, age, score FROM ${mainTvf}
        WHERE age > 25 AND score > 85 ORDER BY 1, 2, 3, 4;
    """

    qt_condition_cache1 """
        SELECT id, name, age, score FROM ${mainTvf}
        WHERE name LIKE 'A%' OR score < 80 ORDER BY 1, 2, 3, 4;
    """

    // ---- Test 2: Enable cache, same queries (MISS → populate) ----
    sql "set enable_condition_cache=true"

    qt_condition_cache2 """
        SELECT id, name, age, score FROM ${mainTvf}
        WHERE age > 25 AND score > 85 ORDER BY 1, 2, 3, 4;
    """

    qt_condition_cache3 """
        SELECT id, name, age, score FROM ${mainTvf}
        WHERE name LIKE 'A%' OR score < 80 ORDER BY 1, 2, 3, 4;
    """

    // ---- Test 3: Same queries again (HIT → verify) ----
    qt_condition_cache4 """
        SELECT id, name, age, score FROM ${mainTvf}
        WHERE age > 25 AND score > 85 ORDER BY 1, 2, 3, 4;
    """

    qt_condition_cache5 """
        SELECT id, name, age, score FROM ${mainTvf}
        WHERE name LIKE 'A%' OR score < 80 ORDER BY 1, 2, 3, 4;
    """

    // ---- Test 4: Join with bloom filter runtime filter ----
    // First, disable condition cache and reset runtime_filter
    sql "set enable_condition_cache=false"
    sql "set runtime_filter_type=2"

    // Run join query without condition cache
    qt_condition_cache_join0 """
        SELECT t1.id, t1.name, t1.age, t2.department, t2.position
        FROM ${mainTvf} t1
        JOIN ${joinTvf} t2 ON t1.id = t2.id
        WHERE t1.age > 25 AND t2.salary > 90000
        ORDER BY 1, 2, 3, 4, 5
    """
    // Bob(30,Marketing,120000), David(28,Engineering,140000), Eve(26,Finance,95000)

    // Enable condition cache with bloom filter runtime_filter
    sql "set enable_condition_cache=true"
    qt_condition_cache_join1 """
        SELECT t1.id, t1.name, t1.age, t2.department, t2.position
        FROM ${mainTvf} t1
        JOIN ${joinTvf} t2 ON t1.id = t2.id
        WHERE t1.age > 25 AND t2.salary > 90000
        ORDER BY 1, 2, 3, 4, 5
    """

    // Run again for cache hit
    qt_condition_cache_join2 """
        SELECT t1.id, t1.name, t1.age, t2.department, t2.position
        FROM ${mainTvf} t1
        JOIN ${joinTvf} t2 ON t1.id = t2.id
        WHERE t1.age > 25 AND t2.salary > 90000
        ORDER BY 1, 2, 3, 4, 5
    """

    // Run the same join query with condition cache enabled and expr in bloom filter 
    qt_condition_cache_join3 """
        SELECT t1.id, t1.name, t1.age, t2.department, t2.position
        FROM ${mainTvf} t1
        JOIN ${joinTvf} t2 ON t1.id = t2.id + 1
        ORDER BY 1, 2, 3, 4, 5
    """
    // Run the same join query with condition cache enabled and expr different in bloom filter
    qt_condition_cache_join4 """
        SELECT t1.id, t1.name, t1.age, t2.department, t2.position
        FROM ${mainTvf} t1
        JOIN ${joinTvf} t2 ON t1.id = t2.id + 1
        ORDER BY 1, 2, 3, 4, 5
    """

    // ---- Test 5: Join with different runtime_filter_type ----
    sql "set runtime_filter_type=12"

    // Run the same join query again after changing runtime_filter_type
    qt_condition_cache_join5 """
        SELECT t1.id, t1.name, t1.age, t2.department, t2.position
        FROM ${mainTvf} t1
        JOIN ${joinTvf} t2 ON t1.id = t2.id
        WHERE t1.age > 25 AND t2.salary > 90000
        ORDER BY 1, 2, 3, 4, 5
    """

    // ---- Test 6: Different join condition ----
    qt_condition_cache_join6 """
        SELECT t1.id, t1.name, t2.department, t2.salary
        FROM ${mainTvf} t1
        JOIN ${joinTvf} t2 ON t1.id = t2.id
        WHERE t1.score > 85 AND t2.department = 'Engineering'
        ORDER BY 1, 2, 3, 4
    """
    // id=1: Alice(85.5)+Engineering(100000), id=4: David(92)+Engineering(140000)

    // ---- Test 7: LIMIT with large file (verify incomplete cache not stored) ----
    // Large table has 5000 rows spanning multiple granules.
    // age > 25 matches ~4000 rows. LIMIT 10 causes early termination.
    // The incomplete cache must NOT be stored; otherwise the subsequent
    // full query would skip unread granules and return fewer rows.
    sql "set runtime_filter_type=0"

    qt_condition_cache_limit0 """
        SELECT id, name, age, score FROM ${largeTvf}
            WHERE id > 2048 and age > 48 and score > 81
        ORDER BY 1, 2, 3, 4 LIMIT 10
    """
    // assertTrue("${fmt}: limit query should return exactly 10 rows", limitResult.size() == 10)

    // Run LIMIT again — result must be identical
    qt_condition_cache_limit1 """
        SELECT id, name, age, score FROM ${largeTvf}
            WHERE id > 2048 and age > 48 and score > 81
        ORDER BY 1, 2, 3, 4 LIMIT 10
    """

    // Full query without LIMIT — must return ALL matching rows, not just
    // the granules that happened to be read during the LIMIT scan
    qt_condition_cache_no_limit0 """
        SELECT id, name, age, score FROM ${largeTvf}
            WHERE id > 2048 and age > 48 and score > 81
        ORDER BY 1, 2, 3, 4
    """
    // age = 20 + (number % 30), age > 45 means number % 30 in [6..29] → 24/30 of rows
    // assertTrue("${fmt}: full query should return ~4000 rows, got ${fullResult.size()}",
    //            fullResult.size() > 3900)

    // Run full query again (cache HIT from the full scan above)
    qt_condition_cache_no_limit1 """
        SELECT id, name, age, score FROM ${largeTvf}
            WHERE id > 2048 and age > 48 and score > 81
        ORDER BY 1, 2, 3, 4
    """
    sql "set enable_condition_cache=false"
    qt_condition_cache_no_limit_no_condition_cache """
        SELECT id, name, age, score FROM ${largeTvf}
        WHERE age > 48
        ORDER BY 1, 2, 3, 4
    """
    // assertEquals(fullResult, fullHit)

    // ============ Cleanup ============

    // sql """ DROP TABLE IF EXISTS ${srcTable} """
    // sql """ DROP TABLE IF EXISTS ${joinSrcTable} """
    // sql """ DROP TABLE IF EXISTS ${largeSrcTable} """
    // sshExec("root", be_host, "rm -rf ${basePath}", false)
}
