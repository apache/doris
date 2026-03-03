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

suite("condition_cache_orc", "tvf,external,external_docker") {

    List<List<Object>> backends = sql """ show backends """
    assertTrue(backends.size() > 0)
    def be_id = backends[0][0]
    def be_host = backends[0][1]
    def basePath = "/tmp/test_condition_cache_orc"

    sshExec("root", be_host, "rm -rf ${basePath}", false)
    sshExec("root", be_host, "mkdir -p ${basePath}")
    sshExec("root", be_host, "chmod 777 ${basePath}")

    // ============ Source tables ============

    def srcTable = "cc_ext_src"
    def joinSrcTable = "cc_ext_join_src"
    def largeSrcTable = "cc_ext_large_src"

    sql """ DROP TABLE IF EXISTS ${srcTable} """
    sql """
        CREATE TABLE ${srcTable} (
            `id` int NULL,
            `name` varchar(50) NULL,
            `age` int NULL,
            `score` double NULL
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
            `salary` double NULL
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
            `score` double NULL
        ) DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """

    sql """
        INSERT INTO ${largeSrcTable}
        SELECT
            cast(number AS INT),
            concat('name_', cast(number AS VARCHAR(20))),
            cast(20 + (number % 30) AS INT),
            cast(50.0 + (number % 50) AS DOUBLE)
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
        WHERE age > 25
        ORDER BY 1, 2, 3, 4 LIMIT 10
    """
    // assertTrue("${fmt}: limit query should return exactly 10 rows", limitResult.size() == 10)

    // Run LIMIT again — result must be identical
    qt_condition_cache_limit1 """
        SELECT id, name, age, score FROM ${largeTvf}
        WHERE age > 25
        ORDER BY 1, 2, 3, 4 LIMIT 10
    """

    // Full query without LIMIT — must return ALL matching rows, not just
    // the granules that happened to be read during the LIMIT scan
    qt_condition_cache_limit2 """
        SELECT id, name, age, score FROM ${largeTvf}
        WHERE age > 25
        ORDER BY 1, 2, 3, 4
    """
    // age = 20 + (number % 30), age > 25 means number % 30 in [6..29] → 24/30 of rows
    // assertTrue("${fmt}: full query should return ~4000 rows, got ${fullResult.size()}",
    //            fullResult.size() > 3900)

    // Run full query again (cache HIT from the full scan above)
    qt_condition_cache_limit3 """
        SELECT id, name, age, score FROM ${largeTvf}
        WHERE age > 25
        ORDER BY 1, 2, 3, 4
    """
    // assertEquals(fullResult, fullHit)

    // ============ Cleanup ============

    // sql """ DROP TABLE IF EXISTS ${srcTable} """
    // sql """ DROP TABLE IF EXISTS ${joinSrcTable} """
    // sql """ DROP TABLE IF EXISTS ${largeSrcTable} """
    // sshExec("root", be_host, "rm -rf ${basePath}", false)
}
