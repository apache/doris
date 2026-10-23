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

import java.util.concurrent.TimeUnit
import org.awaitility.Awaitility

// A merge-on-write replica keeps overwritten rows and rows written with the delete sign in its
// segments until it compacts them, and each replica compacts on its own schedule. BM25 statistics
// must count only the rows a query sees, so a row scores the same on every replica, before and
// after compaction, and the same as in a table freshly loaded with the visible rows.
suite("test_bm25_score_replica_statistics", "p0") {
    def aliveBackends = sql_return_maparray("SHOW BACKENDS").findAll { it.Alive.toString() == "true" }
    def replicaNum = aliveBackends.size() >= 3 ? 3 : 1

    def createTable = { String name, String keys, int replicas ->
        sql "DROP TABLE IF EXISTS ${name}"
        def mergeOnWrite = keys.startsWith("UNIQUE") ? '"enable_unique_key_merge_on_write" = "true",' : ""
        sql """
            CREATE TABLE ${name} (
                id BIGINT,
                content TEXT NULL,
                INDEX idx_content (content) USING INVERTED
                    PROPERTIES("parser" = "english", "support_phrase" = "true")
            ) ENGINE=OLAP
            ${keys}
            DISTRIBUTED BY HASH(id) BUCKETS 4
            PROPERTIES (
                "replication_num" = "${replicas}",
                ${mergeOnWrite}
                "disable_auto_compaction" = "true"
            );
        """
    }
    createTable("test_bm25_score_replica_statistics", "UNIQUE KEY(id)", replicaNum)
    createTable("test_bm25_score_replica_statistics_fresh", "UNIQUE KEY(id)", 1)

    // Documents of different lengths, some longer than the lengths norms hold exactly, and NULLs.
    def text = { int round ->
        """if((number + ${round}) % 7 = 0, NULL, concat_ws(' ',
               repeat('alpha beta gamma ', cast((number * 7 + ${round} * 13) % 15 AS INT)),
               if((number + ${round}) % 3 = 0, 'jobs', 'work'),
               if((number * 5 + ${round}) % 4 = 0, 'try hard', 'rest'),
               repeat('delta ', cast((number + ${round}) % 5 AS INT))))"""
    }
    for (int batch = 0; batch < 6; batch++) {
        sql "INSERT INTO test_bm25_score_replica_statistics SELECT number + ${batch} * 500, ${text(batch)} FROM numbers('number' = '500')"
    }
    // Overwrite a quarter of the rows three times.
    for (int round = 1; round <= 3; round++) {
        sql "INSERT INTO test_bm25_score_replica_statistics SELECT number * 4 + ${round}, ${text(round + 20)} FROM numbers('number' = '750')"
    }
    // Delete rows with DELETE, and with full rows carrying the delete sign, as CDC loads write them.
    sql "DELETE FROM test_bm25_score_replica_statistics WHERE id % 17 = 3"
    sql "SET show_hidden_columns = true"
    sql """
        INSERT INTO test_bm25_score_replica_statistics (id, content, __DORIS_DELETE_SIGN__)
        SELECT number * 13 + 5, ${text(40)}, 1 FROM numbers('number' = '200')
    """
    sql "SET show_hidden_columns = false"
    sql "INSERT INTO test_bm25_score_replica_statistics_fresh SELECT id, content FROM test_bm25_score_replica_statistics"
    sql "sync"

    def scores = { String table, String vars ->
        def rows = sql """
            SELECT /*+ SET_VAR(${vars}) */ id, score()
            FROM ${table}
            WHERE SEARCH('content:jobs OR content:try')
            ORDER BY score() DESC
            LIMIT 100000
        """
        def byId = [:]
        rows.each { byId[it[0]] = it[1] }
        return byId
    }
    def assertSameScores = { Map expected, Map actual, String state ->
        assertTrue(expected.size() > 0)
        assertEquals(expected.size(), actual.size())
        def differing = expected.findAll { id, score -> actual[id] != score }
        assertTrue(differing.isEmpty(),
                "${state}: ${differing.size()} rows score differently, "
                        + "e.g. ${differing.take(3).collect { id, score -> "id ${id}: ${actual[id]} vs ${score}" }}")
    }

    def expected = scores("test_bm25_score_replica_statistics_fresh", "use_fix_replica = -1")
    def assertScoresLikeFreshTable = { String vars, String state ->
        assertSameScores(expected, scores("test_bm25_score_replica_statistics", vars),
                "${state} (${vars}), compared with a freshly loaded table")
    }

    assertScoresLikeFreshTable("use_fix_replica = -1", "before compaction")

    // Cloud mode keeps one shared copy of each tablet, so there are no replicas to compact apart,
    // and its compaction is driven differently.
    if (!isCloudMode() && replicaNum == 3) {
        // Compact the replicas on one backend only, so the replicas hold different rows.
        def backendIdToIp = [:]
        def backendIdToHttpPort = [:]
        getBackendIpHttpPort(backendIdToIp, backendIdToHttpPort)
        def tablets = sql_return_maparray("SHOW TABLETS FROM test_bm25_score_replica_statistics")
        def compactedBackend = tablets[0].BackendId
        def compactedTablets = tablets.findAll { it.BackendId == compactedBackend }
        for (tablet in compactedTablets) {
            def (code, out, err) = be_run_full_compaction(backendIdToIp["${compactedBackend}"],
                    backendIdToHttpPort["${compactedBackend}"], tablet.TabletId)
            assertEquals(0, code)
            assertTrue(out.toLowerCase().contains("success"), out)
        }
        Awaitility.await().atMost(300, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS).until {
            compactedTablets.every { tablet ->
                def (code, out, err) = be_show_tablet_status(backendIdToIp["${compactedBackend}"],
                        backendIdToHttpPort["${compactedBackend}"], tablet.TabletId)
                code == 0 && parseJson(out.trim()).rowsets.size() == 1
            }
        }

        for (int run = 0; run < 10; run++) {
            assertScoresLikeFreshTable("use_fix_replica = -1", "one replica compacted, run ${run}")
        }
        for (int replica = 0; replica < 3; replica++) {
            assertScoresLikeFreshTable("use_fix_replica = ${replica}", "one replica compacted")
        }
    }

    if (!isCloudMode()) {
        trigger_and_wait_compaction("test_bm25_score_replica_statistics", "full")
        for (int replica = 0; replica < replicaNum; replica++) {
            assertScoresLikeFreshTable("use_fix_replica = ${replica}", "all replicas compacted")
        }
    }

    // NULL rows have no tokens, on a merge-on-write table as on a duplicate table: with lengths the
    // norms hold exactly, both score alike.
    createTable("test_bm25_score_replica_statistics_nulls_mow", "UNIQUE KEY(id)", 1)
    createTable("test_bm25_score_replica_statistics_nulls_dup", "DUPLICATE KEY(id)", 1)
    for (table in ["test_bm25_score_replica_statistics_nulls_mow", "test_bm25_score_replica_statistics_nulls_dup"]) {
        sql """
            INSERT INTO ${table}
            SELECT number, if(number % 2 = 0, NULL,
                              concat_ws(' ', if(number % 4 = 1, 'jobs', 'work'), repeat('alpha ', cast(number % 6 AS INT))))
            FROM numbers('number' = '1000')
        """
    }
    sql "sync"
    assertSameScores(scores("test_bm25_score_replica_statistics_nulls_dup", "use_fix_replica = -1"),
            scores("test_bm25_score_replica_statistics_nulls_mow", "use_fix_replica = -1"),
            "merge-on-write table with NULL rows, compared with a duplicate table")

    // A bucket whose rows are all deleted still has segments but no row a query can match. Scoring
    // must return nothing for it instead of failing.
    createTable("test_bm25_score_replica_statistics_emptied", "UNIQUE KEY(id)", 1)
    sql "INSERT INTO test_bm25_score_replica_statistics_emptied VALUES (1, 'we should try these jobs'), (2, 'more jobs'), (3, 'other')"
    sql "DELETE FROM test_bm25_score_replica_statistics_emptied WHERE id > 0"
    order_qt_emptied """
        SELECT id, score() FROM test_bm25_score_replica_statistics_emptied
        WHERE SEARCH('content:jobs') ORDER BY score() DESC LIMIT 10
    """
    sql "INSERT INTO test_bm25_score_replica_statistics_emptied VALUES (4, 'jobs again')"
    order_qt_refilled """
        SELECT id, score() FROM test_bm25_score_replica_statistics_emptied
        WHERE SEARCH('content:jobs') ORDER BY score() DESC LIMIT 10
    """
}
