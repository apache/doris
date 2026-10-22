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
    def tableName = "test_bm25_score_replica_statistics"
    def freshTableName = "test_bm25_score_replica_statistics_fresh"

    def aliveBackends = sql_return_maparray("SHOW BACKENDS").findAll { it.Alive.toString() == "true" }
    def replicaNum = aliveBackends.size() >= 3 ? 3 : 1

    def createTable = { String name, int replicas ->
        sql "DROP TABLE IF EXISTS ${name}"
        sql """
            CREATE TABLE ${name} (
                id BIGINT,
                content TEXT,
                INDEX idx_content (content) USING INVERTED
                    PROPERTIES("parser" = "english", "support_phrase" = "true")
            ) ENGINE=OLAP
            UNIQUE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 4
            PROPERTIES (
                "replication_num" = "${replicas}",
                "enable_unique_key_merge_on_write" = "true",
                "disable_auto_compaction" = "true"
            );
        """
    }
    createTable(tableName, replicaNum)
    createTable(freshTableName, 1)

    // Documents of different lengths, some longer than the lengths norms hold exactly.
    def text = { int round ->
        """concat_ws(' ',
               repeat('alpha beta gamma ', cast((number * 7 + ${round} * 13) % 15 AS INT)),
               if((number + ${round}) % 3 = 0, 'jobs', 'work'),
               if((number * 5 + ${round}) % 4 = 0, 'try hard', 'rest'),
               repeat('delta ', cast((number + ${round}) % 5 AS INT)))"""
    }
    for (int batch = 0; batch < 6; batch++) {
        sql "INSERT INTO ${tableName} SELECT number + ${batch} * 500, ${text(batch)} FROM numbers('number' = '500')"
    }
    // Overwrite a quarter of the rows three times.
    for (int round = 1; round <= 3; round++) {
        sql "INSERT INTO ${tableName} SELECT number * 4 + ${round}, ${text(round + 20)} FROM numbers('number' = '750')"
    }
    // Delete rows with DELETE, and with full rows carrying the delete sign, as CDC loads write them.
    sql "DELETE FROM ${tableName} WHERE id % 17 = 3"
    sql "SET show_hidden_columns = true"
    sql """
        INSERT INTO ${tableName} (id, content, __DORIS_DELETE_SIGN__)
        SELECT number * 13 + 5, ${text(40)}, 1 FROM numbers('number' = '200')
    """
    sql "SET show_hidden_columns = false"
    sql "INSERT INTO ${freshTableName} SELECT id, content FROM ${tableName}"
    sql "sync"

    def scores = { String name, String vars ->
        def rows = sql """
            SELECT /*+ SET_VAR(${vars}) */ id, score()
            FROM ${name}
            WHERE SEARCH('content:jobs OR content:try')
            ORDER BY score() DESC
            LIMIT 100000
        """
        def byId = [:]
        rows.each { byId[it[0]] = it[1] }
        return byId
    }

    def expected = scores(freshTableName, "use_fix_replica = -1")
    assertTrue(expected.size() > 0)
    def assertScoresLikeFreshTable = { String vars, String state ->
        def actual = scores(tableName, vars)
        assertEquals(expected.size(), actual.size())
        def differing = expected.findAll { id, score -> actual[id] != score }
        assertTrue(differing.isEmpty(),
                "${state} (${vars}): ${differing.size()} rows score differently from a freshly loaded table, "
                        + "e.g. ${differing.take(3).collect { id, score -> "id ${id}: ${actual[id]} vs ${score}" }}")
    }

    assertScoresLikeFreshTable("use_fix_replica = -1", "before compaction")

    // Cloud mode keeps one shared copy of each tablet, so there are no replicas to compact apart,
    // and its compaction is driven differently.
    if (!isCloudMode() && replicaNum == 3) {
        // Compact the replicas on one backend only, so the replicas hold different rows.
        def backendIdToIp = [:]
        def backendIdToHttpPort = [:]
        getBackendIpHttpPort(backendIdToIp, backendIdToHttpPort)
        def tablets = sql_return_maparray("SHOW TABLETS FROM ${tableName}")
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
        trigger_and_wait_compaction(tableName, "full")
        for (int replica = 0; replica < replicaNum; replica++) {
            assertScoresLikeFreshTable("use_fix_replica = ${replica}", "all replicas compacted")
        }
    }

    sql "DROP TABLE IF EXISTS ${tableName}"
    sql "DROP TABLE IF EXISTS ${freshTableName}"
}
