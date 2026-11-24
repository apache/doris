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

// The text indexes of a merge-on-write table record the rows written with the delete sign, and
// carry the record through compaction and BUILD INDEX. Without the record, BM25 statistics read the
// delete-sign column and give the same scores, so the scores alone cannot tell the record is
// there: here the column read fails through a debug point, and every case must score without it.
suite("test_bm25_score_delete_sign_statistics_recorded", "nonConcurrent") {
    def scores = { String table ->
        def rows = sql """
            SELECT id, score()
            FROM ${table}
            WHERE SEARCH('content:jobs OR content:try') ORDER BY score() DESC LIMIT 100000
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
    def createTable = { String table, String keys, String format, boolean withIndex ->
        def mergeOnWrite = keys.startsWith("UNIQUE") ? '"enable_unique_key_merge_on_write" = "true",' : ""
        def index = withIndex ? """, INDEX idx_content (content) USING INVERTED
                PROPERTIES("parser" = "english", "support_phrase" = "true")""" : ""
        sql "DROP TABLE IF EXISTS ${table}"
        sql """
            CREATE TABLE ${table} (
                id BIGINT,
                content TEXT NULL
                ${index}
            ) ENGINE=OLAP
            ${keys}
            DISTRIBUTED BY HASH(id) BUCKETS 2
            PROPERTIES (
                "replication_num" = "1",
                ${mergeOnWrite}
                "inverted_index_storage_format" = "${format}",
                "disable_auto_compaction" = "true"
            );
        """
    }
    // Rows, rows a later load overwrites, and rows deleted with the delete sign over two loads, so
    // a cumulative compaction merges segments that each record some.
    def load = { String table ->
        sql """
            INSERT INTO ${table}
            SELECT number, if(number % 9 = 0, NULL,
                              concat_ws(' ', if(number % 3 = 0, 'jobs', 'work'),
                                        if(number % 4 = 0, 'try', 'rest'),
                                        repeat('alpha ', cast(number % 5 AS INT))))
            FROM numbers('number' = '600')
        """
        sql "SET show_hidden_columns = true"
        sql """
            INSERT INTO ${table} (id, content, __DORIS_DELETE_SIGN__)
            SELECT number * 7 + 1, concat_ws(' ', 'jobs deleted', repeat('gamma ', cast(number % 3 AS INT))), 1
            FROM numbers('number' = '40')
        """
        sql "SET show_hidden_columns = false"
        sql """
            INSERT INTO ${table}
            SELECT number * 5, concat_ws(' ', 'jobs again', repeat('beta ', cast(number % 4 AS INT)))
            FROM numbers('number' = '100')
        """
        sql "SET show_hidden_columns = true"
        sql """
            INSERT INTO ${table} (id, content, __DORIS_DELETE_SIGN__)
            SELECT number * 7 + 281, concat_ws(' ', 'jobs deleted', repeat('try ', cast(number % 3 AS INT))), 1
            FROM numbers('number' = '40')
        """
        sql "SET show_hidden_columns = false"
        sql "DELETE FROM ${table} WHERE id % 11 = 2"
    }
    def waitBuildIndex = { String table ->
        for (int waited = 0; waited < 300; ++waited) {
            def jobs = sql """SHOW BUILD INDEX WHERE TableName = "${table}" ORDER BY JobId"""
            if (jobs.size() > 0) {
                def state = jobs[jobs.size() - 1][7]
                assertTrue(state != "CANCELLED", "BUILD INDEX on ${table} was cancelled: ${jobs}")
                if (state == "FINISHED") {
                    return
                }
            }
            sleep(1000)
        }
        assertTrue(false, "BUILD INDEX on ${table} did not finish")
    }
    def readsColumn = "CollectionStatistics.read_delete_sign_column"
    def olderBe = "VerticalSegmentWriter._records_delete_sign_rows.disable"

    for (format in ["V2", "V3", "SNII"]) {
        def suffix = format.toLowerCase()
        def mow = "test_bm25_score_delete_sign_recorded_mow_${suffix}"
        def visible = "test_bm25_score_delete_sign_recorded_visible_${suffix}"
        def older = "test_bm25_score_delete_sign_recorded_older_${suffix}"
        createTable(mow, "UNIQUE KEY(id)", format, true)
        createTable(visible, "DUPLICATE KEY(id)", format, true)
        createTable(older, "UNIQUE KEY(id)", format, true)
        load(mow)
        sql "INSERT INTO ${visible} SELECT id, content FROM ${mow}"
        // Written as an older BE writes them: the index records no rows.
        try {
            GetDebugPoint().enableDebugPointForAllBEs(olderBe)
            load(older)
        } finally {
            GetDebugPoint().disableDebugPointForAllBEs(olderBe)
        }
        sql "sync"

        try {
            GetDebugPoint().enableDebugPointForAllBEs(readsColumn)
            // The debug point fails a query whose statistics read the column.
            test {
                sql """
                    SELECT id, score() FROM ${older}
                    WHERE SEARCH('content:jobs OR content:try') ORDER BY score() DESC LIMIT 10
                """
                exception "BM25 statistics read the delete-sign column"
            }

            assertSameScores(scores(visible), scores(mow), "${format}, after the loads")
            // A threshold of 0 compresses the row-id conversion of every compaction that may
            // compress it, as a large one does, so SNII moves the record through it.
            setBeConfigTemporary([compressed_rowid_conversion_threshold_bytes: 0]) {
                trigger_and_wait_compaction(mow, "cumulative")
                assertSameScores(scores(visible), scores(mow), "${format}, after a cumulative compaction")
                trigger_and_wait_compaction(mow, "full")
                assertSameScores(scores(visible), scores(mow), "${format}, after a full compaction")
            }
        } finally {
            GetDebugPoint().disableDebugPointForAllBEs(readsColumn)
        }

        if (!isCloudMode()) {
            // The index is built on rows already loaded, which BUILD INDEX reads with their delete
            // sign.
            def built = "test_bm25_score_delete_sign_recorded_built_${suffix}"
            createTable(built, "UNIQUE KEY(id)", format, false)
            load(built)
            sql """
                ALTER TABLE ${built} ADD INDEX idx_content (content) USING INVERTED
                PROPERTIES("parser" = "english", "support_phrase" = "true")
            """
            sql "BUILD INDEX idx_content ON ${built}"
            waitBuildIndex(built)
            try {
                GetDebugPoint().enableDebugPointForAllBEs(readsColumn)
                assertSameScores(scores(visible), scores(built), "${format}, after BUILD INDEX")
            } finally {
                GetDebugPoint().disableDebugPointForAllBEs(readsColumn)
            }
        }
    }
}
