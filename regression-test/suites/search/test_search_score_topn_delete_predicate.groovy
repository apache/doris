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

suite("test_search_score_topn_delete_predicate", "p0") {
    def insertRows = { tableName, rows ->
        sql "INSERT INTO ${tableName} VALUES ${rows}"
        sql "SYNC"
    }

    def assertScoreTopN = { tableName, predicate, expectedId ->
        assertEquals(expectedId, sql("""
        SELECT id
        FROM ${tableName}
        WHERE ${predicate}
        ORDER BY score() DESC
        LIMIT 1
        """)[0][0] as int)
    }

    sql "DROP TABLE IF EXISTS test_search_score_topn_delete_predicate"
    sql """
        CREATE TABLE test_search_score_topn_delete_predicate (
            id INT,
            title TEXT,
            INDEX idx_title (title) USING INVERTED PROPERTIES("parser" = "english")
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "disable_auto_compaction" = "true"
        )
    """
    insertRows("test_search_score_topn_delete_predicate",
            "(1, 'alpha alpha alpha alpha alpha alpha'), (2, 'alpha')")
    assertScoreTopN("test_search_score_topn_delete_predicate", "SEARCH('title:alpha')", 1)
    assertScoreTopN("test_search_score_topn_delete_predicate", "title MATCH_ANY 'alpha'", 1)

    sql "DELETE FROM test_search_score_topn_delete_predicate WHERE id = 1"

    assertScoreTopN("test_search_score_topn_delete_predicate", "SEARCH('title:alpha')", 2)
    assertScoreTopN("test_search_score_topn_delete_predicate", "title MATCH_ANY 'alpha'", 2)

    // A delete predicate only applies to data rowsets older than the delete rowset.
    sql "INSERT INTO test_search_score_topn_delete_predicate VALUES (1, 'beta beta')"
    sql "SYNC"

    assertScoreTopN("test_search_score_topn_delete_predicate", "title MATCH_ANY 'beta'", 1)

    sql "DROP TABLE IF EXISTS test_search_score_topn_delete_mow_light"
    sql """
        CREATE TABLE test_search_score_topn_delete_mow_light (
            id INT,
            title TEXT,
            INDEX idx_title (title) USING INVERTED PROPERTIES("parser" = "english")
        ) ENGINE=OLAP
        UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "disable_auto_compaction" = "true",
            "enable_unique_key_merge_on_write" = "true",
            "enable_mow_light_delete" = "true"
        )
    """
    insertRows("test_search_score_topn_delete_mow_light",
            "(1, 'alpha alpha alpha alpha alpha alpha'), (2, 'alpha')")
    assertScoreTopN("test_search_score_topn_delete_mow_light", "SEARCH('title:alpha')", 1)

    sql "DELETE FROM test_search_score_topn_delete_mow_light WHERE id = 1"

    assertScoreTopN("test_search_score_topn_delete_mow_light", "SEARCH('title:alpha')", 2)

    sql "DROP TABLE IF EXISTS test_search_score_topn_delete_bitmap"
    sql """
        CREATE TABLE test_search_score_topn_delete_bitmap (
            id INT,
            title TEXT,
            INDEX idx_title (title) USING INVERTED PROPERTIES("parser" = "english")
        ) ENGINE=OLAP
        UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "disable_auto_compaction" = "true",
            "enable_unique_key_merge_on_write" = "true",
            "enable_mow_light_delete" = "false"
        )
    """
    insertRows("test_search_score_topn_delete_bitmap",
            "(1, 'alpha alpha alpha alpha alpha alpha'), (2, 'alpha')")
    assertScoreTopN("test_search_score_topn_delete_bitmap", "SEARCH('title:alpha')", 1)

    sql "DELETE FROM test_search_score_topn_delete_bitmap WHERE id = 1"

    assertScoreTopN("test_search_score_topn_delete_bitmap", "SEARCH('title:alpha')", 2)
}
