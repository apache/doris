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

// A TopN query fetches a column read only by search() or MATCH above the TopN, for the rows that survive it:
// the scan answers the predicate from the inverted index and does not read the column.
suite("test_topn_lazy_index_predicate_slots", "nonConcurrent") {
    sql "DROP TABLE IF EXISTS topn_lazy_index_predicate_slots"
    sql """
        CREATE TABLE topn_lazy_index_predicate_slots (
            `id` INT NOT NULL,
            `body` STRING NULL,
            `level` VARCHAR(16) NULL,
            INDEX idx_body (`body`) USING INVERTED PROPERTIES("parser" = "english", "support_phrase" = "true")
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "disable_auto_compaction" = "true"
        )
    """
    sql """
        INSERT INTO topn_lazy_index_predicate_slots VALUES
            (1, 'disk error on node a', 'ERROR'),
            (2, 'request served', 'INFO'),
            (3, 'error while reading block', 'ERROR'),
            (4, 'cache miss', 'WARN'),
            (5, 'timeout error from upstream', 'ERROR'),
            (6, NULL, 'INFO'),
            (7, 'error error error', 'ERROR')
    """
    sql "sync"

    // Only search() or MATCH reads body, so body is fetched lazily with level.
    qt_search_shape """
        explain shape plan
        select id, body, level from topn_lazy_index_predicate_slots
        where search('body:error') order by id desc limit 2
    """
    qt_search """
        select id, body, level from topn_lazy_index_predicate_slots
        where search('body:error') order by id desc limit 2
    """
    qt_match_shape """
        explain shape plan
        select id, body, level from topn_lazy_index_predicate_slots
        where body match_any 'error' order by id limit 2
    """
    qt_match """
        select id, body, level from topn_lazy_index_predicate_slots
        where body match_any 'error' order by id limit 2
    """
    qt_or_shape """
        explain shape plan
        select id, body, level from topn_lazy_index_predicate_slots
        where (body match_all 'disk node' or body match_any 'timeout') order by id limit 2
    """
    qt_or """
        select id, body, level from topn_lazy_index_predicate_slots
        where (body match_all 'disk node' or body match_any 'timeout') order by id limit 2
    """
    qt_alias """
        select id, body as b from topn_lazy_index_predicate_slots
        where search('body:error') and level = 'ERROR' order by id limit 3
    """
    // LIKE reads body row by row, so the scan reads body and keeps it.
    qt_like_shape """
        explain shape plan
        select id, body, level from topn_lazy_index_predicate_slots
        where search('body:error') and body like '%block%' order by id limit 2
    """
    qt_like """
        select id, body, level from topn_lazy_index_predicate_slots
        where search('body:error') and body like '%block%' order by id limit 2
    """

    try {
        // Any read of body during the scan fails the query.
        GetDebugPoint().enableDebugPointForAllBEs("segment_iterator._read_columns_by_index", [column_name: "body"])
        qt_search_without_reading_body """
            select id, body, level from topn_lazy_index_predicate_slots
            where search('body:error') order by id desc limit 2
        """
        qt_match_without_reading_body """
            select id, body, level from topn_lazy_index_predicate_slots
            where body match_any 'error' order by id limit 2
        """
        // Without lazy materialization the scan reads body for the output.
        test {
            sql """
                select /*+ SET_VAR(topn_lazy_materialization_threshold=0) */ id, body, level
                from topn_lazy_index_predicate_slots where search('body:error') order by id desc limit 2
            """
            exception "does not need to read data"
        }
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("segment_iterator._read_columns_by_index")
    }

    // Where a segment has no index, MATCH falls back to row-by-row evaluation and the scan reads body for it. Here the
    // first segment is written before the index is added for new data only, the second one after.
    sql "DROP TABLE IF EXISTS topn_lazy_index_predicate_slots_fallback"
    sql """
        CREATE TABLE topn_lazy_index_predicate_slots_fallback (
            `id` INT NOT NULL,
            `body` STRING NULL,
            `level` VARCHAR(16) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "disable_auto_compaction" = "true"
        )
    """
    sql """
        INSERT INTO topn_lazy_index_predicate_slots_fallback VALUES
            (1, 'disk error on node a', 'ERROR'),
            (2, 'request served', 'INFO'),
            (3, 'error while reading block', 'ERROR')
    """
    sql "SET enable_add_index_for_new_data = true"
    sql """
        ALTER TABLE topn_lazy_index_predicate_slots_fallback
        ADD INDEX idx_body (`body`) USING INVERTED PROPERTIES("parser" = "english")
    """
    wait_for_last_schema_change_finish("topn_lazy_index_predicate_slots_fallback", 60000)
    sql """
        INSERT INTO topn_lazy_index_predicate_slots_fallback VALUES
            (4, 'cache miss', 'WARN'),
            (5, 'timeout error from upstream', 'ERROR'),
            (6, NULL, 'INFO'),
            (7, 'error error error', 'ERROR')
    """
    sql "sync"

    // The plan still fetches body after the TopN; rows 1 and 3 match row by row, row 5 through the index.
    qt_fallback_shape """
        explain shape plan
        select id, body, level from topn_lazy_index_predicate_slots_fallback
        where body match_any 'error' order by id limit 3
    """
    qt_fallback_match """
        select id, body, level from topn_lazy_index_predicate_slots_fallback
        where body match_any 'error' order by id limit 3
    """
    qt_fallback_match_without_lazy_materialization """
        select /*+ SET_VAR(topn_lazy_materialization_threshold=0) */ id, body, level
        from topn_lazy_index_predicate_slots_fallback where body match_any 'error' order by id limit 3
    """
    // With inverted index queries off, every segment evaluates MATCH row by row.
    qt_match_without_index """
        select /*+ SET_VAR(enable_inverted_index_query=false) */ id, body, level
        from topn_lazy_index_predicate_slots where body match_any 'error' order by id limit 2
    """
}
