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

// BM25 statistics open the CLucene searcher of a segment the searcher cache does not hold yet.
// Once its meta is read, the searcher's stream must drop the opening query's I/O context and stop
// reading as index meta, as the query path's searcher does: CSIndexInput.readInternal fails any
// postings read that still reads as meta.
suite("test_bm25_score_statistics_searcher_io_context", "nonConcurrent") {
    sql "DROP TABLE IF EXISTS test_bm25_score_statistics_searcher_io_context"
    sql """
        CREATE TABLE test_bm25_score_statistics_searcher_io_context (
            id BIGINT,
            content TEXT NULL,
            INDEX idx_content (content) USING INVERTED PROPERTIES("parser" = "english")
        ) ENGINE=OLAP
        UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "disable_auto_compaction" = "true"
        );
    """
    sql """
        INSERT INTO test_bm25_score_statistics_searcher_io_context
        SELECT number, concat_ws(' ', if(number % 4 = 1, 'jobs', 'work'), 'alpha') FROM numbers('number' = '200')
    """
    // Overwritten rows make the statistics read postings of the hidden rows.
    sql """
        INSERT INTO test_bm25_score_statistics_searcher_io_context
        SELECT number * 2, 'jobs again' FROM numbers('number' = '50')
    """
    sql "sync"

    try {
        GetDebugPoint().enableDebugPointForAllBEs("CSIndexInput.readInternal")
        order_qt_scores """
            SELECT id, score() FROM test_bm25_score_statistics_searcher_io_context
            WHERE content MATCH_ANY 'jobs' ORDER BY score() DESC LIMIT 1000
        """
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("CSIndexInput.readInternal")
    }
}
