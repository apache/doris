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

suite("test_search_score_raw_index", "p0") {
    sql "DROP TABLE IF EXISTS test_search_score_raw_index"
    sql """
        CREATE TABLE test_search_score_raw_index (
            id INT NOT NULL,
            raw_index_content STRING NULL,
            INDEX idx_raw_content(raw_index_content) USING INVERTED
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default:1")
    """

    sql """INSERT INTO test_search_score_raw_index VALUES
        (1, 'apple'),
        (2, 'banana'),
        (3, 'apple'),
        (4, 'orange'),
        (5, 'apple banana')
    """
    sql "sync"

    sql """
        SELECT id FROM test_search_score_raw_index
        WHERE search('raw_index_content:"apple"',
                     '{"default_operator":"AND","mode":"lucene","minimum_should_match":0}')
        ORDER BY id
    """

    test {
        sql """
            SELECT id, score() AS bm25_score FROM test_search_score_raw_index
            WHERE search('raw_index_content:"apple"',
                         '{"default_operator":"AND","mode":"lucene","minimum_should_match":0}')
            ORDER BY bm25_score DESC LIMIT 100
        """
        exception "BM25 scoring requires an analyzed inverted index"
    }

    test {
        sql """
            SELECT /*+SET_VAR(enable_inverted_index_wand_query=false) */ id, score() AS bm25_score
            FROM test_search_score_raw_index
            WHERE search('raw_index_content:"apple"',
                         '{"default_operator":"AND","mode":"lucene","minimum_should_match":0}')
            ORDER BY bm25_score DESC LIMIT 100
        """
        exception "BM25 scoring requires an analyzed inverted index"
    }

    test {
        sql """
            SELECT id, score() AS bm25_score FROM test_search_score_raw_index
            WHERE search('raw_index_content:"apple"',
                         '{"default_operator":"AND","mode":"lucene","minimum_should_match":0}')
            ORDER BY bm25_score ASC LIMIT 100
        """
        exception "BM25 scoring requires an analyzed inverted index"
    }
}
