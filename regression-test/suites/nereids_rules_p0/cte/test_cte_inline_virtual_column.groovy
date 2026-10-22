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

// The score and ANN TopN and the MATCH projection add virtual columns to the scan of a CTE,
// which is copied when the optimizer inlines the CTE.
suite("test_cte_inline_virtual_column") {
    sql "DROP TABLE IF EXISTS test_cte_inline_virtual_column_text"
    sql """
        CREATE TABLE test_cte_inline_virtual_column_text (
            id INT,
            c1 TEXT,
            INDEX idx_c1 (c1) USING INVERTED PROPERTIES ("parser" = "english")
        ) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1", "inverted_index_storage_format" = "V3")
    """
    sql """
        INSERT INTO test_cte_inline_virtual_column_text VALUES
            (1, 'apple apple apple'), (2, 'apple banana'), (3, 'banana cherry')
    """

    sql "DROP TABLE IF EXISTS test_cte_inline_virtual_column_ann"
    sql """
        CREATE TABLE test_cte_inline_virtual_column_ann (
            id INT NOT NULL,
            embedding ARRAY<FLOAT> NOT NULL,
            INDEX idx_ann (embedding) USING ANN PROPERTIES (
                "index_type" = "hnsw", "metric_type" = "l2_distance", "dim" = "2")
        ) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO test_cte_inline_virtual_column_ann VALUES
            (1, [1.0, 2.0]), (2, [2.0, 3.0]), (3, [10.0, 10.0])
    """

    order_qt_score_topn """
        WITH c AS (
            SELECT id, score() AS s FROM test_cte_inline_virtual_column_text
            WHERE c1 MATCH 'apple' ORDER BY s DESC LIMIT 2
        )
        SELECT * FROM c
    """
    order_qt_ann_topn """
        WITH c AS (
            SELECT id FROM test_cte_inline_virtual_column_ann
            ORDER BY l2_distance_approximate(embedding, [1.0, 2.0]) LIMIT 2
        )
        SELECT * FROM c
    """
    order_qt_match_projection """
        WITH c AS (SELECT id, c1 MATCH 'apple' AS m FROM test_cte_inline_virtual_column_text)
        SELECT * FROM c UNION ALL SELECT * FROM c WHERE 1 = 0
    """
}
