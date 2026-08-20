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

suite("test_search_score_residual_predicate", "p0") {
    sql "DROP TABLE IF EXISTS search_score_residual_predicate"
    sql """
        CREATE TABLE search_score_residual_predicate (
            id INT,
            title VARCHAR(200),
            status INT REPLACE,
            INDEX idx_title(title) USING INVERTED PROPERTIES(
                "parser" = "english",
                "support_phrase" = "true"
            )
        ) ENGINE=OLAP
        AGGREGATE KEY(id, title)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """

    sql """INSERT INTO search_score_residual_predicate VALUES
        (1, 'needle needle needle needle needle', 0),
        (2, 'needle needle needle needle', 0),
        (3, 'needle needle needle', 1),
        (4, 'needle needle', 1),
        (5, 'needle', 1)
    """
    sql "sync"

    sql "set enable_common_expr_pushdown = true"
    sql "set enable_inverted_index_query = true"
    sql "set enable_inverted_index_wand_query = true"

    qt_match_any_score_residual """
        SELECT id
        FROM (
            SELECT id, score() AS s
            FROM search_score_residual_predicate
            WHERE title MATCH_ANY 'needle' AND abs(status) = 1
            ORDER BY s DESC
            LIMIT 2
        ) t
        ORDER BY id
    """

    qt_search_dsl_score_residual """
        SELECT id
        FROM (
            SELECT id, score() AS s
            FROM search_score_residual_predicate
            WHERE search('title:needle') AND abs(status) = 1
            ORDER BY s DESC
            LIMIT 2
        ) t
        ORDER BY id
    """

    qt_search_dsl_score_key_range """
        SELECT id
        FROM (
            SELECT id, score() AS s
            FROM search_score_residual_predicate
            WHERE search('title:needle') AND id > 2
            ORDER BY s DESC
            LIMIT 2
        ) t
        ORDER BY id
    """

    sql "DROP TABLE IF EXISTS search_score_common_expr_predicate"
    sql """
        CREATE TABLE search_score_common_expr_predicate (
            id INT,
            title VARCHAR(200),
            INDEX idx_title(title) USING INVERTED PROPERTIES(
                "parser" = "english",
                "support_phrase" = "true"
            )
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """

    sql """INSERT INTO search_score_common_expr_predicate VALUES
        (1, 'needle needle needle needle needle'),
        (2, 'needle needle needle needle'),
        (3, 'needle needle needle'),
        (4, 'needle needle'),
        (5, 'needle')
    """
    sql "sync"

    qt_search_dsl_score_common_expr """
        SELECT id
        FROM (
            SELECT id, score() AS s
            FROM search_score_common_expr_predicate
            WHERE search('title:needle') AND abs(id) > 2
            ORDER BY s DESC
            LIMIT 2
        ) t
        ORDER BY id
    """

    sql "DROP TABLE IF EXISTS search_score_delete_predicate"
    sql """
        CREATE TABLE search_score_delete_predicate (
            id INT,
            title VARCHAR(200),
            status INT,
            INDEX idx_title(title) USING INVERTED PROPERTIES(
                "parser" = "english",
                "support_phrase" = "true"
            )
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """

    sql """INSERT INTO search_score_delete_predicate VALUES
        (1, 'needle needle needle needle needle', 0),
        (2, 'needle needle needle needle', 0),
        (3, 'needle needle needle', 1),
        (4, 'needle needle', 1),
        (5, 'needle', 1)
    """
    sql "sync"
    sql "DELETE FROM search_score_delete_predicate WHERE status = 0"
    sql "sync"

    qt_match_any_score_delete_predicate """
        SELECT id
        FROM (
            SELECT id, score() AS s
            FROM search_score_delete_predicate
            WHERE title MATCH_ANY 'needle'
            ORDER BY s DESC
            LIMIT 2
        ) t
        ORDER BY id
    """

    qt_search_dsl_score_delete_predicate """
        SELECT id
        FROM (
            SELECT id, score() AS s
            FROM search_score_delete_predicate
            WHERE search('title:needle')
            ORDER BY s DESC
            LIMIT 2
        ) t
        ORDER BY id
    """
}
