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

suite("test_score_filter_usage", "p0") {
    sql "DROP TABLE IF EXISTS test_score_filter_usage"
    sql """
        CREATE TABLE test_score_filter_usage (
            id INT,
            c1 TEXT,
            c2 TEXT,
            INDEX idx_c1 (c1) USING INVERTED PROPERTIES (
                "lower_case" = "true", "parser" = "english", "support_phrase" = "true")
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1", "inverted_index_storage_format" = "V2")
    """
    sql """
        INSERT INTO test_score_filter_usage VALUES
            (1, 'apple red', 'sweet'),
            (2, 'apple green', 'sour'),
            (3, 'banana yellow', 'sweet')
    """

    order_qt_where_only_score """
        SELECT id FROM test_score_filter_usage
        WHERE c1 MATCH 'apple' AND score() > 0.00001
    """

    order_qt_projected_score_range """
        SELECT id, score() AS s FROM test_score_filter_usage
        WHERE c1 MATCH 'apple' AND score() > 0.00001
    """

    order_qt_repeated_score_aliases """
        SELECT id, score() AS first_score, score() AS second_score
        FROM test_score_filter_usage
        WHERE c1 MATCH 'apple' AND score() > 0.00001
    """

    qt_ordinary_order_with_offset """
        SELECT id, c2, score() AS s FROM test_score_filter_usage
        WHERE c1 MATCH 'apple' AND score() > 0.00001
        ORDER BY id LIMIT 1 OFFSET 1
    """

    qt_multiple_order_keys """
        SELECT id, c2, score() AS s FROM test_score_filter_usage
        WHERE c1 MATCH 'apple' AND score() > 0.00001
        ORDER BY s DESC, id DESC LIMIT 1 OFFSET 1
    """

    qt_reverse_key_order """
        SELECT id, c2, score() AS s FROM test_score_filter_usage
        WHERE c1 MATCH 'apple' AND score() > 0.00001
        ORDER BY id DESC LIMIT 1
    """

    order_qt_score_sort_without_limit """
        SELECT id, c2, score() AS s FROM test_score_filter_usage
        WHERE c1 MATCH 'apple' AND score() > 0.00001
        ORDER BY s DESC
    """

    test {
        sql """
            SELECT id, score() AS s FROM test_score_filter_usage
            WHERE c1 MATCH 'apple' AND score() > 0.00001
            ORDER BY id LIMIT 9223372036854775807 OFFSET 9223372036854775807
        """
        exception "limit + offset overflows the long range"
    }

    test {
        sql """
            SELECT id FROM test_score_filter_usage
            WHERE c1 MATCH 'apple' AND CAST(score() AS INT) > 0
        """
        exception "score() predicate in WHERE clause must be in the form"
    }
}
