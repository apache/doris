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

suite("test_regexp_extended_nonconstant_pattern") {
    sql "DROP TABLE IF EXISTS test_regexp_extended_nonconstant_pattern"
    sql """
        CREATE TABLE test_regexp_extended_nonconstant_pattern (
            id INT,
            s VARCHAR(32),
            p VARCHAR(64)
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO test_regexp_extended_nonconstant_pattern VALUES
            (1, 'foobar', '(?<=foo)bar'),
            (2, 'fobar', '(?<=foo)bar'),
            (3, 'foo123bar', 'foo(?=123)'),
            (4, 'foo124bar', 'foo(?=123)'),
            (5, 'foobar', '^foo'),
            (6, NULL, 'foo(?=123)'),
            (7, 'foobar', NULL)
    """

    sql "SET enable_extended_regex = true"
    // Lookaround patterns work the same whether they are constants or read from a column.
    order_qt_constant_pattern """
        SELECT id, s, s REGEXP '(?<=foo)bar', REGEXP(s, 'foo(?=123)')
        FROM test_regexp_extended_nonconstant_pattern
    """
    order_qt_column_pattern """
        SELECT id, s, p, s REGEXP p, REGEXP(s, p), s RLIKE p
        FROM test_regexp_extended_nonconstant_pattern
    """
    order_qt_column_pattern_filter """
        SELECT id FROM test_regexp_extended_nonconstant_pattern WHERE s REGEXP p
    """
    order_qt_column_pattern_not """
        SELECT id FROM test_regexp_extended_nonconstant_pattern WHERE NOT s REGEXP p
    """
    // A column pattern that Boost.Regex rejects as well ('(' from substr) is still an error.
    test {
        sql """
            SELECT id, s REGEXP substr(p, 1, 1) FROM test_regexp_extended_nonconstant_pattern
            WHERE id = 1
        """
        exception "Invalid regex expression: ("
    }
    // Boost.Regex exhausts its backtracking budget on this column pattern; the query must fail
    // with an error instead of crashing the backend.
    test {
        sql """
            SELECT id FROM test_regexp_extended_nonconstant_pattern
            WHERE repeat('a', 60) REGEXP concat(substr(p, 1, 0), '(?=a)(a+)+b')
        """
        exception "Failed to match regex expression"
    }

    sql "SET enable_extended_regex = false"
    // Without the session variable, column patterns report the same error as constant ones.
    test {
        sql "SELECT id, s REGEXP p FROM test_regexp_extended_nonconstant_pattern ORDER BY id"
        exception "try setting enable_extended_regex=true"
    }
    test {
        sql "SELECT id, s REGEXP '(?<=foo)bar' FROM test_regexp_extended_nonconstant_pattern ORDER BY id"
        exception "try setting enable_extended_regex=true"
    }
}
