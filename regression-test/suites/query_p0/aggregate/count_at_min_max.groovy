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

suite("count_at_min_max") {
    sql """drop table if exists count_at_min_max_t"""

    sql """
        CREATE TABLE count_at_min_max_t (
            id INT NOT NULL,
            grp VARCHAR(8) NOT NULL,
            i BIGINT,
            s STRING,
            d DECIMAL(10, 2),
            dt DATETIME
        )
        ENGINE = OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        )
    """

    // grp a: i min=10 twice, max=30 once; s min='a', max='c'
    // grp b: i min=max=5 twice; the NULL row must be ignored
    // grp c: only NULL values, so both counts are zero
    sql """INSERT INTO count_at_min_max_t VALUES
        (1, 'a', 10, 'b', 10.50, '2024-01-01 10:00:00'),
        (2, 'a', 10, 'a', 10.50, '2024-01-01 09:00:00'),
        (3, 'a', 30, 'c', 30.50, '2024-01-01 11:00:00'),
        (4, 'b', 5,  'x', 5.50,  '2024-01-02 10:00:00'),
        (5, 'b', 5,  'y', 5.50,  '2024-01-02 09:00:00'),
        (6, 'b', NULL, NULL, NULL, NULL),
        (7, 'c', NULL, NULL, NULL, NULL)"""

    order_qt_grouped_integer """
        SELECT grp, count_at_min(i) AS min_cnt, count_at_max(i) AS max_cnt
        FROM count_at_min_max_t
        GROUP BY grp
    """

    order_qt_grouped_string """
        SELECT grp, count_at_min(s) AS min_cnt, count_at_max(s) AS max_cnt
        FROM count_at_min_max_t
        GROUP BY grp
    """

    order_qt_grouped_decimal """
        SELECT grp, count_at_min(d) AS min_cnt, count_at_max(d) AS max_cnt
        FROM count_at_min_max_t
        GROUP BY grp
    """

    order_qt_grouped_datetime """
        SELECT grp, count_at_min(dt) AS min_cnt, count_at_max(dt) AS max_cnt
        FROM count_at_min_max_t
        GROUP BY grp
    """

    order_qt_scalar """
        SELECT count_at_min(i) AS min_i_cnt, count_at_max(i) AS max_i_cnt,
               count_at_min(s) AS min_s_cnt, count_at_max(s) AS max_s_cnt
        FROM count_at_min_max_t
    """

    // Empty input returns zero, not NULL.
    order_qt_empty_input """
        SELECT count_at_min(i) AS min_cnt, count_at_max(i) AS max_cnt
        FROM count_at_min_max_t
        WHERE id < 0
    """

    // A group whose only rows are NULL has no non-NULL value.
    order_qt_all_null_group """
        SELECT count_at_min(i) AS min_cnt, count_at_max(i) AS max_cnt
        FROM count_at_min_max_t
        WHERE grp = 'c'
    """

    test {
        sql "SELECT count_at_min(DISTINCT i) FROM count_at_min_max_t"
        exception "COUNT_AT_MIN does not support DISTINCT"
    }

    test {
        sql "SELECT count_at_max(DISTINCT i) FROM count_at_min_max_t"
        exception "COUNT_AT_MAX does not support DISTINCT"
    }
}
