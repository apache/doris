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

suite("test_asof_join_null_safe_string_key", "query_p0") {
    sql "DROP TABLE IF EXISTS asof_null_safe_string_left"
    sql "DROP TABLE IF EXISTS asof_null_safe_string_right"

    sql """
        CREATE TABLE asof_null_safe_string_left (
            id INT,
            s VARCHAR(10),
            ts DATETIME
        ) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """

    sql """
        CREATE TABLE asof_null_safe_string_right (
            id INT,
            s VARCHAR(10),
            ts DATETIME
        ) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """

    sql """
        INSERT INTO asof_null_safe_string_left VALUES
        (1, NULL, '2024-01-01 10:00:20'),
        (2, '',   '2024-01-01 10:00:20'),
        (3, NULL, '2024-01-01 10:00:05'),
        (4, '',   '2024-01-01 10:00:05')
    """

    sql """
        INSERT INTO asof_null_safe_string_right VALUES
        (101, NULL, '2024-01-01 10:00:10'),
        (102, '',   '2024-01-01 10:00:15'),
        (103, NULL, '2024-01-01 10:00:30'),
        (104, '',   '2024-01-01 10:00:30')
    """

    // NULL and the real empty string must use separate equality-key buckets.
    order_qt_asof_null_safe_string_inner """
        SELECT l.id, r.id AS rid
        FROM asof_null_safe_string_left l
        ASOF INNER JOIN asof_null_safe_string_right r
        MATCH_CONDITION(l.ts >= r.ts)
        ON l.s <=> r.s
        ORDER BY l.id
    """

    order_qt_asof_null_safe_string_left """
        SELECT l.id, r.id AS rid
        FROM asof_null_safe_string_left l
        ASOF LEFT JOIN asof_null_safe_string_right r
        MATCH_CONDITION(l.ts >= r.ts)
        ON l.s <=> r.s
        ORDER BY l.id
    """

    // Removing the ASOF probe-side NULL shortcut must not change ordinary equality:
    // its reserved NULL bucket is empty, while a real empty string still matches.
    order_qt_asof_ordinary_string_left """
        SELECT l.id, r.id AS rid
        FROM asof_null_safe_string_left l
        ASOF LEFT JOIN asof_null_safe_string_right r
        MATCH_CONDITION(l.ts >= r.ts)
        ON l.s = r.s
        ORDER BY l.id
    """
}
