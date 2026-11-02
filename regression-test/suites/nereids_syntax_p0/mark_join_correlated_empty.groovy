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

suite("mark_join_correlated_empty") {
    sql "DROP TABLE IF EXISTS mark_join_correlated_empty_l"
    sql "DROP TABLE IF EXISTS mark_join_correlated_empty_r"

    sql """
        CREATE TABLE mark_join_correlated_empty_l (
            id INT NOT NULL, k INT NULL, v INT NOT NULL
        ) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ('replication_num' = '1')
    """
    sql """
        CREATE TABLE mark_join_correlated_empty_r (
            id INT NOT NULL, k INT NOT NULL, v INT NOT NULL
        ) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ('replication_num' = '1')
    """

    sql "INSERT INTO mark_join_correlated_empty_l VALUES (1, NULL, 0), (2, 42, -1)"
    sql "INSERT INTO mark_join_correlated_empty_r VALUES (1, 7, -2)"

    explain {
        sql """
            SELECT l.id, l.k IN (SELECT r.k FROM mark_join_correlated_empty_r r
                                  WHERE r.v > l.v) AS m
            FROM (SELECT * FROM mark_join_correlated_empty_l ORDER BY id LIMIT 2) l
            ORDER BY l.id
        """
        contains "NULL AWARE LEFT SEMI JOIN"
        contains "mark join predicates"
        contains "other join predicates"
    }

    order_qt_in_empty """
        SELECT l.id, l.k IN (SELECT r.k FROM mark_join_correlated_empty_r r
                              WHERE r.v > l.v) AS m
        FROM (SELECT * FROM mark_join_correlated_empty_l ORDER BY id LIMIT 2) l
        ORDER BY l.id
    """

    order_qt_not_in_empty """
        SELECT l.id, l.k NOT IN (SELECT r.k FROM mark_join_correlated_empty_r r
                                  WHERE r.v > l.v) AS m
        FROM (SELECT * FROM mark_join_correlated_empty_l ORDER BY id LIMIT 2) l
        ORDER BY l.id
    """

    order_qt_in_empty_single_row """
        SELECT l.id, l.k IN (SELECT r.k FROM mark_join_correlated_empty_r r
                              WHERE r.v > l.v) AS m
        FROM (SELECT * FROM mark_join_correlated_empty_l WHERE id = 2 ORDER BY id LIMIT 1) l
        ORDER BY l.id
    """
}
