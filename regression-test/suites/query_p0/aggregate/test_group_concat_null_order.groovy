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

suite("test_group_concat_null_order", "query,p0") {
    sql "DROP TABLE IF EXISTS test_group_concat_null_order"
    sql """
        CREATE TABLE test_group_concat_null_order (
            id INT, g INT, v VARCHAR(10), k INT NULL
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 4
        PROPERTIES('replication_num' = '1')
    """
    sql """
        INSERT INTO test_group_concat_null_order VALUES
            (1, 1, 'a', NULL), (2, 1, 'b', 1),
            (3, 2, 'c', 2), (4, 2, 'd', NULL), (5, 2, 'e', 1),
            (6, 2, 'f', NULL), (7, 2, 'c', 2),
            (8, 3, 'h', NULL), (9, 3, 'g', NULL),
            (10, 4, 'i', 1), (11, 4, 'j', 2),
            (12, 5, NULL, NULL), (13, 5, 'l', 2)
    """

    for (def phase : [1, 2]) {
        sql "SET agg_phase = ${phase}"

        "order_qt_repro_ordinary_${phase}" """
            SELECT GROUP_CONCAT(v, '|' ORDER BY k DESC NULLS LAST),
                   GROUP_CONCAT(v, '|' ORDER BY k DESC NULLS FIRST)
            FROM test_group_concat_null_order WHERE g = 1
        """
        "order_qt_repro_distinct_${phase}" """
            SELECT GROUP_CONCAT(DISTINCT v, '|' ORDER BY k DESC NULLS LAST),
                   GROUP_CONCAT(DISTINCT v, '|' ORDER BY k DESC NULLS FIRST)
            FROM test_group_concat_null_order WHERE g = 1
        """
        "order_qt_default_separator_${phase}" """
            SELECT GROUP_CONCAT(v ORDER BY k DESC NULLS LAST),
                   GROUP_CONCAT(DISTINCT v ORDER BY k DESC NULLS LAST)
            FROM test_group_concat_null_order WHERE g = 1
        """

        for (def distinct : ["", "DISTINCT"]) {
            for (def grouped : [false, true]) {
                "order_qt_${distinct ? 'distinct' : 'ordinary'}_${grouped ? 'grouped' : 'global'}_${phase}" """
                    SELECT ${grouped ? 'g,' : ''}
                           GROUP_CONCAT(${distinct} v, '|' ORDER BY k ASC NULLS FIRST, id),
                           GROUP_CONCAT(${distinct} v, '|' ORDER BY k ASC NULLS LAST, id),
                           GROUP_CONCAT(${distinct} v, '|' ORDER BY k DESC NULLS FIRST, id),
                           GROUP_CONCAT(${distinct} v, '|' ORDER BY k DESC NULLS LAST, id)
                    FROM test_group_concat_null_order
                    ${grouped ? 'GROUP BY g ORDER BY g' : ''}
                """
            }
        }
    }

    qt_plain_order_by """
        SELECT v, k FROM test_group_concat_null_order
        WHERE g = 1 ORDER BY k DESC NULLS LAST
    """
}
