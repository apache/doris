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

suite("topn_unlimited") {
    sql "DROP TABLE IF EXISTS test_topn_unlimited"
    sql """
        CREATE TABLE test_topn_unlimited (
            g INT,
            s STRING,
            v INT,
            w BIGINT
        ) DISTRIBUTED BY HASH(v) BUCKETS 3
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO test_topn_unlimited VALUES
        (1, 'a', 1, 1), (1, 'b', 2, 10), (1, 'a', 1, 2),
        (1, 'c', 3, 3), (1, 'a', 1, 1), (1, 'b', 2, 2),
        (2, 'x', 4, 1), (2, 'y', 5, 5), (2, 'x', 4, 2),
        (3, NULL, NULL, 1)
    """

    sql "SET enable_bucketed_hash_agg = false"
    sql "SET parallel_pipeline_task_num = 1"
    for (def phase : [1, 2]) {
        sql "SET agg_phase = ${phase}"
        // Non-positive rates retain every candidate, including after partial serialization.
        for (def rate : [0, -1, -2147483648]) {
            order_qt_unlimited """
                SELECT topn(s, 2, ${rate}),
                       topn_array(s, 2, ${rate}),
                       topn_array(v, 2, ${rate}),
                       topn_weighted(s, w, 2, ${rate}),
                       topn_weighted(v, w, 2, ${rate})
                FROM test_topn_unlimited
            """
            order_qt_grouped """
                SELECT g, topn(s, 2, ${rate}),
                       topn_array(s, 2, ${rate}),
                       topn_array(v, 2, ${rate}),
                       topn_weighted(s, w, 2, ${rate}),
                       topn_weighted(v, w, 2, ${rate})
                FROM test_topn_unlimited GROUP BY g
            """
            order_qt_empty """
                SELECT topn(s, 1, ${rate}), topn_array(v, 1, ${rate}),
                       topn_weighted(v, w, 1, ${rate})
                FROM test_topn_unlimited WHERE g = 4
            """
            order_qt_constant_input """
                SELECT topn(s, 1, ${rate}), topn_array(s, 1, ${rate})
                FROM (SELECT 'a' AS s UNION ALL SELECT 'b' UNION ALL SELECT 'a') t
            """
        }
        order_qt_positive_and_default """
            SELECT topn(s, 2), topn(s, 2, 50),
                   topn_array(v, 2), topn_array(v, 2, 50),
                   topn_weighted(v, w, 2), topn_weighted(v, w, 2, 50)
            FROM test_topn_unlimited
        """
    }
}
