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

suite("test_numeric_distinct_merge") {
    sql "DROP TABLE IF EXISTS test_numeric_distinct_merge"
    sql """
        CREATE TABLE test_numeric_distinct_merge (
            id INT NOT NULL,
            g INT NULL,
            v BIGINT NULL,
            nonnull_v BIGINT NOT NULL
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 4
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO test_numeric_distinct_merge VALUES
            (1, 0, 1, 1), (2, 0, 2, 2), (3, 0, 2, 2), (4, 0, NULL, 0),
            (5, 1, 2, 2), (6, 1, 3, 3), (7, 1, -1, -1), (8, 1, NULL, 0),
            (9, 2, NULL, 0), (10, NULL, 4, 4), (11, NULL, 4, 4), (12, NULL, NULL, 0)
    """

    for (phase in [1, 2]) {
        sql "SET agg_phase = ${phase}"

        "order_qt_integer_types_${phase}" """
            SELECT multi_distinct_sum(CAST(v AS TINYINT)),
                   multi_distinct_sum(CAST(v AS SMALLINT)),
                   multi_distinct_sum(CAST(v AS INT)),
                   multi_distinct_sum(v),
                   multi_distinct_sum(CAST(v AS LARGEINT)),
                   multi_distinct_sum(nonnull_v)
            FROM test_numeric_distinct_merge
        """
        "order_qt_grouped_${phase}" """
            SELECT g, multi_distinct_sum(v), multi_distinct_sum(nonnull_v)
            FROM test_numeric_distinct_merge GROUP BY g
        """
        "order_qt_all_null_${phase}" """
            SELECT multi_distinct_sum(v) FROM test_numeric_distinct_merge WHERE g = 2
        """
        "order_qt_empty_${phase}" """
            SELECT multi_distinct_sum(v), multi_distinct_sum(nonnull_v)
            FROM test_numeric_distinct_merge WHERE id < 0
        """
        "order_qt_state_merge_${phase}" """
            SELECT multi_distinct_sum_merge(multi_distinct_sum_state(v))
            FROM test_numeric_distinct_merge
        """
    }

    explain {
        sql "SELECT multi_distinct_sum(v) FROM test_numeric_distinct_merge"
        contains "merge finalize"
    }
}
