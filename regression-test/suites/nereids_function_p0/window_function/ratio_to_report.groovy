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

suite("ratio_to_report") {
    sql "set enable_nereids_planner=true"
    sql "set enable_fallback_to_original_planner=false"

    sql "DROP TABLE IF EXISTS ratio_to_report_test"
    sql """
        CREATE TABLE ratio_to_report_test (
            id INT,
            grp VARCHAR(8),
            val DOUBLE
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        )
    """

    sql """
        INSERT INTO ratio_to_report_test VALUES
            (1, 'a', 10.0),
            (2, 'a', 20.0),
            (3, 'a', NULL),
            (4, 'a', 30.0),
            (5, 'b', 0.0),
            (6, 'b', 0.0),
            (7, 'c', -10.0),
            (8, 'c', 40.0)
    """

    order_qt_partition """
        SELECT grp, id, val, ROUND(ratio_to_report(val) OVER (PARTITION BY grp), 6) AS rr
        FROM ratio_to_report_test
        ORDER BY grp, id
    """

    order_qt_all_rows """
        SELECT id, val, ROUND(ratio_to_report(val) OVER (), 6) AS rr
        FROM ratio_to_report_test
        WHERE grp = 'a'
        ORDER BY id
    """

    order_qt_expression """
        SELECT grp, id, ROUND(ratio_to_report(val + 10) OVER (PARTITION BY grp), 6) AS rr
        FROM ratio_to_report_test
        WHERE grp IN ('a', 'c')
        ORDER BY grp, id
    """

    order_qt_constant """
        SELECT id, ROUND(ratio_to_report(10) OVER (), 6) AS rr
        FROM ratio_to_report_test
        ORDER BY id
    """

    order_qt_null_literal """
        SELECT id, ratio_to_report(NULL) OVER () AS rr
        FROM ratio_to_report_test
        ORDER BY id
    """

    test {
        sql """
            SELECT ratio_to_report(val)
            OVER (PARTITION BY grp ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
            FROM ratio_to_report_test
        """
        exception "WindowFrame for ratio_to_report() must be null or match with"
    }
}
