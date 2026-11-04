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

suite("test_quantile_state_explicit_boundary") {
    sql "DROP TABLE IF EXISTS test_quantile_state_boundary_partials"
    sql """
        CREATE TABLE test_quantile_state_boundary_partials (
            layout_id INT NOT NULL,
            part INT NOT NULL,
            s QUANTILE_STATE QUANTILE_UNION NOT NULL
        ) AGGREGATE KEY(layout_id, part)
        DISTRIBUTED BY HASH(layout_id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """

    // The same 2048 zeros and singleton 100 are grouped three ways:
    // EXPLICIT + SINGLE, SINGLE + EXPLICIT, and EXPLICIT + EXPLICIT.
    sql """
        INSERT INTO test_quantile_state_boundary_partials
        SELECT 0, CAST(number = 2048 AS INT),
               quantile_union(to_quantile_state(IF(number = 2048, 100.0, 0.0), 2048))
        FROM numbers("number" = "2049") GROUP BY 2
    """
    sql """
        INSERT INTO test_quantile_state_boundary_partials
        SELECT 1, CAST(number != 2048 AS INT),
               quantile_union(to_quantile_state(IF(number = 2048, 100.0, 0.0), 2048))
        FROM numbers("number" = "2049") GROUP BY 2
    """
    sql """
        INSERT INTO test_quantile_state_boundary_partials
        SELECT 2, CAST(number >= 2047 AS INT),
               quantile_union(to_quantile_state(IF(number = 2048, 100.0, 0.0), 2048))
        FROM numbers("number" = "2049") GROUP BY 2
    """

    qt_partial_extrema """
        SELECT layout_id, part, quantile_percent(s, 0), quantile_percent(s, 1)
        FROM test_quantile_state_boundary_partials ORDER BY layout_id, part
    """
    qt_merged_extrema """
        SELECT layout_id, quantile_percent(quantile_union(s), 0),
               quantile_percent(quantile_union(s), 1)
        FROM test_quantile_state_boundary_partials GROUP BY layout_id ORDER BY layout_id
    """
    // Window ordering guarantees both directions of the boundary conversion.
    qt_ordered_merge_extrema """
        SELECT layout_id, part, quantile_percent(s, 0), quantile_percent(s, 1)
        FROM (
            SELECT layout_id, part, quantile_union(s) OVER (
                PARTITION BY layout_id ORDER BY part
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS s
            FROM test_quantile_state_boundary_partials
        ) t ORDER BY layout_id, part
    """
}
