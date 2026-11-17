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

suite("test_quantile_union_empty_window") {
    qt_initial_empty """
        SELECT id, quantile_percent(q, 0) AS q_min,
               quantile_percent(q, 0.5) AS q_median, quantile_percent(q, 1) AS q_max
        FROM (
            SELECT number AS id,
                   quantile_union(to_quantile_state(10 + 10 * number, 2048)) OVER (
                       ORDER BY number ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING
                   ) AS q
            FROM numbers("number" = "1")
        ) w ORDER BY id
    """

    qt_nonempty_then_empty """
        SELECT id, quantile_percent(q, 0) AS q_min,
               quantile_percent(q, 0.5) AS q_median, quantile_percent(q, 1) AS q_max
        FROM (
            SELECT number AS id,
                   quantile_union(to_quantile_state(10 + 10 * number, 2048)) OVER (
                       ORDER BY number ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING
                   ) AS q
            FROM numbers("number" = "2")
        ) w ORDER BY id
    """

    qt_consecutive_nonempty_then_empty """
        SELECT id, quantile_percent(q, 0) AS q_min,
               quantile_percent(q, 0.5) AS q_median, quantile_percent(q, 1) AS q_max
        FROM (
            SELECT number AS id,
                   quantile_union(to_quantile_state(10 + 10 * number, 2048)) OVER (
                       ORDER BY number ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING
                   ) AS q
            FROM numbers("number" = "3")
        ) w ORDER BY id
    """

    qt_multi_value_frame_then_empty """
        SELECT id, quantile_percent(q, 0) AS q_min,
               quantile_percent(q, 0.5) AS q_median, quantile_percent(q, 1) AS q_max
        FROM (
            SELECT number AS id,
                   quantile_union(to_quantile_state(10 + 10 * number, 2048)) OVER (
                       ORDER BY number ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING
                   ) AS q
            FROM numbers("number" = "3")
        ) w ORDER BY id
    """

    qt_current_row_control """
        SELECT id, quantile_percent(q, 0) AS q_min,
               quantile_percent(q, 0.5) AS q_median, quantile_percent(q, 1) AS q_max
        FROM (
            SELECT number AS id,
                   quantile_union(to_quantile_state(10 + 10 * number, 2048)) OVER (
                       ORDER BY number ROWS BETWEEN CURRENT ROW AND CURRENT ROW
                   ) AS q
            FROM numbers("number" = "3")
        ) w ORDER BY id
    """

    qt_partition_reset """
        SELECT p, id, quantile_percent(q, 0) AS q_min,
               quantile_percent(q, 0.5) AS q_median, quantile_percent(q, 1) AS q_max
        FROM (
            SELECT number DIV 2 AS p, number % 2 AS id,
                   quantile_union(to_quantile_state(10 + 10 * number, 2048)) OVER (
                       PARTITION BY number DIV 2 ORDER BY number % 2
                       ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING
                   ) AS q
            FROM numbers("number" = "4")
        ) w ORDER BY p, id
    """
}
