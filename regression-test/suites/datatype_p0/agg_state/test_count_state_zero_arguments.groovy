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

suite("test_count_state_zero_arguments") {
    sql "set enable_agg_state=true"

    // Exercise the original crash path, whose result is an opaque aggregate state.
    sql """
        SELECT count_union(count_state())
        FROM (SELECT 1 AS v UNION ALL SELECT 2 AS v UNION ALL SELECT 3 AS v) t
    """

    order_qt_single_row "SELECT count_merge(count_state())"

    order_qt_three_rows """
        SELECT count_merge(count_state())
        FROM (SELECT 1 AS v UNION ALL SELECT 2 AS v UNION ALL SELECT 3 AS v) t
    """

    order_qt_union_merge """
        SELECT count_merge(s)
        FROM (
            SELECT count_union(count_state()) AS s
            FROM (SELECT 1 AS v UNION ALL SELECT 2 AS v UNION ALL SELECT 3 AS v) t
        ) u
    """

    order_qt_empty_input """
        SELECT count_merge(count_state())
        FROM numbers("number"="10") WHERE number < 0
    """

    order_qt_empty_union """
        SELECT count_merge(s)
        FROM (
            SELECT count_union(count_state()) AS s
            FROM numbers("number"="10") WHERE number < 0
        ) t
    """

    order_qt_nullable_input """
        SELECT count_merge(count_state()), count_merge(count_state(v)),
               count_merge(count_state(1))
        FROM (SELECT CAST(NULL AS INT) AS v UNION ALL SELECT 1 UNION ALL SELECT 2) t
    """

    order_qt_multiple_batches """
        SELECT count_merge(count_state()) FROM numbers("number"="10001")
    """

    order_qt_grouped_union """
        SELECT count_merge(s)
        FROM (
            SELECT number % 17 AS k, count_union(count_state()) AS s
            FROM numbers("number"="10001") GROUP BY k
        ) t
    """

    order_qt_decimal_arguments """
        SELECT sum_merge(sum_state(CAST(number AS DECIMAL(18, 2)))),
               avg_merge(avg_state(CAST(number AS DECIMAL(18, 2))))
        FROM numbers("number"="10")
    """
}
