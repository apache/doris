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

suite("test_agg_state_parameters") {
    sql "set enable_agg_state=true"

    for (def function : ["window_funnel", "window_funnel_v1", "window_funnel_v2"]) {
        def implementation = function == "window_funnel" ? "window_funnel_v2" : function
        for (def suffix : ["", "_state", "_combine"]) {
            test {
                sql """
                    SELECT ${function}${suffix}(number, 'default',
                           cast('2024-01-01' AS datetime), true, false)
                    FROM numbers("number" = "3")
                """
                exception "The window parameter of ${implementation} must be a constant"
            }
            test {
                sql """
                    SELECT ${function}${suffix}(10, if(number = 0, 'default', 'fixed'),
                           cast('2024-01-01' AS datetime), true, false)
                    FROM numbers("number" = "3")
                """
                exception "The mode parameter of ${implementation} must be a constant"
            }
        }
    }

    for (def function : ["collect_set", "collect_set_state", "collect_set_combine"]) {
        test {
            sql """
                SELECT ${function}(number, cast(number AS int))
                FROM numbers("number" = "3")
            """
            exception "collect_set requires second parameter must be a constant"
        }
    }

    for (def function : ["exponential_moving_average", "exponential_moving_average_state",
                         "exponential_moving_average_combine"]) {
        test {
            sql "SELECT ${function}(cast('NaN' AS double), cast(7 AS double), cast(1 AS double))"
            exception "half decay must not be NaN"
        }
    }

    // Each pair has the same AggState type but incompatible configuration values.
    def cases = [
        ["topn", "'a', 1", "'a', 3"],
        ["topn", "'a', 3, 2", "'a', 3, 5"],
        ["topn", "'a', 1, 6", "'a', 3, 2"],
        ["topn", "'a', 1, 0", "'a', 1, 2"],
        ["topn_array", "1, 1", "1, 3"],
        ["topn_array", "1, 3, 2", "1, 3, 5"],
        ["topn_array", "'a', 1, 6", "'a', 3, 2"],
        ["topn_array", "1, 1, 0", "1, 1, 2"],
        ["topn_weighted", "1, 1, 1", "1, 1, 3"],
        ["topn_weighted", "1, 1, 3, 2", "1, 1, 3, 5"],
        ["topn_weighted", "1, 1, 1, 0", "1, 1, 1, 2"],
        ["histogram", "7, 1", "7, 3"],
        ["percentile", "7, 0.25", "7, 0.75"],
        ["percentile_array", "7, [0.25]", "7, [0.75]"],
        ["percentile_array", "7, [0.25]", "7, [0.25, 0.75]"],
        ["percentile_array", "7, cast([] as array<double>)", "7, [0.25]"],
        ["percentile_approx", "7, 0.25", "7, 0.75"],
        ["percentile_approx", "7, 0.5, 2048", "7, 0.5, 4096"],
        ["percentile_approx_array", "7, [0.25]", "7, [0.75]"],
        ["percentile_approx_array", "7, [0.25], 2048", "7, [0.25], 4096"],
        ["percentile_approx_weighted", "7, 1, 0.25", "7, 1, 0.75"],
        ["percentile_approx_weighted", "7, 0, 0.25", "7, 1, 0.75"],
        ["percentile_approx_weighted", "7, 1, 0.5, 2048", "7, 1, 0.5, 4096"],
        ["percentile_reservoir", "7, 0.25", "7, 0.75"],
        ["collect_list", "7, 1", "7, 3"],
        ["collect_list", "'a', 1", "'a', 3"],
        ["collect_list", "[7], 1", "[7], 3"],
        ["collect_set", "7, 1", "7, 3"],
        ["collect_set", "'a', 1", "'a', 3"],
        ["intersect_count", "to_bitmap(1), 1, 1", "to_bitmap(1), 1, 2"],
        ["intersect_count", "to_bitmap(1), 'a', 'a'", "to_bitmap(1), 'a', 'b'"],
        ["intersect_count", "bitmap_empty(), 1, 1", "to_bitmap(1), 1, 2"],
        ["group_concat", "'a', ','", "'a', ';'"],
        ["group_concat", "cast('' as string), ','", "cast('a' as string), ';'"],
        ["group_concat", "cast('' as string), ','", "cast('' as string), ';'"],
        ["exponential_moving_average", "1, 7, 1", "2, 7, 1"],
        ["exponential_moving_average", "0, 7, 1", "1, 7, 1"],
        ["exponential_moving_average", "0, 0, 0", "1, 7, 1"],
        ["sequence_match", "'(?1)', non_nullable(cast('2024-01-01' as datetime)), true, false",
                           "'(?2)', non_nullable(cast('2024-01-01' as datetime)), true, false"],
        ["sequence_count", "'(?1)', non_nullable(cast('2024-01-01' as datetime)), true, false",
                           "'(?2)', non_nullable(cast('2024-01-01' as datetime)), true, false"]
    ]
    // A non-null input establishes configuration even if no sample is retained.
    // Keep AggState argument nullability identical across both sides of the UNION.
    for (def quantile : ["0.0", "0.25", "1.0"]) {
        for (def sample : ["'NaN'", "7"]) {
            cases.add(["percentile_reservoir", "non_nullable(cast('NaN' as double)), ${quantile}",
                       "non_nullable(cast(${sample} as double)), 0.75"])
        }
    }
    for (def function : ["collect_list", "collect_set"]) {
        for (def value : ["7", "'a'"]) {
            for (def limit : [-2, -1, 0]) {
                cases.add([function, "${value}, ${limit}", "${value}, 1"])
            }
            cases.add([function, "${value}, -1", "${value}, -2"])
        }
    }
    for (def limit : [-2, -1, 0]) {
        cases.add(["collect_list", "[7], ${limit}", "[7], 1"])
    }
    // All-false event rows retain their pattern, including when both states have no events.
    for (def function : ["sequence_match", "sequence_count"]) {
        for (def event : ["true", "false"]) {
            cases.add([function,
                       "'(?1)', non_nullable(cast('2024-01-01' as datetime)), false, false",
                       "'(?2)', non_nullable(cast('2024-01-01' as datetime)), ${event}, false"])
        }
    }
    // Use STRING for both modes to keep the AggState argument types identical.
    for (def function : ["window_funnel", "window_funnel_v1", "window_funnel_v2"]) {
        cases.add([function, "1, cast('default' as string), non_nullable(cast('2024-01-01' as datetime)), true, false",
                             "3, cast('default' as string), non_nullable(cast('2024-01-01' as datetime)), true, false"])
        cases.add([function, "1, cast('default' as string), non_nullable(cast('2024-01-01' as datetime)), true, false",
                             "1, cast('fixed' as string), non_nullable(cast('2024-01-01' as datetime)), true, false"])
        // V2 stores no events for an all-false row, but its configuration still participates.
        for (def event : ["true", "false"]) {
            cases.add([function, "0, cast('default' as string), non_nullable(cast('2024-01-01' as datetime)), false, false",
                                 "3, cast('default' as string), non_nullable(cast('2024-01-01' as datetime)), ${event}, false"])
            cases.add([function, "0, cast('default' as string), non_nullable(cast('2024-01-01' as datetime)), false, false",
                                 "0, cast('fixed' as string), non_nullable(cast('2024-01-01' as datetime)), ${event}, false"])
        }
    }

    for (def entry : cases) {
        def function = entry[0]
        for (def args : [[entry[1], entry[2]], [entry[2], entry[1]]]) {
            for (def suffix : ["merge", "union"]) {
                test {
                    sql """
                        SELECT ${function}_${suffix}(s)
                        FROM (
                            SELECT ${function}_state(${args[0]}) AS s
                            UNION ALL
                            SELECT ${function}_state(${args[1]}) AS s
                        ) states
                    """
                    exception "aggregate states have incompatible"
                }
            }
        }
    }

    order_qt_reservoir_compatible_nan """
        SELECT percentile_reservoir_merge(s) FROM (
            SELECT percentile_reservoir_state(non_nullable(cast('NaN' as double)), 0.25) s
            UNION ALL
            SELECT percentile_reservoir_state(cast(100 as double), 0.25) s
        ) states
    """
    order_qt_ema_compatible_zero """
        SELECT exponential_moving_average_merge(s) FROM (
            SELECT exponential_moving_average_state(0, 0, 0) s
            UNION ALL
            SELECT exponential_moving_average_state(0, 7, 1) s
        ) states
    """
    for (def function : ["collect_list", "collect_set"]) {
        "order_qt_${function}_negative_limit"("""
            SELECT array_sort(${function}_merge(s)) FROM (
                SELECT ${function}_state(7, -1) s
                UNION ALL
                SELECT ${function}_state(8, -1) s
            ) states
        """)
    }

    // The last following-row frame is empty after a populated frame. Its reset state
    // must accept a different configuration when the serialized result is merged again.
    sql "DROP TABLE IF EXISTS test_agg_state_reservoir_reset"
    sql """
        CREATE TABLE test_agg_state_reservoir_reset (
            number BIGINT NOT NULL,
            s AGG_STATE<percentile_reservoir(DOUBLE NOT NULL, DOUBLE NOT NULL)> GENERIC
        ) AGGREGATE KEY(number)
        DISTRIBUTED BY HASH(number) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """
        INSERT INTO test_agg_state_reservoir_reset
        SELECT number, percentile_reservoir_state(cast(7 as double), 0.25)
        FROM numbers("number" = "2")
    """
    order_qt_percentile_reservoir_reset_frame """
        WITH framed AS (
            SELECT number, percentile_reservoir_union(s) OVER (
                ORDER BY number ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING
            ) s FROM test_agg_state_reservoir_reset
        )
        SELECT percentile_reservoir_merge(s) FROM (
            SELECT s FROM framed WHERE number = 1
            UNION ALL
            SELECT percentile_reservoir_state(cast(100 as double), 0.75) s
        ) states
    """
    def resetCases = [
        ["exponential_moving_average", "0, 0, 0", "1, 7, 1"],
        ["collect_list", "7, -1", "8, 1"],
        ["collect_set", "7, -1", "8, 1"],
        ["histogram", "7, 10", "8, 20"],
        ["topn", "'a', 1, 0", "'b', 2, 1"],
        ["topn_array", "1, 1, 0", "2, 2, 1"],
        ["topn_weighted", "1, 1, 1, 0", "2, 1, 2, 1"]
    ]
    for (def entry : resetCases) {
        def function = entry[0]
        "order_qt_${function}_reset_frame"("""
            WITH framed AS (
                SELECT number, ${function}_union(${function}_state(${entry[1]})) OVER (
                    ORDER BY number ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING
                ) s FROM numbers("number" = "2")
            )
            SELECT ${function}_merge(s) FROM (
                SELECT s FROM framed WHERE number = 1
                UNION ALL
                SELECT ${function}_state(${entry[2]}) s
            ) states
        """)
    }
}
