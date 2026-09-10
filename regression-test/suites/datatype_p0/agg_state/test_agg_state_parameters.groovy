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
        ["topn_array", "1, 1", "1, 3"],
        ["topn_array", "1, 3, 2", "1, 3, 5"],
        ["topn_array", "'a', 1, 6", "'a', 3, 2"],
        ["topn_weighted", "1, 1, 1", "1, 1, 3"],
        ["topn_weighted", "1, 1, 3, 2", "1, 1, 3, 5"],
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
        ["percentile_approx_weighted", "7, 1, 0.5, 2048", "7, 1, 0.5, 4096"],
        ["percentile_reservoir", "7, 0.25", "7, 0.75"],
        ["collect_list", "7, 1", "7, 3"],
        ["collect_list", "'a', 1", "'a', 3"],
        ["collect_list", "[7], 1", "[7], 3"],
        ["collect_set", "7, 1", "7, 3"],
        ["collect_set", "'a', 1", "'a', 3"],
        ["intersect_count", "to_bitmap(1), 1, 1", "to_bitmap(1), 1, 2"],
        ["intersect_count", "to_bitmap(1), 'a', 'a'", "to_bitmap(1), 'a', 'b'"],
        ["group_concat", "'a', ','", "'a', ';'"],
        ["exponential_moving_average", "1, 7, 1", "2, 7, 1"],
        ["sequence_match", "'(?1)', non_nullable(cast('2024-01-01' as datetime)), true, false",
                           "'(?2)', non_nullable(cast('2024-01-01' as datetime)), true, false"],
        ["sequence_count", "'(?1)', non_nullable(cast('2024-01-01' as datetime)), true, false",
                           "'(?2)', non_nullable(cast('2024-01-01' as datetime)), true, false"]
    ]
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
}
