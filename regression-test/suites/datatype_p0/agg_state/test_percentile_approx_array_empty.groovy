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

suite("test_percentile_approx_array_empty") {
    sql "set enable_agg_state=true"
    sql "DROP TABLE IF EXISTS test_percentile_approx_array_empty_input"
    sql """
        CREATE TABLE test_percentile_approx_array_empty_input (id INT, v DOUBLE NULL)
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO test_percentile_approx_array_empty_input VALUES
        (0, cast('NaN' AS double)), (0, cast('NaN' AS double)),
        (1, cast('NaN' AS double)), (1, 7), (2, NULL)
    """
    for (def phase : [1, 2]) {
        "order_qt_regular_phase_${phase}"("""
            SELECT /*+ SET_VAR(agg_phase=${phase}) */ id,
                percentile_approx_array(v, [0.25, 0.75]),
                percentile_approx_array(v, [0.25, 0.75], 2048)
            FROM test_percentile_approx_array_empty_input GROUP BY id ORDER BY id
        """)
        "order_qt_combine_phase_${phase}"("""
            SELECT id, percentile_approx_array_merge(s) FROM (
                SELECT /*+ SET_VAR(agg_phase=${phase}) */ id,
                    percentile_approx_array_combine(v, [0.25, 0.75], 2048) s
                FROM test_percentile_approx_array_empty_input GROUP BY id
            ) states GROUP BY id ORDER BY id
        """)
    }
    order_qt_no_rows """
        SELECT percentile_approx_array(v, [0.25, 0.75])
        FROM test_percentile_approx_array_empty_input WHERE id = 3
    """

    for (def producer : ["state", "combine"]) {
        def first = """
            SELECT percentile_approx_array_${producer}(
                non_nullable(cast('NaN' AS double)), [0.25], 2048) s
        """
        def second = """
            SELECT percentile_approx_array_${producer}(
                non_nullable(cast('NaN' AS double)), [0.25, 0.75], 4096) s
        """
        def noLevels = """
            SELECT percentile_approx_array_${producer}(
                non_nullable(cast(7 AS double)), cast([] AS array<double>), 6144) s
        """
        def populated = """
            SELECT percentile_approx_array_${producer}(
                non_nullable(cast(7 AS double)), [0.1, 0.5, 0.9], 10000) s
        """
        int caseIndex = 0
        for (def pair : [[first, second], [second, first], [first, noLevels], [noLevels, first]]) {
            def states = "${pair[0]} UNION ALL ${pair[1]}"
            def united = "SELECT percentile_approx_array_union(s) s FROM (${states}) states"
            "order_qt_${producer}_empty_merge_${caseIndex}"("""
                SELECT percentile_approx_array_merge(s) FROM (${states}) states
            """)
            "order_qt_${producer}_empty_union_${caseIndex}"("""
                SELECT percentile_approx_array_merge(s) FROM (${united}) united
            """)
            for (def contributorFirst : [false, true]) {
                def branches = contributorFirst ? "${populated} UNION ALL ${united}"
                                                : "${united} UNION ALL ${populated}"
                "order_qt_${producer}_later_${caseIndex}_${contributorFirst}"("""
                    SELECT percentile_approx_array_merge(s) FROM (${branches}) states
                """)
            }
            // Group the contributor with the second empty state before merging the first.
            "order_qt_${producer}_association_${caseIndex}"("""
                SELECT percentile_approx_array_merge(s) FROM (
                    ${pair[0]} UNION ALL
                    SELECT percentile_approx_array_union(s) s FROM (
                        ${pair[1]} UNION ALL ${populated}
                    ) inner_states
                ) states
            """)
            caseIndex++
        }

        for (def parameters : ["[0.25], 10000", "[0.1, 0.5, 0.9], 2048"]) {
            def incompatible = """
                SELECT percentile_approx_array_${producer}(
                    non_nullable(cast(8 AS double)), ${parameters}) s
            """
            for (def pair : [[populated, incompatible], [incompatible, populated]]) {
                for (def consumer : ["merge", "union"]) {
                    test {
                        sql """
                            SELECT percentile_approx_array_${consumer}(s) FROM (
                                ${pair[0]} UNION ALL ${pair[1]}
                            ) states
                        """
                        exception "aggregate states have incompatible quantiles or compression"
                    }
                }
            }
        }
    }
}
