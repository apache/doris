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

suite("agg_state_preagg") {
    sql "SET enable_agg_state = true"
    sql "DROP TABLE IF EXISTS agg_state_preagg_generic"
    sql """
        CREATE TABLE agg_state_preagg_generic (
            k1 INT,
            k2 INT,
            m AGG_STATE<max_by(INT NOT NULL, INT)> GENERIC,
            s AGG_STATE<sum(INT)> GENERIC
        )
        AGGREGATE KEY(k1, k2)
        DISTRIBUTED BY HASH(k2) BUCKETS 3
        PROPERTIES("replication_num" = "1", "disable_auto_compaction" = "true")
    """
    // Separate rowsets keep partial states for the same full key. The SUM state
    // also checks multiplicity, which an idempotent MAX_BY alone would not detect.
    sql """
        INSERT INTO agg_state_preagg_generic VALUES
            (1, 1, max_by_state(10, 1), sum_state(10)),
            (1, 2, max_by_state(30, 3), sum_state(NULL)),
            (2, 1, max_by_state(40, 4), sum_state(40)),
            (3, 1, max_by_state(50, NULL), sum_state(NULL))
    """
    sql """
        INSERT INTO agg_state_preagg_generic VALUES
            (1, 1, max_by_state(20, 2), sum_state(20)),
            (1, 2, max_by_state(35, 5), sum_state(35)),
            (2, 1, max_by_state(40, 4), sum_state(5)),
            (3, 1, max_by_state(60, NULL), sum_state(NULL))
    """
    sql """
        INSERT INTO agg_state_preagg_generic VALUES
            (1, 1, max_by_state(5, NULL), sum_state(NULL)),
            (1, 2, max_by_state(35, 5), sum_state(5)),
            (2, 2, max_by_state(45, 6), sum_state(NULL))
    """

    def mergeQueries = [
        full_key: """SELECT k1, k2, max_by_merge(m), sum_merge(s)
                     FROM agg_state_preagg_generic GROUP BY k1, k2""",
        group_key: """SELECT k1, max_by_merge(m), sum_merge(s)
                      FROM agg_state_preagg_generic GROUP BY k1""",
        global: """SELECT max_by_merge(m), sum_merge(s) FROM agg_state_preagg_generic""",
        alias: """SELECT k1, max_by_merge(ms), sum_merge(ss)
                  FROM (SELECT k1, m AS ms, s AS ss FROM agg_state_preagg_generic) t
                  GROUP BY k1""",
        key_filter: """SELECT k1, max_by_merge(m), sum_merge(s)
                       FROM agg_state_preagg_generic WHERE k2 = 1 GROUP BY k1"""
    ]
    mergeQueries.each { name, query ->
        explain {
            sql query
            contains "(agg_state_preagg_generic), PREAGGREGATION: ON"
        }
        "order_qt_merge_${name}"(query)
    }

    for (def phase : [1, 2]) {
        def unionQuery = """
            SELECT /*+ SET_VAR(agg_phase=${phase}) */ k1, max_by_union(m) m, sum_union(s) s
            FROM agg_state_preagg_generic GROUP BY k1
        """
        explain {
            sql unionQuery
            contains "(agg_state_preagg_generic), PREAGGREGATION: ON"
        }
        "order_qt_union_phase_${phase}"("""
            SELECT k1, max_by_merge(m), sum_merge(s) FROM (${unionQuery}) t GROUP BY k1
        """)
        "order_qt_merge_phase_${phase}"("""
            SELECT /*+ SET_VAR(agg_phase=${phase}) */ k1, max_by_merge(m), sum_merge(s)
            FROM agg_state_preagg_generic GROUP BY k1
        """)
    }
    order_qt_union_global """
        SELECT max_by_merge(m), sum_merge(s) FROM (
            SELECT max_by_union(m) m, sum_union(s) s FROM agg_state_preagg_generic
        ) t
    """
    order_qt_empty_merge """
        SELECT max_by_merge(m), sum_merge(s) FROM agg_state_preagg_generic WHERE k1 = 100
    """
    order_qt_empty_union """
        SELECT max_by_merge(m), sum_merge(s) FROM (
            SELECT max_by_union(m) m, sum_union(s) s
            FROM agg_state_preagg_generic WHERE k1 = 100
        ) t
    """

    // A sibling COUNT needs storage-merged rows. Enabling merge/union must not
    // bypass checks on other aggregates, value filters, or argument expressions.
    for (def suffix : ["merge", "union"]) {
        explain {
            sql """SELECT k1, max_by_${suffix}(m), count(*)
                   FROM agg_state_preagg_generic GROUP BY k1"""
            contains "(agg_state_preagg_generic), PREAGGREGATION: OFF"
        }
        explain {
            sql """SELECT k1, max_by_${suffix}(m)
                   FROM agg_state_preagg_generic WHERE length(m) > 0 GROUP BY k1"""
            contains "(agg_state_preagg_generic), PREAGGREGATION: OFF"
        }
        explain {
            sql """SELECT k1, max_by_${suffix}(if(k2 = 1, m, NULL))
                   FROM agg_state_preagg_generic GROUP BY k1"""
            contains "(agg_state_preagg_generic), PREAGGREGATION: OFF"
        }
    }
    order_qt_merge_with_count """
        SELECT k1, max_by_merge(m), sum_merge(s), count(*)
        FROM agg_state_preagg_generic GROUP BY k1
    """
    order_qt_union_with_count """
        SELECT k1, max_by_merge(m), sum_merge(s), max(n) FROM (
            SELECT k1, max_by_union(m) m, sum_union(s) s, count(*) n
            FROM agg_state_preagg_generic GROUP BY k1
        ) t GROUP BY k1
    """

}
