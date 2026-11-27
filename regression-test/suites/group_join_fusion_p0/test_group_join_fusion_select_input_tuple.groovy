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

// Regression for the input tuple of the SelectNode inserted above a fused GROUP JOIN.
//
// The fused GROUP JOIN materializes the final aggregate result per group into its own
// materialization tuple, and the generic final-projection attach may hang a projection on the
// very same node (the projected tuple is a second, distinct tuple). GroupJoinNode
// .getOutputTupleIds() returned the materialization tuple in both cases, so a parent that
// asked the fused node what it emits - the SelectNode the translator inserts when the child
// already carries final projections - described its input with the pre-projection tuple.
// Attaching a projection re-binds the projected expressions' ExprIds to the projection tuple
// (PlanTranslatorContext.createSlotDesc), so every expression translated afterwards refers to
// that tuple, and the BE rejected the plan with
//   VSlotRef have invalid slot id: <slot>
//   slot <slot> belongs to Tuple(id=<projection tuple>), not Tuple(id=<materialization tuple>)
// A filter on a volatile projected expression keeps the SelectNode above the GroupJoin projection.
//
// This suite pins the correct rows (a 4-row join produces GROUP_CONCAT 'z,z,z,z', length 7)
// together with the structural guarantee that the SelectNode reads the projected tuple, and
// keeps fusion enabled in every case: falling back to the non-fused plan is not a fix.
suite("test_group_join_fusion_select_input_tuple") {
    sql "DROP TABLE IF EXISTS gj_sel_tuple_l"
    sql "DROP TABLE IF EXISTS gj_sel_tuple_r"

    sql """
        CREATE TABLE gj_sel_tuple_l (
            k INT NULL,
            s VARCHAR(16) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 2
        PROPERTIES ("replication_num" = "1")
        """

    sql "CREATE TABLE gj_sel_tuple_r LIKE gj_sel_tuple_l"

    sql "INSERT INTO gj_sel_tuple_l VALUES (1, 'z'), (1, 'z')"
    sql "INSERT INTO gj_sel_tuple_r VALUES (1, 'z'), (1, 'z')"

    sql "ANALYZE TABLE gj_sel_tuple_l WITH SYNC"
    sql "ANALYZE TABLE gj_sel_tuple_r WITH SYNC"

    // Use one-phase aggregation to allow GroupJoin fusion.
    sql "SET runtime_filter_mode = 'OFF'"
    sql "SET enable_sql_cache = false"
    sql "SET query_cache_force_refresh = true"
    sql "SET agg_phase = 1"
    sql "SET eager_agg_broadcast_row_count = 0"
    sql "SET enable_aggregate_cse = true"

    // P0 fuzzes both spill settings independently.
    def noSpill = "/*+ SET_VAR(enable_spill=false, enable_force_spill=false) */"

    def cseQuery = """
        SELECT ${noSpill} COUNT(*), SUM(LENGTH(s)), MIN(LENGTH(s)), MAX(LENGTH(s))
        FROM (
            SELECT l.k, GROUP_CONCAT(r.s, ',') AS s
            FROM gj_sel_tuple_l l JOIN [shuffle] gj_sel_tuple_r r ON l.k = r.k
            GROUP BY l.k
        ) q
        """

    // 1. Reference rows, computed without fusion. The 2x2 join produces 4 'z' rows, so the
    //    inner GROUP_CONCAT is 'z,z,z,z' (length 7) and the outer aggregate sees one group.
    sql "SET enable_group_join_fusion = false"
    order_qt_fusion_off cseQuery

    sql "SET enable_group_join_fusion = true"

    // 2. The filter must consume the projected UUID. Its value varies, but its length is fixed.
    def projectedTupleQuery = """
        SELECT ${noSpill} COUNT(*), SUM(n), MIN(n), MAX(n)
        FROM (
            SELECT LENGTH(GROUP_CONCAT(r.s, ',')) AS n, UUID() AS token
            FROM gj_sel_tuple_l l JOIN [shuffle] gj_sel_tuple_r r ON l.k = r.k
            GROUP BY l.k
        ) q
        WHERE LENGTH(token) = 36
        """
    explain {
        sql projectedTupleQuery
        verbose(true)
        contains("VGROUP JOIN")
        contains("VSELECT")
        check { String plan ->
            def lines = plan.readLines()
            int selectIdx = lines.findIndexOf { it ==~ /.*\d+:VSELECT.*/ }
            int joinIdx = lines.findIndexOf { it ==~ /.*\d+:VGROUP JOIN.*/ }
            assertTrue(joinIdx > selectIdx && selectIdx >= 0,
                    "expected a VSELECT above the VGROUP JOIN, explain:\n" + plan)
            assertTrue(!lines.subList(selectIdx + 1, joinIdx).any { it ==~ /.*\d+:V[A-Z].*/ },
                    "expected the VSELECT directly above the VGROUP JOIN, explain:\n" + plan)
            def selectInputTupleIds = lines.subList(selectIdx, joinIdx).findResult {
                def m = it =~ /tuple ids:\s*([0-9 ]+?)\s*$/
                m.find() ? m.group(1).trim() : null
            }
            def joinProjectedTupleId = lines.drop(joinIdx + 1).takeWhile { !(it ==~ /.*\d+:V[A-Z].*/) }.findResult {
                def m = it =~ /final project output tuple id:\s*(\d+)/
                m.find() ? m.group(1).trim() : null
            }
            assertTrue(selectInputTupleIds != null && joinProjectedTupleId != null,
                    "missing tuple information, explain:\n" + plan)
            assertEquals(joinProjectedTupleId, selectInputTupleIds,
                    "the VSELECT above the fused GROUP JOIN must read the projected tuple "
                            + "${joinProjectedTupleId} but its input tuples are "
                            + "${selectInputTupleIds}, explain:\n" + plan)
        }
    }
    order_qt_fusion_on_projected_tuple projectedTupleQuery

    // The CSE path may attach LENGTH(s) directly to the fused node's final projection.
    explain {
        sql cseQuery
        contains("VGROUP JOIN")
    }
    order_qt_fusion_on_cse_on cseQuery

    // 3. Control: with CSE disabled there is no extracted expression, hence no projection above
    //    the fused node. The join must still be fused and must still return the same rows.
    sql "SET enable_aggregate_cse = false"
    explain {
        sql cseQuery
        contains("VGROUP JOIN")
    }
    order_qt_fusion_on_cse_off cseQuery
    sql "SET enable_aggregate_cse = true"

    // 4. Control: a single consumer of the extracted expression needs no projection above the
    //    fused node, and must keep working.
    def singleConsumerQuery = """
        SELECT ${noSpill} SUM(LENGTH(s))
        FROM (
            SELECT l.k, GROUP_CONCAT(r.s, ',') AS s
            FROM gj_sel_tuple_l l JOIN [shuffle] gj_sel_tuple_r r ON l.k = r.k
            GROUP BY l.k
        ) q
        """
    explain {
        sql singleConsumerQuery
        contains("VGROUP JOIN")
    }
    order_qt_fusion_on_single_consumer singleConsumerQuery

    // 5. Control: a filter between the outer aggregate and the fused node. Here the projection
    //    is attached to the SelectNode the filter itself creates (the fused node keeps its own
    //    materialization tuple), so the input tuple of that SelectNode must stay the
    //    materialization tuple, and the rows must be filtered correctly.
    def filteredQuery = """
        SELECT ${noSpill} COUNT(*), SUM(LENGTH(s))
        FROM (
            SELECT l.k, GROUP_CONCAT(r.s, ',') AS s
            FROM gj_sel_tuple_l l JOIN [shuffle] gj_sel_tuple_r r ON l.k = r.k
            GROUP BY l.k
        ) q
        WHERE LENGTH(q.s) > 3
        """
    explain {
        sql filteredQuery
        contains("VGROUP JOIN")
        contains("VSELECT")
    }
    order_qt_fusion_on_filtered filteredQuery

    // Restore defaults so other suites are not affected.
    sql "SET enable_group_join_fusion = false"
    sql "SET enable_aggregate_cse = true"
    sql "SET agg_phase = 0"
    sql "SET eager_agg_broadcast_row_count = 250000"
    sql "SET runtime_filter_mode = 'GLOBAL'"
    sql "SET enable_sql_cache = true"
    sql "SET query_cache_force_refresh = false"
}
