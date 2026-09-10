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
// The plan shape needs an outer aggregate that consumes the same inner expression more than
// once (SUM/MIN/MAX of LENGTH(s)) so that aggregate CSE materializes it in a Project above the
// fused aggregate.
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

    // agg_phase = 1 together with eager_agg_broadcast_row_count = 0 keeps the Project that
    // aggregate CSE extracts above the fused aggregate on the plan; with the default settings
    // the planner collapses that projection and the Shape under test is not produced.
    sql "SET runtime_filter_mode = 'OFF'"
    sql "SET enable_sql_cache = false"
    sql "SET query_cache_force_refresh = true"
    sql "SET agg_phase = 1"
    sql "SET eager_agg_broadcast_row_count = 0"
    sql "SET enable_aggregate_cse = true"

    def cseQuery = """
        SELECT COUNT(*), SUM(LENGTH(s)), MIN(LENGTH(s)), MAX(LENGTH(s))
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

    // 2. Regression: fusion + CSE. The fused GROUP JOIN carries the final projections of the
    //    inner aggregate and the SelectNode that feeds the CSE'd LENGTH(s) sits directly above
    //    it. That SelectNode must read the tuple the GROUP JOIN projects into, not the raw
    //    tuple the BE operator materializes its result into.
    explain {
        sql cseQuery
        verbose(true)
        contains("VGROUP JOIN")
        contains("VSELECT")
        check { String plan ->
            def lines = plan.readLines()
            int selectIdx = lines.findIndexOf { it ==~ /.*\d+:VSELECT.*/ }
            int joinIdx = lines.findIndexOf { it ==~ /.*\d+:VGROUP JOIN.*/ }
            assertTrue(joinIdx > selectIdx && selectIdx >= 0,
                    "expected a VSELECT above the VGROUP JOIN, explain:\n" + plan)
            def selectInputTupleIds = lines.subList(selectIdx, joinIdx).findResult {
                def m = it =~ /tuple ids:\s*([0-9 ]+?)\s*$/
                m.find() ? m.group(1).trim() : null
            }
            def joinProjectedTupleId = lines.subList(joinIdx, lines.size()).findResult {
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
        SELECT SUM(LENGTH(s))
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
        SELECT COUNT(*), SUM(LENGTH(s))
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
