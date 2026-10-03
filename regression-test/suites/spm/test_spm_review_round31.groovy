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

suite("test_spm_review_round31", "spm") {

    // Thirty-first review round.
    //
    // SQL-visible fixes covered here:
    //  - #2: a quoted user column named `count()` is a DATA argument. The decompiler
    //    decided "count-star buffer" by the slot NAME, so `count(`count()`)` was frozen as
    //    count(*) and every baseline hit counted ROWS where the column is NULL (2 -> 3).
    //    The buffer is now told apart by provenance: a column some relation exports is a
    //    data argument whatever it is called.
    //  - #6: a legal user column named GROUPING_ID flows through projections. The
    //    ROLLUP execution marker was skipped by NAME, so the frozen child SELECT dropped
    //    the column while the outer projection still referenced it - every replay failed
    //    with "Unknown column 'GROUPING_ID' in 'table list'".
    //
    // #1 (a user UDAF whose own name starts with partial_ was folded as an execution
    // stage) needs a user aggregate function and is covered by
    // SPMPlan2SQLBuilderTest#testUserAggregateNamedPartialIsNotAnInternalStage; #3 (the
    // paginated baseline snapshot must come from one state) needs a DDL racing the page
    // loop and #5 (checkpoint pattern columns must store any accepted regex) is a schema
    // property - covered by BaselineManagerConcurrencyTest / InternalSchemaInitializerTest.
    // #4 (the rewound retry window pins its page's filter) is covered by
    // PlanCaptureCycleHandoffTest.

    // SPM regression pins the fallback switch CLOSED: a rewritten-plan failure must
    // surface as an error, never silently re-run the original query.
    sql """set enable_spm_fallback = false"""
    sql """set enable_spm_rewrite = true"""

    // ==================== setup: tables (drop before use, keep after) ====================
    sql """DROP TABLE IF EXISTS spm_r31_t2"""
    sql """
        CREATE TABLE spm_r31_t2 (
            k INT,
            `count()` INT
        )
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO spm_r31_t2 VALUES (10, 1), (20, NULL), (30, 3)"""

    sql """DROP TABLE IF EXISTS spm_r31_t3"""
    sql """
        CREATE TABLE spm_r31_t3 (
            k INT,
            `GROUPING_ID` INT
        )
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO spm_r31_t3 VALUES (1, 10), (2, 20)"""

    // Global baselines are cluster-wide state and other SPM suites may run their own in
    // parallel: every SHOW here is scoped to this suite's tables and only baselines
    // matching them are dropped or asserted on.
    def ownBaselines = {
        sql("""SHOW BASELINE PLANS""").findAll { row ->
            row[1].toString().contains("spm_r31_t2") || row[1].toString().contains("spm_r31_t3")
        }
    }
    def dropOwnBaselines = {
        ownBaselines().each { row ->
            sql """DROP BASELINE PLAN ${row[0]}"""
        }
    }
    def explainOf = { String stmt ->
        sql("""EXPLAIN ${stmt}""").toString()
    }

    dropOwnBaselines()
    try {
        // ==================== #2: the quoted count() column stays an argument ====================
        String countCol = "SELECT count(`count()`) AS c FROM spm_r31_t2"
        List<List<Object>> created = sql(
                """CREATE GLOBAL BASELINE PLAN '${countCol}' WITH '${countCol}'""")
        assertEquals(1, created.size(), "CREATE should return one row, got: ${created}")
        long countId = Long.parseLong(created[0][0].toString())
        String countPlan = ownBaselines().find { Long.parseLong(it[0].toString()) == countId }[4]
                .toString()
        assertTrue(countPlan.contains("count(`count()`)"),
                "the frozen SQL must keep the quoted column as the argument, not count(*): "
                        + countPlan)
        assertTrue(explainOf(countCol).contains("SPM baseline hit: id=${countId}"),
                "the count(`count()`) query must hit its baseline: " + explainOf(countCol))
        order_qt_count_column_is_not_a_star """SELECT count(`count()`) AS c FROM spm_r31_t2"""

        // ==================== #6: a user column named GROUPING_ID ====================
        String groupingCol = "SELECT `GROUPING_ID`, k + 1 AS kk FROM spm_r31_t3"
        List<List<Object>> createdGrouping = sql(
                """CREATE GLOBAL BASELINE PLAN '${groupingCol}' WITH '${groupingCol}'""")
        assertEquals(1, createdGrouping.size(),
                "CREATE should return one row, got: ${createdGrouping}")
        long groupingId = Long.parseLong(createdGrouping[0][0].toString())
        String groupingPlan = ownBaselines()
                .find { Long.parseLong(it[0].toString()) == groupingId }[4].toString()
        assertTrue(groupingPlan.contains("GROUPING_ID"),
                "the frozen SQL must export the user column GROUPING_ID: " + groupingPlan)
        assertTrue(explainOf(groupingCol).contains("SPM baseline hit: id=${groupingId}"),
                "the GROUPING_ID query must hit its baseline: " + explainOf(groupingCol))
        order_qt_user_column_grouping_id """SELECT `GROUPING_ID`, k + 1 AS kk FROM spm_r31_t3
                ORDER BY k"""

        // leave no baselines behind for other runs
        dropOwnBaselines()
        assertEquals(0, ownBaselines().size(), "all spm_r31_ baselines must be dropped")
    } finally {
        dropOwnBaselines()
    }
}
