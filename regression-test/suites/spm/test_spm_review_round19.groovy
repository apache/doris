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

suite("test_spm_review_round19", "spm") {

    // SQL-level regression for the frozen top-level clauses.
    //
    // Covered here (end-to-end through CREATE BASELINE + EXPLAIN hit + replay result,
    // BEFORE and AFTER one periodic reload):
    //  - "SELECT a FROM t ORDER BY b + 1" freezes as a Project over Sort: the ORDER BY
    //    must stay at the OUTER query instead of being buried in the derived table,
    //    where the replay planner may drop an inner sort
    //  - "ORDER BY b + 1 LIMIT 10" freezes as a Project over TopN: the cap must stay
    //    reachable so a matching query's LIMIT (20) REPLACES the captured one instead
    //    of the replay returning only the captured 10 rows
    //  - a Window over a filter on a quoted live column ("a b") must EXPORT that
    //    column in its SELECT list, otherwise the frozen SQL references a slot the
    //    derived table never produced and binding fails after the reload

    sql """set enable_spm_rewrite = true"""
    sql """set enable_spm_fallback = false"""

    // ==================== setup: tables (drop before use, keep after) ====================
    sql """DROP TABLE IF EXISTS spm_r19_t1"""
    sql """
        CREATE TABLE spm_r19_t1 (k INT, k2 INT)
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO spm_r19_t1 SELECT number, number FROM numbers("number" = "25")"""
    sql """DROP TABLE IF EXISTS spm_r19_t2"""
    sql """
        CREATE TABLE spm_r19_t2 (k INT, `a b` INT)
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO spm_r19_t2 VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)"""

    def ownBaselines = {
        sql("""SHOW BASELINE PLANS""").findAll { it[1].toString().contains("spm_r19_") }
    }
    def dropOwnBaselines = {
        ownBaselines().each { row ->
            sql """DROP BASELINE PLAN ${row[0]}"""
        }
    }
    dropOwnBaselines()

    def explainOf = { String query -> sql("""EXPLAIN ${query}""").toString() }
    def planSqlOf = { long id ->
        sql("""SELECT plan_sql FROM __internal_schema.spm_baselines WHERE id = ${id}""")[0][0]
                .toString()
    }
    def createBaseline = { String text ->
        (sql('CREATE GLOBAL BASELINE PLAN "' + text + '" WITH "' + text + '"')[0][0] as Long)
    }
    def rowsWithRewriteOff = { String query ->
        sql """set enable_spm_rewrite = false"""
        List<List<Object>> rows = sql(query)
        sql """set enable_spm_rewrite = true"""
        rows
    }

    // ==================== ORDER BY without LIMIT stays at the outer query ====================
    String sortBind = "SELECT k FROM spm_r19_t1 ORDER BY k2 + 1"
    long sortId = createBaseline(sortBind)
    String sortPlanSql = planSqlOf(sortId)
    assertTrue(sortPlanSql.lastIndexOf("ORDER BY") > sortPlanSql.lastIndexOf(")"),
            "the top-level ORDER BY must sit BEHIND the derived table (an inner sort is a"
                    + " droppable hint): " + sortPlanSql)

    String sortReplay = "SELECT k FROM spm_r19_t1 ORDER BY k2 + 2"
    assertTrue(explainOf(sortReplay).contains("SPM baseline hit: id=${sortId}"),
            "the value-variant sort query must replay the baseline: " + explainOf(sortReplay))
    List<List<Object>> sortRows = sql(sortReplay)
    assertTrue(sortRows == rowsWithRewriteOff(sortReplay),
            "the replayed order must equal the direct order: " + sortRows)
    // numbers() is zero-based: the inserted keys are 0..24 and ORDER BY k2 + 1 == ORDER BY k
    assertTrue(sortRows.collect { it[0] as int } == (0..24).toList(),
            "the replayed rows must be fully ordered: " + sortRows)

    // ==================== ORDER BY + LIMIT: the user's LIMIT replaces the captured one ====================
    String topNBind = "SELECT k FROM spm_r19_t1 ORDER BY k2 + 1 LIMIT 10"
    long topNId = createBaseline(topNBind)
    String topNPlanSql = planSqlOf(topNId)
    assertTrue(topNPlanSql.trim().endsWith("LIMIT 10"),
            "the captured cap must stay reachable at the statement tail: " + topNPlanSql)
    assertTrue(topNPlanSql.lastIndexOf("ORDER BY") > topNPlanSql.lastIndexOf(")"),
            "the ORDER BY must move WITH its LIMIT: " + topNPlanSql)

    String topNReplay = "SELECT k FROM spm_r19_t1 ORDER BY k2 + 2 LIMIT 20"
    assertTrue(explainOf(topNReplay).contains("SPM baseline hit: id=${topNId}"),
            "the limit-variant query must replay the baseline: " + explainOf(topNReplay))
    List<List<Object>> topNRows = sql(topNReplay)
    assertTrue(topNRows == rowsWithRewriteOff(topNReplay),
            "the replay must adopt the USER's LIMIT instead of the captured one: " + topNRows)
    assertTrue(topNRows.size() == 20,
            "a matching LIMIT 20 must not return the captured 10 rows: " + topNRows)

    // ==================== a window exports its quoted live column ====================
    String winBind = "SELECT `a b`, ROW_NUMBER() OVER (ORDER BY k) FROM spm_r19_t2 WHERE `a b` > 5 ORDER BY k"
    long winId = createBaseline(winBind)
    String winPlanSql = planSqlOf(winId)
    assertTrue(winPlanSql.contains("`a b`"),
            "the window must export the quoted live column: " + winPlanSql)
    assertTrue(winPlanSql != winBind,
            "the builder (not the self planSql fallback) must have produced the frozen SQL: " + winPlanSql)

    String winReplay = "SELECT `a b`, ROW_NUMBER() OVER (ORDER BY k) FROM spm_r19_t2 WHERE `a b` > 25 ORDER BY k"
    assertTrue(explainOf(winReplay).contains("SPM baseline hit: id=${winId}"),
            "the value-variant window query must replay the baseline: " + explainOf(winReplay))
    List<List<Object>> winRows = sql(winReplay)
    assertTrue(winRows == rowsWithRewriteOff(winReplay),
            "the replayed window must equal the direct window: " + winRows)
    assertTrue(winRows.size() == 3 && (winRows*.get(0) as List) == [30, 40, 50],
            "the parameterized filter must keep exactly the > 25 rows: " + winRows)

    // ==================== one periodic reload: all baselines must survive it ====================
    // the refresh daemon re-parameterizes every persisted row and the snapshot is
    // AUTHORITATIVE: a row whose rebuilt trees no longer line up disappears from the
    // cache, and the frozen clauses would silently stop applying
    Thread.sleep(65000)
    assertTrue(explainOf(sortReplay).contains("SPM baseline hit: id=${sortId}"),
            "the sort baseline must survive the reload: " + explainOf(sortReplay))
    assertTrue(sql(sortReplay) == rowsWithRewriteOff(sortReplay),
            "the reloaded sort replay must keep the user's order")
    assertTrue(explainOf(topNReplay).contains("SPM baseline hit: id=${topNId}"),
            "the topN baseline must survive the reload: " + explainOf(topNReplay))
    List<List<Object>> topNRowsAfterReload = sql(topNReplay)
    assertTrue(topNRowsAfterReload == rowsWithRewriteOff(topNReplay)
                    && topNRowsAfterReload.size() == 20,
            "the reloaded topN replay must adopt the USER's LIMIT: " + topNRowsAfterReload)
    assertTrue(explainOf(winReplay).contains("SPM baseline hit: id=${winId}"),
            "the window baseline must survive the reload: " + explainOf(winReplay))
    assertTrue(sql(winReplay) == rowsWithRewriteOff(winReplay),
            "the reloaded window replay must keep the user's rows")

    // leave no baselines behind for other runs
    dropOwnBaselines()
    assertEquals(0, ownBaselines().size(), "all spm_r19_ baselines must be dropped")
}
