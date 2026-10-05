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

suite("test_spm_review_round35", "spm") {

    // Thirty-fifth review round.
    //
    // SQL-visible fixes covered here:
    //  - #3: the retained-LIMIT check must compare WHERE a cap applies, not only its
    //    value. A manual plan capping t2 passed the multiset check against a caller
    //    whose own cap of the same value sits on t1; the positional merge left the cap
    //    on t2 and the variant returned ONE row where the caller's own plan returns
    //    two. The candidate is now SKIPPED (the query plans normally) instead of being
    //    rewritten into a truncated result.
    //  - #4: the same walker visited children() only, so a cap inside a CTE body
    //    (LogicalCTE.extraPlans()) was invisible - the caller raising only the OUTER
    //    limit kept the frozen WITH body's captured cap. Also skipped now.
    //  - #5: the BARE clock keywords (CURRENT_DATE / CURRENT_TIME / CURRENT_TIMESTAMP /
    //    LOCALTIME / LOCALTIMESTAMP) parse into BOUND CurrentDate / CurrentTime / Now
    //    leaves, so the parenthesized rejections did not cover them: a baseline for
    //    'SELECT CURRENT_DATE AS d FROM t' froze the CREATE date and served it to every
    //    later matching query. CREATE GLOBAL BASELINE PLAN rejects them now.
    //  - round-41 #9: the inner-cap walk now runs even when the caller's outer LIMIT
    //    EQUALS the captured one, so the exact-limit queries of #3 / #4 skip as well
    //    (their caps have no counterpart in the caller's tree) and run their own plan.

    // SPM regression pins the fallback switch CLOSED: a rewritten-plan failure must
    // surface as an error, never silently re-run the original query.
    sql """set enable_spm_fallback = false"""
    sql """set enable_spm_rewrite = true"""

    // ==================== setup: tables (drop before use, keep after) ====================
    sql """DROP TABLE IF EXISTS spm_r35_a"""
    sql """
        CREATE TABLE spm_r35_a (k INT)
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO spm_r35_a VALUES (1)"""
    // TWO rows matching one spm_r35_a row: the join of the reviewer's example (one
    // matching t1 row x two matching t2 rows) returns two rows, one per #3 shape.
    sql """DROP TABLE IF EXISTS spm_r35_b"""
    sql """
        CREATE TABLE spm_r35_b (g INT)
        DUPLICATE KEY(g)
        DISTRIBUTED BY HASH(g) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO spm_r35_b VALUES (1), (1)"""
    sql """DROP TABLE IF EXISTS spm_r35_c"""
    sql """
        CREATE TABLE spm_r35_c (v INT)
        DUPLICATE KEY(v)
        DISTRIBUTED BY HASH(v) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO spm_r35_c VALUES (1), (2)"""

    // Global baselines are cluster-wide state and other SPM suites may run their own in
    // parallel: every SHOW here is scoped to this suite's tables and only baselines
    // matching them are dropped or asserted on.
    def ownBaselines = {
        sql("""SHOW BASELINE PLANS""").findAll {
            it[1].toString().contains("spm_r35_")
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
        // ==================== #3: a cap on ANOTHER input is not justified ====================
        // bind caps spm_r35_a (the caller's own placement); the manual plan caps
        // spm_r35_b instead. The (limit, offset) VALUES match on both sides, so the old
        // multiset check accepted the pairing.
        String capOnA = "SELECT b.g FROM (SELECT k FROM spm_r35_a ORDER BY k LIMIT 1) x" +
                " JOIN spm_r35_b b ON x.k = b.g ORDER BY b.g LIMIT 1"
        String capOnB = "SELECT a.k FROM spm_r35_a a" +
                " JOIN (SELECT g FROM spm_r35_b ORDER BY g LIMIT 1) y ON a.k = y.g" +
                " ORDER BY a.k LIMIT 1"
        List<List<Object>> created3 = sql(
                """CREATE GLOBAL BASELINE PLAN '${capOnA}' WITH '${capOnB}'""")
        assertEquals(1, created3.size(), "CREATE should return one row, got: ${created3}")
        long id3 = Long.parseLong(created3[0][0].toString())
        // round-41 #9: an inner cap of the manual plan that has NO counterpart in the
        // caller's tree skips the candidate even at the captured outer limit - the
        // frozen cap on spm_r35_b would truncate the wrong input
        assertTrue(!explainOf(capOnA).contains("SPM baseline hit: id=${id3}"),
                "a cap on ANOTHER input must skip the candidate: ${explainOf(capOnA)}")
        order_qt_r35_moved_cap_exact """
            SELECT b.g FROM (SELECT k FROM spm_r35_a ORDER BY k LIMIT 1) x
            JOIN spm_r35_b b ON x.k = b.g ORDER BY b.g LIMIT 1
        """

        // the VARIANT (outer limit 2) must NOT be rewritten: the frozen cap on
        // spm_r35_b would truncate the join to one row while the caller's own plan
        // (cap on spm_r35_a) returns two
        String variantOnB = capOnA.replace("ORDER BY b.g LIMIT 1", "ORDER BY b.g LIMIT 2")
        assertTrue(!explainOf(variantOnB).contains("SPM baseline hit"),
                "a cap on ANOTHER input must skip the candidate: ${explainOf(variantOnB)}")
        order_qt_r35_misplaced_cap """
            SELECT b.g FROM (SELECT k FROM spm_r35_a ORDER BY k LIMIT 1) x
            JOIN spm_r35_b b ON x.k = b.g ORDER BY b.g LIMIT 2
        """

        // ==================== #4: a cap inside a CTE body is seen ====================
        // bind's WITH body has NO cap; the manual plan's body caps the CTE at one row.
        // The caller raising only the OUTER limit must not keep that body cap.
        String cteNoBodyCap = "WITH c AS (SELECT v FROM spm_r35_c)" +
                " SELECT v FROM c ORDER BY v LIMIT 1"
        String cteWithBodyCap = "WITH c AS (SELECT v FROM spm_r35_c ORDER BY v LIMIT 1)" +
                " SELECT v FROM c ORDER BY v LIMIT 1"
        List<List<Object>> created4 = sql(
                """CREATE GLOBAL BASELINE PLAN '${cteNoBodyCap}' WITH '${cteWithBodyCap}'""")
        assertEquals(1, created4.size(), "CREATE should return one row, got: ${created4}")
        long id4 = Long.parseLong(created4[0][0].toString())
        // round-41 #9: the manual plan's CTE BODY cap has no counterpart in the
        // caller's tree, so the exact query skips the candidate as well
        assertTrue(!explainOf(cteNoBodyCap).contains("SPM baseline hit: id=${id4}"),
                "a cap inside the WITH body must skip the candidate:" +
                        " ${explainOf(cteNoBodyCap)}")
        order_qt_r35_cte_body_cap_exact """
            WITH c AS (SELECT v FROM spm_r35_c)
            SELECT v FROM c ORDER BY v LIMIT 1
        """

        String cteVariant = cteNoBodyCap.replace("ORDER BY v LIMIT 1", "ORDER BY v LIMIT 2")
        assertTrue(!explainOf(cteVariant).contains("SPM baseline hit"),
                "a cap inside the WITH body must skip the candidate: ${explainOf(cteVariant)}")
        order_qt_r35_cte_body_cap """
            WITH c AS (SELECT v FROM spm_r35_c)
            SELECT v FROM c ORDER BY v LIMIT 2
        """

        // ==================== #5: bare clock keywords are rejected ====================
        // Each of these was constant-folded at CREATE time, freezing the statement start
        // time (its date) into the persisted planSql and serving it to every later
        // matching query.
        test {
            sql """CREATE GLOBAL BASELINE PLAN 'SELECT CURRENT_DATE AS d FROM spm_r35_a' WITH 'SELECT CURRENT_DATE AS d FROM spm_r35_a'"""
            exception "replay-time context"
        }
        test {
            sql """CREATE GLOBAL BASELINE PLAN 'SELECT CURRENT_TIME AS d FROM spm_r35_a' WITH 'SELECT CURRENT_TIME AS d FROM spm_r35_a'"""
            exception "replay-time context"
        }
        test {
            sql """CREATE GLOBAL BASELINE PLAN 'SELECT CURRENT_TIMESTAMP AS d FROM spm_r35_a' WITH 'SELECT CURRENT_TIMESTAMP AS d FROM spm_r35_a'"""
            exception "replay-time context"
        }
        test {
            sql """CREATE GLOBAL BASELINE PLAN 'SELECT LOCALTIME AS d FROM spm_r35_a' WITH 'SELECT LOCALTIME AS d FROM spm_r35_a'"""
            exception "replay-time context"
        }
        test {
            sql """CREATE GLOBAL BASELINE PLAN 'SELECT LOCALTIMESTAMP AS d FROM spm_r35_a' WITH 'SELECT LOCALTIMESTAMP AS d FROM spm_r35_a'"""
            exception "replay-time context"
        }
    } finally {
        dropOwnBaselines()
    }
}
