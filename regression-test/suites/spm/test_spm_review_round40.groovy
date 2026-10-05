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

suite("test_spm_review_round40", "spm") {

    // Fortieth review round.
    //
    // SQL-visible fixes covered here:
    //  - #1: the retained-LIMIT guard now identifies a cap by its OCCURRENCE (the
    //    derived-alias / set-operand chain), not only by (limit, offset, relations):
    //    a manual plan that moved ORDER BY k LIMIT 1 to another same-table occurrence
    //    can no longer be replayed for a caller whose own cap sits elsewhere - the
    //    candidate is SKIPPED and the caller's query returns its own rows.
    //  - #6: the sequence identity column is an unbounded STRING now; a bind whose
    //    canonical digest exceeds 4096 chars used to fail the reservation INSERT, so a
    //    valid GLOBAL CREATE errored before writing anything.
    //  - #11: OLAP_SCAN_PARTITION_PRUNE is excluded at CREATE time; it used to freeze
    //    the CURRENT partition set (even as an empty relation / WHERE FALSE), while an
    //    ADD PARTITION changes nothing the schema fingerprint hashes - the same query
    //    then kept returning zero rows instead of the new partition's rows.
    //  - #13: ELIMINATE_GROUP_BY is excluded at CREATE time; a DECLARED UNIQUE key made
    //    it freeze a row-wise projection in place of the aggregate, which is wrong once
    //    the constraint is dropped and a duplicate key is inserted.

    // SPM regression pins the fallback switch CLOSED: a rewritten-plan failure must
    // surface as an error, never silently re-run the original query.
    sql """set enable_spm_fallback = false"""
    sql """set enable_spm_rewrite = true"""

    // ==================== setup: tables (drop before use, keep after) ====================
    sql """DROP TABLE IF EXISTS spm_r40_t"""
    sql """
        CREATE TABLE spm_r40_t (k INT)
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO spm_r40_t VALUES (1), (2)"""

    // Global baselines are cluster-wide state and other SPM suites may run their own in
    // parallel: every SHOW here is scoped to this suite's tables and only baselines
    // matching them are dropped or asserted on.
    def ownBaselines = {
        sql("""SHOW BASELINE PLANS""").findAll {
            it[1].toString().contains("spm_r40_")
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
        // ==================== #1: a cap moved between same-table occurrences ====================
        // bind caps the derived table aliased a (the caller's own placement); the manual
        // plan moved the same ORDER BY k LIMIT 1 to the derived table aliased b while
        // both read spm_r40_t - the old (limit, offset, relations) key could not tell
        // the two inner caps apart.
        String capOnA = "SELECT a.k AS ak, b.k AS bk" +
                " FROM (SELECT k FROM spm_r40_t ORDER BY k LIMIT 1) a" +
                " CROSS JOIN (SELECT k FROM spm_r40_t) b ORDER BY ak, bk LIMIT 1"
        String capOnB = "SELECT a.k AS ak, b.k AS bk" +
                " FROM (SELECT k FROM spm_r40_t) a" +
                " CROSS JOIN (SELECT k FROM spm_r40_t ORDER BY k LIMIT 1) b" +
                " ORDER BY ak, bk LIMIT 1"
        List<List<Object>> created1 = sql(
                """CREATE GLOBAL BASELINE PLAN '${capOnA}' WITH '${capOnB}'""")
        assertEquals(1, created1.size(), "CREATE should return one row, got: ${created1}")
        long id1 = Long.parseLong(created1[0][0].toString())
        // the EXACT captured limit is the baseline's own contract: still hit
        assertTrue(explainOf(capOnA).contains("SPM baseline hit: id=${id1}"),
                "the exact-limit query must keep hitting its baseline: ${explainOf(capOnA)}")

        // the VARIANT (outer limit 1 -> 2) must NOT be rewritten: the frozen cap on the
        // b occurrence would truncate the wrong side (a={1,2}, b={1} instead of a={1},
        // b={1,2}) and return (1,1),(2,1) instead of (1,1),(1,2)
        String variant1 = capOnA.replace("ORDER BY ak, bk LIMIT 1", "ORDER BY ak, bk LIMIT 2")
        assertTrue(!explainOf(variant1).contains("SPM baseline hit"),
                "a cap on ANOTHER occurrence must skip the candidate: ${explainOf(variant1)}")
        order_qt_r40_moved_cap """
            SELECT a.k AS ak, b.k AS bk
            FROM (SELECT k FROM spm_r40_t ORDER BY k LIMIT 1) a
            CROSS JOIN (SELECT k FROM spm_r40_t) b
            ORDER BY ak, bk LIMIT 2
        """

        // ==================== #6: a bind whose digest exceeds the old VARCHAR ====================
        // The canonical digest renders every literal as '?' but keeps every expression /
        // output name, so hundreds of projected expressions push it past 4096 chars; the
        // reservation INSERT used to fail before the baseline row was written.
        String projections = (1..600).collect { "k + ${it} AS c${it}" }.join(", ")
        String longBind = "SELECT ${projections} FROM spm_r40_t WHERE k = 1"
        List<List<Object>> created6 = sql(
                """CREATE GLOBAL BASELINE PLAN '${longBind}' WITH '${longBind}'""")
        assertEquals(1, created6.size(),
                "a bind whose digest exceeds 4096 chars must create successfully, got: ${created6}")
        long id6 = Long.parseLong(created6[0][0].toString())
        assertTrue(explainOf(longBind).contains("SPM baseline hit: id=${id6}"),
                "the long-bind baseline must be usable: ${explainOf(longBind)}")

        // ==================== #11: partition pruning must not freeze growth ====================
        sql """DROP TABLE IF EXISTS spm_r40_p"""
        sql """
            CREATE TABLE spm_r40_p (p INT)
            PARTITION BY LIST (p) (PARTITION p1 VALUES IN (1))
            DISTRIBUTED BY HASH(p) BUCKETS 1
            PROPERTIES("replication_num" = "1")
        """
        sql """INSERT INTO spm_r40_p VALUES (1)"""
        String pruneSql = "SELECT p FROM spm_r40_p WHERE p = 99"
        List<List<Object>> created11 = sql(
                """CREATE GLOBAL BASELINE PLAN '${pruneSql}' WITH '${pruneSql}'""")
        assertEquals(1, created11.size(), "CREATE should return one row, got: ${created11}")
        long id11 = Long.parseLong(created11[0][0].toString())
        // nothing matches yet (the partition set has no 99 and there is no row)
        assertTrue(explainOf(pruneSql).contains("SPM baseline hit: id=${id11}"),
                "the pruned query must hit its baseline: ${explainOf(pruneSql)}")
        order_qt_r40_prune_before """ SELECT p FROM spm_r40_p WHERE p = 99 """

        // the partition set grows WITHOUT changing the table id / base schema the
        // fingerprint hashes - the same query must return the new partition's row
        sql """ALTER TABLE spm_r40_p ADD PARTITION p99 VALUES IN (99)"""
        sql """INSERT INTO spm_r40_p VALUES (99)"""
        assertTrue(explainOf(pruneSql).contains("SPM baseline hit: id=${id11}"),
                "the baseline keeps matching after the partition growth: ${explainOf(pruneSql)}")
        order_qt_r40_prune_after """ SELECT p FROM spm_r40_p WHERE p = 99 """

        // ==================== #13: GROUP BY elimination must not freeze uniqueness ====================
        sql """DROP TABLE IF EXISTS spm_r40_u"""
        sql """
            CREATE TABLE spm_r40_u (k INT NOT NULL, v INT)
            DUPLICATE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES("replication_num" = "1")
        """
        sql """INSERT INTO spm_r40_u VALUES (1, 10)"""
        // the declared UNIQUE key is what DataTrait uses to eliminate the GROUP BY
        sql """ALTER TABLE spm_r40_u ADD CONSTRAINT uk_spm_r40 UNIQUE (k)"""
        String aggSql = "SELECT k, SUM(v) AS s FROM spm_r40_u GROUP BY k ORDER BY s"
        List<List<Object>> created13 = sql(
                """CREATE GLOBAL BASELINE PLAN '${aggSql}' WITH '${aggSql}'""")
        assertEquals(1, created13.size(), "CREATE should return one row, got: ${created13}")
        long id13 = Long.parseLong(created13[0][0].toString())
        assertTrue(explainOf(aggSql).contains("SPM baseline hit: id=${id13}"),
                "the aggregate query must hit its baseline: ${explainOf(aggSql)}")

        // drop the constraint the elimination depended on and add a duplicate key: the
        // frozen plan must still AGGREGATE (one 30 row), not return two raw rows
        sql """ALTER TABLE spm_r40_u DROP CONSTRAINT uk_spm_r40"""
        sql """INSERT INTO spm_r40_u VALUES (1, 20)"""
        assertTrue(explainOf(aggSql).contains("SPM baseline hit: id=${id13}"),
                "the baseline keeps matching after the constraint drop: ${explainOf(aggSql)}")
        order_qt_r40_uniqueness """ SELECT k, SUM(v) AS s FROM spm_r40_u GROUP BY k ORDER BY s """
    } finally {
        dropOwnBaselines()
    }
}
