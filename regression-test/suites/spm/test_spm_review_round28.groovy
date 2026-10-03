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

suite("test_spm_review_round28", "spm") {

    // Twenty-eighth review round: the creator's row policy must never enter the frozen
    // SQL - including for a relation INSIDE A SUBQUERY.
    //
    //  - #1: for "SELECT k FROM t WHERE k IN (SELECT k FROM u)" the nested relation's
    //    LogicalCheckPolicy marker lives in SubqueryExpr.queryPlan, which the
    //    children-only walk of SPMPlanTreeSupport#stripCheckPolicy never visited. CREATE's
    //    nested analyzer expanded the CREATOR's policy on u into an ordinary filter inside
    //    the frozen SQL, so a GLOBAL baseline authored by a non-root ADMIN served every
    //    other user the creator's row filter (here: a policy that keeps only k = 1).
    //    The strip now recurses into expression-owned plans, so the frozen plan is
    //    policy-free and every replay re-applies the EXECUTING user's own policy.
    //
    // #2 (absolute create/update times), #3 (post-forward sync failure invalidates the
    // cache), #4 / #7 (DST-safe audit scan windows) and #6 (paginated snapshot read) are
    // covered by BaselineManagerConcurrencyTest, BaselinePlanDurableWinnerTest and
    // AuditLogScannerCursorTest: none of them has a SQL surface a single-node suite can
    // drive deterministically.

    sql """set enable_spm_rewrite = true"""
    sql """set enable_spm_fallback = false"""

    String db = "spm_r28_db"
    String creator = "spm_r28_creator"
    String caller = "spm_r28_caller"
    String callerPolicyUser = "spm_r28_caller_policy"
    String pwd = "spm_r28_Pwd123"

    def ownBaselines = {
        sql("""SHOW BASELINE PLANS""").findAll { it[1].toString().contains("spm_r28_") }
    }
    def dropOwnBaselines = {
        ownBaselines().each { row ->
            sql """DROP BASELINE PLAN ${row[0]}"""
        }
    }
    def dropFixtures = {
        dropOwnBaselines()
        sql """DROP ROW POLICY IF EXISTS spm_r28_creator_allow ON ${db}.spm_r28_u FOR '${creator}'"""
        sql """DROP ROW POLICY IF EXISTS spm_r28_creator_filter ON ${db}.spm_r28_u FOR '${creator}'"""
        sql """DROP ROW POLICY IF EXISTS spm_r28_caller_allow ON ${db}.spm_r28_u FOR '${callerPolicyUser}'"""
        sql """DROP ROW POLICY IF EXISTS spm_r28_caller_filter ON ${db}.spm_r28_u FOR '${callerPolicyUser}'"""
        sql """DROP USER IF EXISTS '${creator}'"""
        sql """DROP USER IF EXISTS '${caller}'"""
        sql """DROP USER IF EXISTS '${callerPolicyUser}'"""
        sql """DROP DATABASE IF EXISTS ${db}"""
    }

    dropFixtures()
    sql """CREATE DATABASE ${db}"""
    sql """
        CREATE TABLE ${db}.spm_r28_t (k INT)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """
        CREATE TABLE ${db}.spm_r28_u (k INT)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO ${db}.spm_r28_t VALUES (1), (2)"""
    sql """INSERT INTO ${db}.spm_r28_u VALUES (1), (2)"""

    sql """CREATE USER '${creator}' IDENTIFIED BY '${pwd}'"""
    sql """CREATE USER '${caller}' IDENTIFIED BY '${pwd}'"""
    sql """CREATE USER '${callerPolicyUser}' IDENTIFIED BY '${pwd}'"""
    sql """GRANT ADMIN_PRIV ON *.*.* TO '${creator}'"""
    sql """GRANT SELECT_PRIV ON ${db}.spm_r28_t TO '${caller}'"""
    sql """GRANT SELECT_PRIV ON ${db}.spm_r28_u TO '${caller}'"""
    sql """GRANT SELECT_PRIV ON ${db}.spm_r28_t TO '${callerPolicyUser}'"""
    sql """GRANT SELECT_PRIV ON ${db}.spm_r28_u TO '${callerPolicyUser}'"""

    // the creator keeps only k = 1; the policy-taking caller keeps only k = 2 (a
    // permissive "all rows" policy is added so the restrictive one is the only filter)
    sql """CREATE ROW POLICY spm_r28_creator_allow ON ${db}.spm_r28_u AS PERMISSIVE TO '${creator}' USING (k IN (1, 2))"""
    sql """CREATE ROW POLICY spm_r28_creator_filter ON ${db}.spm_r28_u AS RESTRICTIVE TO '${creator}' USING (k = 1)"""
    sql """CREATE ROW POLICY spm_r28_caller_allow ON ${db}.spm_r28_u AS PERMISSIVE TO '${callerPolicyUser}' USING (k IN (1, 2))"""
    sql """CREATE ROW POLICY spm_r28_caller_filter ON ${db}.spm_r28_u AS RESTRICTIVE TO '${callerPolicyUser}' USING (k = 2)"""

    String query = "SELECT k FROM ${db}.spm_r28_t" +
            " WHERE k IN (SELECT k FROM ${db}.spm_r28_u)"
    String url = org.apache.doris.regression.Config.buildUrlWithDb(
            context.config.jdbcUrl, db)

    try {
        long baselineId
        connect(creator, pwd, url) {
            sql """SET enable_spm_rewrite = true"""
            sql """SET enable_spm_fallback = false"""
            // control: the creator's own ordinary query applies its policy
            assertEquals([[1]], sql("SELECT k FROM ${db}.spm_r28_u ORDER BY k"))
            List<List<Object>> created = sql(
                    """CREATE GLOBAL BASELINE PLAN '${query}' WITH '${query}'""")
            baselineId = Long.parseLong(created[0][0].toString())
            String explain = sql("""EXPLAIN ${query}""").toString()
            assertTrue(explain.contains("SPM baseline hit: id=${baselineId}"), explain)
            assertEquals([[1]], sql(query),
                    "the creator's own replay re-applies its policy")
        }

        // the plain caller has NO policy: the whole (1, 2) of u must be visible, i.e. the
        // creator's k = 1 filter must not have been frozen into the shared baseline
        connect(caller, pwd, url) {
            sql """SET enable_spm_rewrite = true"""
            sql """SET enable_spm_fallback = false"""
            String explain = sql("""EXPLAIN ${query}""").toString()
            assertTrue(explain.contains("SPM baseline hit: id=${baselineId}"), explain)
            assertEquals([[1], [2]], sql(query).sort(),
                    "a user without a policy must not inherit the creator's row filter")
        }

        // a caller WITH its own policy keeps exactly that policy (not the creator's)
        connect(callerPolicyUser, pwd, url) {
            sql """SET enable_spm_rewrite = true"""
            sql """SET enable_spm_fallback = false"""
            String explain = sql("""EXPLAIN ${query}""").toString()
            assertTrue(explain.contains("SPM baseline hit: id=${baselineId}"), explain)
            assertEquals([[2]], sql(query),
                    "the replay must apply the EXECUTING user's own policy")
        }
    } finally {
        dropFixtures()
    }
}
