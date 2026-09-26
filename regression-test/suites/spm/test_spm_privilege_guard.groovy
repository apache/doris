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

suite("test_spm_privilege_guard", "spm") {

    // Security regression guard for the SPM rewrite path when the rewritten plan
    // fails at planning.
    //
    // Scenario: an admin authors a GLOBAL baseline whose frozen planSql reads a table
    // the LOW-PRIVILEGE user may access (spm_guard_pub) while its bindSql matches the
    // same user's query on a table they may NOT access (spm_guard_secret). A block rule
    // bound to that user is arranged to trip on the REWRITTEN plan only AFTER its
    // privilege check has passed.
    //
    // SPM regression pins enable_spm_fallback CLOSED (its default): the rewritten plan
    // failure must surface as an error and the ORIGINAL statement must never be
    // re-planned or executed, so the low-privilege user cannot reach spm_guard_secret
    // through the baseline.
    //
    // Historical note: when the fallback is opted into (production availability), it
    // must re-authorize the original statement instead of inheriting the abandoned
    // rewrite's privilege result: before the 2026-09-16 fix StmtExecutor reused the
    // statement's privChecked flag and the fallback executed the secret query without
    // any privilege check (data leak, reproduced live 2026-09-16). The re-check stays
    // in the product for that opt-in path but is not exercised here, because the
    // switch is closed in regression.

    // pin the fallback switch CLOSED for this connection and lock its default
    sql """SET enable_spm_fallback = false"""
    def fallbackVar = sql("""SHOW VARIABLES LIKE 'enable_spm_fallback'""")
    assertTrue(fallbackVar.size() >= 1
                    && "false".equalsIgnoreCase(fallbackVar[0][1].toString()),
            "enable_spm_fallback must default to false (SPM fallback CLOSED), got: ${fallbackVar}")

    String guardDb = "spm_guard_db"
    String pubTable = "spm_guard_db.spm_guard_pub"
    String secretTable = "spm_guard_db.spm_guard_secret"
    String userName = "spm_guard_u"
    String userPwd = "spm_guard_1"
    String ruleName = "spm_guard_rule"
    long baselineId = -1

    // ==================== setup (root) ====================
    try_sql("DROP USER IF EXISTS '${userName}'")
    try_sql("DROP SQL_BLOCK_RULE IF EXISTS ${ruleName}")
    sql """CREATE DATABASE IF NOT EXISTS ${guardDb}"""
    sql """DROP TABLE IF EXISTS ${pubTable}"""
    sql """DROP TABLE IF EXISTS ${secretTable}"""
    // pub: partitioned, and the frozen plan filters only on k -> the plan has no
    // partition predicate, so the user-bound require_partition_filter rule trips
    sql """
        CREATE TABLE ${pubTable} (k INT, p INT)
        PARTITION BY RANGE(p) (PARTITION p1 VALUES LESS THAN ('10'), PARTITION p2 VALUES LESS THAN ('20'))
        DISTRIBUTED BY HASH(k) BUCKETS 4
        PROPERTIES("replication_num" = "1")
    """
    sql """
        CREATE TABLE ${secretTable} (k INT)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO ${pubTable} VALUES (7, 1), (2000, 2)"""
    sql """INSERT INTO ${secretTable} VALUES (1), (2), (3000)"""

    sql """CREATE USER '${userName}' IDENTIFIED BY '${userPwd}'"""
    sql """GRANT SELECT_PRIV ON ${pubTable} TO '${userName}'"""

    try {
        // admin-authored baseline: bind on the SECRET table, frozen plan on the PUB table
        List<List<Object>> created = sql """
            CREATE GLOBAL BASELINE PLAN 'select k from ${secretTable} where k = 1'
            WITH 'select k from ${pubTable} where k = 1'
        """
        baselineId = Long.parseLong(created[0][0].toString())

        // pin the path: the rewrite must really match this query shape
        sql """SET enable_spm_rewrite = true"""
        String explain = sql("""EXPLAIN SELECT k FROM ${secretTable} WHERE k = 111""").toString()
        assertTrue(explain.contains("SPM baseline hit: id=${baselineId}"),
                "the baseline must match the guarded query shape, got: ${explain}")
        sql """SET enable_spm_rewrite = false"""

        // block rule bound to the guarded user only (no cluster-wide effect); it trips
        // the rewritten pub-table plan at the post-plan scan check, after privileges
        sql """CREATE SQL_BLOCK_RULE ${ruleName} PROPERTIES(
            'require_partition_filter' = 'true', 'global' = 'false', 'enable' = 'true'
        )"""
        sql """SET PROPERTY FOR '${userName}' 'sql_block_rules' = '${ruleName}'"""

        // the low-privilege user connects to the database where its only grant lives
        String guardUrl = context.config.jdbcUrl.replaceFirst("(://[^/]+)/[^?]*", '$1/' + guardDb)

        connect(userName, "${userPwd}", guardUrl) {
            // control: without SPM the secret table is properly protected
            test {
                sql """SELECT k FROM ${secretTable} WHERE k = 3000"""
                exception "denied"
            }
            // sanity: the user-bound rule does trip on the pub-table plan
            test {
                sql """SELECT k FROM ${pubTable} WHERE k = 7"""
                exception "block rule"
            }
            // the guard: the rewrite matches, the rewritten (pub) plan passes its own
            // privilege check, then the user-bound block rule trips the plan; with the
            // fallback CLOSED the ORIGINAL secret query is never re-planned/executed -
            // the query surfaces the rewrite failure and leaks nothing
            sql """SET enable_spm_rewrite = true"""
            sql """SET enable_spm_fallback = false"""
            test {
                sql """SELECT k FROM ${secretTable} WHERE k = 3000"""
                exception "SPM rewritten plan failed"
            }
        }
    } finally {
        // drop the shared/global state; keep the tables for debugging (drop-before-use)
        if (baselineId > 0) {
            try_sql("DROP BASELINE PLAN IF EXISTS ${baselineId}")
        }
        try_sql("DROP SQL_BLOCK_RULE IF EXISTS ${ruleName}")
        try_sql("DROP USER IF EXISTS '${userName}'")
    }
}
