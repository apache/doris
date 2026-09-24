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

suite("test_spm_digest_whitespace", "spm") {

    // SPM digest whitespace insensitivity regression.
    //
    // The matching key (bind_sql_digest / bind_sql_hash) is rendered from the PARSED
    // plan tree - never from the SQL text. Spaces / newlines / TABs / CRs / comments
    // are lexical separators of the parser and can never reach the tree, so a query
    // that differs from the baseline only in such characters produces the SAME digest,
    // lands in the same Level 1 hash bucket, passes the Level 2 digest comparison and
    // is matched by the Level 3 structural check; it must hit the baseline exactly
    // like the canonical text (and return exactly the same result as without SPM).
    //
    // Verified here end to end:
    // 1. CREATE with a whitespace-mangled text de-duplicates onto the canonical
    //    baseline (same hash + digest + frozen text) - the engine itself proves the
    //    digests are equal.
    // 2. After dropping the canonical baseline, a fresh CREATE from the mangled text
    //    yields the identical digest STRING as the canonical text.
    // 3. EXPLAIN of every whitespace / comment variant (and of a value-changed
    //    variant) reports the baseline hit.
    // 4. SPM on/off results are identical for the mangled and the value-changed
    //    variants.

    // SPM regression pins the fallback switch CLOSED: a rewritten-plan failure must
    // surface as an error, never silently re-run the original query.
    sql 'set enable_spm_fallback=false'

    // ==================== setup: table (drop before use, keep after) ====================
    sql """DROP TABLE IF EXISTS spm_ws_t"""
    sql """
        CREATE TABLE spm_ws_t (
            k1 INT,
            k2 INT,
            v INT
        )
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO spm_ws_t VALUES (1, 2, 10), (1, 2, 20), (2, 4, 40)"""

    // canonical and whitespace-mangled variants of ONE query (identical tokens)
    String canonicalSql = "select k1, k2, sum(v) as total from spm_ws_t " +
            "where k1 = 1 and k2 = 2 group by k1, k2"
    String mangledSql = "select\tk1,\tk2,\tSUM(v)\tas\ttotal\n" +
            "from\tspm_ws_t\n" +
            "where\tk1\t=\t1\r\n" +
            "\tand\tk2\t=\t2\n" +
            "group\tby\tk1,\tk2"
    // comment variant: line comment + block comment
    String commentSql = "select k1, k2, sum(v) as total /* block comment */ from spm_ws_t " +
            "where k1 = 1 -- line comment\nand k2 = 2 group by k1, k2"
    // value-changed variant of the SAME structure (must match as well - value-free digest)
    String changedValueSql = "select k1, k2, sum(v) as total from spm_ws_t " +
            "where k1 = 2 and k2 = 4 group by k1, k2"

    // ==================== cleanup: drop this suite's leftover baselines ====================
    // Global baselines are cluster-wide state and other SPM suites may run in parallel:
    // every SHOW here is scoped to this suite's table (spm_ws_t).
    def ownBaselines = {
        sql("""SHOW BASELINE PLANS""").findAll { it[1].toString().contains("spm_ws_t") }
    }
    def dropOwnBaselines = {
        ownBaselines().each { row ->
            sql """DROP BASELINE PLAN ${row[0]}"""
        }
    }
    def createStmt = { String text ->
        'CREATE GLOBAL BASELINE PLAN "' + text.replace('"', '\\"') +
                '" WITH "' + text.replace('"', '\\"') + '"'
    }
    def digestOf = { long id ->
        sql("""SELECT bind_sql_digest FROM __internal_schema.spm_baselines WHERE id = ${id}""")[0][0]
                .toString()
    }
    dropOwnBaselines()
    assertEquals(0, ownBaselines().size(), "no spm_ws_t baseline should be left after cleanup")

    try {
        // ==================== CREATE canonical baseline ====================
        List<List<Object>> canonicalRes = sql(createStmt(canonicalSql))
        assertEquals(1, canonicalRes.size(), "CREATE should return one row, got: ${canonicalRes}")
        long canonicalId = Long.parseLong(canonicalRes[0][0].toString())
        String canonicalDigest = digestOf(canonicalId)

        // ==================== the mangled text is an exact duplicate (same hash+digest+frozen text) ====================
        List<List<Object>> mangledDupRes = sql(createStmt(mangledSql))
        assertEquals(canonicalId, Long.parseLong(mangledDupRes[0][0].toString()),
                "whitespace-only variant must de-duplicate onto the canonical baseline " +
                        "(same hash + digest + frozen plan text), got: ${mangledDupRes}")

        // ==================== digest string equality (fresh baseline from the mangled text) ====================
        sql """DROP BASELINE PLAN ${canonicalId}"""
        List<List<Object>> mangledRes = sql(createStmt(mangledSql))
        long id = Long.parseLong(mangledRes[0][0].toString())
        String mangledDigest = digestOf(id)
        assertEquals(canonicalDigest, mangledDigest,
                "whitespace-only differences must not change the digest string")

        // ==================== match verification: every variant must hit the baseline ====================
        sql 'set enable_spm_rewrite=true'
        sql 'set spm_rewrite_timeout_ms=60000'
        def canonicalHit = sql("EXPLAIN " + canonicalSql)
        def mangledHit = sql("EXPLAIN " + mangledSql)
        def commentHit = sql("EXPLAIN " + commentSql)
        def changedHit = sql("EXPLAIN " + changedValueSql)
        assertTrue(canonicalHit.toString().contains("SPM baseline hit: id=" + id),
                "canonical text must hit the baseline, got: " + canonicalHit)
        assertTrue(mangledHit.toString().contains("SPM baseline hit: id=" + id),
                "whitespace-mangled text must hit the same baseline, got: " + mangledHit)
        assertTrue(commentHit.toString().contains("SPM baseline hit: id=" + id),
                "comment variant must hit the same baseline, got: " + commentHit)
        assertTrue(changedHit.toString().contains("SPM baseline hit: id=" + id),
                "value-changed variant must hit the same baseline, got: " + changedHit)

        // ==================== result equality with SPM on/off ====================
        def mangledWithSpm = sql mangledSql
        sql 'set enable_spm_rewrite=false'
        def mangledWithoutSpm = sql mangledSql
        assertEquals(mangledWithSpm, mangledWithoutSpm,
                "SPM rewrite must preserve the result of the whitespace-mangled query")
        sql 'set enable_spm_rewrite=true'
        def changedWithSpm = sql changedValueSql
        sql 'set enable_spm_rewrite=false'
        def changedWithoutSpm = sql changedValueSql
        assertEquals(changedWithSpm, changedWithoutSpm,
                "SPM rewrite must preserve the result of the value-changed query")
        // the frozen plan really replayed the rewritten (baseline) structure
        assertEquals([[1, 2, 30L]], mangledWithoutSpm,
                "the whitespace-mangled query must aggregate the two source rows")

        // ==================== print the (value-free) digest of the final baseline ====================
        // deterministic: rendered from the parsed tree, no ids / whitespace / values
        order_qt_spm_bind_digest """
            SELECT bind_sql_digest FROM __internal_schema.spm_baselines WHERE id = ${id}
        """
    } finally {
        dropOwnBaselines()
    }
    assertEquals(0, ownBaselines().size(), "own baselines must be cleaned up")
}
