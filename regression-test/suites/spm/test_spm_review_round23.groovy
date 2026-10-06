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

import org.apache.doris.regression.util.JdbcUtils

suite("test_spm_review_round23", "spm") {

    // E2e coverage for the derived-label contract, the
    // create-time plan alignment and the schema-fingerprint nullability.
    //
    // Covered here:
    //  - a DERIVED output label pins the captured expression text (the frozen sink emits
    //    it as an explicit alias): a value variant inside the output expression stays
    //    matchable, and the replay hands the CALLER's own header back (asserted through
    //    the JDBC column label below), while an explicit alias keeps pinning the alias
    //  - a manual plan whose literals parameterize under ids the bind side never
    //    supplies (a renamed table alias) is rejected at CREATE with a clear error
    //  - a column's NULLABILITY is part of the schema fingerprint: MODIFY COLUMN v INT
    //    NULL invalidates a baseline whose NOT NULL elimination was frozen away
    //
    // #1/#2/#3/#4/#5/#6/#9/#10 are covered by AuditLogScannerCursorTest /
    // BaselineManagerConcurrencyTest / SPMMatchingSafetyTest / SPMRound18SafetyTest /
    // SPMOptimizerTest / SPMRound15SafetyTest.

    sql """set enable_spm_rewrite = true"""
    sql """set enable_spm_fallback = false"""

    def ownBaselines = {
        sql("""SHOW BASELINE PLANS""").findAll { it[1].toString().contains("spm_r23_") }
    }
    def dropOwnBaselines = {
        ownBaselines().each { row ->
            sql """DROP BASELINE PLAN ${row[0]}"""
        }
    }
    dropOwnBaselines()

    def explainOf = { String query -> sql("""EXPLAIN ${query}""").toString() }
    def createBaseline = { String text ->
        (sql('CREATE GLOBAL BASELINE PLAN "' + text + '" WITH "' + text + '"')[0][0] as Long)
    }
    def rowsWithRewriteOff = { String query ->
        sql """set enable_spm_rewrite = false"""
        List<List<Object>> rows = sql(query)
        sql """set enable_spm_rewrite = true"""
        rows
    }

    // ==================== setup ====================
    sql """DROP TABLE IF EXISTS spm_r23_t"""
    sql """
        CREATE TABLE spm_r23_t (k INT)
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO spm_r23_t VALUES (1), (2), (3)"""

    sql """DROP TABLE IF EXISTS spm_r23_n"""
    sql """
        CREATE TABLE spm_r23_n (k INT, v INT NOT NULL)
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO spm_r23_n VALUES (1, 10), (2, 20)"""

    // ==================== #7: a derived output label follows the CALLER ====================
    String derivedBind = "SELECT k + 1 FROM spm_r23_t"
    long derivedId = createBaseline(derivedBind)
    assertTrue(explainOf(derivedBind).contains("SPM baseline hit: id=${derivedId}"),
            "the identical derived-label query must still replay: " + explainOf(derivedBind))

    // a value variant must STILL hit (the matcher compares the parameterized expression)
    // and must report the caller's own derived header: the frozen sink pinned the
    // CAPTURED label (`k + 1`), which the protocol would have sent as the column label.
    // The metadata call below must run the EXACT hit query - an added ORDER BY creates a
    // sort node, changes the matched plan and could observe ordinary planning instead of
    // the replay (the rows are normalized here instead of in SQL).
    String derivedVariant = "SELECT k + 2 FROM spm_r23_t"
    assertTrue(explainOf(derivedVariant).contains("SPM baseline hit: id=${derivedId}"),
            "a k + 2 variant must keep matching the captured k + 1 baseline: "
                    + explainOf(derivedVariant))
    def (derivedVariantRows, derivedVariantMeta) = JdbcUtils.executeToList(
            context.getConnection(), derivedVariant)
    assertEquals("k + 2", derivedVariantMeta.getColumnLabel(1),
            "the replayed plan must expose the CALLER's derived header, not the captured one")
    List<List<Object>> derivedRows = sql(derivedVariant)
    assertTrue(derivedRows.sort() == rowsWithRewriteOff(derivedVariant).sort(),
            "the variant must return its own rows: " + derivedRows)
    assertTrue(derivedVariantRows.sort() == rowsWithRewriteOff(derivedVariant).sort(),
            "the metadata query must return the same rows: " + derivedVariantRows)

    // an EXPLICIT alias pins the header, so the value variant stays matchable
    String aliasedBind = "SELECT k + 1 AS total FROM spm_r23_t"
    long aliasedId = createBaseline(aliasedBind)
    String aliasedVariant = "SELECT k + 2 AS total FROM spm_r23_t"
    assertTrue(explainOf(aliasedVariant).contains("SPM baseline hit: id=${aliasedId}"),
            "an explicit alias keeps value variants matchable: " + explainOf(aliasedVariant))
    assertTrue(sql(aliasedVariant) == rowsWithRewriteOff(aliasedVariant),
            "the aliased variant must still return the substituted rows");

    // ==================== #8: a misaligned manual plan is rejected ====================
    test {
        sql """CREATE GLOBAL BASELINE PLAN
                'SELECT * FROM spm_r23_t a WHERE a.k = 1'
                WITH 'SELECT * FROM spm_r23_t b WHERE b.k = 1'"""
        exception "align"
    }

    // ==================== #11: nullability invalidates a frozen NOT NULL ====================
    String notNullQuery = "SELECT k FROM spm_r23_n WHERE v IS NOT NULL"
    long notNullId = createBaseline(notNullQuery)
    assertTrue(explainOf(notNullQuery).contains("SPM baseline hit: id=${notNullId}"),
            "the NOT NULL query must hit before the schema change: " + explainOf(notNullQuery))
    assertTrue(sql(notNullQuery).size() == 2);

    sql """ALTER TABLE spm_r23_n MODIFY COLUMN v INT NULL"""
    // the nullability change is metadata-only for the FE, but the BE has to refresh the
    // tablet schema before a NULL can be loaded: retry until the row is accepted
    boolean nullRowLoaded = false
    for (int attempt = 0; attempt < 30 && !nullRowLoaded; attempt++) {
        try {
            sql """INSERT INTO spm_r23_n VALUES (3, NULL)"""
            nullRowLoaded = true
        } catch (Exception e) {
            Thread.sleep(1000)
        }
    }
    assertTrue(nullRowLoaded, "the NULL row must be loadable once the column is nullable")
    // the frozen plan dropped the filter while v was declared NOT NULL, so the changed
    // declaration must retire the baseline
    assertFalse(explainOf(notNullQuery).contains("SPM baseline hit: id=${notNullId}"),
            "the nullability change must invalidate the frozen NOT NULL elimination: "
                    + explainOf(notNullQuery))
    List<List<Object>> notNullRows = sql(notNullQuery)
    assertTrue(notNullRows == rowsWithRewriteOff(notNullQuery) && notNullRows.size() == 2,
            "the NULL row must stay filtered by the original query: " + notNullRows)

    // leave no baselines behind for other runs
    dropOwnBaselines()
    assertEquals(0, ownBaselines().size(), "all spm_r23_ baselines must be dropped")
}
