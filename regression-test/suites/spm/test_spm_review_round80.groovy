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

suite("test_spm_review_round80", "spm") {

    // Round-80 review comments on the capture / replay side:
    //
    // #1 the MAP overload of `element_at` takes its result type from the map's VALUE type, so
    //    `element_at(map('x', NULL), 'x')` is a NullType producer exactly like the ARRAY
    //    spelling: the Boolean consumer used to replace the whole call (and its value marker)
    //    with a typed constant NULL, and the `map('x', TRUE)` variant then replayed NULL.
    // #4 a bracket ARRAY literal keeps its elements as LITERALS, so its marker is the whole
    //    literal: `element_at([NULL], 1)` stayed ARRAY<NULL_TYPE>, the OR binding replaced the
    //    call and the `[TRUE]` variant had nothing to substitute.
    // #2 an output label a top-level ORDER BY key CONSUMES is semantic (the sort resolves the
    //    block's output name before the base columns), so a manual plan that renames the item
    //    while both sort texts still read `abs(x)` must be rejected at CREATE.
    // #3 / #5 / #6 are manager- and fingerprint-level protocols without a SQL surface here:
    //    they are covered by SPMRound80ManagerTest and SPMRound80PlannerTest.

    sql """set enable_spm_rewrite = true"""
    sql """set enable_spm_fallback = false"""

    sql """DROP TABLE IF EXISTS spm_r80_t"""
    sql """
        CREATE TABLE spm_r80_t (k INT, v INT)
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    // k = -1 makes k > 0 false, so the element_at row decides the OR.
    sql """INSERT INTO spm_r80_t VALUES (-1, 10), (0, 20), (2, 30)"""

    def dropOwnBaselines = {
        sql("""SHOW BASELINE PLANS""").findAll { it[1].toString().contains("spm_r80_") }
                .each { row -> sql """DROP BASELINE PLAN ${row[0]}""" }
    }
    dropOwnBaselines()

    def createBaseline = { String text ->
        (sql('CREATE GLOBAL BASELINE PLAN "' + text + '" WITH "' + text + '"')[0][0] as Long)
    }
    def explainOf = { String query -> sql("""EXPLAIN ${query}""").toString() }
    def directRows = { String query ->
        sql """set enable_spm_rewrite = false"""
        try {
            return sql(query)
        } finally {
            sql """set enable_spm_rewrite = true"""
        }
    }

    // ==================== #1: the MAP value marker survives Boolean binding ====================
    String mapBind = "SELECT k, k > 0 OR element_at(map('x', NULL), 'x') AS c" +
            " FROM spm_r80_t ORDER BY k"
    long mapId = createBaseline(mapBind)
    assertTrue(explainOf(mapBind).contains("SPM baseline hit: id=${mapId}"),
            "the identical MAP query must replay its baseline: " + explainOf(mapBind))
    // The `map('x', TRUE)` variant: for k = -1 the captured NULL value gave NULL, the user's
    // TRUE must win. The pre-fix frozen text had lost the value marker, so the replay kept NULL.
    String mapVariant = "SELECT k, k > 0 OR element_at(map('x', TRUE), 'x') AS c" +
            " FROM spm_r80_t ORDER BY k"
    assertTrue(explainOf(mapVariant).contains("SPM baseline hit: id=${mapId}"),
            "the MAP value variant must replay the baseline: " + explainOf(mapVariant))
    List<List<Object>> mapReplay = sql(mapVariant)
    assertTrue(mapReplay[0][1] != null,
            "k = -1: the map('x', TRUE) variant must report TRUE, not NULL: " + mapReplay)
    List<List<Object>> mapDirect = directRows(mapVariant)
    assertTrue(mapReplay == mapDirect,
            "the replay must return exactly the direct result: replay=" + mapReplay
                    + " direct=" + mapDirect)
    order_qt_r80_map_variant """SELECT k, k > 0 OR element_at(map('x', TRUE), 'x') AS c
                FROM spm_r80_t ORDER BY k"""

    // ==================== #4: the bracket ARRAY marker survives Boolean binding ================
    String arrayBind = "SELECT k, k > 0 OR element_at([NULL], 1) AS c FROM spm_r80_t ORDER BY k"
    long arrayId = createBaseline(arrayBind)
    assertTrue(explainOf(arrayBind).contains("SPM baseline hit: id=${arrayId}"),
            "the identical bracket-array query must replay its baseline: " + explainOf(arrayBind))
    String arrayVariant = "SELECT k, k > 0 OR element_at([TRUE], 1) AS c" +
            " FROM spm_r80_t ORDER BY k"
    assertTrue(explainOf(arrayVariant).contains("SPM baseline hit: id=${arrayId}"),
            "the [TRUE] variant must replay the baseline: " + explainOf(arrayVariant))
    List<List<Object>> arrayReplay = sql(arrayVariant)
    assertTrue(arrayReplay[0][1] != null,
            "k = -1: the [TRUE] variant must report TRUE, not NULL: " + arrayReplay)
    List<List<Object>> arrayDirect = directRows(arrayVariant)
    assertTrue(arrayReplay == arrayDirect,
            "the replay must return exactly the direct result: replay=" + arrayReplay
                    + " direct=" + arrayDirect)
    order_qt_r80_array_variant """SELECT k, k > 0 OR element_at([TRUE], 1) AS c
                FROM spm_r80_t ORDER BY k"""

    // ==================== #2: ORDER BY consumes output aliases ================================
    // Both sort texts read `abs(x)`, but the bind sorts the ALIASED value while a plan that
    // renames the item to `y` falls through to the base column t.k (absent here, or a
    // DIFFERENT column in general): the pair must not be accepted.
    test {
        sql """CREATE GLOBAL BASELINE PLAN
                "SELECT k AS x FROM spm_r80_t ORDER BY abs(x) LIMIT 1"
                WITH "SELECT k AS y FROM spm_r80_t ORDER BY abs(x) LIMIT 1" """
        exception "resolve to different expressions"
    }

    dropOwnBaselines()
    sql """DROP TABLE IF EXISTS spm_r80_t"""
}
