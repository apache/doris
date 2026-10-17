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

suite("test_spm_review_round56", "spm") {

    // Round-56 #3: the SPM text columns must be UNBOUNDED. A canonical bind digest /
    // schema fingerprint far beyond the legacy VARCHAR(4096) has to reserve, store,
    // tombstone and replay, and the upgrade path widens a legacy fixed-width column
    // instead of accepting it by name (the column TYPE is pinned below). Then the
    // same text is dropped, which also writes a >4096-char DROP TOMBSTONE.
    //
    // #1 (same-millisecond mutation windows of two FEs), #2 (keyed drop tombstones),
    // #4 (forwarded-DDL sync failure) and #5/#6 (filter-snapshot-scoped dedup and the
    // length-prefixed snapshot key) are two-master / in-memory orderings without a SQL
    // surface: they are covered by BaselineManagerMutationClockTest and

    sql """set enable_spm_rewrite = true"""
    sql """set enable_spm_fallback = false"""

    sql """DROP TABLE IF EXISTS spm_r56_wide"""
    sql """CREATE TABLE spm_r56_wide (k int, v int) DUPLICATE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES("replication_num" = "1")"""
    sql """INSERT INTO spm_r56_wide VALUES (1, 10), (2, 20), (3, 30)"""

    // the unbounded value type of both text columns (a legacy fixed-width column would
    // have been left at its old width by a name-only upgrade check)
    order_qt_r56_seq_digest_type """SHOW COLUMNS FROM __internal_schema.spm_baselines_seq LIKE 'bind_sql_digest'"""
    order_qt_r56_baselines_fingerprint_type """SHOW COLUMNS FROM __internal_schema.spm_baselines LIKE 'schema_fingerprint'"""

    def dropOwnBaselines = {
        sql("""SHOW BASELINE PLANS""").findAll { it[1].toString().contains("spm_r56_wide") }
                .each { row -> sql """DROP BASELINE PLAN ${row[0]}""" }
    }
    dropOwnBaselines()

    // A bind whose NORMALIZED digest (the literal-parameterized text the identity is keyed
    // by) itself exceeds 4096: 500 chained predicates survive the normalization, whereas
    // the 2500 IN literals below are parameterized into a single `?`.
    def predicates = (2..501).collect { "v > ${it}" }.join(" AND ")
    def digestBind = "SELECT v FROM spm_r56_wide WHERE ${predicates}"
    def digestId = sql('CREATE GLOBAL BASELINE PLAN "' + digestBind + '" WITH "'
            + digestBind + '"')[0][0] as Long
    order_qt_r56_seq_long_digest """SELECT length(bind_sql_digest) > 4096
                FROM __internal_schema.spm_baselines_seq WHERE last_id = ${digestId} LIMIT 1"""
    order_qt_r56_row_long_digest """SELECT length(bind_sql) > 4096, length(bind_sql_digest) > 4096, status
                FROM __internal_schema.spm_baselines WHERE id = ${digestId}"""

    // well past 4096 characters of stored bind text (the literals stay in bind_sql)
    def literals = (1..2500).collect { it.toString() }.join(",")
    def wideBind = "SELECT sum(v) FROM spm_r56_wide WHERE k IN (${literals})"
    def id = sql('CREATE GLOBAL BASELINE PLAN "' + wideBind + '" WITH "' + wideBind + '"')[0][0] as Long
    order_qt_r56_stored_row """SELECT length(bind_sql) > 4096, status FROM __internal_schema.spm_baselines
                WHERE id = ${id}"""
    // the applied baseline id is dynamic, so the hit LINE is asserted (like the other
    // SPM suites), not compared against a golden
    def explained = sql("""EXPLAIN SELECT sum(v) FROM spm_r56_wide WHERE k IN (${literals})""").toString()
    assertTrue(explained.contains("SPM baseline hit: id=${id}"),
            "the wide bind must replay its own baseline: " + explained)

    sql """DROP BASELINE PLAN ${id}"""
    sql """DROP BASELINE PLAN ${digestId}"""
    // the drop tombstone carries the same over-long identity
    order_qt_r56_drop_tombstone """SELECT length(bind_sql_digest) > 4096, dropped
                FROM __internal_schema.spm_baselines_seq
                WHERE last_id = ${digestId} AND dropped = 1 ORDER BY id LIMIT 1"""
    order_qt_r56_gone """SELECT count(*) FROM __internal_schema.spm_baselines
                WHERE id IN (${id}, ${digestId})"""

    sql """DROP TABLE IF EXISTS spm_r56_wide"""
}
