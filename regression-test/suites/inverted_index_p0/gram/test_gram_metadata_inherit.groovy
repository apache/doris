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

import java.util.regex.Pattern

import org.apache.doris.regression.action.ProfileAction

suite("test_gram_metadata_inherit", "p0") {
    // Cloud does not support the named-index BUILD syntax used here. Keep this case scoped
    // to adding one requested index while IndexBuilder inherits the unchanged gram indexes.
    if (isCloudMode()) {
        return
    }

    def waitAnalyzerInstalled = { String name ->
        awaitUntil(180) {
            try {
                sql """SELECT TOKENIZE('abcdefgh', '"analyzer"="${name}"')"""
                return true
            } catch (Exception e) {
                if (!e.message.contains("Policy not found")) {
                    throw e
                }
                return false
            }
        }
    }

    sql "DROP TABLE IF EXISTS test_gram_metadata_inherit"
    ['dense', 'sparse'].each { name ->
        sql "DROP INVERTED INDEX ANALYZER IF EXISTS gram_inherit_${name}"
        sql "DROP INVERTED INDEX TOKENIZER IF EXISTS gram_inherit_${name}_tok"
    }
    sql """CREATE INVERTED INDEX TOKENIZER gram_inherit_dense_tok PROPERTIES (
        "type"="ngram", "mode"="dense", "min_gram"="3")"""
    sql """CREATE INVERTED INDEX TOKENIZER gram_inherit_sparse_tok PROPERTIES (
        "type"="ngram", "mode"="sparse", "min_gram"="4", "max_gram"="8",
        "density"="1.0", "stop_gram_df"="0")"""
    ['dense', 'sparse'].each { name ->
        sql """CREATE INVERTED INDEX ANALYZER gram_inherit_${name}
            PROPERTIES ("tokenizer"="gram_inherit_${name}_tok")"""
        waitAnalyzerInstalled("gram_inherit_${name}")
    }

    sql """CREATE TABLE test_gram_metadata_inherit (
        id INT,
        dense VARCHAR(128),
        sparse VARCHAR(128),
        tag VARCHAR(32),
        INDEX idx_dense (dense) USING INVERTED PROPERTIES ("analyzer"="gram_inherit_dense"),
        INDEX idx_sparse (sparse) USING INVERTED PROPERTIES ("analyzer"="gram_inherit_sparse")
    ) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
    PROPERTIES ("replication_num"="1", "disable_auto_compaction"="true",
                "inverted_index_storage_format"="SNII")"""
    // Row 1 is a dense3 false positive, but not a sparse4 candidate. Both indexes must
    // preserve row 0 and the scalar LIKE/REGEXP recheck must reject row 1.
    sql """INSERT INTO test_gram_metadata_inherit VALUES
        (0, 'abcdefgh', 'abcdefgh', 'red fox'),
        (1, 'abc!bcd!cde!def!efg!fgh', 'abc!bcd!cde!def!efg!fgh', 'blue fox'),
        (2, 'unrelated', 'unrelated', 'green dog')"""
    sql "sync"
    sql "SET enable_sql_cache=false"
    // The condition cache must be off too: gram deliberately keeps its LIKE / REGEXP expression
    // in _common_expr_ctxs_push_down for the row-level recheck, so the segment iterator never
    // zeroes the condition cache digest, and that digest ignores enable_inverted_index_query.
    // Left on, the index-on pass fills the per-segment granule cache and the index-off pass hits
    // it, so both passes would read one and the same filter result and this parity check would
    // degenerate into a tautology.
    sql "SET enable_condition_cache=false"

    def checkPatterns = { String phase ->
        [false, true].each { useIndex ->
            sql "SET enable_inverted_index_query=${useIndex}"
            ['dense', 'sparse'].each { column ->
                "order_qt_${phase}_${column}_${useIndex}_like"("""SELECT id
                    FROM test_gram_metadata_inherit WHERE ${column} LIKE '%abcdefgh%'""")
                "order_qt_${phase}_${column}_${useIndex}_regexp"("""SELECT id
                    FROM test_gram_metadata_inherit WHERE ${column} REGEXP 'abcdefgh'""")
            }
        }

        // A correct full-scan fallback must not hide lost gram metadata after inheritance.
        // The framework waits for the completed profile instead of relying on a fixed sleep.
        sql "SET enable_profile=true"
        sql "SET profile_level=2"
        try {
            ['dense', 'sparse'].each { column ->
                [like: "LIKE '%abcdefgh%'", regexp: "REGEXP 'abcdefgh'"].each { kind, predicate ->
                    def profileId = "gram_inherit_${phase}_${column}_${kind}_${System.nanoTime()}"
                    sql """/* ${profileId} */ SELECT id FROM test_gram_metadata_inherit
                        WHERE ${column} ${predicate} ORDER BY id"""
                    def profile = new ProfileAction(context).getProfileBySql(
                            profileId, ["RowsGramIndexFiltered"])
                    def filtered = Pattern.compile("RowsGramIndexFiltered:\\s*(\\d+)")
                            .matcher(profile)
                    assertTrue(filtered.find(), "RowsGramIndexFiltered missing from ${profileId}")
                    assertTrue(Long.parseLong(filtered.group(1)) > 0,
                            "${profileId} must still use the gram index")
                    // Evidence that "SET enable_condition_cache=false" above took effect: a
                    // non-zero ConditionCacheHit would mean the index-on and index-off passes
                    // share one cached per-granule filter result, so the comparison above would
                    // no longer be able to expose a row the gram index dropped.
                    def cacheHit = Pattern.compile("ConditionCacheHit:[^\\n]*[1-9][^\\n]*")
                            .matcher(profile)
                    assertTrue(!cacheHit.find(),
                            "${profileId} hit the condition cache, so index on/off no longer "
                                    + "compare two independent scans")
                }
            }
        } finally {
            sql "SET enable_profile=false"
        }
    }

    checkPatterns("before")
    // Add only metadata here: old rowsets still lack idx_tag, and retain both gram indexes.
    // The following named BUILD INDEX must copy those unchanged logical indexes into the
    // replacement SNII container while it builds the new English dictionary from raw tag data.
    sql "SET enable_add_index_for_new_data=true"
    sql """CREATE INDEX idx_tag ON test_gram_metadata_inherit(tag) USING INVERTED
        PROPERTIES ("parser"="english")"""
    sql "BUILD INDEX idx_tag ON test_gram_metadata_inherit"
    wait_for_last_build_index_finish("test_gram_metadata_inherit", 180_000)
    // Require a real FINISHED job even if the shared wait helper reaches its timeout.
    def jobs = sql_return_maparray("""SHOW BUILD INDEX
        WHERE TableName='test_gram_metadata_inherit' ORDER BY JobId DESC LIMIT 1""")
    if (jobs.isEmpty() || jobs[0].State != "FINISHED") {
        throw new IllegalStateException("gram inheritance BUILD INDEX did not finish: ${jobs}")
    }
    sql "sync"
    checkPatterns("after")

    // Disallow MATCH's scalar fallback: success on the old rows proves the newly built index
    // is usable as well as the inherited gram dictionaries.
    order_qt_new_index """SELECT /*+ SET_VAR(enable_match_without_inverted_index=false) */ id
        FROM test_gram_metadata_inherit WHERE tag MATCH 'fox'"""
    order_qt_inherited_and_new """SELECT /*+ SET_VAR(enable_match_without_inverted_index=false) */ id
        FROM test_gram_metadata_inherit WHERE dense LIKE '%abcdefgh%' AND tag MATCH 'fox'"""
}
