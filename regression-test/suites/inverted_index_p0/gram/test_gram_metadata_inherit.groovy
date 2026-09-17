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
        "density"="1.0")"""
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
    // The sparse column carries context around the needle, and that is load-bearing rather than
    // decoration. A sparse scheme cuts on content, so the grams of an occurrence depend on the
    // bytes around it, and the writer solves its own density from the segment's data: on three
    // tiny rows the solved rate is sparse enough that a bare eight-byte value promises no gram
    // an eight-byte literal could ask for, and the compiler correctly answers ALL. Measured on
    // a live cluster: with row 0 as 'abcdefgh' the sparse index reports candidates 0, filtered
    // 0, gave-up 0 -- it never runs -- while with the context below it reports candidates 1,
    // filtered 2. The dense column needs no context because its grams are fixed-length.
    sql """INSERT INTO test_gram_metadata_inherit VALUES
        (0, 'abcdefgh', 'prefix_abcdefgh_suffix', 'red fox'),
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
    // With the scan node's function push-down on, a LIKE becomes a storage-layer
    // LikeColumnPredicate and never reaches the gram index, so the profile assertion below
    // would see nothing pruned. The pipeline randomises this variable per session, so pin
    // it to its default.
    sql "SET enable_function_pushdown=false"

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

    // A second inheritance, over a segment big enough for stop-gram to have fired. The phases
    // above run on three rows, and a segment under kHighDfDigestDivisor rows never drops a
    // posting, so they cannot tell whether a dictionary holding locator-less entries survives
    // BUILD INDEX. This one holds 4,000 rows in a single segment, where the commonest grams
    // are over the threshold and their posting lists are gone from the file. The index has to
    // keep answering the same rows as a plain scan after the rewrite -- an inherited entry that
    // lost its dropped-posting declaration reads as absent rather than as matching everything,
    // which turns a common literal into zero rows.
    sql "DROP TABLE IF EXISTS test_gram_inherit_stopped"
    sql """CREATE TABLE test_gram_inherit_stopped (
        id INT,
        msg VARCHAR(128),
        tag VARCHAR(32),
        INDEX idx_msg (msg) USING INVERTED PROPERTIES ("analyzer"="gram_inherit_dense")
    ) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
    PROPERTIES ("replication_num"="1", "disable_auto_compaction"="true",
                "inverted_index_storage_format"="SNII")"""
    def stoppedRows = []
    for (int i = 0; i < 4000; i++) {
        // Every row carries "shared_prefix_common_text", whose grams land far above the
        // threshold and lose their postings; one row in a thousand carries a rare marker whose
        // grams stay under it and keep theirs.
        def marker = (i % 1000 == 0) ? "rare_marker_${i}" : "filler_${i}"
        stoppedRows.add("(${i}, 'shared_prefix_common_text ${marker}', 'tag_${i % 3}')")
    }
    // One statement, so all 4,000 rows land in one segment and the floor is crossed.
    sql "INSERT INTO test_gram_inherit_stopped VALUES ${stoppedRows.join(',')}"
    sql "sync"

    // The inverted-index result cache is keyed by the raw query bytes, so the "after" phase
    // could otherwise be served the "before" phase's bitmaps and never reach the rewritten
    // container at all -- which is exactly what this phase exists to read.
    sql "SET enable_inverted_index_query_cache=false"

    // Answers with the index on and off, for one setting of enable_inverted_index_query.
    def runStopped = { boolean useIndex ->
        sql "SET enable_inverted_index_query=${useIndex}"
        def out = [:]
        // A literal whose grams were dropped: the index cannot narrow it and has to say so.
        out.common = sql("""SELECT COUNT(*) FROM test_gram_inherit_stopped
            WHERE msg LIKE '%shared_prefix_common%'""")[0][0] as long
        // A literal whose grams were kept, so the index really does filter here.
        out.rare = sql("""SELECT id FROM test_gram_inherit_stopped
            WHERE msg LIKE '%rare_marker_%' ORDER BY id""").collect { it[0] as int }
        out.regexp = sql("""SELECT COUNT(*) FROM test_gram_inherit_stopped
            WHERE msg REGEXP 'shared_prefix_[a-z]+'""")[0][0] as long
        return out
    }

    // Asserted rather than compared against a golden file: this suite runs only on a non-cloud
    // cluster, so -forceGenOut on a cloud one emits no block for these tags and the comparison
    // could never be regenerated. The parity of the two arms is what the phase is for, and the
    // corpus is fixed, so the absolute answers are pinned too -- without them a phase that
    // silently stopped matching anything would still agree with itself.
    def checkStopped = { String phaseName ->
        def scanned = runStopped(false)
        def indexed = runStopped(true)
        sql "SET enable_inverted_index_query=true"
        assertEquals(scanned, indexed,
                "[${phaseName}] the gram index changed the answer: ${scanned} vs ${indexed}")
        assertEquals(4000L, scanned.common,
                "[${phaseName}] every row carries the common literal")
        assertEquals([0, 1000, 2000, 3000], scanned.rare,
                "[${phaseName}] the rare marker sits on one row in a thousand")
        assertEquals(4000L, scanned.regexp,
                "[${phaseName}] every row matches the common regexp")
    }

    checkStopped("before")
    sql "SET enable_add_index_for_new_data=true"
    sql """CREATE INDEX idx_stopped_tag ON test_gram_inherit_stopped(tag) USING INVERTED
        PROPERTIES ("parser"="english")"""
    sql "BUILD INDEX idx_stopped_tag ON test_gram_inherit_stopped"
    wait_for_last_build_index_finish("test_gram_inherit_stopped", 180_000)
    def stoppedJobs = sql_return_maparray("""SHOW BUILD INDEX
        WHERE TableName='test_gram_inherit_stopped' ORDER BY JobId DESC LIMIT 1""")
    if (stoppedJobs.isEmpty() || stoppedJobs[0].State != "FINISHED") {
        throw new IllegalStateException("stopped-posting BUILD INDEX did not finish: ${stoppedJobs}")
    }
    sql "sync"
    checkStopped("after")
    sql "DROP TABLE IF EXISTS test_gram_inherit_stopped"

    // The policies live cluster-wide and the cluster is shared, so a suite that leaves
    // its own behind eats into the instance-wide policy limit for everyone else.
    sql "DROP TABLE IF EXISTS test_gram_metadata_inherit"
    ['dense', 'sparse'].each { name ->
        sql "DROP INVERTED INDEX ANALYZER IF EXISTS gram_inherit_${name}"
        sql "DROP INVERTED INDEX TOKENIZER IF EXISTS gram_inherit_${name}_tok"
    }

}
