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

// SNII keeps BM25 norms only for the rows that carry one when that is smaller than a byte per row
// (the sparse norms layout). Mostly NULL columns and VARIANT paths are exactly that case. The BE
// config enable_snii_sparse_norms (default true) allows that layout; turned off, the writer emits
// the dense layout every earlier SNII writer produced. This suite loads the same batches three
// ways -- config on, config off, and toggled between batches (old and new layouts side by side) --
// and checks that MATCH, IS NULL and score() agree, before and after a full compaction, which
// runs with the config off for the dense table and on for the others.
// Scores use per-field document counts: N and avgdl count the non-NULL rows of the field.
// It changes a BE config, so it must not share the cluster with other suites.
suite("test_storage_format_snii_sparse_norms", "p0,nonConcurrent") {
    sql """ set enable_match_without_inverted_index = false """
    sql """ set default_variant_enable_typed_paths_to_sparse = false """
    sql """ set default_variant_enable_doc_mode = false """

    def tables = ["snii_sparse_norms_adaptive", "snii_sparse_norms_dense", "snii_sparse_norms_mixed"]

    def createTable = { String table ->
        sql "DROP TABLE IF EXISTS ${table}"
        sql """
            CREATE TABLE ${table} (
                id INT,
                content TEXT NULL,
                v variant<
                    's_*' : text,
                    PROPERTIES("variant_max_subcolumns_count"="0")
                > NULL,
                INDEX idx_content (content) USING INVERTED PROPERTIES(
                    "parser"="english",
                    "support_phrase"="true"
                ),
                INDEX idx_v (v) USING INVERTED PROPERTIES(
                    "parser"="english",
                    "support_phrase"="true",
                    "field_pattern"="s_*"
                )
            ) ENGINE=OLAP DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "disable_auto_compaction" = "true",
                "inverted_index_storage_format" = "SNII"
            )
        """
    }

    // One batch = one segment of 20000 rows. content is set on about 1% of the rows; each s_N
    // path on 2%, s_rare on 0.1%. "omega" occurs a different number of times in each row that
    // has it, so its scores are distinct.
    def loadBatch = { String table, int batch ->
        def offset = batch * 20000
        sql """
            INSERT INTO ${table}
            SELECT number + ${offset},
                   CASE WHEN number % 97 = 0 THEN concat('alpha ', repeat('beta ', number % 5))
                        WHEN number % 2003 = 7 THEN concat(
                                repeat('omega ', cast(floor((number + ${offset}) / 2003) + 1 as int)),
                                'tail')
                        ELSE NULL END,
                   parse_to_variant(concat('{"s_', number % 50, '":"delta ', repeat('eps ', number % 4),
                           '"', if(number % 1000 = 3, ',"s_rare":"omega delta omega"', ''), '}'))
            FROM numbers("number" = "20000")
        """
    }

    // Loads one batch with enable_snii_sparse_norms set explicitly; the BE value is restored after.
    def loadWithSparseNorms = { String table, int batch, boolean sparseNorms ->
        setBeConfigTemporary([enable_snii_sparse_norms: sparseNorms]) {
            loadBatch(table, batch)
        }
    }

    def checkTable = { String tag, String table ->
        "order_qt_${tag}_alpha_count" """
            select count(*) from ${table} where content match_any 'alpha'
        """
        "order_qt_${tag}_phrase_count" """
            select count(*) from ${table} where content match_phrase 'alpha beta'
        """
        "order_qt_${tag}_content_null" """
            select count(*) from ${table} where content is null
        """
        "order_qt_${tag}_not_beta" """
            select count(*) from ${table} where not (content match_any 'beta')
        """
        "order_qt_${tag}_path_count" """
            select count(*) from ${table} where cast(v['s_7'] as string) match_any 'eps'
        """
        "order_qt_${tag}_path_phrase_count" """
            select count(*) from ${table} where cast(v['s_7'] as string) match_phrase 'delta eps'
        """
        "order_qt_${tag}_path_null" """
            select count(*) from ${table} where cast(v['s_7'] as string) is null
        """
        "order_qt_${tag}_alpha_scores" """
            select count(*), count(distinct s), min(s), max(s), sum(s) from (
                select id, round(score(), 6) as s from ${table}
                where content match_any 'alpha beta' order by score() desc limit 100000) t
        """
        "order_qt_${tag}_omega_scores" """
            select id, round(score(), 6) from ${table}
            where content match_any 'omega' order by score() desc limit 100
        """
        "order_qt_${tag}_omega_phrase_scores" """
            select id, round(score(), 6) from ${table}
            where content match_phrase 'omega omega tail' order by score() desc limit 100
        """
        "order_qt_${tag}_path_scores" """
            select count(*), count(distinct s), min(s), max(s), sum(s) from (
                select id, round(score(), 6) as s from ${table}
                where cast(v['s_7'] as string) match_any 'delta eps' order by score() desc limit 100000) t
        """
        "order_qt_${tag}_rare_path_scores" """
            select id, round(score(), 6) from ${table}
            where cast(v['s_rare'] as string) match_phrase 'omega delta' order by score() desc limit 100
        """
    }

    // Rows whose score differs between two tables (exact, not rounded).
    def checkSameScores = { String tag, String left, String right ->
        "order_qt_${tag}_content_score_diff" """
            select count(*) from (
                select id, score() as s from ${left}
                where content match_any 'alpha beta omega' order by score() desc limit 100000) a
            full outer join (
                select id, score() as s from ${right}
                where content match_any 'alpha beta omega' order by score() desc limit 100000) b
            on a.id = b.id
            where a.id is null or b.id is null or a.s != b.s
        """
        "order_qt_${tag}_path_score_diff" """
            select count(*) from (
                select id, score() as s from ${left}
                where cast(v['s_3'] as string) match_phrase 'delta eps' order by score() desc limit 100000) a
            full outer join (
                select id, score() as s from ${right}
                where cast(v['s_3'] as string) match_phrase 'delta eps' order by score() desc limit 100000) b
            on a.id = b.id
            where a.id is null or b.id is null or a.s != b.s
        """
    }

    def indexDiskSize = { String table ->
        def tablets = sql_return_maparray """show tablets from ${table}"""
        def size = 0L
        for (tablet in tablets) {
            def rows = sql """
                select sum(INDEX_DISK_SIZE) from information_schema.rowsets
                where TABLET_ID = ${tablet.TabletId}
            """
            size += rows[0][0] as long
        }
        return size
    }

    for (table in tables) {
        createTable(table)
    }
    for (int batch = 0; batch < 4; batch++) {
        loadWithSparseNorms("snii_sparse_norms_adaptive", batch, true)
        loadWithSparseNorms("snii_sparse_norms_dense", batch, false)
        // Old-layout and new-layout segments side by side.
        loadWithSparseNorms("snii_sparse_norms_mixed", batch, batch % 2 == 1)
    }
    sql " sync "

    // Only the norms layout differs between the tables, so the sparse layout makes them smaller.
    def adaptiveSize = indexDiskSize("snii_sparse_norms_adaptive")
    def denseSize = indexDiskSize("snii_sparse_norms_dense")
    def mixedSize = indexDiskSize("snii_sparse_norms_mixed")
    logger.info("SNII index sizes: adaptive=${adaptiveSize}, dense=${denseSize}, mixed=${mixedSize}")
    assertTrue(adaptiveSize < mixedSize)
    assertTrue(mixedSize < denseSize)

    checkTable("adaptive", "snii_sparse_norms_adaptive")
    checkTable("dense", "snii_sparse_norms_dense")
    checkTable("mixed", "snii_sparse_norms_mixed")
    checkSameScores("adaptive_vs_dense", "snii_sparse_norms_adaptive", "snii_sparse_norms_dense")
    checkSameScores("mixed_vs_dense", "snii_sparse_norms_mixed", "snii_sparse_norms_dense")

    // Full compaction: the dense table compacts with the config off, the others with it on; every
    // source mix must merge into the same answers.
    setBeConfigTemporary([enable_snii_sparse_norms: true]) {
        trigger_and_wait_compaction("snii_sparse_norms_adaptive", "full", 1800)
        trigger_and_wait_compaction("snii_sparse_norms_mixed", "full", 1800)
    }
    setBeConfigTemporary([enable_snii_sparse_norms: false]) {
        trigger_and_wait_compaction("snii_sparse_norms_dense", "full", 1800)
    }

    checkTable("adaptive_compacted", "snii_sparse_norms_adaptive")
    checkTable("dense_compacted", "snii_sparse_norms_dense")
    checkTable("mixed_compacted", "snii_sparse_norms_mixed")
    checkSameScores("compacted_adaptive_vs_dense", "snii_sparse_norms_adaptive",
            "snii_sparse_norms_dense")
    checkSameScores("compacted_mixed_vs_dense", "snii_sparse_norms_mixed",
            "snii_sparse_norms_dense")
    // The compaction output follows the config it ran with, whatever layouts its sources had.
    def compactedDenseSize = indexDiskSize("snii_sparse_norms_dense")
    def compactedAdaptiveSize = indexDiskSize("snii_sparse_norms_adaptive")
    def compactedMixedSize = indexDiskSize("snii_sparse_norms_mixed")
    logger.info("SNII compacted index sizes: adaptive=${compactedAdaptiveSize}, " +
            "dense=${compactedDenseSize}, mixed=${compactedMixedSize}")
    assertTrue(compactedAdaptiveSize < compactedDenseSize)
    assertTrue(compactedMixedSize < compactedDenseSize)
}
