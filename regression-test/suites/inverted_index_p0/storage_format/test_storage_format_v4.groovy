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

// V4 storage format end-to-end test (P37c-4).
//
// V4 means the inverted index is written by the in-house SPIMI
// writer and read by SpimiFulltextIndexReader rather than the
// CLucene-backed reader. The test exercises the full pipeline:
//   1. CREATE TABLE with `inverted_index_storage_format = V4`
//   2. INSERT rows (fulltext column is tokenized by analyzer +
//      tapped into the SPIMI buffer, then emitted as
//      `_spimi_0.tis/.tii/.frq/.prx/.fnm` + `spimi_segments_1`).
//   3. SELECT ... WHERE col MATCH '...' — routed through
//      ColumnReader factory's V4 branch → SpimiFulltextIndexReader
//      → SpimiSearcherBuilder → SpimiCLuceneIndexReader →
//      CLucene query engine.
//
// A passing result count proves the entire read path the
// previous SPIMI commits built actually serves a real Doris
// query against a real V4 segment.
suite("test_storage_format_v4", "p0") {
    def testTable = "test_v4_fulltext"

    sql "DROP TABLE IF EXISTS ${testTable}"

    sql """
        CREATE TABLE IF NOT EXISTS ${testTable} (
            `id` int(11) NULL,
            `body` string NULL,
            INDEX body_idx (`body`) USING INVERTED
                PROPERTIES("parser" = "english", "lower_case" = "true") COMMENT ''
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "inverted_index_storage_format" = "V4",
            "disable_auto_compaction" = "true"
        );
        """

    // Insert a small, hand-crafted corpus so the MATCH results are
    // unambiguous: each row is unique and each query has a known
    // exact-match count. Token frequencies span <1 and >1 occurrences
    // per doc to exercise both the kDefault and PFOR .frq paths
    // (the latter triggers above skip_interval=512 — not reached
    // here, but the codec dispatch logic runs either way).
    sql """ INSERT INTO ${testTable} VALUES
        (1, 'the quick brown fox'),
        (2, 'jumps over the lazy dog'),
        (3, 'quick brown rabbit hops'),
        (4, 'the dog barks loudly'),
        (5, 'fox and rabbit are mammals'),
        (6, 'mammals run quickly'),
        (7, 'the the the lazy programmer'),
        (8, 'apache doris fulltext search')
    """

    // -- TermQuery / MATCH_ANY: how many rows contain "fox"?
    //    Expected: row 1 (the quick brown fox) and row 5 (fox and
    //    rabbit are mammals) → 2 rows.
    order_qt_v4_match_any_fox """
        SELECT id FROM ${testTable} WHERE body MATCH_ANY 'fox' ORDER BY id;
    """

    // -- Multi-term MATCH_ALL: how many rows contain BOTH "the"
    //    and "dog"? Expected: row 2 (jumps over the lazy dog) and
    //    row 4 (the dog barks loudly) → 2 rows.
    order_qt_v4_match_all """
        SELECT id FROM ${testTable} WHERE body MATCH_ALL 'the dog' ORDER BY id;
    """

    // -- MATCH_PHRASE: positions matter. "lazy dog" appears as a
    //    consecutive phrase in row 2 only. Row 7 has "the the the
    //    lazy programmer" — the word "lazy" is present but the
    //    phrase "lazy dog" is not. Expected: 1 row.
    order_qt_v4_match_phrase """
        SELECT id FROM ${testTable} WHERE body MATCH_PHRASE 'lazy dog' ORDER BY id;
    """

    // -- Higher-frequency term: "the" appears in rows 1, 2, 4, 7.
    //    Row 7 has it 3 times (the the the) — freq accumulator must
    //    correctly count duplicate positions. Expected: 4 rows.
    order_qt_v4_match_any_the """
        SELECT id FROM ${testTable} WHERE body MATCH_ANY 'the' ORDER BY id;
    """

    // -- Term that does not exist: 0 rows.
    order_qt_v4_match_missing """
        SELECT id FROM ${testTable} WHERE body MATCH_ANY 'kangaroo' ORDER BY id;
    """

    // -- 3-token MATCH_PHRASE — exercises PhraseQuery's `_others`
    //    array path (postings beyond the first two leads). Without
    //    this, multi-term phrase logic is untested. Row 1 only.
    order_qt_v4_match_phrase_3tok """
        SELECT id FROM ${testTable} WHERE body MATCH_PHRASE 'the quick brown' ORDER BY id;
    """

    // -- MATCH_REGEXP — drives RegexpQuery → TermEnum scan →
    //    matches "quick" (rows 1, 3) and "quickly" (row 6).
    order_qt_v4_match_regexp """
        SELECT id FROM ${testTable} WHERE body MATCH_REGEXP 'qui.*' ORDER BY id;
    """

    // -- MATCH_PHRASE_PREFIX — last token is a prefix; rows 1 and 3
    //    both have "quick br" as a consecutive prefix-extending pair.
    order_qt_v4_match_phrase_prefix """
        SELECT id FROM ${testTable} WHERE body MATCH_PHRASE_PREFIX 'quick br' ORDER BY id;
    """

    // -- MATCH_PHRASE on data where MATCH_PHRASE differs from
    //    MATCH_ALL. "the brown" — both tokens are in row 1 but NOT
    //    consecutive (row 1: "the quick brown fox"). Expected: 0 rows
    //    via phrase, but {1} via MATCH_ALL. Without this case the
    //    test cannot distinguish a working phrase impl from a
    //    silent degrade-to-MATCH_ALL.
    order_qt_v4_match_phrase_non_adjacent """
        SELECT id FROM ${testTable} WHERE body MATCH_PHRASE 'the brown' ORDER BY id;
    """
    order_qt_v4_match_all_for_compare """
        SELECT id FROM ${testTable} WHERE body MATCH_ALL 'the brown' ORDER BY id;
    """

    // -- NULL handling on the same column — V4 must skip NULL rows
    //    in the inverted index, but `IS NULL` must still find them
    //    via the null bitmap.
    sql "INSERT INTO ${testTable} VALUES (9, NULL), (10, '')"
    order_qt_v4_null_isnull """
        SELECT id FROM ${testTable} WHERE body IS NULL ORDER BY id;
    """
    order_qt_v4_null_isempty """
        SELECT id FROM ${testTable} WHERE body = '' ORDER BY id;
    """
    // Existing matches must still work after NULL/empty inserts.
    order_qt_v4_post_null_match """
        SELECT id FROM ${testTable} WHERE body MATCH_PHRASE 'lazy dog' ORDER BY id;
    """

    sql "DROP TABLE IF EXISTS ${testTable}"

    // ===== V2 vs V4 byte-for-byte result parity =====
    // Strongest possible black-box correctness check: same data,
    // same queries on a V2 table and a V4 table; result sets must
    // be identical. Anywhere they diverge is a SPIMI bug.
    def v2Table = "test_v2_baseline_for_v4"
    def v4Table = "test_v4_compare_against_v2"
    sql "DROP TABLE IF EXISTS ${v2Table}"
    sql "DROP TABLE IF EXISTS ${v4Table}"
    def schema = { fmt -> """
        CREATE TABLE ${fmt == 'V2' ? v2Table : v4Table} (
            id int NULL, body string NULL,
            INDEX body_idx (body) USING INVERTED
                PROPERTIES("parser"="english", "lower_case"="true",
                           "support_phrase"="true")
        ) ENGINE=OLAP DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_allocation"="tag.location.default: 1",
                    "inverted_index_storage_format"="${fmt}",
                    "disable_auto_compaction"="true")
    """ }
    sql schema('V2')
    sql schema('V4')
    def fillSql = { tbl -> """
        INSERT INTO ${tbl} VALUES
        (1, 'the quick brown fox'), (2, 'jumps over the lazy dog'),
        (3, 'quick brown rabbit hops'), (4, 'the dog barks loudly'),
        (5, 'fox and rabbit are mammals'), (6, 'mammals run quickly'),
        (7, 'the the the lazy programmer'), (8, 'apache doris fulltext search')
    """ }
    sql fillSql(v2Table)
    sql fillSql(v4Table)

    // For each query type, V2 and V4 must produce identical id sets.
    // We record each side's result; the .out file locks both as
    // equal. If a future regression diverges them, the .out will
    // mismatch loudly.
    [["any_fox", "MATCH_ANY 'fox'"],
     ["all_the_dog", "MATCH_ALL 'the dog'"],
     ["phrase_quick_brown", "MATCH_PHRASE 'quick brown'"],
     ["phrase_non_adj", "MATCH_PHRASE 'the brown'"],
     ["regexp_qui", "MATCH_REGEXP 'qui.*'"],
     ["prefix_quick_br", "MATCH_PHRASE_PREFIX 'quick br'"]].each { entry ->
        def tag = entry[0]
        def pred = entry[1]
        "order_qt_v2v4_${tag}_v2"("SELECT id FROM ${v2Table} WHERE body ${pred} ORDER BY id")
        "order_qt_v2v4_${tag}_v4"("SELECT id FROM ${v4Table} WHERE body ${pred} ORDER BY id")
    }

    sql "DROP TABLE IF EXISTS ${v2Table}"
    sql "DROP TABLE IF EXISTS ${v4Table}"

    // ===== omit_tfap (no-prox) path — V4 with support_phrase=false =====
    //
    // When the column index has `support_phrase=false`, V4's writer emits
    // .frq without per-doc positions (`omit_term_freq_and_positions=true`
    // in EmitSegment) and `.fnm` sets `has_prox=false`. The read side
    // (`SpimiQueryTermDocs::seek`) MUST pick the no-prox decoder branch
    // — without it MATCH_ANY / MATCH_ALL silently return wrong results
    // (typically 0 rows). The pre-rename test surface only exercised
    // the with-prox path; this section covers the no-prox writer→reader
    // contract end-to-end on a real V4 segment.
    def v4NoProxTable = "test_v4_no_prox"
    def v2NoProxTable = "test_v2_no_prox_baseline"
    sql "DROP TABLE IF EXISTS ${v4NoProxTable}"
    sql "DROP TABLE IF EXISTS ${v2NoProxTable}"
    def noProxSchema = { fmt, tbl -> """
        CREATE TABLE ${tbl} (
            id int NULL, body string NULL,
            INDEX body_idx (body) USING INVERTED
                PROPERTIES("parser"="english", "lower_case"="true",
                           "support_phrase"="false")
        ) ENGINE=OLAP DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_allocation"="tag.location.default: 1",
                    "inverted_index_storage_format"="${fmt}",
                    "disable_auto_compaction"="true")
    """ }
    sql noProxSchema('V2', v2NoProxTable)
    sql noProxSchema('V4', v4NoProxTable)
    def fillNoProxSql = { tbl -> """
        INSERT INTO ${tbl} VALUES
        (1, 'the quick brown fox'), (2, 'jumps over the lazy dog'),
        (3, 'quick brown rabbit hops'), (4, 'the dog barks loudly'),
        (5, 'fox and rabbit are mammals'), (6, 'mammals run quickly'),
        (7, 'the the the lazy programmer'), (8, 'apache doris fulltext search')
    """ }
    sql fillNoProxSql(v2NoProxTable)
    sql fillNoProxSql(v4NoProxTable)

    // MATCH_ANY / MATCH_ALL must agree byte-for-byte between V2 and V4
    // on the same no-prox column. MATCH_PHRASE is illegal here
    // (support_phrase=false), so we don't query it.
    [["noprox_any_fox", "MATCH_ANY 'fox'"],
     ["noprox_any_dog_rabbit", "MATCH_ANY 'dog rabbit'"],
     ["noprox_all_the_dog", "MATCH_ALL 'the dog'"],
     ["noprox_all_quick_brown", "MATCH_ALL 'quick brown'"]].each { entry ->
        def tag = entry[0]
        def pred = entry[1]
        "order_qt_v2v4_${tag}_v2"("SELECT id FROM ${v2NoProxTable} WHERE body ${pred} ORDER BY id")
        "order_qt_v2v4_${tag}_v4"("SELECT id FROM ${v4NoProxTable} WHERE body ${pred} ORDER BY id")
    }

    sql "DROP TABLE IF EXISTS ${v2NoProxTable}"
    sql "DROP TABLE IF EXISTS ${v4NoProxTable}"
}
