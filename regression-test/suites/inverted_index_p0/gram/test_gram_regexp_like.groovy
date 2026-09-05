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

// A gram (sparse / dense ngram) index accelerates LIKE / REGEXP by "an approximate candidate
// superset plus expression re-verification": the index only narrows the candidate rows, and the
// final match is still decided row by row by the LIKE / REGEXP expression. So the core assertion
// of this suite is a **semantic comparison** -- the same query must return exactly the same set
// of rows with enable_inverted_index_query=true and with false. Any difference means the index
// pruned away a genuinely matching row (a false negative), which is a functional defect.
//
// Two safety nets:
//   1) the .out golden: each mode emits its own idx_true_* / idx_false_* sections, which must be
//      byte-identical when compared by hand or by script;
//   2) programmatic assertions: runParityCheck compares the sorted id lists of the same queries
//      in the two modes directly.
suite("test_gram_regexp_like", "p0") {
    def tbl = "t_gram_regexp_like"
    // Policy names are globally unique: tokenizers and analyzers share one namespace, so the two
    // names must differ
    def sparseTok = "gram_rl_sparse_tok"
    def sparseAna = "gram_rl_sparse"
    def denseLcTok = "gram_rl_dense_lc_tok"
    def denseLcAna = "gram_rl_dense_lc"

    // An analyzer reaches BE asynchronously over the heartbeat, so before creating the table we
    // must confirm BE has loaded it, otherwise the write side gets no gram scheme
    def waitAnalyzerInstalled = { String name ->
        def deadline = System.currentTimeMillis() + 180_000
        Exception lastNotFound = null
        while (System.currentTimeMillis() < deadline) {
            try {
                sql """SELECT TOKENIZE('probe', '\"analyzer\"=\"${name}\"')"""
                return
            } catch (Exception e) {
                if (!e.message.contains("Policy not found")) {
                    throw e
                }
                lastNotFound = e
                sleep(1000)
            }
        }
        throw new IllegalStateException("analyzer ${name} was not installed on BE", lastNotFound)
    }

    // Turn the groovy-side "raw regex/LIKE text" into the contents of a SQL string literal:
    // Doris, like MySQL, eats one more layer of backslashes inside a string literal, so \. has to
    // be written as \\.
    def sqlEsc = { String s -> s.replace('\\', '\\\\').replace("'", "\\'") }

    sql "DROP TABLE IF EXISTS ${tbl}"
    try_sql "DROP INVERTED INDEX ANALYZER IF EXISTS ${sparseAna}"
    try_sql "DROP INVERTED INDEX ANALYZER IF EXISTS ${denseLcAna}"
    try_sql "DROP INVERTED INDEX TOKENIZER IF EXISTS ${sparseTok}"
    try_sql "DROP INVERTED INDEX TOKENIZER IF EXISTS ${denseLcTok}"

    // Sparse gram: density=0.25, min_gram..max_gram = 3..16
    sql """
        CREATE INVERTED INDEX TOKENIZER IF NOT EXISTS ${sparseTok}
        PROPERTIES (
            "type" = "ngram",
            "mode" = "sparse",
            "min_gram" = "3",
            "max_gram" = "16",
            "density" = "0.25"
        )
    """
    // A gram-family analyzer must be a "bare tokenizer" and may carry no token_filter (FE checks)
    sql """
        CREATE INVERTED INDEX ANALYZER IF NOT EXISTS ${sparseAna}
        PROPERTIES ("tokenizer" = "${sparseTok}")
    """
    // Dense gram + lower_case: covers case-insensitive candidate recall
    // ('code = unavailable' vs 'CODE = UNAVAILABLE')
    sql """
        CREATE INVERTED INDEX TOKENIZER IF NOT EXISTS ${denseLcTok}
        PROPERTIES (
            "type" = "ngram",
            "mode" = "dense",
            "min_gram" = "3",
            "lower_case" = "true"
        )
    """
    sql """
        CREATE INVERTED INDEX ANALYZER IF NOT EXISTS ${denseLcAna}
        PROPERTIES ("tokenizer" = "${denseLcTok}")
    """
    waitAnalyzerInstalled(sparseAna)
    waitAnalyzerInstalled(denseLcAna)

    // Three INVERTED indexes on the same column: sparse gram / dense gram (lower_case) / english
    // tokenized index. The first two verify that "whichever gram index the optimizer picks, the
    // semantics stay the same", and the third that gram coexists with a language tokenizer index.
    // A gram index is forced to docs-only and FE defaults support_phrase to false; the storage
    // format must be SNII.
    sql """
        CREATE TABLE ${tbl} (
            id INT,
            msg VARCHAR(512),
            INDEX idx_msg_gram (msg) USING INVERTED PROPERTIES ("analyzer" = "${sparseAna}"),
            INDEX idx_msg_lc   (msg) USING INVERTED PROPERTIES ("analyzer" = "${denseLcAna}"),
            INDEX idx_msg_en   (msg) USING INVERTED PROPERTIES ("parser" = "english")
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "disable_auto_compaction" = "true",
            "inverted_index_storage_format" = "SNII"
        )
    """

    // The first rowset / segment
    sql """INSERT INTO ${tbl} VALUES
        (1, 'rpc error: code = Unavailable desc = error reading from server'),
        (2, 'user_id="eacb47f6-967d-11f0-b88d-8eb93cba8bdb" user_currency="USD"'),
        (3, 'Convert conversion successful'),
        (4, '手机微博 POST 10.68.3.18:8080 error'),
        (5, NULL),
        (6, ''),
        (7, 'ab'),
        (8, 'GET /images/x.gif HTTP/1.0'),
        (9, 'CODE = UNAVAILABLE'),
        (10, 'context deadline exceeded'),
        (11, 'failed to charge card: rpc error'),
        (12, 'timeout after error error error')"""
    sql "sync"
    // The second rowset / segment: checks that the candidate bitmap is still solved correctly per
    // segment when there are several segments
    sql """INSERT INTO ${tbl} VALUES
        (13, 'rpc error: code = Internal desc = boom'),
        (14, '微博手机'),
        (15, 'abc'),
        (16, 'Sending Quote: 12.5'),
        (17, 'progress 100% done'),
        (18, '   '),
        (19, 'MiXeD CaSe UnAvAiLaBlE')"""
    sql "sync"

    // Coverage matrix: long literal / wildcard / alternation / optional group / character class /
    // no literal / anchors / escaped dot / case ((?i) and the lower_case index) / CJK / a literal
    // shorter than min_gram / the empty pattern
    def regexps = [
        'rpc error: code = Unavailable',
        'error.*timeout',
        'code = (Unavailable|Internal)',
        'user_id="[0-9a-f]{8}-',
        'conn(ection)? re(set|fused)',
        'GET|POST',
        '[0-9]{3}-[0-9]{4}',
        '(?i)unavailable',
        '手机微博',
        '微博',
        '^abc$',
        'a.*b',
        'Sending Quote: [0-9]+\\.[0-9]+',
        'failed to (convert|charge)',
        '.*',
        '[0-9]+',
        'code = unavailable',
        'error',
        '\\.gif',
        '^rpc',
        'exceeded$',
        'ab',
        'x',
        '',
        '用户|微博',
        'HTTP/1\\.[0-9]',
    ]

    // LIKE coverage: the % / _ wildcards, the empty string, an exact match, multi-segment
    // wildcards, CJK, and case
    def likes = [
        '%rpc error%',
        '%Unavail%',
        '%手机%',
        'ab%',
        '%',
        '%x.gif%',
        '%code = _navailable%',
        '',
        'abc',
        '%error%error%',
        '_bc',
        '%微博%',
        '%CODE = UNAVAILABLE%',
    ]

    // Compound predicates: an OR of REGEXPs, a REGEXP inside NOT (a AND b), REGEXP mixed with
    // LIKE, and a LIKE with a custom ESCAPE (handled conservatively on the BE side: it is not
    // pushed down, but it must still be correct)
    def compounds = [
        "msg REGEXP 'rpc' OR msg REGEXP '微博'",
        "NOT (msg REGEXP 'rpc' AND id > 5)",
        "msg REGEXP 'error' AND id > 3",
        "msg LIKE '%error%' AND msg REGEXP 'rpc'",
        "NOT (msg REGEXP 'error') OR msg REGEXP 'abc'",
        "NOT (msg LIKE '%rpc%' AND msg REGEXP 'error')",
        "msg REGEXP 'code' AND msg NOT LIKE '%Internal%'",
        "msg LIKE '%100!%%' ESCAPE '!'",
        "msg LIKE '%100!% do%' ESCAPE '!'",
    ]

    // Build the [tag, sql] list; the tag does not contain the raw pattern, which keeps the
    // section names in .out stable and reproducible
    def buildQueries = { ->
        def qs = []
        regexps.eachWithIndex { p, i ->
            def e = sqlEsc(p)
            qs << ["regexp_${i}".toString(),
                   "SELECT id FROM ${tbl} WHERE msg REGEXP '${e}'".toString()]
            qs << ["rlike_${i}".toString(),
                   "SELECT id FROM ${tbl} WHERE msg RLIKE '${e}'".toString()]
            qs << ["notregexp_${i}".toString(),
                   "SELECT id FROM ${tbl} WHERE msg NOT REGEXP '${e}'".toString()]
            qs << ["regexp_and_${i}".toString(),
                   "SELECT id FROM ${tbl} WHERE msg REGEXP '${e}' AND id > 3".toString()]
        }
        likes.eachWithIndex { p, i ->
            def e = sqlEsc(p)
            qs << ["like_${i}".toString(),
                   "SELECT id FROM ${tbl} WHERE msg LIKE '${e}'".toString()]
            qs << ["notlike_${i}".toString(),
                   "SELECT id FROM ${tbl} WHERE msg NOT LIKE '${e}'".toString()]
        }
        compounds.eachWithIndex { c, i ->
            qs << ["compound_${i}".toString(),
                   "SELECT id FROM ${tbl} WHERE ${c}".toString()]
        }
        return qs
    }

    def queries = buildQueries()
    log.info("gram parity matrix: ${queries.size()} queries x 2 modes".toString())

    // Turn the SQL cache off, so the two modes cannot reuse one cached result and blur the
    // comparison
    sql "SET enable_sql_cache=false"

    // Produce the .out golden: run the same batch of queries once with the index on and once off
    def runAll = { boolean useIndex ->
        sql "SET enable_inverted_index_query=${useIndex}"
        queries.each { entry ->
            def tag = "${entry[0]}_idx_${useIndex}".toString()
            "order_qt_${tag}"(entry[1])
        }
        "order_qt_count_idx_${useIndex}"("SELECT count(*) FROM ${tbl} WHERE msg REGEXP 'error'".toString())
    }
    runAll(true)
    runAll(false)

    // Programmatic safety net: instead of eyeballing .out, compare the sorted id lists of the two
    // modes query by query
    def idsOf = { String q ->
        return sql(q).collect { it[0] as Integer }.sort()
    }
    def runParityCheck = { String phase ->
        def mismatches = []
        queries.each { entry ->
            sql "SET enable_inverted_index_query=true"
            def withIdx = idsOf(entry[1])
            sql "SET enable_inverted_index_query=false"
            def noIdx = idsOf(entry[1])
            if (withIdx != noIdx) {
                mismatches << "[${phase}][${entry[0]}] ${entry[1]}\n  idx_on =${withIdx}\n  idx_off=${noIdx}".toString()
            }
        }
        if (!mismatches.isEmpty()) {
            throw new AssertionError(
                    "gram index changed query semantics (${mismatches.size()} mismatches):\n"
                            + mismatches.join("\n") as Object)
        }
        log.info("gram parity check [${phase}] passed for ${queries.size()} queries".toString())
    }
    runParityCheck("base")

    // The profile proves the gram index really took part in the pruning: RowsGramIndexFiltered > 0.
    // A counter may be rendered as "18" or as "12.0K (12000)", and both forms must be parseable.
    def parseProfileCounter = { String profileString, String name ->
        def exact = Pattern.compile(Pattern.quote(name) + ":\\s*[^\\(\\n]*\\((\\d+)\\)").matcher(profileString)
        if (exact.find()) {
            return Long.parseLong(exact.group(1))
        }
        def plain = Pattern.compile(Pattern.quote(name) + ":\\s*(\\d+)").matcher(profileString)
        assertTrue(plain.find(), "${name} is not parseable from profile")
        return Long.parseLong(plain.group(1))
    }
    def checkGramPruned = { String label, String profileString, Throwable exception ->
        assertNull(exception)
        assertTrue(profileString.contains("RowsGramIndexFiltered"),
                "RowsGramIndexFiltered is missing from profile")
        def filtered = parseProfileCounter(profileString, "RowsGramIndexFiltered")
        def candidate = parseProfileCounter(profileString, "GramIndexCandidateRows")
        log.info("[${label}] RowsGramIndexFiltered=${filtered}, GramIndexCandidateRows=${candidate}".toString())
        assertTrue(filtered > 0, "[${label}] RowsGramIndexFiltered must be positive, got ${filtered}")
    }

    sql "SET enable_inverted_index_query=true"
    sql "set enable_profile=true"
    sql "set profile_level=2"
    // The profile is reported asynchronously by FE, so a fixed sleep either wastes time or
    // occasionally comes up empty on a slow machine. Use the framework's own bounded polling
    // ProfileAction#getProfileBySql instead: wait at most 30 s (every 500 ms) until this SQL's
    // profile reads "Profile Completion State: COMPLETE" and both gram counters have been
    // rendered; a timeout fails the test.
    def gramProfileCounters = ["RowsGramIndexFiltered", "GramIndexCandidateRows"]
    def profileAction = new ProfileAction(context)
    // REGEXP takes the gram acceleration
    order_qt_profile_q "/* gram_regexp_profile */ SELECT id FROM ${tbl} WHERE msg REGEXP 'context deadline exceeded'"
    checkGramPruned("regexp",
            profileAction.getProfileBySql("gram_regexp_profile", gramProfileCounters), null)
    // LIKE takes the gram acceleration too
    order_qt_profile_q_like "/* gram_like_profile */ SELECT id FROM ${tbl} WHERE msg LIKE '%Sending Quote%'"
    checkGramPruned("like",
            profileAction.getProfileBySql("gram_like_profile", gramProfileCounters), null)
    sql "set enable_profile=false"

    // Query again after a delete: the delete predicate combined with the gram candidate bitmap
    // must still agree with the no-index path
    sql "SET enable_inverted_index_query=true"
    sql "DELETE FROM ${tbl} WHERE id = 1"
    sql "sync"
    order_qt_after_delete_idx_true "SELECT id FROM ${tbl} WHERE msg REGEXP 'rpc error'"
    sql "SET enable_inverted_index_query=false"
    order_qt_after_delete_idx_false "SELECT id FROM ${tbl} WHERE msg REGEXP 'rpc error'"
    // Compare the whole matrix once more after the delete
    runParityCheck("after_delete")

    sql "SET enable_inverted_index_query=true"
}
