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

import org.apache.doris.regression.action.ProfileAction

import java.util.regex.Pattern

// stop-gram end to end: an index built with the posting lists of very common grams dropped
// must still answer LIKE/REGEXP exactly, and must actually have dropped them.
//
// A dropped gram matches every document, so it can only ever widen the candidate set the
// index proposes, and the predicate is re-evaluated on those candidates. The observable
// contract is therefore equality: every pattern must return exactly what a full scan
// returns. The rows below are built so that the common grams really are common -- a shared
// prefix on every row -- while the patterns being searched for are rare, which is the shape
// where dropping matters.
//
// There is no switch to turn the feature off, so the control arm is the row floor instead:
// a segment under kHighDfDigestDivisor (2000) rows never drops a posting. The same 4000 rows
// are therefore loaded twice, once as a single segment (dropping active, threshold
// 4000 / 2000 * 3 = df 6) and once as five segments of 800 (dropping inactive, every
// posting kept). The two tables must agree on every answer, and the single-segment index
// must be much smaller -- that difference is the dropped postings, and it also pins that
// the floor holds.
suite("test_gram_stop_gram", "p0") {
    def waitAnalyzerInstalled = { String name ->
        def deadline = System.currentTimeMillis() + 180_000
        Exception lastNotFound = null
        while (System.currentTimeMillis() < deadline) {
            try {
                sql """SELECT TOKENIZE('probe', '"analyzer"="${name}"')"""
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

    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort)
    def set_be_config = { key, value ->
        for (String backend_id : backendId_to_backendIP.keySet()) {
            def (code, out, err) = update_be_config(backendId_to_backendIP.get(backend_id),
                    backendId_to_backendHttpPort.get(backend_id), key, value)
            logger.info("update ${key}=${value}: code=${code}, out=${out}, err=${err}")
            // A phase that runs against a config that never changed proves nothing.
            assertEquals(0, code, "updating ${key} on backend ${backend_id} failed: ${err}")
            assertTrue(out.toString().contains("OK"),
                    "updating ${key} on backend ${backend_id} was refused: ${out}")
        }
    }

    // The tables go first: a policy still referenced by a table left behind by an earlier run
    // cannot be dropped.
    sql "DROP TABLE IF EXISTS test_gram_stop_gram_one"
    sql "DROP TABLE IF EXISTS test_gram_stop_gram_five"
    sql "DROP INVERTED INDEX ANALYZER IF EXISTS gram_stop_ana"
    sql "DROP INVERTED INDEX TOKENIZER IF EXISTS gram_stop_tok"
    sql """CREATE INVERTED INDEX TOKENIZER gram_stop_tok PROPERTIES (
        "type"="ngram", "mode"="sparse", "min_gram"="3", "max_gram"="8", "density"="0.5")"""
    sql """CREATE INVERTED INDEX ANALYZER gram_stop_ana
        PROPERTIES ("tokenizer"="gram_stop_tok")"""
    waitAnalyzerInstalled("gram_stop_ana")

    def rows = 4000
    def values = []
    for (int i = 0; i < rows; i++) {
        // Every row shares "shared_prefix_", making its grams as common as a gram gets. Four
        // rows carry a rare marker (df 4, under the single segment's threshold of 6, so its
        // postings survive and the rare queries still filter); one row in seven carries a
        // mid-frequency tail whose grams sit above the line and are dropped.
        def tail = (i % 1000 == 0) ? "rare_marker_${i}" : "filler_${i}"
        values.add("(${i}, 'shared_prefix_common_text ${tail} tail_${i % 7}')")
    }

    def runAll = { String table, String label ->
        def out = [:]
        out["like_rare"] = sql "SELECT COUNT(*) FROM ${table} WHERE msg LIKE '%rare_marker_%'"
        out["like_common"] = sql "SELECT COUNT(*) FROM ${table} WHERE msg LIKE '%shared_prefix_common%'"
        out["regexp_rare"] = sql "SELECT COUNT(*) FROM ${table} WHERE msg REGEXP 'rare_marker_[0-9]+'"
        out["regexp_common"] = sql "SELECT COUNT(*) FROM ${table} WHERE msg REGEXP 'shared_prefix_[a-z]+'"
        out["regexp_mid"] = sql "SELECT COUNT(*) FROM ${table} WHERE msg REGEXP 'tail_3\$'"
        out["regexp_alt"] = sql "SELECT COUNT(*) FROM ${table} WHERE msg REGEXP 'rare_marker_(0|1000|2000)\\\\b'"
        out["regexp_absent"] = sql "SELECT COUNT(*) FROM ${table} WHERE msg REGEXP 'no_such_token_anywhere'"
        out["like_tail"] = sql "SELECT COUNT(*) FROM ${table} WHERE msg LIKE '%tail_3'"
        logger.info("${label}: ${out}")
        return out
    }

    def createTable = { String table ->
        sql "DROP TABLE IF EXISTS ${table}"
        sql """CREATE TABLE ${table} (
            `id` bigint NULL,
            `msg` text NULL,
            INDEX idx_msg (`msg`) USING INVERTED
                PROPERTIES('analyzer'='gram_stop_ana', 'support_phrase'='false')
        ) ENGINE=OLAP DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES('replication_num'='1', 'inverted_index_storage_format'='SNII',
                   'disable_auto_compaction'='true')"""
    }

    // information_schema reports a fresh table's sizes with a delay, and 0 there means "not
    // yet" rather than "empty". A non-zero reading is not enough either: the figure is
    // aggregated across tablets as their reports arrive, so an early sample can be a fraction
    // of the real size -- which once made the comparison below read the dropped index as the
    // larger of the two. Wait for the same value twice in a row before believing it.
    def indexBytes = { String table ->
        def deadline = System.currentTimeMillis() + 180_000
        long previous = -1L
        while (System.currentTimeMillis() < deadline) {
            def r = sql """SELECT INDEX_LENGTH FROM information_schema.tables
                           WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '${table}'"""
            def bytes = r.isEmpty() ? 0L : (r[0][0] as long)
            if (bytes > 0 && bytes == previous) {
                return bytes
            }
            previous = bytes
            sleep(5000)
        }
        throw new IllegalStateException("index size of ${table} never settled")
    }

    def oneSegment = "test_gram_stop_gram_one"
    def fiveSegments = "test_gram_stop_gram_five"
    try {
        sql "SET enable_sql_cache=false"
        sql "SET enable_condition_cache=false"
        // The inverted-index result cache is keyed by the raw query bytes, so a second pass
        // over the same queries could be served the first pass's bitmaps without touching
        // the index at all; every phase below has to reach the index.
        sql "SET enable_inverted_index_query_cache=false"

        // One batch keeps all 4000 rows in a single segment: above the row floor, dropping
        // is in effect.
        createTable(oneSegment)
        sql "INSERT INTO ${oneSegment} VALUES ${values.join(',')}"

        // The same rows in five batches of 800: each segment is under the floor, so every
        // posting list is kept. This is what the index looked like before dropping existed.
        createTable(fiveSegments)
        for (int b = 0; b < 5; b++) {
            sql "INSERT INTO ${fiveSegments} VALUES ${values.subList(b * 800, (b + 1) * 800).join(',')}"
        }

        // Ground truth, taken with the index out of the picture entirely.
        sql "SET enable_inverted_index_query=false"
        def scanned = runAll(oneSegment, "full scan")
        sql "SET enable_inverted_index_query=true"

        def dropped = runAll(oneSegment, "one segment, postings dropped")
        def kept = runAll(fiveSegments, "five segments, postings kept")
        scanned.each { name, value ->
            assertEquals(value[0][0], dropped[name][0][0],
                    "index and scan disagree on ${name} with postings dropped")
            assertEquals(value[0][0], kept[name][0][0],
                    "index and scan disagree on ${name} with postings kept")
        }

        // The postings really were dropped. Measured: the single-segment index is 0.36 of
        // the five-segment one. Had nothing been dropped it would be about 0.9 -- the only
        // difference left would be four fewer per-segment dictionaries -- so a factor of two
        // separates the two outcomes with margin on both sides.
        def droppedBytes = indexBytes(oneSegment)
        def keptBytes = indexBytes(fiveSegments)
        logger.info("index bytes: one segment ${droppedBytes}, five segments ${keptBytes}")
        assertTrue(droppedBytes * 2 < keptBytes,
                "the single-segment index (${droppedBytes} bytes) should be far smaller than " +
                "the five-segment one (${keptBytes} bytes): its common postings were not dropped")

        // A dropped entry has to reach the query path physically, not only through answers
        // that a scan would give as well. This pattern's grams straddle the line: the ones of
        // `common_text` sit in every row and were dropped, `rare_marker_1000` sits in one row
        // and was kept. The AND resolves both in the dictionary, leaves the dropped ones out
        // as match-all and reads the kept posting, so the profile must show a handful of
        // candidates, thousands of rows pruned and no gate give-up.
        def parseProfileCounter = { String profileString, String name ->
            def exact = Pattern.compile(Pattern.quote(name) + ":\\s*[^\\(\\n]*\\((\\d+)\\)")
                    .matcher(profileString)
            if (exact.find()) {
                return Long.parseLong(exact.group(1))
            }
            def plain = Pattern.compile(Pattern.quote(name) + ":\\s*(\\d+)").matcher(profileString)
            assertTrue(plain.find(), "${name} is not parseable from profile")
            return Long.parseLong(plain.group(1))
        }
        def profileAction = new ProfileAction(context)
        def gramCounters = ["RowsGramIndexFiltered", "GramIndexCandidateRows", "GramIndexGateGaveUp"]
        sql "SET enable_inverted_index_query=true"
        sql "SET enable_profile=true"
        sql "SET profile_level=2"
        def straddling = sql """/* gram_stop_gram_straddle */ SELECT COUNT(*) FROM ${oneSegment}
            WHERE msg REGEXP 'common_text rare_marker_1000 '"""
        def straddleProfile = profileAction.getProfileBySql("gram_stop_gram_straddle", gramCounters)
        sql "SET enable_profile=false"
        assertEquals(1L, straddling[0][0] as long, "exactly one row carries rare_marker_1000")
        def candidates = parseProfileCounter(straddleProfile, "GramIndexCandidateRows")
        def pruned = parseProfileCounter(straddleProfile, "RowsGramIndexFiltered")
        def gaveUp = parseProfileCounter(straddleProfile, "GramIndexGateGaveUp")
        logger.info("straddling pattern: candidates=${candidates} pruned=${pruned} gaveUp=${gaveUp}")
        assertEquals(0L, gaveUp, "the retained gram keeps the node under budget; the gate must not give up")
        assertTrue(candidates >= 1L && candidates <= 4L,
                "the kept posting (df 4) bounds the candidates, got ${candidates}: the dropped " +
                "grams must read as match-all and the kept one must still be read")
        assertTrue(pruned >= rows - 4L, "the index pruned only ${pruned} of ${rows} rows")

        // The gate could otherwise mask a broken dropped-gram path by giving up first, so
        // check again with its fallback ratio disabled. The gate's primary budget comes from
        // the segment itself and cannot be switched off -- that is the point of deriving it
        // rather than configuring it -- but zeroing the ratio removes the one arm that a
        // configuration could have been hiding behind.
        set_be_config("gram_index_max_candidate_ratio_bp", "0")
        def ratioOff = runAll(oneSegment, "postings dropped, fallback ratio off")
        scanned.each { name, value ->
            assertEquals(value[0][0], ratioOff[name][0][0],
                    "with the fallback ratio disabled the index changed the answer for ${name}")
        }
    } finally {
        set_be_config("gram_index_max_candidate_ratio_bp", "15")
        sql "DROP TABLE IF EXISTS ${oneSegment}"
        sql "DROP TABLE IF EXISTS ${fiveSegments}"
        sql "DROP INVERTED INDEX ANALYZER IF EXISTS gram_stop_ana"
        sql "DROP INVERTED INDEX TOKENIZER IF EXISTS gram_stop_tok"
    }
}
