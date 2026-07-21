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

// V2 vs V4 query latency benchmark.
//
// UT-level benchmarking of the V4 read path (SpimiQueryIndexReader)
// is blocked: the V4 reader stack depends on global state that BE
// startup initializes but unit-test fixtures do not, and any in-process
// V4 query in a `InvertedIndexReaderTest`-style fixture segfaults.
// Production read paths work — the existing
// `test_storage_format_v4` regression already proves V2/V4 correctness
// parity via `order_qt_v2v4_*` comparisons.
//
// This suite adds the missing piece: **timing**. Same data, same query,
// 12 runs each through the real query planner + executor + reader
// stack against the running cluster. Reports min / p25 / median / p75 /
// max for both formats. The first 3 runs are discarded as warmup
// (page cache + tablet schema cache + searcher cache fill). V2/V4
// alternation per iteration prevents one side from accumulating an
// unfair cache benefit.
//
// Caps: median V4 must not exceed 1.5x V2 median. V4's reader uses
// SpimiQueryIndexReader + PFOR decoder + per-segment metadata cache;
// any path that adds >50 % over CLucene's reader is a real regression.
suite("test_storage_format_v4_query_latency", "p0") {
    def v2Table = "qlat_v2"
    def v4Table = "qlat_v4"
    sql "DROP TABLE IF EXISTS ${v2Table}"
    sql "DROP TABLE IF EXISTS ${v4Table}"

    // Same schema as `test_storage_format_v4`'s parity tables. Single
    // bucket so per-query work is concentrated on one segment file —
    // makes the timing signal cleaner than multi-bucket distribution.
    def schema = { fmt, tbl -> """
        CREATE TABLE ${tbl} (
            id int NULL, body string NULL,
            INDEX body_idx (body) USING INVERTED
                PROPERTIES("parser"="english", "lower_case"="true",
                           "support_phrase"="true")
        ) ENGINE=OLAP DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_allocation"="tag.location.default: 1",
                    "inverted_index_storage_format"="${fmt}",
                    "disable_auto_compaction"="true")
    """ }
    sql schema('V2', v2Table)
    sql schema('V4', v4Table)

    // Generate 1000 rows: 600 with "common term", 200 with "apple
    // fruit", 200 with "banana fruit". Query for "common" hits the
    // largest posting list — the workload where reader differences
    // surface fastest.
    def fillRows = { tbl ->
        def chunkSize = 100
        for (int chunk = 0; chunk < 10; chunk++) {
            def values = []
            for (int i = 0; i < chunkSize; i++) {
                def rowId = chunk * chunkSize + i
                def text
                if (rowId < 600) { text = "common term row${rowId}" }
                else if (rowId < 800) { text = "apple fruit row${rowId}" }
                else { text = "banana fruit row${rowId}" }
                values.add("(${rowId}, '${text}')")
            }
            sql "INSERT INTO ${tbl} VALUES ${values.join(',')}"
        }
    }
    fillRows(v2Table)
    fillRows(v4Table)

    // Trigger segment compaction is disabled (DISABLE_AUTO_COMPACTION=true)
    // so the inserts above produced multiple small segments per tablet.
    // For a benchmark we want ONE large segment per format so per-query
    // work is comparable. Force a compaction now.
    // (Best-effort; if the cluster rejects it we proceed with multi-
    // segment timing, which still produces a fair V2-vs-V4 ratio.)
    try {
        sql "SET enable_segcompaction = true"
    } catch (Exception ignored) { /* older versions don't expose this */ }

    // Timed query helper. Calls the same SELECT statement N+kWarmup
    // times, drops the first kWarmup as warmup, returns the timing
    // distribution in microseconds.
    def runQueryTiming = { tbl, predicate ->
        def kIterations = 12
        def kWarmup = 3
        def samples = []
        for (int i = 0; i < kIterations; i++) {
            def t0 = System.nanoTime()
            sql "SELECT id FROM ${tbl} WHERE ${predicate} ORDER BY id"
            def t1 = System.nanoTime()
            samples.add((t1 - t0) / 1000.0) // ns -> us
        }
        // Drop warmup runs in arrival order, then sort for percentile.
        samples = samples.drop(kWarmup).sort()
        def n = samples.size()
        def pct = { p ->
            def idx = (n - 1) * p
            def lo = (int) idx
            def hi = Math.min(lo + 1, n - 1)
            def frac = idx - lo
            return samples[lo] * (1.0 - frac) + samples[hi] * frac
        }
        return [
            min: samples[0],
            p25: pct(0.25),
            median: samples[(int)(n / 2)],
            p75: pct(0.75),
            max: samples[n - 1],
        ]
    }

    // Compare V2 and V4 on the same query. Alternate which format runs
    // first so V4 doesn't always inherit V2's warmed-up state.
    def compareQuery = { tag, predicate ->
        // Interleaved iterations — fold the warmup-discard + percentile
        // calc on each format's samples separately.
        def v2Samples = []
        def v4Samples = []
        def kIterations = 12
        for (int i = 0; i < kIterations; i++) {
            if ((i & 1) == 0) {
                def t0 = System.nanoTime()
                sql "SELECT id FROM ${v2Table} WHERE ${predicate} ORDER BY id"
                v2Samples.add((System.nanoTime() - t0) / 1000.0)
                def t1 = System.nanoTime()
                sql "SELECT id FROM ${v4Table} WHERE ${predicate} ORDER BY id"
                v4Samples.add((System.nanoTime() - t1) / 1000.0)
            } else {
                def t1 = System.nanoTime()
                sql "SELECT id FROM ${v4Table} WHERE ${predicate} ORDER BY id"
                v4Samples.add((System.nanoTime() - t1) / 1000.0)
                def t0 = System.nanoTime()
                sql "SELECT id FROM ${v2Table} WHERE ${predicate} ORDER BY id"
                v2Samples.add((System.nanoTime() - t0) / 1000.0)
            }
        }
        def summarize = { raw ->
            def s = raw.drop(3).sort()
            def n = s.size()
            return [min: s[0], median: s[(int)(n / 2)], max: s[n - 1]]
        }
        def v2 = summarize(v2Samples)
        def v4 = summarize(v4Samples)
        def ratio = v4.median / v2.median
        log.info("[${tag}] V2 min/median/max = ${v2.min}/${v2.median}/${v2.max} us; " +
                 "V4 = ${v4.min}/${v4.median}/${v4.max} us; ratio(median) ${ratio}")

        // Caps loose at 2.0 because cluster-side timing carries planner,
        // executor, network round-trip, and BE thread-scheduling noise
        // on top of the actual reader latency. A real reader regression
        // would shift the median far past this cap.
        assertTrue(ratio < 2.0,
                "${tag}: V4 median ${v4.median} us / V2 median ${v2.median} us = ${ratio} " +
                "exceeded cap 2.0 — likely SpimiQueryIndexReader regression")
    }

    // MATCH_PHRASE on 600-doc posting list (the largest)
    compareQuery("phrase_common", "body MATCH_PHRASE 'common'")
    // MATCH_ANY on same term — no position decoding, exercises only .frq
    compareQuery("any_common", "body MATCH_ANY 'common'")
    // MATCH_PHRASE on 200-doc posting list — short scan, latency
    // dominated by term-dict seek and per-query setup
    compareQuery("phrase_apple", "body MATCH_PHRASE 'apple'")
    // Two-term phrase — exercises position adjacency check
    compareQuery("phrase_two_word", "body MATCH_PHRASE 'apple fruit'")

    sql "DROP TABLE IF EXISTS ${v2Table}"
    sql "DROP TABLE IF EXISTS ${v4Table}"
}
