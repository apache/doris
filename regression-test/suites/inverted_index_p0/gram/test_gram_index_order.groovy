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

// The order in which a column's indexes were declared must not decide what a query can use.
// Two places where it did:
//   1) LIKE / REGEXP carry no analyzer, so they were handed to the lowest-id tokenized index. An
//      ordinary index declared before the gram index then declined the gram query, and the
//      predicate quietly ran as a scan: the same rows, none of the acceleration.
//   2) MATCH_PHRASE ... USING ANALYZER names a positional index, but phrase support was judged on
//      whichever tokenized index came first. A docs-only index in that place -- and a gram index
//      is docs-only by default -- made a valid query fail.
suite("test_gram_index_order", "p0") {
    def sparseTok = "gram_order_sparse_tok"
    def sparseAna = "gram_order_sparse"
    def positionalAna = "gram_order_positional"
    def likeTbl = "t_gram_order_like"
    def gramPhraseTbl = "t_gram_order_phrase_gram"
    def englishPhraseTbl = "t_gram_order_phrase_english"

    // An analyzer reaches BE asynchronously over the heartbeat; wait until BE can use it.
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
    def idsOf = { String query ->
        return sql(query).collect { it[0] as Integer }.sort()
    }
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

    sql "SET enable_sql_cache=false"
    // Both passes of every parity check must really scan; see test_gram_regexp_like for why the
    // condition cache would otherwise let them share one filter result.
    sql "SET enable_condition_cache=false"
    // With function push-down on, LIKE becomes a storage-layer predicate and never reaches the
    // index, so the profile assertion below would see nothing pruned.
    sql "SET enable_function_pushdown=false"

    [likeTbl, gramPhraseTbl, englishPhraseTbl].each { sql "DROP TABLE IF EXISTS ${it}" }
    try_sql "DROP INVERTED INDEX ANALYZER IF EXISTS ${sparseAna}"
    try_sql "DROP INVERTED INDEX ANALYZER IF EXISTS ${positionalAna}"
    try_sql "DROP INVERTED INDEX TOKENIZER IF EXISTS ${sparseTok}"
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
    sql """
        CREATE INVERTED INDEX ANALYZER IF NOT EXISTS ${sparseAna}
        PROPERTIES ("tokenizer" = "${sparseTok}")
    """
    sql """
        CREATE INVERTED INDEX ANALYZER IF NOT EXISTS ${positionalAna}
        PROPERTIES ("tokenizer" = "standard")
    """
    waitAnalyzerInstalled(sparseAna)
    waitAnalyzerInstalled(positionalAna)

    // 1) An ordinary english index declared first, so it holds the lower index id.
    sql """
        CREATE TABLE ${likeTbl} (
            id INT,
            msg VARCHAR(512),
            INDEX idx_msg_en   (msg) USING INVERTED PROPERTIES ("parser" = "english"),
            INDEX idx_msg_gram (msg) USING INVERTED PROPERTIES ("analyzer" = "${sparseAna}")
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "disable_auto_compaction" = "true",
            "inverted_index_storage_format" = "SNII"
        )
    """
    sql """INSERT INTO ${likeTbl} VALUES
        (1, 'rpc error: code = Unavailable desc = error reading from server'),
        (2, 'user_id="eacb47f6-967d-11f0-b88d-8eb93cba8bdb" user_currency="USD"'),
        (3, 'Convert conversion successful'),
        (4, 'mobile POST 10.68.3.18:8080 error'),
        (5, NULL),
        (6, ''),
        (7, 'ab'),
        (8, 'GET /images/x.gif HTTP/1.0'),
        (9, 'CODE = UNAVAILABLE'),
        (10, 'context deadline exceeded'),
        (11, 'failed to charge card: rpc error'),
        (12, 'timeout after error error error')"""
    sql "sync"
    sql """INSERT INTO ${likeTbl} VALUES
        (13, 'rpc error: code = Internal desc = boom'),
        (14, 'mobile client upload'),
        (15, 'abc'),
        (16, 'Sending Quote: 12.5'),
        (17, 'progress 100% done'),
        (18, '   '),
        (19, 'MiXeD CaSe UnAvAiLaBlE')"""
    sql "sync"

    def profileAction = new ProfileAction(context)
    [
        ["like", "msg LIKE '%Sending Quote%'", [16]],
        ["regexp", "msg REGEXP 'context deadline exceeded'", [10]],
    ].each { entry ->
        def label = entry[0]
        def query = "SELECT id FROM ${likeTbl} WHERE ${entry[1]}".toString()
        sql "SET enable_inverted_index_query=false"
        def withoutIndex = idsOf(query)
        sql "SET enable_inverted_index_query=true"
        def withIndex = idsOf(query)
        assertEquals(entry[2], withoutIndex, "[${label}] scalar answer")
        assertEquals(withoutIndex, withIndex, "[${label}] the index changed the answer")

        // The same rows are not enough: the gram index declared second must actually prune.
        def tag = "gram_order_${label}".toString()
        sql "set enable_profile=true"
        sql "set profile_level=2"
        sql "/* ${tag} */ ${query}"
        def profileString = profileAction.getProfileBySql(tag,
                ["RowsGramIndexFiltered", "GramIndexCandidateRows"])
        sql "set enable_profile=false"
        def filtered = parseProfileCounter(profileString, "RowsGramIndexFiltered")
        log.info("[${label}] RowsGramIndexFiltered=${filtered}".toString())
        assertTrue(filtered > 0,
                "[${label}] with an ordinary index declared first the gram index must still prune, "
                        + "RowsGramIndexFiltered=${filtered}")
    }

    // 2) A docs-only index declared before a positional one: the gram index, docs-only by
    // default, and for contrast an ordinary english index made docs-only explicitly.
    [
        [gramPhraseTbl,
         """INDEX idx_msg_docs (msg) USING INVERTED PROPERTIES ("analyzer" = "${sparseAna}")"""],
        [englishPhraseTbl,
         """INDEX idx_msg_docs (msg) USING INVERTED PROPERTIES ("parser" = "english", "support_phrase" = "false")"""],
    ].each { entry ->
        def tbl = entry[0]
        sql """
            CREATE TABLE ${tbl} (
                id INT,
                msg VARCHAR(512),
                ${entry[1]},
                INDEX idx_msg_positional (msg) USING INVERTED PROPERTIES (
                    "analyzer" = "${positionalAna}", "support_phrase" = "true")
            ) ENGINE=OLAP
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "disable_auto_compaction" = "true",
                "inverted_index_storage_format" = "SNII"
            )
        """
        sql """INSERT INTO ${tbl} VALUES
            (1, 'request ok here'), (2, 'ok request'), (3, 'nothing'), (4, 'a request ok'),
            (5, NULL)"""
        sql "sync"
        [
            "msg MATCH_PHRASE 'request ok' USING ANALYZER ${positionalAna}",
            "msg MATCH_PHRASE_PREFIX 'request o' USING ANALYZER ${positionalAna}",
        ].each { predicate ->
            def query = "SELECT id FROM ${tbl} WHERE ${predicate}".toString()
            sql "SET enable_inverted_index_query=false"
            def withoutIndex = idsOf(query)
            sql "SET enable_inverted_index_query=true"
            def withIndex = idsOf(query)
            assertEquals([1, 4], withoutIndex, "[${tbl}] scalar answer for ${predicate}")
            assertEquals(withoutIndex, withIndex, "[${tbl}] index answer for ${predicate}")
        }
    }

    sql "SET enable_inverted_index_query=true"
    // The policies live cluster-wide and the cluster is shared, so a suite that leaves
    // its own behind eats into the instance-wide policy limit for everyone else.
    [likeTbl, gramPhraseTbl, englishPhraseTbl].each { sql "DROP TABLE IF EXISTS ${it}" }
    sql "DROP INVERTED INDEX ANALYZER IF EXISTS ${sparseAna}"
    sql "DROP INVERTED INDEX ANALYZER IF EXISTS ${positionalAna}"
    sql "DROP INVERTED INDEX TOKENIZER IF EXISTS ${sparseTok}"
}
