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

// Phrase queries restricted to the scan candidates must return the same rows as the unrestricted
// evaluation, for CLucene (V2) and SNII indexes alike. "k < 5" and "tag = 1" keep 5 of 100 rows,
// below the default 0.3 candidate ratio, so the phrase conjuncts run under a candidate set. Each
// query runs first with pushdown (a cold query cache), then with pushdown disabled, then with
// pushdown again over the cached full-segment results.
// It flips a BE config, so it must not share the cluster with other suites.
suite("test_phrase_candidate_pushdown", "p0,nonConcurrent") {
    sql "SET enable_inverted_index_query = true"
    sql "SET enable_common_expr_pushdown = true"
    sql "SET enable_match_without_inverted_index = false"

    def createTable = { String name, String format, boolean unique ->
        sql "DROP TABLE IF EXISTS ${name}"
        sql """
            CREATE TABLE ${name} (
                k INT NOT NULL,
                tag INT NOT NULL,
                a STRING NULL,
                b STRING NULL,
                c STRING NULL,
                INDEX idx_tag(tag) USING INVERTED,
                INDEX idx_a(a) USING INVERTED PROPERTIES("parser"="english", "support_phrase"="true"),
                INDEX idx_b(b) USING INVERTED PROPERTIES("parser"="english", "support_phrase"="true"),
                INDEX idx_c(c) USING INVERTED PROPERTIES("parser"="english", "support_phrase"="true")
            ) ${unique ? 'UNIQUE' : 'DUPLICATE'} KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "disable_auto_compaction" = "true",
                "inverted_index_storage_format" = "${format}"
                ${unique ? ', "enable_unique_key_merge_on_write" = "true"' : ''}
            )
        """
        def values = (0..<100).collect { k ->
            def a = k == 0 ? "NULL" : k == 2 ? "'big reddish fruit'" :
                    k == 3 ? "'big blue red apple'" : k == 4 ? "'red big apple'" : "'big red apple'"
            def b = k == 0 ? "'unrelated text'" : "'big red fox'"
            "(${k}, ${k < 5 ? 1 : 2}, ${a}, ${b}, '${k < 5 ? 'narrow' : 'broad'}')"
        }
        sql "INSERT INTO ${name} VALUES ${values.join(',')}"
        sql "SYNC"
    }

    def predicates = [
        "k < 5 AND a MATCH_PHRASE 'big red'",
        "tag = 1 AND a MATCH_PHRASE_PREFIX 'big re'",
        "c MATCH_ANY 'narrow' AND a MATCH_PHRASE 'big red'",
        "k < 5 AND a MATCH_PHRASE 'big red ~2'",
        "k < 5 AND a MATCH_PHRASE 'big red ~2+'",
        "k = 0 AND NOT (a MATCH_PHRASE 'big red' AND b MATCH_PHRASE 'big red')",
        "k < 5 AND (a MATCH_PHRASE 'big red' OR b MATCH_PHRASE 'big red')",
        "k < 5 AND a MATCH_PHRASE 'big'"
    ]

    for (String format : ["V2", "SNII"]) {
        def table = "test_phrase_candidate_pushdown_${format.toLowerCase()}"
        createTable(table, format, false)
        def queries = predicates.collect { "SELECT k FROM ${table} WHERE ${it} ORDER BY k" }
        queries.add("SELECT count(*) FROM ${table} WHERE a MATCH_PHRASE 'big red'")

        def pushdownResults = null
        setBeConfigTemporary([inverted_index_candidate_pushdown_ratio: "0.3"]) {
            pushdownResults = queries.collect { sql(it) }
        }
        setBeConfigTemporary([inverted_index_candidate_pushdown_ratio: "0"]) {
            queries.eachWithIndex { query, i -> assertEquals(pushdownResults[i], sql(query), query) }
        }
        setBeConfigTemporary([inverted_index_candidate_pushdown_ratio: "0.3"]) {
            queries.eachWithIndex { query, i -> assertEquals(pushdownResults[i], sql(query), query) }
            order_qt_phrase "SELECT k FROM ${table} WHERE k < 5 AND a MATCH_PHRASE 'big red'"
            order_qt_prefix "SELECT k FROM ${table} WHERE tag = 1 AND a MATCH_PHRASE_PREFIX 'big re'"
            order_qt_sloppy "SELECT k FROM ${table} WHERE k < 5 AND a MATCH_PHRASE 'big red ~2'"
            order_qt_nullable """SELECT k FROM ${table}
                    WHERE k = 0 AND NOT (a MATCH_PHRASE 'big red' AND b MATCH_PHRASE 'big red')"""
            order_qt_any_and_phrase """SELECT k FROM ${table}
                    WHERE c MATCH_ANY 'narrow' AND a MATCH_PHRASE 'big red'"""
            order_qt_ordered_sloppy "SELECT k FROM ${table} WHERE k < 5 AND a MATCH_PHRASE 'big red ~2+'"
            order_qt_or """SELECT k FROM ${table}
                    WHERE k < 5 AND (a MATCH_PHRASE 'big red' OR b MATCH_PHRASE 'big red')"""
            order_qt_single_term "SELECT k FROM ${table} WHERE k < 5 AND a MATCH_PHRASE 'big'"
            qt_count "SELECT count(*) FROM ${table} WHERE a MATCH_PHRASE 'big red'"
        }

        def mowTable = "test_phrase_candidate_pushdown_${format.toLowerCase()}_mow"
        createTable(mowTable, format, true)
        sql "DELETE FROM ${mowTable} WHERE k >= 5"
        sql "SYNC"
        def mowQuery = "SELECT k FROM ${mowTable} WHERE a MATCH_PHRASE 'big red' ORDER BY k"
        def mowPushdown = null
        setBeConfigTemporary([inverted_index_candidate_pushdown_ratio: "0.3"]) {
            mowPushdown = sql(mowQuery)
        }
        setBeConfigTemporary([inverted_index_candidate_pushdown_ratio: "0"]) {
            assertEquals(mowPushdown, sql(mowQuery))
        }
        order_qt_mow "SELECT k FROM ${mowTable} WHERE a MATCH_PHRASE 'big red'"
    }
}
