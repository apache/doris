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

// A variant subcolumn that only an index-answered filter reads is not read from the data pages
// when no column of its variant is output. Whatever is selected (the filtered subcolumn, a
// sibling, the whole variant, or nothing of it) must come back exactly as with the index off.
suite("test_variant_index_filter_only_read") {
    def table = "test_variant_index_filter_only_read"
    sql "DROP TABLE IF EXISTS ${table}"
    sql """
        CREATE TABLE ${table} (
            id INT,
            v VARIANT,
            INDEX idx_v (v) USING INVERTED PROPERTIES(
                "parser" = "unicode", "lower_case" = "true", "support_phrase" = "true")
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """
    sql """
        INSERT INTO ${table}
        SELECT number, parse_to_variant(concat('{"title":"',
                   element_at(['software engineer', 'sales manager', 'cloud engineer', 'data analyst'],
                              number % 4 + 1),
                   '","desc":"', repeat(concat('word', number % 7, ' '), 50),
                   'python sql','","n":', number % 10,
                   ',"fs":"', if(number % 2 = 0, 'true', 'false'), '","o":{"city":"',
                   element_at(['manila', 'berlin', 'pune'], number % 3 + 1), '"}}'))
        FROM numbers("number" = "3000")
    """
    sql "sync"
    sql "set enable_common_expr_pushdown = true"

    def filters = [
        "search('v.title:\"software engineer\" OR v.desc:word3')",
        "search('v.title:engineer AND v.o.city:manila')",
        "v['title'] MATCH_PHRASE 'cloud engineer'",
        "v['desc'] MATCH_ANY 'word5 word6'",
        "cast(v['n'] as int) = 4",
        // The subcolumn also feeds an expression no index answers, so it is still read.
        "cast(v['n'] as int) = 4 AND cast(v['n'] as boolean)",
        // A conjunct that is only a cast of the subcolumn.
        "v['fs'] MATCH 'true' AND cast(v['fs'] as boolean)",
        "v['title'] MATCH_ANY 'engineer' AND length(cast(v['title'] as string)) > 14",
        "search('v.title:engineer') AND length(cast(v['title'] as string)) > 14",
    ]
    def outputs = [
        "id",
        "id, cast(v['title'] as string)",
        "id, cast(v['desc'] as string)",
        "id, cast(v['o']['city'] as string), cast(v['n'] as int)",
        "id, cast(v as string)",
    ]
    filters.each { filter ->
        outputs.each { output ->
            def query = "SELECT ${output} FROM ${table} WHERE ${filter} ORDER BY id"
            sql "set enable_inverted_index_query = true"
            def indexed = sql query
            sql "set enable_inverted_index_query = false"
            def scanned = filter.startsWith("search(") ? null : sql(query)
            sql "set enable_inverted_index_query = true"
            assertTrue(indexed.size() > 0, query)
            if (scanned != null) {
                assertEquals(scanned, indexed, query)
            }
            // The same rows, filtered by id only, carry the same values.
            def ids = indexed.collect { it[0] }.join(",")
            def byId = sql "SELECT ${output} FROM ${table} WHERE id IN (${ids}) ORDER BY id"
            assertEquals(byId, indexed, query)
        }
    }

    // SEARCH has no scan to compare with: the same rows as the MATCH it stands for, read without
    // the index.
    sql "set enable_inverted_index_query = false"
    def scanned = sql "SELECT id FROM ${table} WHERE v['title'] MATCH_ANY 'engineer' AND length(cast(v['title'] as string)) > 14 ORDER BY id"
    sql "set enable_inverted_index_query = true"
    assertTrue(scanned.size() > 0)
    assertEquals(scanned, sql("SELECT id FROM ${table} WHERE search('v.title:engineer') AND length(cast(v['title'] as string)) > 14 ORDER BY id"))

    sql "DROP TABLE IF EXISTS ${table}"
}
