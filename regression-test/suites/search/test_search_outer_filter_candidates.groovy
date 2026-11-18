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

// A SEARCH that is a whole conjunct starts from the rows the scan's other filters kept, read
// through an inverted index or a sort key. The expected rows and scores in the .out file are
// those of the SEARCH over the whole segment, filtered afterwards; with the query cache on, a
// partial result must not reach a later query without the filter.
suite("test_search_outer_filter_candidates") {
    def tables = [
        idx: "test_search_outer_filter_idx",
        key: "test_search_outer_filter_key",
    ]
    def textIndex = 'USING INVERTED PROPERTIES("parser" = "unicode", "lower_case" = "true", "support_phrase" = "true")'
    tables.each { kind, table ->
        sql "DROP TABLE IF EXISTS ${table}"
        def columns = kind == "idx" ? "id INT, account INT" : "account INT NOT NULL, id INT"
        def accountIndex = kind == "idx" ? ", INDEX idx_account (account) USING INVERTED" : ""
        sql """
            CREATE TABLE ${table} (
                ${columns},
                title TEXT,
                skill TEXT,
                place TEXT,
                INDEX idx_title (title) ${textIndex},
                INDEX idx_skill (skill) ${textIndex},
                INDEX idx_place (place) ${textIndex}
                ${accountIndex}
            ) ENGINE=OLAP
            DUPLICATE KEY(${kind == "idx" ? "id" : "account, id"})
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES ("replication_allocation" = "tag.location.default: 1")
        """
        // Account 7 holds about a tenth of the rows, spread by a hash so its titles, skills and
        // places vary like the others'; NULL titles on every 13th row.
        sql """
            INSERT INTO ${table} (id, account, title, skill, place)
            SELECT number,
                   if(abs(murmur_hash3_32(concat('account-', number))) % 10 = 0, 7, number % 50 + 100),
                   if(number % 13 = 0, NULL, concat_ws(' ',
                       element_at(['', 'senior', 'lead'], number % 3 + 1),
                       element_at(['software', 'cloud', 'sales', 'data', 'ai'], number % 5 + 1),
                       element_at(['engineer', 'manager', 'developer', 'analyst'], number % 4 + 1))),
                   element_at(['python sql', 'docker', 'team leadership', 'excel', 'project management'],
                              number % 5 + 1),
                   element_at(['Manila PH', 'Cebu Philippines', 'Berlin Germany', 'Pune India'],
                              number % 4 + 1)
            FROM numbers("number" = "1000")
        """
    }
    sql "sync"
    sql "set enable_common_expr_pushdown = true"

    def dsls = [
        // AND of OR groups, as candidate search writes them
        '(skill:ANY(python docker excel) OR skill:"team leadership") AND ' +
            '(title:"software engineer" OR title:"cloud engineer" OR title:"data analyst") AND ' +
            '(place:ANY(ph pilipinas) OR place:"cebu philippines")',
        'title:"software engineer" OR title:"ai developer"',
        '(title:ANY(engineer developer) OR skill:sql) AND NOT place:berlin',
        'skill:"project management" AND NOT title:"sales manager"',
        // a phrase root, which lists its rows itself
        'title:"cloud engineer"',
        // a term root, read through its scorer
        'skill:docker',
    ]
    def modes = [lucene: '{"mode":"lucene"}', standard: '{"mode":"standard"}']

    tables.each { kind, table ->
        dsls.eachWithIndex { dsl, i ->
            modes.each { modeName, mode ->
                def search = "search('${dsl}', '${mode}')"
                def tag = "${kind}_${modeName}_${i}"
                sql "set enable_inverted_index_query_cache = false"
                "order_qt_${tag}_rows"("SELECT id FROM ${table} WHERE account = 7 AND ${search}")
                if (kind == "idx") {
                    "order_qt_${tag}_scores"("""
                        SELECT id, ROUND(score(), 4) AS s FROM ${table} WHERE account = 7 AND ${search}
                        ORDER BY score() DESC, id LIMIT 10""")
                }

                // With the cache on, the filtered query first: the query without the filter
                // must still count every row of the SEARCH.
                sql "set enable_inverted_index_query_cache = true"
                "qt_${tag}_cached_filtered"("SELECT count(*) FROM ${table} WHERE account = 7 AND ${search}")
                "qt_${tag}_cached_full"("SELECT count(*) FROM ${table} WHERE ${search}")
                "qt_${tag}_cached_filtered_again"("SELECT count(*) FROM ${table} WHERE account = 7 AND ${search}")
            }
        }
    }
    sql "set enable_inverted_index_query_cache = false"

    tables.each { kind, table -> sql "DROP TABLE IF EXISTS ${table}" }
}
