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
// through an inverted index or a sort key. Its rows must be those of the full SEARCH within the
// filter, scores must not change, and a partial result must never reach the query cache.
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
        // Account 7 holds every 10th row; titles, skills and places from a few words, with
        // NULL titles on every 13th row.
        sql """
            INSERT INTO ${table} (id, account, title, skill, place)
            SELECT number,
                   if(number % 10 = 0, 7, number % 50 + 100),
                   if(number % 13 = 0, NULL, concat_ws(' ',
                       element_at(['', 'senior', 'lead'], number % 3 + 1),
                       element_at(['software', 'cloud', 'sales', 'data', 'ai'], number % 5 + 1),
                       element_at(['engineer', 'manager', 'developer', 'analyst'], number % 4 + 1))),
                   element_at(['python sql', 'docker', 'team leadership', 'excel', 'project management'],
                              number % 5 + 1),
                   element_at(['Manila PH', 'Cebu Philippines', 'Berlin Germany', 'Pune India'],
                              number % 4 + 1)
            FROM numbers("number" = "2000")
        """
    }
    sql "sync"
    sql "set enable_common_expr_pushdown = true"

    def dsls = [
        // groups written broadest first; the engine reads the rarest first
        '(skill:ANY(python docker excel) OR skill:"team leadership") AND ' +
            '(title:"software engineer" OR title:"cloud engineer" OR title:"data analyst") AND ' +
            '(place:ANY(ph pilipinas) OR place:"cebu philippines")',
        'title:"software engineer" OR title:"ai developer"',
        '(title:ANY(engineer developer) OR skill:sql) AND NOT place:berlin',
        'skill:"project management" AND NOT title:"sales manager"',
    ]
    def modes = ['{"mode":"lucene"}', '{"mode":"standard"}']

    tables.each { kind, table ->
        dsls.each { dsl ->
            modes.each { mode ->
                def search = "search('${dsl}', '${mode}')"
                sql "set enable_inverted_index_query_cache = false"
                // The full SEARCH, filtered afterwards.
                def expected = sql """SELECT id FROM ${table} WHERE account = 7 AND id IN
                                      (SELECT id FROM ${table} WHERE ${search}) ORDER BY id"""
                def actual = sql "SELECT id FROM ${table} WHERE account = 7 AND ${search} ORDER BY id"
                assertEquals(expected, actual, "${kind} ${mode}: ${dsl}")
                assertTrue(sql("SELECT count(*) FROM ${table} WHERE ${search}")[0][0] > actual.size(),
                           "${kind} ${mode}: ${dsl}")

                // Scores are those of the full SEARCH.
                def scored = sql """SELECT id, score() AS s FROM ${table} WHERE account = 7 AND ${search}
                                    ORDER BY s DESC, id LIMIT 1000"""
                def full = sql "SELECT id, score() AS s FROM ${table} WHERE ${search} ORDER BY s DESC, id LIMIT 5000"
                def fullScores = full.collectEntries { [(it[0]): it[1]] }
                scored.each { row -> assertEquals(fullScores[row[0]], row[1]) }

                // With the cache on, the filtered query first: the full query must not read a
                // partial result back.
                def fullCount = sql "SELECT count(*) FROM ${table} WHERE ${search}"
                sql "set enable_inverted_index_query_cache = true"
                assertEquals(actual, sql("SELECT id FROM ${table} WHERE account = 7 AND ${search} ORDER BY id"))
                assertEquals(fullCount, sql("SELECT count(*) FROM ${table} WHERE ${search}"))
                assertEquals(actual, sql("SELECT id FROM ${table} WHERE account = 7 AND ${search} ORDER BY id"))
            }
        }
    }
    sql "set enable_inverted_index_query_cache = false"

    tables.each { kind, table -> sql "DROP TABLE IF EXISTS ${table}" }
}
