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

// Phrases of one field OR'ed together run as one phrase set. The set must keep the rows of its
// phrases run one at a time, and score() must stay the sum of their BM25 scores.
suite("test_search_phrase_sets") {
    def formats = ["V2", "SNII"]
    def tables = [:]
    formats.each { fmt ->
        def table = "test_search_phrase_sets_${fmt.toLowerCase()}"
        tables[fmt] = table
        sql "DROP TABLE IF EXISTS ${table}"
        sql """
            CREATE TABLE ${table} (
                id INT,
                title TEXT,
                skill TEXT,
                work VARIANT,
                INDEX idx_title (title) USING INVERTED PROPERTIES(
                    "parser" = "unicode", "lower_case" = "true", "support_phrase" = "true"),
                INDEX idx_skill (skill) USING INVERTED PROPERTIES(
                    "parser" = "unicode", "lower_case" = "true", "support_phrase" = "true"),
                INDEX idx_work (work) USING INVERTED PROPERTIES(
                    "parser" = "unicode", "lower_case" = "true", "support_phrase" = "true")
            ) ENGINE=OLAP
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 2
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "inverted_index_storage_format" = "${fmt}"
            )
        """
        // Titles and descriptions built from a few HR words, so the words of a phrase often
        // meet without forming it; every 17th row has NULLs.
        sql """
            INSERT INTO ${table}
            SELECT number,
                   if(number % 17 = 0, NULL, concat_ws(' ',
                       element_at(['', 'senior', 'junior', 'lead'], number % 4 + 1),
                       element_at(['hr', 'human resources', 'human resource', 'payroll', 'hr co'],
                                  number % 5 + 1),
                       element_at(['coordinator', 'intern', 'ordinator', 'manager', 'coordinator intern',
                                   'intern hr'], number % 6 + 1))),
                   element_at(['employee relations', 'payroll', 'relations employee', 'hiring'],
                              number % 4 + 1),
                   if(number % 17 = 0, NULL, parse_to_variant(concat('{"work_description":"',
                       array_join(array_map(x -> element_at(['hr', 'intern', 'coordinator', 'human',
                           'resources', 'the', 'of', 'payroll', 'co', 'ordinator'],
                           abs(murmur_hash3_32(concat(number, '-', x))) % 10 + 1), array_range(12)), ' '),
                       '"}')))
            FROM numbers("number" = "300")
        """
    }
    sql "sync"
    sql "set enable_inverted_index_query_cache = false"
    sql "set enable_common_expr_pushdown = true"

    // Phrases sharing words, in both orders, one with a word no row holds.
    def phrases = ['"hr intern"', '"hr coordinator"', '"human resources coordinator"',
                   '"hr co ordinator"', '"human resource coordinator intern"', '"intern hr"',
                   '"not present here"']
    def orOf = { field, list -> "(" + list.collect { "${field}:${it}" }.join(' OR ') + ")" }

    formats.each { fmt ->
        def t = tables[fmt]
        def ids = { where -> sql("SELECT id FROM ${t} WHERE ${where} ORDER BY id").collect { it[0] } }
        def scores = { where ->
            sql("SELECT id, score() AS s FROM ${t} WHERE ${where} ORDER BY s DESC, id LIMIT 1000")
                    .collectEntries { [(it[0]): it[1] as double] }
        }
        def topScores = { where ->
            sql("SELECT id, score() AS s FROM ${t} WHERE ${where} ORDER BY s DESC, id LIMIT 5")
                    .collect { it[1] as double }
        }
        def close = { double a, double b -> Math.abs(a - b) <= 1e-4 * Math.max(1.0, Math.abs(b)) }

        ['{"mode":"lucene"}', '{"mode":"standard"}'].each { mode ->
            def search = { dsl -> "search('${dsl}', '${mode}')" }
            ['title', 'work.work_description'].each { field ->
                def label = "${fmt} ${mode} ${field}"
                def set = search(orOf(field, phrases))

                // The rows of the phrases run one at a time.
                def expected = new TreeSet<Integer>()
                phrases.each { phrase -> expected.addAll(ids(search("${field}:${phrase}"))) }
                assertTrue(expected.size() > 20, "${label}: ${expected.size()} rows")
                assertEquals(expected as List, ids(set), label)
                assertEquals([[expected.size() as long]], sql("SELECT count(*) FROM ${t} WHERE ${set}"),
                             label)

                // score() is the sum of the phrases' scores, on every row and at the top.
                def sums = [:]
                phrases.each { phrase ->
                    scores(search("${field}:${phrase}")).each { id, s -> sums[id] = (sums[id] ?: 0.0d) + s }
                }
                def got = scores(set)
                assertEquals(sums.keySet() as Set, got.keySet() as Set, label)
                got.each { id, s -> assertTrue(close(s, sums[id]), "${label} id ${id}: ${s} vs ${sums[id]}") }
                def top = sums.values().sort { -it }.take(5)
                def gotTop = topScores(set)
                assertEquals(top.size(), gotTop.size(), label)
                top.eachWithIndex { s, i -> assertTrue(close(gotTop[i], s), "${label} top ${i}: ${gotTop[i]} vs ${s}") }
            }

            // ANDed with another clause: the rows of any phrase, run one at a time, that the
            // clause keeps.
            def cases = [
                [orOf('title', phrases.take(3)), phrases.take(3).collect { "title:${it}" },
                 'skill:payroll'],
                ['(' + orOf('title', phrases) + ' OR ' + orOf('work.work_description', phrases.take(4)) + ')',
                 phrases.collect { "title:${it}" } + phrases.take(4).collect { "work.work_description:${it}" },
                 'skill:ANY(employee hiring)'],
            ]
            cases.each { set, leaves, clause ->
                def matched = new TreeSet<Integer>()
                leaves.each { leaf -> matched.addAll(ids(search(leaf))) }
                def kept = ids(search(clause)) as Set
                assertEquals(matched.findAll { kept.contains(it) } as List,
                             ids(search("${set} AND ${clause}")), "${fmt} ${mode}: ${set} AND ${clause}")
            }

            // Excluded: the rows with a title and without any of the phrases.
            def excluded = ids(search('skill:ANY(employee relations) AND NOT ' + orOf('title', phrases.take(2))) +
                               ' AND title IS NOT NULL')
            def withPhrase = new TreeSet<Integer>()
            phrases.take(2).each { phrase -> withPhrase.addAll(ids(search("title:${phrase}"))) }
            def expectedExcluded = ids(search('skill:ANY(employee relations)') + ' AND title IS NOT NULL')
                    .findAll { !withPhrase.contains(it) }
            assertEquals(expectedExcluded, excluded, "${fmt} ${mode}: NOT")
        }

        // The rows of MATCH_PHRASE.
        assertEquals(ids("(" + phrases.collect { "title MATCH_PHRASE '${it.replace('"', '')}'" }.join(' OR ') + ")"),
                     ids("search('${orOf('title', phrases)}')"))
    }

    formats.each { fmt -> sql "DROP TABLE IF EXISTS ${tables[fmt]}" }
}
