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

suite("test_ngram_bloomfilter_index_like_escape") {
    // An NGRAM_BF index prunes a page whose bloom filter misses a token of the LIKE pattern, so
    // the tokens have to occur in every string the pattern matches. Deriving them means reading
    // the pattern's escapes exactly the way LIKE does, which the storage layer does not attempt;
    // it leaves the index alone instead. Whatever the reason, turning the index on must never
    // change a result set.

    // One backslash. Backslashes go through SQL string-literal unescaping before reaching LIKE,
    // so each one is written twice in the statement text.
    def bs = '\\'
    def quote = { String raw -> raw.replace(bs, bs + bs) }

    def values = [
            'a' + bs + 'bc',                // one literal backslash
            'xxab' + bs + bs + 'cdyy',      // two consecutive literal backslashes
            'zab' + bs + 'Zcdz',
            'a' + bs + 'Zbb',               // backslash in front of an ordinary character
            'a' + bs + '中zz',              // backslash in front of a multi-byte character
            '100%off',                      // literal percent in the data
            'x_y',                          // literal underscore in the data
            'qa%bz',                        // for the custom-escape pattern below
            'plain ascii row',
            '中文测试行',
    ]

    // [pattern, escape clause]. The escape-free entries must keep using the index; the rest are
    // the shapes where a mis-read escape silently drops rows.
    def cases = [
            ['a' + bs + bs + '%',       ''],              // "a", a backslash, anything
            ['%ab' + bs * 4 + '%cd%',   ''],              // two backslashes between literals
            ['%ab' + bs + bs + '_cd%',  ''],              // backslash, then the "_" wildcard
            ['a' + bs + 'Z%',           ''],              // backslash in front of "Z"
            ['a' + bs + '中%',          ''],              // backslash in front of a CJK char
            ['%100' + bs + '%%',        ''],              // escaped "%" is a literal percent
            ['%x' + bs + '_y%',         ''],              // escaped "_" is a literal underscore
            ['%' + bs + bs + '%',       ''],              // any row holding a backslash
            ['%' + bs * 4 + '%',        ''],              // any row holding two backslashes
            ['%100!%%',                 "ESCAPE '!'"],    // custom escape, escaped percent
            ['%x!_y%',                  "ESCAPE '!'"],    // custom escape, escaped underscore
            ['%a!%b%',                  "ESCAPE '!'"],    // custom escape, no matching row here
            ['%plain%',                 ''],              // escape-free control
            ['%中文%',                  ''],              // escape-free UTF-8 control
    ]

    def tables = ['ngram_like_escape_g1': 1, 'ngram_like_escape_g2': 2, 'ngram_like_escape_none': 0]
    tables.each { name, gramSize ->
        def indexClause = gramSize > 0 ? """,
            INDEX idx_v (v) USING NGRAM_BF PROPERTIES("gram_size" = "${gramSize}", "bf_size" = "1024")""" : ""
        sql "DROP TABLE IF EXISTS ${name}"
        sql """
        CREATE TABLE ${name} (
            id int,
            v varchar(200)${indexClause}
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        // One statement per row. A page that also holds an unrelated row can carry the very token
        // a mis-read pattern asks for, which hides the pruning bug this suite is guarding.
        values.eachWithIndex { v, i ->
            sql "INSERT INTO ${name} VALUES (${i}, '${quote(v)}')"
        }
    }

    sql "SET enable_function_pushdown = true"

    cases.each { p, escapeClause ->
        def where = "v LIKE '${quote(p)}' ${escapeClause}"
        def expected = sql "SELECT id FROM ngram_like_escape_none WHERE ${where} ORDER BY id"
        // Guard against the comparison below passing because the pattern matches nothing at all.
        assertTrue(expected.size() > 0,
                "pattern LIKE '${p}' ${escapeClause} matches no row, it proves nothing")
        ['ngram_like_escape_g1', 'ngram_like_escape_g2'].each { name ->
            def actual = sql "SELECT id FROM ${name} WHERE ${where} ORDER BY id"
            assertEquals(expected, actual,
                    "NGRAM_BF pruning changed the result of LIKE '${p}' ${escapeClause} on ${name}")
        }
    }
}
