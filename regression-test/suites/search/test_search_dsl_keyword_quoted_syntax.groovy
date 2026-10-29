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

suite("test_search_dsl_keyword_quoted_syntax", "p0") {
    // SEARCH requires expression pushdown on 4.1, including under fuzzy session settings.
    sql "set enable_common_expr_pushdown = true"

    // SEARCH DSL parses boolean keywords case-insensitively. In value position they stay
    // ordinary terms, so `field:and` keeps searching for the term "and" (DORIS-23194).
    def keywordTable = "test_search_dsl_keyword_values"
    sql "DROP TABLE IF EXISTS ${keywordTable}"
    sql """
        CREATE TABLE ${keywordTable} (
            id INT,
            ch VARCHAR(64),
            INDEX idx_ch (ch) USING INVERTED PROPERTIES("lower_case" = "true")
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """
    sql """INSERT INTO ${keywordTable} VALUES
        (1, 'and'), (2, 'or'), (3, 'not'), (4, 'AND'), (5, 'hello')"""
    sql "sync"

    // Every casing parses and resolves to the same term as the MATCH_ANY control; the keyword
    // index compares terms case-sensitively, so the expected ids differ per casing.
    for (String keyword : ["AND", "and", "And", "OR", "or", "NOT", "not"]) {
        def searchRows = sql("SELECT id FROM ${keywordTable} WHERE SEARCH('ch:${keyword}') ORDER BY id")
                .collect { it[0] as int }
        def matchRows = sql("SELECT id FROM ${keywordTable} WHERE ch MATCH_ANY '${keyword}' ORDER BY id")
                .collect { it[0] as int }
        assertEquals(matchRows, searchRows, "SEARCH('ch:${keyword}') must match MATCH_ANY")
    }
    assertEquals([4], sql("SELECT id FROM ${keywordTable} WHERE SEARCH('ch:AND') ORDER BY id")
            .collect { it[0] as int })
    assertEquals([1], sql("SELECT id FROM ${keywordTable} WHERE SEARCH('ch:and') ORDER BY id")
            .collect { it[0] as int })
    // Lowercase OR still combines two clauses instead of becoming a term.
    assertEquals([1, 2], sql("""
        SELECT id FROM ${keywordTable} WHERE SEARCH('ch:and or ch:or') ORDER BY id""")
            .collect { it[0] as int })

    // Lowercase operators combine clauses instead of being parsed as a bare term
    // (DORIS-23236). Parentheses inside a quoted value must not terminate ANY/ALL early.
    def phraseTable = "test_search_dsl_quoted_paren"
    sql "DROP TABLE IF EXISTS ${phraseTable}"
    sql """
        CREATE TABLE ${phraseTable} (
            id INT,
            ch VARCHAR(1024),
            INDEX idx_ch (ch) USING INVERTED PROPERTIES(
                "lower_case" = "true", "parser" = "unicode", "support_phrase" = "true")
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY RANDOM BUCKETS AUTO
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """
    sql """INSERT INTO ${phraseTable} VALUES
        (1, 'All Quiet on the Western Front (2022 film)'),
        (2, 'F. Murray Abraham'),
        (3, 'unrelated text')"""
    sql "sync"

    assertEquals([1], sql("""
        SELECT id FROM ${phraseTable}
        WHERE SEARCH('ch:ANY("All Quiet on the Western Front (2022 film)")')
        ORDER BY id""").collect { it[0] as int })
    assertEquals([2], sql("""
        SELECT id FROM ${phraseTable}
        WHERE SEARCH('ch:ALL("F. Murray Abraham")')
        ORDER BY id""").collect { it[0] as int })
    assertEquals([1, 2], sql("""
        SELECT id FROM ${phraseTable}
        WHERE SEARCH('ch:ANY("All Quiet on the Western Front (2022 film)") or ch:ALL("F. Murray Abraham")')
        ORDER BY id""").collect { it[0] as int })

    // Escapes are resolved before the term is looked up, so quoted values and closing
    // parentheses remain searchable (DORIS-23239).
    def specialTable = "test_search_dsl_special_chars"
    sql "DROP TABLE IF EXISTS ${specialTable}"
    sql """
        CREATE TABLE ${specialTable} (
            id INT,
            ch VARCHAR(1024),
            INDEX idx_ch (ch) USING INVERTED PROPERTIES("lower_case" = "false")
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY RANDOM BUCKETS AUTO
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """
    sql """INSERT INTO ${specialTable} VALUES (1, '\"ABC\"'), (2, 'ABC)'), (3, 'ABC')"""
    sql "sync"

    // The keyword index stores the value including its quote characters, so the term has to
    // keep the escaped quotes: SEARCH('ch:exact(\"ABC\")').
    assertEquals([1], sql("SELECT id FROM ${specialTable} WHERE ch MATCH_ANY '\"ABC\"'")
            .collect { it[0] as int })
    assertEquals([1], sql("""
        SELECT id FROM ${specialTable} WHERE SEARCH('ch:exact(\\"ABC\\")')""")
            .collect { it[0] as int })
    assertEquals([2], sql("""
        SELECT id FROM ${specialTable} WHERE SEARCH('ch:exact(ABC\\\\))')""")
            .collect { it[0] as int })
}
