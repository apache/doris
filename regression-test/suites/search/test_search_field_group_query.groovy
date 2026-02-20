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

/**
 * Tests for field-grouped query syntax in search() function.
 *
 * Supports ES query_string field-grouped syntax: field:(term1 OR term2)
 * All terms inside the parentheses inherit the field prefix.
 *
 * Equivalent transformations:
 *   title:(rock OR jazz)          → (title:rock OR title:jazz)
 *   title:(rock jazz) AND:and     → (+title:rock +title:jazz)
 *   title:(rock OR jazz) AND music → (title:rock OR title:jazz) AND music
 */
suite("test_search_field_group_query") {
    def tableName = "search_field_group_test"

    // Pin enable_common_expr_pushdown to prevent CI flakiness from fuzzy testing.
    // When false, search() expressions are not pushed to the inverted index evaluation
    // path, causing "SearchExpr should not be executed without inverted index" errors.
    sql """ set enable_common_expr_pushdown = true """

    sql "DROP TABLE IF EXISTS ${tableName}"

    sql """
        CREATE TABLE ${tableName} (
            id INT,
            title VARCHAR(255),
            content TEXT,
            category VARCHAR(100),
            INDEX idx_title (title) USING INVERTED PROPERTIES("parser" = "english"),
            INDEX idx_content (content) USING INVERTED PROPERTIES("parser" = "english"),
            INDEX idx_category (category) USING INVERTED
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """

    sql """INSERT INTO ${tableName} VALUES
        (1, 'rock music history',    'The history of rock and roll music',          'Music'),
        (2, 'jazz music theory',     'Jazz harmony and improvisation theory',        'Music'),
        (3, 'classical music guide', 'Guide to classical music composers',           'Music'),
        (4, 'python programming',    'Python language tutorial for beginners',       'Tech'),
        (5, 'rock climbing tips',    'Tips and techniques for rock climbing',        'Sports'),
        (6, 'jazz and blues fusion', 'The fusion of jazz and blues in modern music', 'Music'),
        (7, 'machine learning',      'Introduction to machine learning algorithms',  'Tech'),
        (8, 'rock and jazz review',  'Review of rock and jazz music festivals',     'Music')
    """

    sql "sync"

    // === Basic field-grouped OR query ===

    // title:(rock OR jazz) should match rows with "rock" or "jazz" in title
    def res1 = sql """
        SELECT id FROM ${tableName}
        WHERE search('title:(rock OR jazz)', '{"default_operator":"or"}')
        ORDER BY id
    """
    // rows 1 (rock music history), 2 (jazz music theory), 5 (rock climbing tips),
    // 6 (jazz and blues fusion), 8 (rock and jazz review)
    assertEquals([[1], [2], [5], [6], [8]], res1)

    // === Field-grouped query vs equivalent expanded query ===

    // title:(rock OR jazz) should give same results as explicit (title:rock OR title:jazz)
    def res2a = sql """
        SELECT id FROM ${tableName}
        WHERE search('title:(rock OR jazz)', '{"default_operator":"or"}')
        ORDER BY id
    """
    def res2b = sql """
        SELECT id FROM ${tableName}
        WHERE search('title:rock OR title:jazz', '{"default_operator":"or"}')
        ORDER BY id
    """
    assertEquals(res2a, res2b)

    // === Field-grouped AND (implicit) ===

    // title:(rock jazz) with default_operator:AND → both "rock" AND "jazz" must be in title
    def res3 = sql """
        SELECT id FROM ${tableName}
        WHERE search('title:(rock jazz)', '{"default_operator":"and"}')
        ORDER BY id
    """
    // Only row 8 has both "rock" and "jazz" in title
    assertEquals([[8]], res3)

    // === Field-grouped query combined with bare query ===

    // title:(rock OR jazz) AND music - explicit title terms + bare "music" on default field
    def res4 = sql """
        SELECT id FROM ${tableName}
        WHERE search('title:(rock OR jazz) AND music', '{"default_field":"title","default_operator":"and"}')
        ORDER BY id
    """
    // Must have "rock" or "jazz" in title AND "music" in title
    // Row 1: title="rock music history" → has "rock" and "music" ✓
    // Row 2: title="jazz music theory" → has "jazz" and "music" ✓
    // Row 8: title="rock and jazz review" → has "rock" and "jazz" but no "music" in title ✗
    // Row 5: title="rock climbing tips" → has "rock" but no "music" ✗
    assertEquals([[1], [2]], res4)

    // === Field-grouped query in multi-field mode ===

    // title:(rock OR jazz) with fields=[title,content]
    // Explicit title:(rock OR jazz) should NOT expand to content field
    def res5a = sql """
        SELECT id FROM ${tableName}
        WHERE search('title:(rock OR jazz)', '{"fields":["title","content"],"type":"cross_fields"}')
        ORDER BY id
    """
    // Only rows where title contains "rock" or "jazz"
    // NOT rows where only content has those terms (row 3 has "rock and roll" in content)
    def res5b = sql """
        SELECT id FROM ${tableName}
        WHERE search('title:rock OR title:jazz', '{"fields":["title","content"],"type":"cross_fields"}')
        ORDER BY id
    """
    assertEquals(res5a, res5b)

    // Rows 1,2,5,6,8 have rock/jazz in title; content-only matches should not appear
    assertEquals(true, res5a.size() >= 1)
    // Row 4 has "python" in title, so it should NOT appear
    assert !res5a.collect { it[0] }.contains(4)
    // Row 7 has "machine learning", so it should NOT appear
    assert !res5a.collect { it[0] }.contains(7)

    // === Phrase inside field group ===

    // title:("rock and") - phrase query inside group
    def res6 = sql """
        SELECT id FROM ${tableName}
        WHERE search('title:("rock and")', '{"default_operator":"or"}')
        ORDER BY id
    """
    // Row 8: "rock and jazz review" → contains "rock and" ✓
    assert res6.collect { it[0] }.contains(8)

    // === Field-grouped query in Lucene mode ===

    // title:(rock OR jazz) in lucene mode
    def res7 = sql """
        SELECT id FROM ${tableName}
        WHERE search('title:(rock OR jazz)', '{"mode":"lucene"}')
        ORDER BY id
    """
    // Should match rows with "rock" or "jazz" in title (SHOULD semantics)
    assertEquals([[1], [2], [5], [6], [8]], res7)

    // title:(rock AND jazz) in lucene mode
    def res8 = sql """
        SELECT id FROM ${tableName}
        WHERE search('title:(rock AND jazz)', '{"mode":"lucene"}')
        ORDER BY id
    """
    // Must have both "rock" AND "jazz" in title
    assertEquals([[8]], res8)

    // === Field-grouped combined with explicit field query ===

    // title:(rock OR jazz) AND category:Music
    def res9 = sql """
        SELECT id FROM ${tableName}
        WHERE search('title:(rock OR jazz) AND category:Music', '{"default_operator":"and"}')
        ORDER BY id
    """
    // Must have (rock or jazz in title) AND category=Music
    // Row 1: rock music history, Music ✓
    // Row 2: jazz music theory, Music ✓
    // Row 5: rock climbing tips, Sports ✗
    // Row 6: jazz and blues fusion, Music ✓
    // Row 8: rock and jazz review, Music ✓
    assertEquals([[1], [2], [6], [8]], res9)

    // === Verify it was previously broken (would have been a syntax error) ===
    // This verifies the fix: parsing title:(rock OR jazz) should not throw
    try {
        def resSyntax = sql """
            SELECT COUNT(*) FROM ${tableName}
            WHERE search('title:(rock OR jazz)', '{"default_operator":"or"}')
        """
        assert resSyntax[0][0] >= 0 : "Should parse and execute without error"
    } catch (Exception e) {
        throw new AssertionError("title:(rock OR jazz) syntax should be supported: " + e.message)
    }

    sql "DROP TABLE IF EXISTS ${tableName}"
}
