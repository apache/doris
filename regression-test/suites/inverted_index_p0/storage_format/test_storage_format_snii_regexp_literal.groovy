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

// A regular expression without an enumeration prefix names the literal every match holds, and
// an expansion skips the terms without it before running the expression. Every expression here
// must return the same rows on SNII and V3 over dictionaries of several blocks, whether it names
// a literal (inside a concatenation, through anchors, inside a repeated group), names none (an
// alternation, a case-folded or optional part, a class) or is anchored on a prefix.
suite("test_storage_format_snii_regexp_literal", "p0") {
    sql "DROP TABLE IF EXISTS test_snii_regexp_literal"
    sql "DROP TABLE IF EXISTS test_v3_regexp_literal"
    for (format in ["SNII", "V3"]) {
        def table = format == "SNII" ? "test_snii_regexp_literal" : "test_v3_regexp_literal"
        sql """
            CREATE TABLE ${table} (
              id INT,
              body VARCHAR(200),
              tag VARCHAR(50),
              INDEX idx_body (`body`) USING INVERTED PROPERTIES("parser" = "english"),
              INDEX idx_tag (`tag`) USING INVERTED
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY RANDOM BUCKETS 1
            PROPERTIES (
              "replication_allocation" = "tag.location.default: 1",
              "inverted_index_storage_format" = "${format}"
            )
        """
        // 30000 rows of distinct tokens fill several dictionary blocks around the rows below.
        sql """
            INSERT INTO ${table}
            SELECT number, concat('item', lpad(cast(number as string), 6, '0'), ' k', number * 7),
                   concat('tag', lpad(cast(number as string), 6, '0'))
            FROM numbers("number" = "30000")
        """
        sql """
            INSERT INTO ${table} VALUES
              (30000, 'acache miss on the first read', 'acache'),
              (30001, 'the entry was mcached twice', 'mcached'),
              (30002, 'zcache hit', 'zcache'),
              (30003, 'cache', 'cache'),
              (30004, 'cbche and caabche', 'cbche'),
              (30005, 'chechecheche', 'chechecheche'),
              (30006, 'xxxache 7cache', 'xxxache'),
              (30007, 'cacheable caching', 'cacheable'),
              (30008, NULL, NULL)
        """
        sql "sync"
    }

    def expressions = [
        "cache", "cach.", "^zcache\$", "^cach.", "^.*cache", "(?i)CACHE", "cache|zcache", "m.*ed",
        "c[ab]che", "cach?e", "(che)+", "x{0,3}ache", "[0-9]+cache", "ca(ch)+e", "cach(e|ing)",
    ]
    for (int i = 0; i < expressions.size(); i++) {
        def expression = expressions[i]
        "order_qt_body_${i}"("""
            SELECT '${expression}' AS expression, format, id
            FROM (
              SELECT 'SNII' AS format, id FROM test_snii_regexp_literal WHERE body MATCH_REGEXP '${expression}'
              UNION ALL
              SELECT 'V3' AS format, id FROM test_v3_regexp_literal WHERE body MATCH_REGEXP '${expression}'
            ) results
            ORDER BY format, id
        """)
    }
    for (expression in ["cach.", "^cach.", "che\$", "(?i)CBCHE"]) {
        def tag = expression.replaceAll("[^a-zA-Z]", "_")
        "order_qt_tag_${tag}"("""
            SELECT '${expression}' AS expression, format, id
            FROM (
              SELECT 'SNII' AS format, id FROM test_snii_regexp_literal WHERE tag MATCH_REGEXP '${expression}'
              UNION ALL
              SELECT 'V3' AS format, id FROM test_v3_regexp_literal WHERE tag MATCH_REGEXP '${expression}'
            ) results
            ORDER BY format, id
        """)
    }
}
