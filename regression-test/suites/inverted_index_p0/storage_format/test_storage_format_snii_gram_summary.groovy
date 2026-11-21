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

// An expansion over an SNII dictionary that must find a text in every term (MATCH_PHRASE_EDGE,
// whose one token is a contains and whose first token is a suffix) consults the DICT gram summary
// and opens only the blocks that can hold the text. The dictionaries here span several blocks
// (tens of thousands of distinct tokens), and every expansion must return the same rows as the V3
// index of the same data, whether the summary can prune (a text of three bytes or more), cannot (a
// two-byte text, a prefix) or proves the text absent; the glob forms are checked alongside.
suite("test_storage_format_snii_gram_summary", "p0") {
    sql "DROP TABLE IF EXISTS test_snii_gram_summary"
    sql "DROP TABLE IF EXISTS test_v3_gram_summary"
    for (format in ["SNII", "V3"]) {
        def table = format == "SNII" ? "test_snii_gram_summary" : "test_v3_gram_summary"
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
        // 30000 rows of distinct tokens: the tokenized dictionary and the keyword dictionary both
        // run to several DICT blocks, and no token of theirs holds "cache".
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
              (30004, NULL, NULL)
        """
        sql "sync"
    }

    def expansions = [
        ["infix", "body:*cache*"],
        ["infix_longer", "body:*cached*"],
        ["infix_absent", "body:*zqzqz*"],
        ["infix_two_bytes", "body:*ca*"],
        ["prefix", "body:item0299*"],
        ["suffix", "body:*cache"],
        ["keyword_infix", "tag:*cache*"],
        ["keyword_prefix", "tag:tag0001*"],
    ]
    for (expansion in expansions) {
        def (name, pattern) = expansion
        "order_qt_${name}"("""
            SELECT format, count(*) AS row_count, min(id) AS first_id, max(id) AS last_id
            FROM (
              SELECT 'SNII' AS format, id FROM test_snii_gram_summary
              WHERE SEARCH('${pattern}', '{"mode":"standard"}')
              UNION ALL
              SELECT 'V3' AS format, id FROM test_v3_gram_summary
              WHERE SEARCH('${pattern}', '{"mode":"standard"}')
            ) results
            GROUP BY format
            ORDER BY format
        """)
    }

    order_qt_infix_rows """
        SELECT format, id
        FROM (
          SELECT 'SNII' AS format, id FROM test_snii_gram_summary
          WHERE SEARCH('body:*cache*', '{"mode":"standard"}')
          UNION ALL
          SELECT 'V3' AS format, id FROM test_v3_gram_summary
          WHERE SEARCH('body:*cache*', '{"mode":"standard"}')
        ) results
        ORDER BY format, id
    """

    def edges = [
        ["edge_contains", "body", "ache"],
        ["edge_contains_longer", "body", "cached"],
        ["edge_contains_absent", "body", "zqzqz"],
        ["edge_contains_two_bytes", "body", "ca"],
        ["edge_suffix_prefix", "body", "ache miss"],
        ["edge_three_tokens", "body", "ntry was mcach"],
        ["edge_suffix_absent", "body", "zqzqz hit"],
        ["edge_keyword", "tag", "ache"],
    ]
    for (edge in edges) {
        def (name, column, text) = edge
        "order_qt_${name}"("""
            SELECT format, id
            FROM (
              SELECT 'SNII' AS format, id FROM test_snii_gram_summary WHERE ${column} MATCH_PHRASE_EDGE '${text}'
              UNION ALL
              SELECT 'V3' AS format, id FROM test_v3_gram_summary WHERE ${column} MATCH_PHRASE_EDGE '${text}'
            ) results
            ORDER BY format, id
        """)
    }
}
