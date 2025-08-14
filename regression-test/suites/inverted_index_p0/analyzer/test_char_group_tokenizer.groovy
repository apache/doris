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

import java.sql.SQLException

suite("test_char_group_tokenizer", "p0") {
    def tbl = "test_char_group_tokenizer_tbl"

    // 1) Basic whitespace + punctuation splitting
    sql """
        CREATE INVERTED INDEX TOKENIZER IF NOT EXISTS char_group_ws_punct_tokenizer
        PROPERTIES
        (
            "type" = "char_group",
            "tokenize_on_chars" = "[whitespace], [punctuation]"
        );
    """

    sql """
        CREATE INVERTED INDEX ANALYZER IF NOT EXISTS char_group_ws_punct_analyzer
        PROPERTIES
        (
            "tokenizer" = "char_group_ws_punct_tokenizer"
        );
    """

    // 2) Split CJK characters (each CJK char becomes a token when enabled)
    sql """
        CREATE INVERTED INDEX TOKENIZER IF NOT EXISTS char_group_cjk_tokenizer
        PROPERTIES
        (
            "type" = "char_group",
            "tokenize_on_chars" = "[cjk]"
        );
    """

    sql """
        CREATE INVERTED INDEX ANALYZER IF NOT EXISTS char_group_cjk_analyzer
        PROPERTIES
        (
            "tokenizer" = "char_group_cjk_tokenizer"
        );
    """

    // 3) Custom chars and escaped chars: split on '-' and '_' and also newline/tab/carriage return
    sql """
        CREATE INVERTED INDEX TOKENIZER IF NOT EXISTS char_group_custom_tokenizer
        PROPERTIES
        (
            "type" = "char_group",
            "tokenize_on_chars" = "[-], [_], [\\n], [\\t], [\\r]"
        );
    """

    sql """
        CREATE INVERTED INDEX ANALYZER IF NOT EXISTS char_group_custom_analyzer
        PROPERTIES
        (
            "tokenizer" = "char_group_custom_tokenizer"
        );
    """

    // Wait for analyzers to be ready
    sql """ select sleep(10) """

    // Tokenize checks for whitespace + punctuation
    qt_tokenize_sql """ select tokenize("Hello,World!Test?End", '"analyzer"="char_group_ws_punct_analyzer"'); """
    // Chinese punctuation should split with punctuation enabled
    qt_tokenize_sql """ select tokenize("你好，世界！测试？结束。", '"analyzer"="char_group_ws_punct_analyzer"'); """

    // CJK split behavior mixed with ASCII and digits
    qt_tokenize_sql """ select tokenize("abc中文123", '"analyzer"="char_group_cjk_analyzer"'); """

    // Custom and escaped characters
    qt_tokenize_sql """ select tokenize("hello-world_test", '"analyzer"="char_group_custom_analyzer"'); """
    qt_tokenize_sql """ select tokenize("hello\nworld\ttest\rend", '"analyzer"="char_group_custom_analyzer"'); """

    // Create a table to validate integration with inverted index + analyzer
    sql "DROP TABLE IF EXISTS ${tbl}"
    sql """
        CREATE TABLE ${tbl} (
            `id` bigint NOT NULL AUTO_INCREMENT(1),
            `ch` text NULL,
            INDEX idx_ch (`ch`) USING INVERTED PROPERTIES("support_phrase" = "true", "analyzer" = "char_group_ws_punct_analyzer")
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """ insert into ${tbl} values (1, "Hello,World!Test?End"); """
    sql """ insert into ${tbl} values (2, "你好，世界！测试？结束。"); """
    sql """ insert into ${tbl} values (3, "abc中文123"); """

    try {
        sql "sync"
        sql """ set enable_common_expr_pushdown = true; """

        // Match queries leveraging the analyzer
        qt_sql """ select id, ch from ${tbl} where ch match 'World'; """
        qt_sql """ select id, ch from ${tbl} where ch match '世界'; """
        qt_sql """ select id, ch from ${tbl} where ch match 'Test'; """
    } finally {
        // keep objects for further cases if needed
    }

    // Optional cleanup for analyzers (skip if used by index)
    try {
        sql "drop inverted index analyzer char_group_ws_punct_analyzer"
        sql "drop inverted index analyzer char_group_cjk_analyzer"
        sql "drop inverted index analyzer char_group_custom_analyzer"
    } catch (SQLException e) {
        // It may be used by index; ignore
    }
}