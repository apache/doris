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

suite("test_custom_analyzer", "p0") {
    def indexTbName1 = "test_custom_analyzer_1"
    def indexTbName2 = "test_custom_analyzer_2"

    sql """
        CREATE INVERTED INDEX TOKENIZER IF NOT EXISTS edge_ngram_phone_number_tokenizer
        PROPERTIES
        (
            "type" = "edge_ngram",
            "min_gram" = "3",
            "max_gram" = "10",
            "token_chars" = "digit"
        );
    """

    sql """
        CREATE INVERTED INDEX TOKEN_FILTER IF NOT EXISTS word_splitter
        PROPERTIES
        (
            "type" = "word_delimiter",
            "split_on_numerics" = "false",
            "split_on_case_change" = "false"
        );
    """

    sql """
        CREATE INVERTED INDEX ANALYZER IF NOT EXISTS edge_ngram_phone_number
        PROPERTIES
        (
            "tokenizer" = "edge_ngram_phone_number_tokenizer"
        );
    """

    sql """
        CREATE INVERTED INDEX ANALYZER IF NOT EXISTS lowercase_delimited
        PROPERTIES
        (    
            "tokenizer" = "standard",
            "token_filter" = "asciifolding, word_splitter, lowercase"
        );
    """

    sql """
        CREATE INVERTED INDEX ANALYZER IF NOT EXISTS keyword_lowercase
        PROPERTIES
        (
            "tokenizer" = "keyword",
            "token_filter" = "asciifolding, lowercase"
        );
    """

    sql """
        CREATE INVERTED INDEX ANALYZER IF NOT EXISTS basic_analyzer
        PROPERTIES
        (
            "tokenizer" = "basic",
            "token_filter" = "lowercase"
        );
    """

    sql """
        CREATE INVERTED INDEX ANALYZER IF NOT EXISTS icu_analyzer
        PROPERTIES
        (
            "tokenizer" = "icu",
            "token_filter" = "lowercase"
        );
    """

    // Create pinyin tokenizer - first letter only
    sql """
        CREATE INVERTED INDEX TOKENIZER IF NOT EXISTS pinyin_abbr_tokenizer
        PROPERTIES
        (
            "type" = "pinyin",
            "keep_first_letter" = "true",
            "keep_full_pinyin" = "false",
            "keep_joined_full_pinyin    " = "false",
            "keep_original" = "false",
            "lowercase" = "true"
        );
    """

    // Create pinyin tokenizer - joined full pinyin
    sql """
        CREATE INVERTED INDEX TOKENIZER IF NOT EXISTS pinyin_joined_tokenizer
        PROPERTIES
        (
            "type" = "pinyin",
            "keep_first_letter" = "true",
            "keep_full_pinyin" = "false",
            "keep_joined_full_pinyin" = "true",
            "keep_original" = "false",
            "keep_none_chinese" = "true",
            "keep_none_chinese_in_joined_full_pinyin" = "true",
            "lowercase" = "true"
        );
    """

    // Create pinyin tokenizer - separate first letter
    sql """
        CREATE INVERTED INDEX TOKENIZER IF NOT EXISTS pinyin_separate_tokenizer
        PROPERTIES
        (
            "type" = "pinyin",
            "keep_first_letter" = "true",
            "keep_separate_first_letter" = "true",
            "keep_full_pinyin" = "true",
            "keep_original" = "false",
            "lowercase" = "true"
        );
    """

    // Create pinyin tokenizer - with limit and remove duplicates
    sql """
        CREATE INVERTED INDEX TOKENIZER IF NOT EXISTS pinyin_limited_tokenizer
        PROPERTIES
        (
            "type" = "pinyin",
            "keep_first_letter" = "true",
            "keep_full_pinyin" = "false",
            "keep_original" = "false",
            "limit_first_letter_length" = "10",
            "remove_duplicated_term" = "true",
            "lowercase" = "true"
        );
    """

    // Create pinyin filter - default full features
    sql """
        CREATE INVERTED INDEX TOKEN_FILTER IF NOT EXISTS pinyin_filter_default
        PROPERTIES
        (
            "type" = "pinyin",
            "keep_first_letter" = "true",
            "keep_full_pinyin" = "true",
            "keep_original" = "true",
            "lowercase" = "true"
        );
    """

    // Create pinyin filter - first letter only
    sql """
        CREATE INVERTED INDEX TOKEN_FILTER IF NOT EXISTS pinyin_filter_abbr
        PROPERTIES
        (
            "type" = "pinyin",
            "keep_first_letter" = "true",
            "keep_separate_first_letter" = "false",
            "keep_full_pinyin" = "false",
            "keep_original" = "false",
            "lowercase" = "true"
        );
    """

    // Create pinyin filter - with none chinese options
    sql """
        CREATE INVERTED INDEX TOKEN_FILTER IF NOT EXISTS pinyin_filter_mixed
        PROPERTIES
        (
            "type" = "pinyin",
            "keep_first_letter" = "true",
            "keep_full_pinyin" = "true",
            "keep_original" = "true",
            "keep_none_chinese" = "true",
            "keep_none_chinese_together" = "false",
            "none_chinese_pinyin_tokenize" = "true",
            "lowercase" = "true",
            "remove_duplicated_term" = "true"
        );
    """

    // Create analyzer with pinyin tokenizer - first letter
    sql """
        CREATE INVERTED INDEX ANALYZER IF NOT EXISTS pinyin_analyzer
        PROPERTIES
        (
            "tokenizer" = "pinyin_abbr_tokenizer"
        );
    """

    // Create analyzer with pinyin tokenizer - joined
    sql """
        CREATE INVERTED INDEX ANALYZER IF NOT EXISTS pinyin_joined_analyzer
        PROPERTIES
        (
            "tokenizer" = "pinyin_joined_tokenizer"
        );
    """

    // Create analyzer with pinyin tokenizer - separate
    sql """
        CREATE INVERTED INDEX ANALYZER IF NOT EXISTS pinyin_separate_analyzer
        PROPERTIES
        (
            "tokenizer" = "pinyin_separate_tokenizer"
        );
    """

    // Create analyzer with keyword tokenizer + pinyin filter
    sql """
        CREATE INVERTED INDEX ANALYZER IF NOT EXISTS keyword_pinyin
        PROPERTIES
        (
            "tokenizer" = "keyword",
            "token_filter" = "pinyin_filter_default"
        );
    """

    // Create analyzer with standard tokenizer + pinyin filter
    sql """
        CREATE INVERTED INDEX ANALYZER IF NOT EXISTS standard_pinyin
        PROPERTIES
        (
            "tokenizer" = "standard",
            "token_filter" = "pinyin_filter_abbr"
        );
    """

    // Create analyzer with keyword tokenizer + mixed pinyin filter
    sql """
        CREATE INVERTED INDEX ANALYZER IF NOT EXISTS keyword_pinyin_mixed
        PROPERTIES
        (
            "tokenizer" = "keyword",
            "token_filter" = "pinyin_filter_mixed"
        );
    """

    // Wait for all analyzers to be ready - increased timeout due to many objects
    sql """ select sleep(15) """

    qt_tokenize_sql """ select tokenize("Србија Херцеговина Щучин гурзуф  Ψ4  Босна", '"analyzer"="keyword_lowercase"'); """
    qt_tokenize_sql """ select tokenize("Wasted…Again", '"analyzer"="keyword_lowercase"'); """
    qt_tokenize_sql """ select tokenize("♯P-complete", '"analyzer"="keyword_lowercase"'); """
    qt_tokenize_sql """ select tokenize("a∴a∴", '"analyzer"="keyword_lowercase"'); """
    qt_tokenize_sql """ select tokenize("RX J1242−11", '"analyzer"="keyword_lowercase"'); """
    qt_tokenize_sql """ select tokenize("1080º Avalanche", '"analyzer"="keyword_lowercase"'); """
    qt_tokenize_sql """ select tokenize("clayfighter 63⅓", '"analyzer"="lowercase_delimited"'); """
    qt_tokenize_sql """ select tokenize("β-carbon nitride", '"analyzer"="lowercase_delimited"'); """
    qt_tokenize_sql """ select tokenize("ǁŨǁe language", '"analyzer"="lowercase_delimited"'); """
    qt_tokenize_sql """ select tokenize("1080º Avalanche", '"analyzer"="lowercase_delimited"'); """
    qt_tokenize_sql """ select tokenize("GET /images/hm_bg.jpg HTTP/1.0", '"analyzer"="basic_analyzer"'); """
    qt_tokenize_sql """ select tokenize("让我们说「Hello」そして世界とつながろう！", '"analyzer"="icu_analyzer"'); """
    
    // Test pinyin tokenize functions - different analyzers
    qt_tokenize_pinyin1 """ select tokenize("刘德华", '"analyzer"="pinyin_analyzer"'); """
    qt_tokenize_pinyin2 """ select tokenize("张学友", '"analyzer"="pinyin_analyzer"'); """
    qt_tokenize_pinyin3 """ select tokenize("刘德华", '"analyzer"="keyword_pinyin"'); """
    qt_tokenize_pinyin4 """ select tokenize("DJ音乐家", '"analyzer"="keyword_pinyin"'); """
    
    // Test polyphone phrases - these should use polyphone dictionary
    qt_tokenize_pinyin_poly1 """ select tokenize("你呢", '"analyzer"="keyword_pinyin"'); """
    qt_tokenize_pinyin_poly2 """ select tokenize("做不了", '"analyzer"="keyword_pinyin"'); """
    qt_tokenize_pinyin_poly3 """ select tokenize("空调", '"analyzer"="keyword_pinyin"'); """
    qt_tokenize_pinyin_poly4 """ select tokenize("厦门", '"analyzer"="keyword_pinyin"'); """
    qt_tokenize_pinyin_poly5 """ select tokenize("长城", '"analyzer"="keyword_pinyin"'); """
    qt_tokenize_pinyin_poly6 """ select tokenize("重要", '"analyzer"="keyword_pinyin"'); """
    
    // Test joined full pinyin
    qt_tokenize_pinyin6 """ select tokenize("刘德华", '"analyzer"="pinyin_joined_analyzer"'); """
    qt_tokenize_pinyin7 """ select tokenize("刘a德华", '"analyzer"="pinyin_joined_analyzer"'); """
    
    // Test separate first letter
    qt_tokenize_pinyin8 """ select tokenize("刘德华", '"analyzer"="pinyin_separate_analyzer"'); """
    
    // Test standard tokenizer with pinyin filter
    qt_tokenize_pinyin9 """ select tokenize("刘德华", '"analyzer"="standard_pinyin"'); """
    qt_tokenize_pinyin10 """ select tokenize("刘德华 张学友", '"analyzer"="standard_pinyin"'); """
    qt_tokenize_pinyin11 """ select tokenize("DJ音乐家", '"analyzer"="standard_pinyin"'); """

    // Test mixed mode with none chinese
    qt_tokenize_pinyin12 """ select tokenize("DJ音乐家", '"analyzer"="keyword_pinyin_mixed"'); """
    qt_tokenize_pinyin13 """ select tokenize("刘德华ABC123", '"analyzer"="keyword_pinyin_mixed"'); """
     
    sql "DROP TABLE IF EXISTS ${indexTbName1}"
    sql """
        CREATE TABLE ${indexTbName1} (
            `a` bigint NOT NULL AUTO_INCREMENT(1),
            `ch` text NULL,
            INDEX idx_ch (`ch`) USING INVERTED PROPERTIES("support_phrase" = "true", "analyzer" = "lowercase_delimited")
        ) ENGINE=OLAP
        DUPLICATE KEY(`a`)
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """ insert into ${indexTbName1} values(1, "abcDEF"); """
    sql """ insert into ${indexTbName1} values(2, "中国人民"); """

    try {
        sql "sync"
        sql """ set enable_common_expr_pushdown = true; """

        qt_sql """ select * from ${indexTbName1} where ch match 'abcDEF'; """
        qt_sql """ select * from ${indexTbName1} where ch match '中'; """
    } finally {
    }

    sql "DROP TABLE IF EXISTS ${indexTbName2}"
    sql """
        CREATE TABLE ${indexTbName2} (
            `a` bigint NOT NULL AUTO_INCREMENT(1),
            `ch` text NULL,
            INDEX idx_ch (`ch`) USING INVERTED PROPERTIES("support_phrase" = "true", "analyzer" = "edge_ngram_phone_number")
        ) ENGINE=OLAP
        DUPLICATE KEY(`a`)
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """ INSERT INTO ${indexTbName2} VALUES ('3', 'Wikipedia;Miscellaneous-Jj102786 / 3tle Born Oct 27th 1986 @ Blytheville, Arkansas @ 9:14pm 23 yrs of age male,white Cucassion American raised Religion:Pentocostal,Church of God'); """

    try {
        sql "sync"
        sql """ set enable_common_expr_pushdown = true; """

        qt_sql """ select * from ${indexTbName2} where ch match '102'; """
    } finally {
    }

    try {
        sql "drop inverted index analyzer edge_ngram_phone_number"
    } catch (SQLException e) {
        if (e.message.contains("is used by index")) {
            logger.info("used by index")
        }
    }

    try {
        sql "DROP TABLE IF EXISTS test_custom_analyzer_3"
        sql """
            CREATE TABLE test_custom_analyzer_3 (
                `a` bigint NOT NULL AUTO_INCREMENT(1),
                `ch` text NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`a`)
            DISTRIBUTED BY RANDOM BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
            );
        """

        sql """ insert into test_custom_analyzer_3 values(1, "GET /french/images/nav_venue_off.gif HTTP/1.0"); """
        sql """ alter table test_custom_analyzer_3 add index idx_ch(`ch`) using inverted properties("support_phrase" = "true", "analyzer" = "lowercase_delimited"); """

        qt_sql """ select * from test_custom_analyzer_3 where ch match 'nav_venue_off.gif'; """
    } catch (SQLException e) {
    }

    sql "DROP TABLE IF EXISTS ${indexTbName1}"
    sql """
        CREATE TABLE ${indexTbName1} (
            `a` bigint NOT NULL,
            `ch` text NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`a`)
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """ insert into ${indexTbName1} values(1, "FOO BAR"); """
    qt_sql """ select tokenize("FOO BAR", '"analyzer"="lowercase_delimited"'); """
    qt_sql """ select tokenize("FOO", '"analyzer"="lowercase_delimited"'); """
    qt_sql """ select tokenize("BAR", '"analyzer"="lowercase_delimited"'); """

    sql """ alter table ${indexTbName1} add index idx_ch_default(`ch`)  using inverted; """
    wait_for_last_build_index_finish("${indexTbName1}", 60000)
    sql """ alter table ${indexTbName1} add index idx_ch(`ch`) using inverted properties("support_phrase" = "true", "analyzer" = "lowercase_delimited"); """
    wait_for_last_build_index_finish("${indexTbName1}", 60000)

    qt_sql """ select * from ${indexTbName1} where ch match_all 'FOO'; """
    qt_sql """ select * from ${indexTbName1} where ch match_all 'BAR'; """
    qt_sql """ select * from ${indexTbName1} where ch match_all 'FOO BAR'; """

    qt_sql """ select * from ${indexTbName1} where ch match_phrase_prefix 'FOO'; """
    qt_sql """ select * from ${indexTbName1} where ch match_phrase_prefix 'BAR'; """
    qt_sql """ select * from ${indexTbName1} where ch match_phrase_prefix 'FOO BAR'; """

    def variantTableName = "test_custom_analyzer_2"
    sql "DROP TABLE IF EXISTS ${variantTableName}"
    sql """
        CREATE TABLE ${variantTableName} (
            `a` bigint NOT NULL,
            `var` variant<'string_*' : string,
                properties("variant_max_subcolumns_count" = "1", "variant_enable_typed_paths_to_sparse" = "true")
            > NULL,
            INDEX idx_string (var) USING INVERTED PROPERTIES("field_pattern" = "string_*"),
            INDEX idx_string_prefix (var) USING INVERTED PROPERTIES("field_pattern" = "string_*", "support_phrase" = "true", "analyzer" = "lowercase_delimited")
        ) ENGINE=OLAP
        DUPLICATE KEY(`a`)
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """ insert into ${variantTableName} values(1, '{"string_1" : "FOO BAR", "string_2" : "FOO BAR", "string_3" : "FOO BAR"}'), (2, '{"string_3" : "FOO BAR"}'); """

    qt_sql """ select * from ${variantTableName} where cast(var['string_1'] as varchar) match_all 'FOO'; """
    qt_sql """ select * from ${variantTableName} where cast(var['string_1'] as varchar) match_all 'BAR'; """
    qt_sql """ select * from ${variantTableName} where cast(var['string_1'] as varchar) match_all 'FOO BAR'; """

    qt_sql """ select * from ${variantTableName} where cast(var['string_1'] as varchar) match_phrase_prefix 'FOO'; """
    qt_sql """ select * from ${variantTableName} where cast(var['string_1'] as varchar) match_phrase_prefix 'BAR'; """
    qt_sql """ select * from ${variantTableName} where cast(var['string_1'] as varchar) match_phrase_prefix 'FOO BAR'; """

    // Test pinyin tokenizer with table - keyword_pinyin analyzer
    def indexTbName4 = "test_custom_analyzer_pinyin_4"
    sql "DROP TABLE IF EXISTS ${indexTbName4}"
    sql """
        CREATE TABLE ${indexTbName4} (
            `a` bigint NOT NULL AUTO_INCREMENT(1),
            `name` text NULL,
            INDEX idx_name (`name`) USING INVERTED PROPERTIES("support_phrase" = "true", "analyzer" = "keyword_pinyin")
        ) ENGINE=OLAP
        DUPLICATE KEY(`a`)
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """ INSERT INTO ${indexTbName4} VALUES (1, "刘德华"); """
    sql """ INSERT INTO ${indexTbName4} VALUES (2, "张学友"); """
    sql """ INSERT INTO ${indexTbName4} VALUES (3, "郭富城"); """
    sql """ INSERT INTO ${indexTbName4} VALUES (4, "DJ音乐家"); """
    sql """ INSERT INTO ${indexTbName4} VALUES (5, "刘德华ABC"); """
    sql """ INSERT INTO ${indexTbName4} VALUES (6, "你呢"); """
    sql """ INSERT INTO ${indexTbName4} VALUES (7, "做不了"); """
    sql """ INSERT INTO ${indexTbName4} VALUES (8, "空调"); """
    sql """ INSERT INTO ${indexTbName4} VALUES (9, "厦门"); """
    sql """ INSERT INTO ${indexTbName4} VALUES (10, "长城"); """
    sql """ INSERT INTO ${indexTbName4} VALUES (11, "重要"); """

    try {
        sql "sync"
        sql """ set enable_common_expr_pushdown = true; """

        // Test full pinyin
        qt_sql_pinyin1 """ select * from ${indexTbName4} where name match 'liudehua' order by a; """
        qt_sql_pinyin2 """ select * from ${indexTbName4} where name match 'zhang' order by a; """
        
        // Test first letter
        qt_sql_pinyin3 """ select * from ${indexTbName4} where name match 'ldh' order by a; """
        qt_sql_pinyin4 """ select * from ${indexTbName4} where name match 'zxy' order by a; """
        
        // Test original Chinese
        qt_sql_pinyin5 """ select * from ${indexTbName4} where name match '刘德华' order by a; """
        
        // Test mixed Chinese and English
        qt_sql_pinyin6 """ select * from ${indexTbName4} where name match 'dj' order by a; """
        qt_sql_pinyin7 """ select * from ${indexTbName4} where name match 'abc' order by a; """
        
        // Test polyphone phrases - should match correct pinyin from polyphone dictionary
        qt_sql_pinyin_poly1 """ select * from ${indexTbName4} where name match 'ni' order by a; """
        qt_sql_pinyin_poly2 """ select * from ${indexTbName4} where name match 'ne' order by a; """
        qt_sql_pinyin_poly3 """ select * from ${indexTbName4} where name match 'zuo' order by a; """
        qt_sql_pinyin_poly4 """ select * from ${indexTbName4} where name match 'liao' order by a; """
        qt_sql_pinyin_poly5 """ select * from ${indexTbName4} where name match 'kong' order by a; """
        qt_sql_pinyin_poly6 """ select * from ${indexTbName4} where name match 'tiao' order by a; """
        qt_sql_pinyin_poly7 """ select * from ${indexTbName4} where name match 'xia' order by a; """
        qt_sql_pinyin_poly8 """ select * from ${indexTbName4} where name match 'men' order by a; """
        qt_sql_pinyin_poly9 """ select * from ${indexTbName4} where name match 'chang' order by a; """
        qt_sql_pinyin_poly10 """ select * from ${indexTbName4} where name match 'zhong' order by a; """
        
        // Test first letter abbreviations for polyphone phrases
        qt_sql_pinyin_poly11 """ select * from ${indexTbName4} where name match 'nn' order by a; """
        qt_sql_pinyin_poly12 """ select * from ${indexTbName4} where name match 'zbl' order by a; """
        qt_sql_pinyin_poly13 """ select * from ${indexTbName4} where name match 'kt' order by a; """
        qt_sql_pinyin_poly14 """ select * from ${indexTbName4} where name match 'xm' order by a; """
        qt_sql_pinyin_poly15 """ select * from ${indexTbName4} where name match 'cc' order by a; """
        qt_sql_pinyin_poly16 """ select * from ${indexTbName4} where name match 'zy' order by a; """

    } finally {
        sql "DROP TABLE IF EXISTS ${indexTbName4}"
    }

    // Test pinyin_joined_analyzer with table
    def indexTbName5 = "test_custom_analyzer_pinyin_5"
    sql "DROP TABLE IF EXISTS ${indexTbName5}"
    sql """
        CREATE TABLE ${indexTbName5} (
            `a` bigint NOT NULL AUTO_INCREMENT(1),
            `name` text NULL,
            INDEX idx_name (`name`) USING INVERTED PROPERTIES("support_phrase" = "true", "analyzer" = "pinyin_joined_analyzer")
        ) ENGINE=OLAP
        DUPLICATE KEY(`a`)
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """ INSERT INTO ${indexTbName5} VALUES (1, "刘德华"); """
    sql """ INSERT INTO ${indexTbName5} VALUES (2, "张学友"); """
    sql """ INSERT INTO ${indexTbName5} VALUES (3, "郭富城黎明"); """
    sql """ INSERT INTO ${indexTbName5} VALUES (4, "你呢"); """
    sql """ INSERT INTO ${indexTbName5} VALUES (5, "做不了"); """
    sql """ INSERT INTO ${indexTbName5} VALUES (6, "厦门空调"); """

    try {
        sql "sync"
        sql """ set enable_common_expr_pushdown = true; """

        // Test joined full pinyin
        qt_sql_pinyin_joined1 """ select * from ${indexTbName5} where name match 'liudehua' order by a; """
        qt_sql_pinyin_joined2 """ select * from ${indexTbName5} where name match 'zhangxueyou' order by a; """
        qt_sql_pinyin_joined3 """ select * from ${indexTbName5} where name match 'guofuchengliming' order by a; """
        
        // Test first letter still works
        qt_sql_pinyin_joined4 """ select * from ${indexTbName5} where name match 'ldh' order by a; """
        
        // Test polyphone phrases with joined pinyin
        qt_sql_pinyin_joined_poly1 """ select * from ${indexTbName5} where name match 'nine' order by a; """
        qt_sql_pinyin_joined_poly2 """ select * from ${indexTbName5} where name match 'zuobuliao' order by a; """
        qt_sql_pinyin_joined_poly3 """ select * from ${indexTbName5} where name match 'xiamenkongtiao' order by a; """
        
        // Test first letter abbreviations for polyphone phrases
        qt_sql_pinyin_joined_poly4 """ select * from ${indexTbName5} where name match 'nn' order by a; """
        qt_sql_pinyin_joined_poly5 """ select * from ${indexTbName5} where name match 'zbl' order by a; """
        qt_sql_pinyin_joined_poly6 """ select * from ${indexTbName5} where name match 'xmkt' order by a; """

    } finally {
        sql "DROP TABLE IF EXISTS ${indexTbName5}"
    }

    // Test standard_pinyin analyzer with table (tokenizes by space first)
    def indexTbName6 = "test_custom_analyzer_pinyin_6"
    sql "DROP TABLE IF EXISTS ${indexTbName6}"
    sql """
        CREATE TABLE ${indexTbName6} (
            `a` bigint NOT NULL AUTO_INCREMENT(1),
            `content` text NULL,
            INDEX idx_content (`content`) USING INVERTED PROPERTIES("support_phrase" = "true", "analyzer" = "standard_pinyin")
        ) ENGINE=OLAP
        DUPLICATE KEY(`a`)
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """ INSERT INTO ${indexTbName6} VALUES (1, "刘德华"); """
    sql """ INSERT INTO ${indexTbName6} VALUES (2, "刘德华 张学友"); """
    sql """ INSERT INTO ${indexTbName6} VALUES (3, "四大天王"); """

    try {
        sql "sync"
        sql """ set enable_common_expr_pushdown = true; """

        // Standard tokenizer splits by space, then applies pinyin filter
        qt_sql_standard_pinyin1 """ select * from ${indexTbName6} where content match 'l' order by a; """
        qt_sql_standard_pinyin2 """ select * from ${indexTbName6} where content match 'd' order by a; """
        qt_sql_standard_pinyin3 """ select * from ${indexTbName6} where content match 'h' order by a; """
        qt_sql_standard_pinyin4 """ select * from ${indexTbName6} where content match 'z' order by a; """

    } finally {
        sql "DROP TABLE IF EXISTS ${indexTbName6}"
    }
}