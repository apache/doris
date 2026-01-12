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

suite("test_variant_custom_analyzer", "p0") {
    def indexTbName1 = "test_custom_analyzer_1"
    def indexTbName2 = "test_custom_analyzer_2"
    def indexTbName3 = "test_custom_analyzer_3"
    sql "set default_variant_max_subcolumns_count = 100"
    sql """ set enable_match_without_inverted_index = false """
    sql """ set default_variant_enable_typed_paths_to_sparse = false """

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

    sql """ select sleep(10) """

    qt_tokenize_sql """ select tokenize("Србија Херцеговина Щучин гурзуф  Ψ4  Босна", '"analyzer"="keyword_lowercase"'); """
    qt_tokenize_sql """ select tokenize("Wasted…Again", '"analyzer"="keyword_lowercase"'); """
    qt_tokenize_sql """ select tokenize("♯P-complete", '"analyzer"="keyword_lowercase"'); """
    qt_tokenize_sql """ select tokenize("a∴a∴", '"analyzer"="keyword_lowercase"'); """
    qt_tokenize_sql """ select tokenize("RX J1242−11", '"analyzer"="keyword_lowercase"'); """
    qt_tokenize_sql """ select tokenize("1080º Avalanche", '"analyzer"="keyword_lowercase"'); """
    qt_tokenize_sql """ select tokenize("clayfighter 63⅓", '"analyzer"="lowercase_delimited"'); """
    qt_tokenize_sql """ select tokenize("β-carbon nitride", '"analyzer"="lowercase_delimited"'); """
    qt_tokenize_sql """ select tokenize("ǁŨǁe language", '"analyzer"="lowercase_delimited"'); """
     
    sql "DROP TABLE IF EXISTS ${indexTbName1}"
    sql """
        CREATE TABLE ${indexTbName1} (
            `a` bigint NOT NULL AUTO_INCREMENT(1),
            `ch` variant<'ch' : string> NULL,
            INDEX idx_ch (`ch`) USING INVERTED PROPERTIES("field_pattern" = "ch", "support_phrase" = "true", "analyzer" = "lowercase_delimited")
        ) ENGINE=OLAP
        DUPLICATE KEY(`a`)
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """ insert into ${indexTbName1} values(1, '{"ch" : "abcDEF"}'); """
    sql """ insert into ${indexTbName1} values(2, '{"ch" : "中国人民"}'); """
    sql """ insert into ${indexTbName1} values(3, '{"ch" : "ǁŨǁe Language"}'); """
    sql """ insert into ${indexTbName1} values(4, '{"ch" : "RX J1242−11"}'); """
    sql """ insert into ${indexTbName1} values(5, '{"ch" : "clayfighter 63⅓"}'); """
    sql """ insert into ${indexTbName1} values(6, '{"ch" : "β-carbon nitrid"}'); """

    try {
        trigger_and_wait_compaction(indexTbName1, "full")
        sql "sync"
        sql """ set enable_common_expr_pushdown = true; """

        qt_sql """ select a, ch['ch'] from ${indexTbName1} where ch['ch'] match 'abcDEF'; """
        qt_sql """ select a, ch['ch'] from ${indexTbName1} where ch['ch'] match '中'; """
        qt_sql """ select a, ch['ch'] from ${indexTbName1} where ch['ch'] match 'language'; """
    } finally {
    }

    sql "DROP TABLE IF EXISTS ${indexTbName2}"
    sql """
        CREATE TABLE ${indexTbName2} (
            `a` bigint NOT NULL AUTO_INCREMENT(1),
            `ch` variant<'ch' : string> NULL,
            INDEX idx_ch (`ch`) USING INVERTED PROPERTIES("field_pattern" = "ch", "support_phrase" = "true", "analyzer" = "edge_ngram_phone_number")
        ) ENGINE=OLAP
        DUPLICATE KEY(`a`)
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """ INSERT INTO ${indexTbName2} VALUES ('3', '{"ch" : "Wikipedia;Miscellaneous-Jj102786 / 3tle Born Oct 27th 1986 @ Blytheville, Arkansas @ 9:14pm 23 yrs of age male,white Cucassion American raised Religion:Pentocostal,Church of God"}'); """

    try {
        sql "sync"
        sql """ set enable_common_expr_pushdown = true; """

        qt_sql """ select a, ch['ch'] from ${indexTbName2} where ch['ch'] match '102'; """
    } finally {
    }

     sql "DROP TABLE IF EXISTS ${indexTbName3}"
    sql """
        CREATE TABLE ${indexTbName3} (
            `a` bigint NOT NULL AUTO_INCREMENT(1),
            `ch` variant<'ch' : string> NULL,
            INDEX idx_ch (`ch`) USING INVERTED PROPERTIES("field_pattern" = "ch", "support_phrase" = "true", "analyzer" = "keyword_lowercase")
        ) ENGINE=OLAP
        DUPLICATE KEY(`a`)
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """ INSERT INTO ${indexTbName3} VALUES ('4', '{"ch" : "1080º Avalanche"}'); """

    try {
        sql "sync"
        sql """ set enable_common_expr_pushdown = true; """

        qt_sql """ select a, ch['ch'] from ${indexTbName3} where ch['ch'] match '1080º avalanche'; """
    } finally {
    }

    try {
        sql "drop inverted index analyzer edge_ngram_phone_number"
    } catch (SQLException e) {
        if (e.message.contains("is used by index")) {
            logger.info("used by index")
        }
    }
}