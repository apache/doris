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

suite("test_storage_format_snii_custom_analyzer", "p0,nonConcurrent") {
    def charFilter = "doris_27738_char_filter"
    def basicAnalyzer = "doris_27738_basic_analyzer"
    def keywordAnalyzer = "doris_27738_keyword_analyzer"
    def sniiTable = "tbl_snii_types_all_strs"
    def v3Table = "tbl_v3_types_all_strs"
    def v2Table = "tbl_v2_types_all_strs"

    def waitAnalyzerInstalled = { String name ->
        def deadline = System.currentTimeMillis() + 180_000
        Exception lastNotFound = null
        while (System.currentTimeMillis() < deadline) {
            try {
                sql """SELECT TOKENIZE('probe', '\"analyzer\"=\"${name}\"')"""
                return
            } catch (Exception e) {
                if (!e.message.contains("Policy not found")) {
                    throw e
                }
                lastNotFound = e
                sleep(1000)
            }
        }
        throw new IllegalStateException("analyzer ${name} was not installed on BE", lastNotFound)
    }

    def createTable = { String tableName, String indexFormat ->
        sql """
            CREATE TABLE ${tableName} (
                k1 INT NULL,
                c_none CHAR(255) NULL,
                v_cus VARCHAR(255) NULL,
                v_keyword VARCHAR(255) NULL,
                v_raw VARCHAR(255) NULL,
                INDEX idx_c_none (c_none) USING INVERTED,
                INDEX idx_v_cus (v_cus) USING INVERTED PROPERTIES(
                    "analyzer" = "${basicAnalyzer}",
                    "support_phrase" = "true"
                ),
                INDEX idx_v_keyword (v_keyword) USING INVERTED PROPERTIES(
                    "analyzer" = "${keywordAnalyzer}",
                    "support_phrase" = "true"
                ),
                INDEX idx_v_raw (v_raw) USING INVERTED
            ) ENGINE=OLAP
            DUPLICATE KEY(k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "disable_auto_compaction" = "true",
                "inverted_index_storage_format" = "${indexFormat}"
            )
        """
    }

    sql "DROP TABLE IF EXISTS ${sniiTable}"
    sql "DROP TABLE IF EXISTS ${v3Table}"
    sql "DROP TABLE IF EXISTS ${v2Table}"
    try_sql "DROP INVERTED INDEX ANALYZER IF EXISTS ${basicAnalyzer}"
    try_sql "DROP INVERTED INDEX ANALYZER IF EXISTS ${keywordAnalyzer}"
    try_sql "DROP INVERTED INDEX CHAR_FILTER IF EXISTS ${charFilter}"

    sql """
        CREATE INVERTED INDEX CHAR_FILTER ${charFilter}
        PROPERTIES("type" = "char_replace", "pattern" = "_")
    """
    sql """
        CREATE INVERTED INDEX ANALYZER ${basicAnalyzer}
        PROPERTIES("tokenizer" = "basic", "char_filter" = "${charFilter}")
    """
    sql """
        CREATE INVERTED INDEX ANALYZER ${keywordAnalyzer}
        PROPERTIES("tokenizer" = "keyword", "token_filter" = "lowercase")
    """
    waitAnalyzerInstalled(basicAnalyzer)
    waitAnalyzerInstalled(keywordAnalyzer)

    createTable(sniiTable, "SNII")
    createTable(v3Table, "V3")
    createTable(v2Table, "V2")

    [sniiTable, v3Table, v2Table].each { tableName ->
        sql """
            INSERT INTO ${tableName} VALUES
                (1, 'Apple', 'A_B_C', 'ABC DEF', 'ABC DEF'),
                (2, 'Banana', 'D_E_F', 'FAILED ORDER', 'FAILED ORDER')
        """
    }
    sql "sync"

    qt_snii_char_eq "SELECT count(*) FROM ${sniiTable} WHERE c_none = 'Apple'"
    qt_v3_char_eq "SELECT count(*) FROM ${v3Table} WHERE c_none = 'Apple'"
    qt_v2_char_eq "SELECT count(*) FROM ${v2Table} WHERE c_none = 'Apple'"

    qt_snii_custom_match_all \
        "SELECT count(*) FROM ${sniiTable} WHERE v_cus MATCH_ALL 'D E F'"
    qt_v3_custom_match_all \
        "SELECT count(*) FROM ${v3Table} WHERE v_cus MATCH_ALL 'D E F'"
    qt_v2_custom_match_all \
        "SELECT count(*) FROM ${v2Table} WHERE v_cus MATCH_ALL 'D E F'"

    qt_snii_keyword_upper \
        "SELECT count(*) FROM ${sniiTable} WHERE v_keyword MATCH_ALL 'FAILED ORDER'"
    qt_v3_keyword_upper \
        "SELECT count(*) FROM ${v3Table} WHERE v_keyword MATCH_ALL 'FAILED ORDER'"
    qt_v2_keyword_upper \
        "SELECT count(*) FROM ${v2Table} WHERE v_keyword MATCH_ALL 'FAILED ORDER'"
    qt_snii_keyword_lower \
        "SELECT count(*) FROM ${sniiTable} WHERE v_keyword MATCH_ALL 'failed order'"
    qt_v3_keyword_lower \
        "SELECT count(*) FROM ${v3Table} WHERE v_keyword MATCH_ALL 'failed order'"
    qt_v2_keyword_lower \
        "SELECT count(*) FROM ${v2Table} WHERE v_keyword MATCH_ALL 'failed order'"

    qt_snii_raw_exact \
        "SELECT count(*) FROM ${sniiTable} WHERE v_raw MATCH_ALL 'FAILED ORDER'"
    qt_v3_raw_exact \
        "SELECT count(*) FROM ${v3Table} WHERE v_raw MATCH_ALL 'FAILED ORDER'"
    qt_v2_raw_exact \
        "SELECT count(*) FROM ${v2Table} WHERE v_raw MATCH_ALL 'FAILED ORDER'"
    qt_snii_raw_case_sensitive \
        "SELECT count(*) FROM ${sniiTable} WHERE v_raw MATCH_ALL 'failed order'"
    qt_v3_raw_case_sensitive \
        "SELECT count(*) FROM ${v3Table} WHERE v_raw MATCH_ALL 'failed order'"
    qt_v2_raw_case_sensitive \
        "SELECT count(*) FROM ${v2Table} WHERE v_raw MATCH_ALL 'failed order'"
}
