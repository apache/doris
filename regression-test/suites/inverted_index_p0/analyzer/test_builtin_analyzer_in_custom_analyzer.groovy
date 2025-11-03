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

suite("test_builtin_analyzer_in_custom_analyzer", "p0") {
    // Define all built-in analyzers
    def builtinAnalyzers = ["none", "standard", "unicode", "english", "chinese", "icu", "basic", "ik"]
    
    // Helper function to test that creating analyzer with builtin name should fail
    def testBuiltinAnalyzerNameConflict = { String analyzerName ->
        test {
            sql """
                CREATE INVERTED INDEX ANALYZER IF NOT EXISTS ${analyzerName}
                PROPERTIES
                (
                    "tokenizer" = "basic",
                    "token_filter" = "lowercase"
                );
            """
            exception "conflicts with built-in"
        }
    }
    
    // Test: Cannot create custom analyzer with built-in analyzer names
    logger.info("Testing that built-in analyzer names cannot be used for custom analyzers")
    
    builtinAnalyzers.each { analyzerName ->
        logger.info("Testing conflict with built-in analyzer: ${analyzerName}")
        testBuiltinAnalyzerNameConflict(analyzerName)
    }
    
    // Define tokenize test cases: [testName, testText, analyzerName]
    def tokenizeTestCases = [
        ["standard", "Apache Doris is a fast MPP database", "standard"],
        ["unicode", "Hello World 你好世界", "unicode"],
        ["basic", "GET /images/test.jpg HTTP/1.0", "basic"],
        ["icu", "让我们说「Hello」そして世界とつながろう！", "icu"],
        ["chinese_standard", "Apache Doris是一个现代化的MPP数据库", "standard"],
        ["chinese_basic", "Apache Doris是一个现代化的MPP数据库", "basic"],
        ["chinese_icu", "Apache Doris是一个现代化的MPP数据库", "icu"],
        ["special_standard", "test@example.com user_name 123-456", "standard"],
        ["empty", "", "standard"],
        ["mixed", "中文English日本語한국어", "icu"]
    ]
    
    // Execute tokenize tests in a loop
    tokenizeTestCases.each { testCase ->
        def testName = testCase[0]
        def testText = testCase[1]
        def analyzerName = testCase[2]
        
        logger.info("Testing tokenize with ${analyzerName}: ${testName}")
        "qt_tokenize_${testName}"(""" 
            select tokenize("${testText}", '"analyzer"="${analyzerName}"'); 
        """)
    }

    // Test table creation with analyzer_with_standard
    def indexTblName = "test_builtin_analyzer_table"
    
    sql "DROP TABLE IF EXISTS ${indexTblName}"
    
    sql """
        CREATE TABLE ${indexTblName} (
            `id` int(11) NOT NULL,
            `url` text NULL,
            INDEX idx_url (`url`) USING INVERTED PROPERTIES("support_phrase" = "true", "analyzer" = "basic")
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """
    
    // Insert test data
    sql """ INSERT INTO ${indexTblName} VALUES (1, 'GET /images/logo.png HTTP/1.0'); """
    sql """ INSERT INTO ${indexTblName} VALUES (2, 'POST /api/v1/users HTTP/1.1'); """
    sql """ INSERT INTO ${indexTblName} VALUES (3, 'GET /docs/index.html HTTP/1.0'); """
    
    try {
        sql "sync"
        sql """ set enable_common_expr_pushdown = true; """
        
        // Test MATCH queries with analyzer_with_standard
        qt_sql_basic_match_logo """ SELECT * FROM ${indexTblName} WHERE url MATCH 'logo' ORDER BY id; """
        qt_sql_basic_match_images """ SELECT * FROM ${indexTblName} WHERE url MATCH 'images' ORDER BY id; """
        qt_sql_basic_match_api """ SELECT * FROM ${indexTblName} WHERE url MATCH 'api' ORDER BY id; """

    } finally {
    }
    
    // Test with another analyzer - analyzer_with_basic
    def indexTblName2 = "test_builtin_analyzer_table_basic"
    
    sql "DROP TABLE IF EXISTS ${indexTblName2}"
    
    sql """
        CREATE TABLE ${indexTblName2} (
            `id` int(11) NOT NULL,
            `url` text NULL,
            INDEX idx_url (`url`) USING INVERTED PROPERTIES("support_phrase" = "true", "analyzer" = "basic")
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """
    
    // Insert URL test data
    sql """ INSERT INTO ${indexTblName2} VALUES (1, 'GET /images/logo.png HTTP/1.0'); """
    sql """ INSERT INTO ${indexTblName2} VALUES (2, 'POST /api/v1/users HTTP/1.1'); """
    sql """ INSERT INTO ${indexTblName2} VALUES (3, 'GET /docs/index.html HTTP/1.0'); """
    
    try {
        sql "sync"
        sql """ set enable_common_expr_pushdown = true; """
        
        // Test basic analyzer on URL-like strings
        qt_sql_basic_match_logo """ SELECT * FROM ${indexTblName2} WHERE url MATCH 'logo' ORDER BY id; """
        qt_sql_basic_match_images """ SELECT * FROM ${indexTblName2} WHERE url MATCH 'images' ORDER BY id; """
        qt_sql_basic_match_api """ SELECT * FROM ${indexTblName2} WHERE url MATCH 'api' ORDER BY id; """
        
    } finally {
    }
}