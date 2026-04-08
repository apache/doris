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

suite("test_match_using_analyzer", "p0") {
    def tableName = "tbl_match_using_analyzer"
    def tokenizerName = "edge_ngram_name_tokenizer_match_test"
    def analyzerStandard = "first_name_standard_match_test"
    def analyzerEdge = "edge_ngram_name_match_test"
    def analyzerKeyword = "first_name_keyword_match_test"

    def waitAnalyzerReady = { analyzerName ->
        sleep(10000)
        int maxRetry = 30
        boolean ready = false
        Exception lastException = null
        for (int i = 0; i < maxRetry && !ready; i++) {
            try {
                sql """ select tokenize("probe", '"analyzer"="${analyzerName}"'); """
                ready = true
            } catch (Exception e) {
                lastException = e
                logger.info("Analyzer ${analyzerName} not ready yet: ${e.message}")
                sql """ select sleep(1) """
            }
        }
        assertTrue(ready, "Analyzer ${analyzerName} not ready after waiting: ${lastException?.message}")
    }

    try {
        sql "DROP TABLE IF EXISTS ${tableName}"

        sql """
            CREATE INVERTED INDEX TOKENIZER IF NOT EXISTS ${tokenizerName}
            PROPERTIES
            (
                "type" = "edge_ngram",
                "min_gram" = "1",
                "max_gram" = "20",
                "token_chars" = "letter"
            );
        """

        sql """
            CREATE INVERTED INDEX ANALYZER IF NOT EXISTS ${analyzerStandard}
            PROPERTIES
            (
                "tokenizer" = "standard",
                "token_filter" = "lowercase"
            );
        """

        sql """
            CREATE INVERTED INDEX ANALYZER IF NOT EXISTS ${analyzerEdge}
            PROPERTIES
            (
                "tokenizer" = "${tokenizerName}",
                "token_filter" = "lowercase"
            );
        """

        sql """
            CREATE INVERTED INDEX ANALYZER IF NOT EXISTS ${analyzerKeyword}
            PROPERTIES
            (
                "tokenizer" = "keyword",
                "token_filter" = "lowercase"
            );
        """

        //waitAnalyzerReady(analyzerStandard)
        //waitAnalyzerReady(analyzerEdge)
        //waitAnalyzerReady(analyzerKeyword)

        sql """
            CREATE TABLE ${tableName} (
                id INT,
                first_name STRING,
                INDEX idx_fn_std (first_name) USING INVERTED PROPERTIES("analyzer"="${analyzerStandard}", "support_phrase"="true"),
                INDEX idx_fn_ngr (first_name) USING INVERTED PROPERTIES("analyzer"="${analyzerEdge}"),
                INDEX idx_fn_exact (first_name) USING INVERTED PROPERTIES("analyzer"="${analyzerKeyword}")
            ) DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "disable_auto_compaction" = "true"
            )
        """
        sleep(10000)
        sql """
            INSERT INTO ${tableName} VALUES
                (1, 'alice'),
                (2, 'alice cooper'),
                (3, 'bob'),
                (4, 'allyson');
        """

        sql "sync"

        // default analyzer should match both tokenized rows that contain "alice"
        qt_match_default """
            SELECT id FROM ${tableName}
            WHERE first_name MATCH 'alice'
            ORDER BY id
        """

        // keyword analyzer only matches the full literal "alice"
        qt_match_keyword """
            SELECT id FROM ${tableName}
            WHERE first_name MATCH 'alice' USING ANALYZER ${analyzerKeyword}
            ORDER BY id
        """

        // edge ngram analyzer should cover names beginning with "al"
        qt_match_edge """
            SELECT id FROM ${tableName}
            WHERE first_name MATCH_PHRASE_PREFIX 'al' USING ANALYZER ${analyzerEdge}
            ORDER BY id
        """

        // show create table should contain analyzer names
        def showCreate = sql """ SHOW CREATE TABLE ${tableName} """
        def createStmt = showCreate.collect { row -> row[1].toString().toLowerCase() }.join("\n")
        assertTrue(createStmt.contains(analyzerStandard.toLowerCase()))
        assertTrue(createStmt.contains(analyzerEdge.toLowerCase()))
        assertTrue(createStmt.contains(analyzerKeyword.toLowerCase()))
    } finally {
        /*sql "DROP TABLE IF EXISTS ${tableName}"

        try {
            sql "DROP INVERTED INDEX ANALYZER ${analyzerStandard}"
        } catch (Exception ignored) {
        }

        try {
            sql "DROP INVERTED INDEX ANALYZER ${analyzerEdge}"
        } catch (Exception ignored) {
        }

        try {
            sql "DROP INVERTED INDEX ANALYZER ${analyzerKeyword}"
        } catch (Exception ignored) {
        }

        try {
            sql "DROP INVERTED INDEX TOKENIZER ${tokenizerName}"
        } catch (Exception ignored) {
        }*/
    }
}

