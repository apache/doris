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

suite("test_ngram_max_diff_custom_analyzer", "p0") {
    def defaultLimitTokenizer = "test_ngram_default_limit_tokenizer"
    def ngramTokenizer = "test_ngram_1_8_tokenizer"
    def ngramAnalyzer = "test_ngram_1_8_analyzer"

    try_sql "DROP INVERTED INDEX ANALYZER IF EXISTS ${ngramAnalyzer}"
    try_sql "DROP INVERTED INDEX TOKENIZER IF EXISTS ${defaultLimitTokenizer}"
    try_sql "DROP INVERTED INDEX TOKENIZER IF EXISTS ${ngramTokenizer}"

    test {
        sql """
            CREATE INVERTED INDEX TOKENIZER ${defaultLimitTokenizer}
            PROPERTIES (
                "type" = "ngram",
                "min_gram" = "1",
                "max_gram" = "8"
            )
        """
        exception "less than or equal to: [ 1 ]"
    }

    sql """
        CREATE INVERTED INDEX TOKENIZER IF NOT EXISTS ${ngramTokenizer}
        PROPERTIES (
            "type" = "ngram",
            "min_gram" = "1",
            "max_gram" = "8",
            "max_ngram_diff" = "7"
        )
    """
    sql """
        CREATE INVERTED INDEX ANALYZER IF NOT EXISTS ${ngramAnalyzer}
        PROPERTIES ("tokenizer" = "${ngramTokenizer}")
    """

    int maxRetry = 30
    Exception lastException = null
    for (int i = 0; i < maxRetry; i++) {
        try {
            sql """SELECT TOKENIZE('probe', '"analyzer"="${ngramAnalyzer}"')"""
            lastException = null
            break
        } catch (Exception e) {
            lastException = e
            sleep(1000)
        }
    }
    assertTrue(lastException == null,
            "Analyzer ${ngramAnalyzer} was not ready: ${lastException?.message}")

    def ngramTokens = sql """SELECT TOKENIZE('abcdefgh', '"analyzer"="${ngramAnalyzer}"')"""
    def actualTokens = parseJson(ngramTokens[0][0].toString()).collect { it.token }
    def expectedTokens = [
            "a", "ab", "abc", "abcd", "abcde", "abcdef", "abcdefg", "abcdefgh",
            "b", "bc", "bcd", "bcde", "bcdef", "bcdefg", "bcdefgh",
            "c", "cd", "cde", "cdef", "cdefg", "cdefgh",
            "d", "de", "def", "defg", "defgh",
            "e", "ef", "efg", "efgh",
            "f", "fg", "fgh",
            "g", "gh",
            "h"
    ]
    assertEquals(expectedTokens, actualTokens)
}
