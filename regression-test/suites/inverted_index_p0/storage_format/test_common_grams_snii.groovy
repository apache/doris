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

import java.util.regex.Pattern

import org.apache.doris.regression.action.ProfileAction

suite("test_common_grams_snii", "p0,nonConcurrent") {
    def backendIdToIp = [:]
    def backendIdToHttpPort = [:]
    getBackendIpHttpPort(backendIdToIp, backendIdToHttpPort)

    def readBeConfig = { backendId, String key ->
        def (code, out, err) = show_be_config(
                backendIdToIp.get(backendId), backendIdToHttpPort.get(backendId))
        assertEquals(0, code, err)
        def row = parseJson(out.trim()).find { it[0] == key }
        assertNotNull(row, "BE config ${key} is missing on backend ${backendId}")
        return row[2].toString()
    }

    def originalBeConfigs = [:]
    ["enable_common_grams_query_plan", "enable_common_grams_index_build",
            "common_grams_plan_cost_ratio_percent"].each { key ->
        backendIdToIp.keySet().each { backendId ->
            originalBeConfigs.computeIfAbsent(backendId) { [:] }[key] =
                    readBeConfig(backendId, key)
        }
    }

    def setBeConfig = { String key, String value ->
        backendIdToIp.keySet().each { backendId ->
            def (code, out, err) = update_be_config(
                    backendIdToIp.get(backendId), backendIdToHttpPort.get(backendId), key, value)
            assertEquals(0, code, "update ${key}=${value} failed on backend ${backendId}: ${out} ${err}")
            assertEquals(value, readBeConfig(backendId, key))
        }
    }

    def createAnalyzer = { String name, String tokenizer, String tokenFilters ->
        sql """
            CREATE INVERTED INDEX ANALYZER ${name}
            PROPERTIES (
                "tokenizer" = "${tokenizer}",
                "token_filter" = "${tokenFilters}"
            )
        """
    }

    def waitAnalyzerInstalled = { String name ->
        def deadline = System.currentTimeMillis() + 180_000
        Exception lastNotFound = null
        while (System.currentTimeMillis() < deadline) {
            try {
                sql """SELECT TOKENIZE('probe', '"analyzer"="${name}"')"""
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

    def assertProfileCounterPositive = { String label, String query, String counter ->
        def profileId = "test_common_grams_${label}_${System.nanoTime()}"
        String profileString
        sql "SET enable_profile = true"
        try {
            sql """
                /* ${profileId} */
                ${query}
            """
            profileString = new ProfileAction(context).getProfileBySql(
                    profileId, [counter])
            def matcher = Pattern.compile(Pattern.quote(counter) + ":\\s*(\\d+)")
                    .matcher(profileString)
            assertTrue(matcher.find(), "${counter} is missing from profile")
            assertTrue(Long.parseLong(matcher.group(1)) > 0, "${counter} must be positive")
        } finally {
            sql "SET enable_profile = false"
        }
        return profileString
    }

    try {
        sql "DROP TABLE IF EXISTS test_common_grams_snii"
        sql "DROP TABLE IF EXISTS test_common_grams_v3_plain"
        sql "DROP TABLE IF EXISTS test_common_grams_mixed_v3_plain"
        sql "DROP TABLE IF EXISTS test_common_grams_missing"
        sql "DROP TABLE IF EXISTS test_common_grams_mixed"
        sql "DROP TABLE IF EXISTS test_common_grams_no_index"
        try_sql "DROP INVERTED INDEX ANALYZER IF EXISTS cg_default_analyzer"
        try_sql "DROP INVERTED INDEX ANALYZER IF EXISTS cg_plain_analyzer"
        try_sql "DROP INVERTED INDEX TOKEN_FILTER IF EXISTS cg_default_grams"
        try_sql "DROP INVERTED INDEX TOKENIZER IF EXISTS cg_char_group_tokenizer"

        setBeConfig("enable_common_grams_query_plan", "true")
        setBeConfig("enable_common_grams_index_build", "true")
        // 100 is the validator maximum: grams win whenever not estimated more
        // expensive than plain. The filler rows below make the probed stopword
        // postings genuinely dominate so the gate stays decisively open.
        setBeConfig("common_grams_plan_cost_ratio_percent", "100")

        sql """
            CREATE INVERTED INDEX TOKENIZER cg_char_group_tokenizer
            PROPERTIES (
                "type" = "char_group",
                "tokenize_on_chars" = "[whitespace], [punctuation]"
            )
        """
        sql """
            CREATE INVERTED INDEX ANALYZER cg_plain_analyzer
            PROPERTIES (
                "tokenizer" = "cg_char_group_tokenizer",
                "token_filter" = "lowercase"
            )
        """
        sql """
            CREATE INVERTED INDEX TOKEN_FILTER cg_default_grams
            PROPERTIES ("type" = "common_grams")
        """
        // The word list lives on the BE (<inverted_index_dict_path>/common_grams/default_words.txt)
        // and is deliberately not selectable per policy, so naming one has to fail the DDL rather
        // than be silently ignored.
        test {
            sql """
                CREATE INVERTED INDEX TOKEN_FILTER cg_words_rejected
                PROPERTIES (
                    "type" = "common_grams",
                    "words" = "FILE:db/cg_words/common_grams_words.txt"
                )
            """
            exception "does not support parameter 'words'"
        }

        createAnalyzer("cg_default_analyzer", "cg_char_group_tokenizer",
                "lowercase,cg_default_grams")

        test {
            sql """
                CREATE INVERTED INDEX ANALYZER cg_unsafe_tokenizer
                PROPERTIES (
                    "tokenizer" = "standard",
                    "token_filter" = "cg_default_grams"
                )
            """
            exception "does not guarantee unit position increments"
        }
        test {
            sql """
                CREATE INVERTED INDEX ANALYZER cg_non_terminal
                PROPERTIES (
                    "tokenizer" = "cg_char_group_tokenizer",
                    "token_filter" = "cg_default_grams,lowercase"
                )
            """
            exception "exactly once as the terminal token filter"
        }

        waitAnalyzerInstalled("cg_plain_analyzer")
        waitAnalyzerInstalled("cg_default_analyzer")

        sql """
            CREATE TABLE test_common_grams_snii (
                id INT NOT NULL,
                body STRING NULL,
                INDEX idx_body (body) USING INVERTED PROPERTIES (
                    "analyzer" = "cg_default_analyzer",
                    "support_phrase" = "true"
                ),
                INDEX idx_body_plain (body) USING INVERTED PROPERTIES (
                    "analyzer" = "cg_plain_analyzer",
                    "support_phrase" = "true"
                )
            ) ENGINE=OLAP
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "disable_auto_compaction" = "true",
                "inverted_index_storage_format" = "SNII"
            )
        """

        test {
            sql """
                CREATE TABLE test_common_grams_v3_rejected (
                    id INT NOT NULL,
                    body STRING NULL,
                    INDEX idx_body (body) USING INVERTED PROPERTIES (
                        "analyzer" = "cg_default_analyzer",
                        "support_phrase" = "true"
                    )
                ) ENGINE=OLAP
                DUPLICATE KEY(id)
                DISTRIBUTED BY HASH(id) BUCKETS 1
                PROPERTIES (
                    "replication_num" = "1",
                    "inverted_index_storage_format" = "V3"
                )
            """
            exception "supported only by SNII inverted indexes"
        }
        test {
            sql """
                CREATE TABLE test_common_grams_array_rejected (
                    id INT NOT NULL,
                    body ARRAY<STRING> NULL,
                    INDEX idx_body (body) USING INVERTED PROPERTIES (
                        "analyzer" = "cg_default_analyzer",
                        "support_phrase" = "true"
                    )
                ) ENGINE=OLAP
                DUPLICATE KEY(id)
                DISTRIBUTED BY HASH(id) BUCKETS 1
                PROPERTIES (
                    "replication_num" = "1",
                    "inverted_index_storage_format" = "SNII"
                )
            """
            exception "does not support ARRAY columns"
        }
        test {
            sql """
                CREATE TABLE test_common_grams_phrase_rejected (
                    id INT NOT NULL,
                    body STRING NULL,
                    INDEX idx_body (body) USING INVERTED PROPERTIES (
                        "analyzer" = "cg_default_analyzer"
                    )
                ) ENGINE=OLAP
                DUPLICATE KEY(id)
                DISTRIBUTED BY HASH(id) BUCKETS 1
                PROPERTIES (
                    "replication_num" = "1",
                    "inverted_index_storage_format" = "SNII"
                )
            """
            exception "requires support_phrase=true"
        }

        sql """
            CREATE TABLE test_common_grams_v3_plain (
                id INT NOT NULL,
                body STRING NULL,
                INDEX idx_body (body) USING INVERTED PROPERTIES (
                    "analyzer" = "cg_plain_analyzer",
                    "support_phrase" = "true"
                )
            ) ENGINE=OLAP
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "disable_auto_compaction" = "true",
                "inverted_index_storage_format" = "V3"
            )
        """
        sql """
            CREATE TABLE test_common_grams_no_index (
                id INT NOT NULL,
                body STRING NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES ("replication_num" = "1")
        """
        sql """
            CREATE TABLE test_common_grams_mixed_v3_plain (
                id INT NOT NULL,
                body STRING NULL,
                INDEX idx_body (body) USING INVERTED PROPERTIES (
                    "analyzer" = "cg_plain_analyzer",
                    "support_phrase" = "true"
                )
            ) ENGINE=OLAP
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "disable_auto_compaction" = "true",
                "inverted_index_storage_format" = "V3"
            )
        """

        streamLoad {
            table "test_common_grams_snii"
            set "column_separator", "|"
            set "columns", "id,body"
            file "common_grams_docs.csv"
            time 30_000
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(20, json.NumberLoadedRows)
            }
        }
        // Cost-gate ballast: the plain-plan estimate is driven by the candidate
        // df of the probed terms (rarest term included), while the gram side is
        // driven by the pair-term df. Isolated single-token rows raise every
        // probed term's df without creating a single new gram pair (no
        // adjacency), so the pair estimates stay put and the <=100% cost gate
        // picks the gram plan deterministically. Single tokens also never match
        // any probed phrase, prefix, or MATCH_ALL golden.
        def costGateBallast = (0..<500).collect {
            ",\n                (${1000 + it}, '${["the", "of", "and", "foo", "bar"][it % 5]}')"
        }.join("")
        sql """
            INSERT INTO test_common_grams_snii VALUES
                (21, 'alpha gamma beta'),
                (22, 'alpha beta delta'),
                (23, 'foo bar and'),
                (24, 'bar foo the'),
                (25, 'the bar foo'),
                (26, 'the and of'),
                (27, 'the the of'),
                (28, 'foo of thinker'),
                (29, 'alpha beta gamma delta epsilon zeta eta theta iota lambda'),
                (30, 'the world wide web'),
                (31, 'alpha the beta of gamma and omega'),
                (32, NULL)${costGateBallast}
        """
        sql "INSERT INTO test_common_grams_v3_plain SELECT id, body FROM test_common_grams_snii"
        sql "INSERT INTO test_common_grams_no_index SELECT id, body FROM test_common_grams_snii"
        sql "SYNC"
        sql "SET enable_profile = true"
        sql "SET profile_level = 2"
        sql "SET parallel_fragment_exec_instance_num = 1"
        sql "SET enable_sql_cache = false"
        sql "SET enable_inverted_index_query_cache = false"
        sql "SET enable_segment_limit_pushdown = true"

        def metamorphicQueries = [
            exact_1: "SELECT id FROM test_common_grams_snii WHERE body MATCH_PHRASE 'alpha' ORDER BY id",
            exact_2: "SELECT id FROM test_common_grams_snii WHERE body MATCH_PHRASE 'alpha beta' ORDER BY id",
            exact_2_common: "SELECT id FROM test_common_grams_snii WHERE body MATCH_PHRASE 'the of' ORDER BY id",
            exact_3: "SELECT id FROM test_common_grams_snii WHERE body MATCH_PHRASE 'alpha beta gamma' ORDER BY id",
            exact_6: "SELECT id FROM test_common_grams_snii WHERE body MATCH_PHRASE 'alpha the beta of gamma and' ORDER BY id",
            exact_10: "SELECT id FROM test_common_grams_snii WHERE body MATCH_PHRASE 'alpha the beta of gamma and delta in epsilon to' ORDER BY id",
            shape_nnn: "SELECT id FROM test_common_grams_snii WHERE body MATCH_PHRASE 'foo bar baz' ORDER BY id",
            shape_nns: "SELECT id FROM test_common_grams_snii WHERE body MATCH_PHRASE 'foo bar the' ORDER BY id",
            shape_nsn: "SELECT id FROM test_common_grams_snii WHERE body MATCH_PHRASE 'foo the bar' ORDER BY id",
            shape_nss: "SELECT id FROM test_common_grams_snii WHERE body MATCH_PHRASE 'foo the of' ORDER BY id",
            shape_snn: "SELECT id FROM test_common_grams_snii WHERE body MATCH_PHRASE 'the foo bar' ORDER BY id",
            shape_sns: "SELECT id FROM test_common_grams_snii WHERE body MATCH_PHRASE 'the foo of' ORDER BY id",
            shape_ssn: "SELECT id FROM test_common_grams_snii WHERE body MATCH_PHRASE 'the of foo' ORDER BY id",
            shape_sss: "SELECT id FROM test_common_grams_snii WHERE body MATCH_PHRASE 'the of and' ORDER BY id",
            repeated_common: "SELECT id FROM test_common_grams_snii WHERE body MATCH_PHRASE 'the the the' ORDER BY id",
            prefix_1: "SELECT id FROM test_common_grams_snii WHERE body MATCH_PHRASE_PREFIX 'alph' ORDER BY id",
            prefix_2: "SELECT id FROM test_common_grams_snii WHERE body MATCH_PHRASE_PREFIX 'alpha be' ORDER BY id",
            prefix_3: "SELECT id FROM test_common_grams_snii WHERE body MATCH_PHRASE_PREFIX 'alpha beta ga' ORDER BY id",
            prefix_6: "SELECT id FROM test_common_grams_snii WHERE body MATCH_PHRASE_PREFIX 'alpha the beta of gamma an' ORDER BY id",
            prefix_10: "SELECT id FROM test_common_grams_snii WHERE body MATCH_PHRASE_PREFIX 'alpha the beta of gamma and delta in epsilon t' ORDER BY id",
            prefix_the_wo: "SELECT id FROM test_common_grams_snii WHERE body MATCH_PHRASE_PREFIX 'the wo' ORDER BY id",
            prefix_foo_the: "SELECT id FROM test_common_grams_snii WHERE body MATCH_PHRASE_PREFIX 'foo the' ORDER BY id",
            prefix_foo_of_th: "SELECT id FROM test_common_grams_snii WHERE body MATCH_PHRASE_PREFIX 'foo of th' ORDER BY id",
            prefix_the_bar_ba: "SELECT id FROM test_common_grams_snii WHERE body MATCH_PHRASE_PREFIX 'the bar ba' ORDER BY id",
            authoritative_miss_exact: "SELECT id FROM test_common_grams_snii WHERE body MATCH_PHRASE 'the nonexistent phrase' ORDER BY id",
            authoritative_miss_prefix: "SELECT id FROM test_common_grams_snii WHERE body MATCH_PHRASE_PREFIX 'the nonex' ORDER BY id"
        ]

        order_qt_cg_any_one """
            SELECT id FROM test_common_grams_snii WHERE body MATCH_ANY 'alpha' ORDER BY id
        """
        order_qt_cg_any_many """
            SELECT id FROM test_common_grams_snii WHERE body MATCH_ANY 'alpha beta' ORDER BY id
        """
        order_qt_cg_any_three """
            SELECT id FROM test_common_grams_snii WHERE body MATCH_ANY 'alpha beta gamma' ORDER BY id
        """
        // id < 1000 keeps the cost-gate ballast rows (which necessarily contain
        // the probed stopwords) out of the any/all/regexp/match-all goldens.
        order_qt_cg_any_common_many """
            SELECT id FROM test_common_grams_snii
            WHERE body MATCH_ANY 'alpha the beta of gamma and' AND id < 1000 ORDER BY id
        """
        order_qt_cg_all_one """
            SELECT id FROM test_common_grams_snii WHERE body MATCH_ALL 'alpha' ORDER BY id
        """
        order_qt_cg_all_many """
            SELECT id FROM test_common_grams_snii WHERE body MATCH_ALL 'alpha beta' ORDER BY id
        """
        order_qt_cg_all_three """
            SELECT id FROM test_common_grams_snii WHERE body MATCH_ALL 'alpha beta gamma' ORDER BY id
        """
        order_qt_cg_all_common_many """
            SELECT id FROM test_common_grams_snii
            WHERE body MATCH_ALL 'alpha the beta of gamma and' ORDER BY id
        """
        order_qt_cg_all_common_non_adjacent """
            SELECT id FROM test_common_grams_snii
            WHERE body MATCH_ALL 'the of and' AND id < 1000 ORDER BY id
        """

        order_qt_cg_exact_1 """
            SELECT id FROM test_common_grams_snii WHERE body MATCH_PHRASE 'alpha' ORDER BY id
        """
        order_qt_cg_exact_2 """
            SELECT id FROM test_common_grams_snii WHERE body MATCH_PHRASE 'alpha beta' ORDER BY id
        """
        order_qt_cg_exact_2_common """
            SELECT id FROM test_common_grams_snii WHERE body MATCH_PHRASE 'the of' ORDER BY id
        """
        order_qt_cg_exact_3 """
            SELECT id FROM test_common_grams_snii WHERE body MATCH_PHRASE 'alpha beta gamma' ORDER BY id
        """
        order_qt_cg_exact_6 """
            SELECT id FROM test_common_grams_snii
            WHERE body MATCH_PHRASE 'alpha the beta of gamma and' ORDER BY id
        """
        order_qt_cg_exact_10 """
            SELECT id FROM test_common_grams_snii
            WHERE body MATCH_PHRASE 'alpha the beta of gamma and delta in epsilon to' ORDER BY id
        """

        order_qt_cg_shape_nnn """
            SELECT id FROM test_common_grams_snii WHERE body MATCH_PHRASE 'foo bar baz' ORDER BY id
        """
        order_qt_cg_shape_nns """
            SELECT id FROM test_common_grams_snii WHERE body MATCH_PHRASE 'foo bar the' ORDER BY id
        """
        order_qt_cg_shape_nsn """
            SELECT id FROM test_common_grams_snii WHERE body MATCH_PHRASE 'foo the bar' ORDER BY id
        """
        order_qt_cg_shape_nss """
            SELECT id FROM test_common_grams_snii WHERE body MATCH_PHRASE 'foo the of' ORDER BY id
        """
        order_qt_cg_shape_snn """
            SELECT id FROM test_common_grams_snii WHERE body MATCH_PHRASE 'the foo bar' ORDER BY id
        """
        order_qt_cg_shape_sns """
            SELECT id FROM test_common_grams_snii WHERE body MATCH_PHRASE 'the foo of' ORDER BY id
        """
        order_qt_cg_shape_ssn """
            SELECT id FROM test_common_grams_snii WHERE body MATCH_PHRASE 'the of foo' ORDER BY id
        """
        order_qt_cg_shape_sss """
            SELECT id FROM test_common_grams_snii WHERE body MATCH_PHRASE 'the of and' ORDER BY id
        """
        order_qt_cg_repeated_common """
            SELECT id FROM test_common_grams_snii WHERE body MATCH_PHRASE 'the the the' ORDER BY id
        """
        assertProfileCounterPositive("shape_nnn",
                "SELECT id FROM test_common_grams_snii "
                        + "WHERE body MATCH_PHRASE 'foo bar baz' ORDER BY id",
                "SniiCommonGramsFallbackNoGram")
        [
            ["exact_2_common", "the of"],
            ["shape_nns", "foo bar the"],
            ["shape_nss", "foo the of"],
            ["shape_snn", "the foo bar"],
            ["shape_sns", "the foo of"],
            ["shape_ssn", "the of foo"],
            ["shape_sss", "the of and"]
        ].each { label, terms ->
            assertProfileCounterPositive(label,
                    "SELECT id FROM test_common_grams_snii "
                            + "WHERE body MATCH_PHRASE '${terms}' ORDER BY id",
                    "SniiCommonGramsGramPlans")
        }
        // A lone interior stopword (n-s-n) keeps every plain clause in the
        // HybridV1 query plan -- the pair clauses are purely additive -- so its
        // gram plan estimate is plain + pairs and can never pass the <=100%
        // cost gate. Pin the gate rejecting it instead of asserting a gram
        // plan that is unreachable by construction.
        assertProfileCounterPositive("shape_nsn",
                "SELECT id FROM test_common_grams_snii "
                        + "WHERE body MATCH_PHRASE 'foo the bar' ORDER BY id",
                "SniiCommonGramsFallbackCost")

        sql "SET inverted_index_max_expansions = 50"
        order_qt_cg_prefix_1 """
            SELECT id FROM test_common_grams_snii WHERE body MATCH_PHRASE_PREFIX 'alph' ORDER BY id
        """
        order_qt_cg_prefix_2 """
            SELECT id FROM test_common_grams_snii WHERE body MATCH_PHRASE_PREFIX 'alpha be' ORDER BY id
        """
        order_qt_cg_prefix_3 """
            SELECT id FROM test_common_grams_snii
            WHERE body MATCH_PHRASE_PREFIX 'alpha beta ga' ORDER BY id
        """
        order_qt_cg_prefix_6 """
            SELECT id FROM test_common_grams_snii
            WHERE body MATCH_PHRASE_PREFIX 'alpha the beta of gamma an' ORDER BY id
        """
        order_qt_cg_prefix_10 """
            SELECT id FROM test_common_grams_snii
            WHERE body MATCH_PHRASE_PREFIX 'alpha the beta of gamma and delta in epsilon t' ORDER BY id
        """
        order_qt_cg_prefix_the_wo """
            SELECT id FROM test_common_grams_snii WHERE body MATCH_PHRASE_PREFIX 'the wo' ORDER BY id
        """
        order_qt_cg_prefix_foo_the """
            SELECT id FROM test_common_grams_snii WHERE body MATCH_PHRASE_PREFIX 'foo the' ORDER BY id
        """
        order_qt_cg_prefix_foo_of_th """
            SELECT id FROM test_common_grams_snii WHERE body MATCH_PHRASE_PREFIX 'foo of th' ORDER BY id
        """
        order_qt_cg_prefix_the_bar_ba """
            SELECT id FROM test_common_grams_snii WHERE body MATCH_PHRASE_PREFIX 'the bar ba' ORDER BY id
        """
        // Like shape_nsn: the phrase-prefix hybrid plan keeps the plain
        // stopword clause alongside the pair expansions, so its estimate is
        // plain + pairs and the <=100% cost gate must reject it.
        assertProfileCounterPositive("prefix_common",
                "SELECT id FROM test_common_grams_snii "
                        + "WHERE body MATCH_PHRASE_PREFIX 'the wo' ORDER BY id",
                "SniiCommonGramsFallbackCost")
        def assertSloppyMatchesPlainOracle = { String label, String phrase ->
            def v3SloppyRows = sql """
                SELECT id FROM test_common_grams_v3_plain
                WHERE body MATCH_PHRASE '${phrase} ~1' ORDER BY id
            """
            def sniiPlainRows = sql """
                SELECT id FROM test_common_grams_snii
                WHERE body MATCH_PHRASE '${phrase}' USING ANALYZER cg_plain_analyzer
                ORDER BY id
            """
            assertFalse(v3SloppyRows.isEmpty(), "sloppy phrase ${label} must have a positive match")
            assertEquals(sniiPlainRows, v3SloppyRows,
                    "V3 sloppy phrase ${label} differs from the SNII plain oracle")
        }
        [
            ["1", "alpha"],
            ["2", "the world"],
            ["3", "the world wide"],
            ["6", "alpha the beta of gamma and"],
            ["10", "alpha the beta of gamma and delta in epsilon to"]
        ].each { label, phrase -> assertSloppyMatchesPlainOracle(label, phrase) }

        order_qt_v3_sloppy_plain_1 """
            SELECT id FROM test_common_grams_v3_plain
            WHERE body MATCH_PHRASE 'alpha ~1' ORDER BY id
        """
        order_qt_snii_plain_oracle_1 """
            SELECT id FROM test_common_grams_snii
            WHERE body MATCH_PHRASE 'alpha' USING ANALYZER cg_plain_analyzer ORDER BY id
        """
        order_qt_v3_sloppy_plain_2 """
            SELECT id FROM test_common_grams_v3_plain
            WHERE body MATCH_PHRASE 'the world ~1' ORDER BY id
        """
        order_qt_snii_plain_oracle_2 """
            SELECT id FROM test_common_grams_snii
            WHERE body MATCH_PHRASE 'the world' USING ANALYZER cg_plain_analyzer ORDER BY id
        """
        order_qt_v3_sloppy_plain_3 """
            SELECT id FROM test_common_grams_v3_plain
            WHERE body MATCH_PHRASE 'the world wide ~1' ORDER BY id
        """
        order_qt_snii_plain_oracle_3 """
            SELECT id FROM test_common_grams_snii
            WHERE body MATCH_PHRASE 'the world wide' USING ANALYZER cg_plain_analyzer ORDER BY id
        """
        order_qt_v3_sloppy_plain_6 """
            SELECT id FROM test_common_grams_v3_plain
            WHERE body MATCH_PHRASE 'alpha the beta of gamma and ~1' ORDER BY id
        """
        order_qt_snii_plain_oracle_6 """
            SELECT id FROM test_common_grams_snii
            WHERE body MATCH_PHRASE 'alpha the beta of gamma and'
            USING ANALYZER cg_plain_analyzer ORDER BY id
        """
        order_qt_v3_sloppy_plain_10 """
            SELECT id FROM test_common_grams_v3_plain
            WHERE body MATCH_PHRASE 'alpha the beta of gamma and delta in epsilon to ~1'
            ORDER BY id
        """
        order_qt_snii_plain_oracle_10 """
            SELECT id FROM test_common_grams_snii
            WHERE body MATCH_PHRASE 'alpha the beta of gamma and delta in epsilon to'
            USING ANALYZER cg_plain_analyzer ORDER BY id
        """
        order_qt_cg_regexp_namespace """
            SELECT id FROM test_common_grams_snii
            WHERE body MATCH_REGEXP '^the.*' AND id < 1000 ORDER BY id
        """
        String literalCommonGram = "\u001fDORIS_COMMON_GRAM_V1\u001f00000003:theof"
        order_qt_cg_literal_marker_namespace """
            SELECT id FROM test_common_grams_snii
            WHERE body MATCH_ANY '${literalCommonGram}' ORDER BY id
        """
        order_qt_cg_regexp_leading_namespace """
            SELECT id FROM test_common_grams_snii
            WHERE body MATCH_REGEXP '.*lpha' ORDER BY id
        """
        String wildcardSearchOptions =
                '{"default_operator":"and","default_field":"body","minimum_should_match":0,"mode":"lucene"}'
        order_qt_cg_search_leading_wildcard """
            SELECT id FROM test_common_grams_snii
            WHERE search('body:*lpha', '${wildcardSearchOptions}') ORDER BY id
        """
        order_qt_v3_search_leading_wildcard """
            SELECT id FROM test_common_grams_v3_plain
            WHERE search('body:*lpha', '${wildcardSearchOptions}') ORDER BY id
        """
        order_qt_cg_search_internal_gram_namespace """
            SELECT id FROM test_common_grams_snii
            WHERE search('body:*theof*', '${wildcardSearchOptions}') ORDER BY id
        """
        order_qt_v3_search_internal_gram_namespace """
            SELECT id FROM test_common_grams_v3_plain
            WHERE search('body:*theof*', '${wildcardSearchOptions}') ORDER BY id
        """
        String commonGramMarker = "\u001fDORIS_COMMON_GRAM_V1\u001f"
        order_qt_cg_search_literal_marker_namespace """
            SELECT id FROM test_common_grams_snii
            WHERE search('body:*${commonGramMarker}*', '${wildcardSearchOptions}') ORDER BY id
        """
        order_qt_cg_search_field_exists """
            SELECT id FROM test_common_grams_snii
            WHERE search('body:**', '${wildcardSearchOptions}') AND id < 1000 ORDER BY id
        """
        order_qt_cg_search_field_exists_oracle """
            SELECT id FROM test_common_grams_snii
            WHERE body IS NOT NULL AND id < 1000 ORDER BY id
        """
        order_qt_explicit_plain_index_routing """
            SELECT id
            FROM test_common_grams_snii
            WHERE body MATCH_PHRASE 'alpha beta' USING ANALYZER cg_plain_analyzer
            ORDER BY id
        """

        // The BE-local switch is the only gate, and flipping it bumps the query-plan config
        // generation that feeds the inverted index query cache key, so the very next query
        // observes the new mode instead of a stale cached bitmap.
        setBeConfig("enable_common_grams_query_plan", "false")
        order_qt_safety_off_plain """
            SELECT id FROM test_common_grams_snii WHERE body MATCH_PHRASE 'the of and' ORDER BY id
        """
        def safetyOffProfile = assertProfileCounterPositive("safety_off",
                "SELECT id FROM test_common_grams_snii "
                        + "WHERE body MATCH_PHRASE 'the of and' ORDER BY id",
                "SniiCommonGramsFallbackKillSwitch")
        assertFalse(safetyOffProfile.contains("SniiCommonGramsGramPlans:"))
        setBeConfig("enable_common_grams_query_plan", "true")

        order_qt_no_index_exact """
            SELECT /*+ SET_VAR(enable_match_without_inverted_index=true) */ id
            FROM test_common_grams_no_index WHERE body MATCH_PHRASE 'alpha beta gamma' ORDER BY id
        """

        sql "SET enable_inverted_index_query_cache = true"
        def gramMetamorphicRows = metamorphicQueries.collectEntries { label, query ->
            [(label): sql(query)]
        }
        setBeConfig("enable_common_grams_query_plan", "false")
        def plainCacheSeedProfile = assertProfileCounterPositive("plain_cache_seed",
                "SELECT id FROM test_common_grams_snii "
                        + "WHERE body MATCH_PHRASE 'the of and' ORDER BY id",
                "SniiCommonGramsFallbackKillSwitch")
        assertFalse(plainCacheSeedProfile.contains("SniiCommonGramsGramPlans:"))
        order_qt_plain_exact_1 """
            SELECT id FROM test_common_grams_snii WHERE body MATCH_PHRASE 'alpha' ORDER BY id
        """
        order_qt_plain_exact_2 """
            SELECT id FROM test_common_grams_snii WHERE body MATCH_PHRASE 'alpha beta' ORDER BY id
        """
        order_qt_plain_exact_2_common """
            SELECT id FROM test_common_grams_snii WHERE body MATCH_PHRASE 'the of' ORDER BY id
        """
        order_qt_plain_exact_3 """
            SELECT id FROM test_common_grams_snii WHERE body MATCH_PHRASE 'alpha beta gamma' ORDER BY id
        """
        order_qt_plain_exact_6 """
            SELECT id FROM test_common_grams_snii
            WHERE body MATCH_PHRASE 'alpha the beta of gamma and' ORDER BY id
        """
        order_qt_plain_exact_10 """
            SELECT id FROM test_common_grams_snii
            WHERE body MATCH_PHRASE 'alpha the beta of gamma and delta in epsilon to' ORDER BY id
        """
        order_qt_plain_shape_nnn """
            SELECT id FROM test_common_grams_snii WHERE body MATCH_PHRASE 'foo bar baz' ORDER BY id
        """
        order_qt_plain_shape_nns """
            SELECT id FROM test_common_grams_snii WHERE body MATCH_PHRASE 'foo bar the' ORDER BY id
        """
        order_qt_plain_shape_nsn """
            SELECT id FROM test_common_grams_snii WHERE body MATCH_PHRASE 'foo the bar' ORDER BY id
        """
        order_qt_plain_shape_nss """
            SELECT id FROM test_common_grams_snii WHERE body MATCH_PHRASE 'foo the of' ORDER BY id
        """
        order_qt_plain_shape_snn """
            SELECT id FROM test_common_grams_snii WHERE body MATCH_PHRASE 'the foo bar' ORDER BY id
        """
        order_qt_plain_shape_sns """
            SELECT id FROM test_common_grams_snii WHERE body MATCH_PHRASE 'the foo of' ORDER BY id
        """
        order_qt_plain_shape_ssn """
            SELECT id FROM test_common_grams_snii WHERE body MATCH_PHRASE 'the of foo' ORDER BY id
        """
        order_qt_plain_shape_sss """
            SELECT id FROM test_common_grams_snii WHERE body MATCH_PHRASE 'the of and' ORDER BY id
        """
        order_qt_plain_repeated_common """
            SELECT id FROM test_common_grams_snii WHERE body MATCH_PHRASE 'the the the' ORDER BY id
        """
        order_qt_plain_prefix_1 """
            SELECT id FROM test_common_grams_snii WHERE body MATCH_PHRASE_PREFIX 'alph' ORDER BY id
        """
        order_qt_plain_prefix_2 """
            SELECT id FROM test_common_grams_snii WHERE body MATCH_PHRASE_PREFIX 'alpha be' ORDER BY id
        """
        order_qt_plain_prefix_3 """
            SELECT id FROM test_common_grams_snii
            WHERE body MATCH_PHRASE_PREFIX 'alpha beta ga' ORDER BY id
        """
        order_qt_plain_prefix_6 """
            SELECT id FROM test_common_grams_snii
            WHERE body MATCH_PHRASE_PREFIX 'alpha the beta of gamma an' ORDER BY id
        """
        order_qt_plain_prefix_10 """
            SELECT id FROM test_common_grams_snii
            WHERE body MATCH_PHRASE_PREFIX 'alpha the beta of gamma and delta in epsilon t' ORDER BY id
        """
        order_qt_plain_prefix_the_wo """
            SELECT id FROM test_common_grams_snii WHERE body MATCH_PHRASE_PREFIX 'the wo' ORDER BY id
        """
        order_qt_plain_prefix_foo_the """
            SELECT id FROM test_common_grams_snii WHERE body MATCH_PHRASE_PREFIX 'foo the' ORDER BY id
        """
        order_qt_plain_prefix_foo_of_th """
            SELECT id FROM test_common_grams_snii WHERE body MATCH_PHRASE_PREFIX 'foo of th' ORDER BY id
        """
        order_qt_plain_prefix_the_bar_ba """
            SELECT id FROM test_common_grams_snii WHERE body MATCH_PHRASE_PREFIX 'the bar ba' ORDER BY id
        """
        metamorphicQueries.each { label, query ->
            assertEquals(gramMetamorphicRows[label], sql(query),
                    "CommonGrams and forced-plain results differ for ${label}")
        }

        setBeConfig("enable_common_grams_query_plan", "true")
        assertProfileCounterPositive("first_query_after_plan_toggle",
                "SELECT id FROM test_common_grams_snii "
                        + "WHERE body MATCH_PHRASE 'the of and' ORDER BY id",
                "SniiCommonGramsGramPlans")
        order_qt_cg_cache_reuse_across_plan_toggle """
            SELECT id FROM test_common_grams_snii WHERE body MATCH_PHRASE 'the of and' ORDER BY id
        """
        assertProfileCounterPositive("cache_reuse_across_plan_toggle",
                "SELECT id FROM test_common_grams_snii "
                        + "WHERE body MATCH_PHRASE 'the of and' ORDER BY id",
                "InvertedIndexQueryCacheHit")
        sql "SET enable_inverted_index_query_cache = false"
        assertProfileCounterPositive("plan_reenabled",
                "SELECT id FROM test_common_grams_snii "
                        + "WHERE body MATCH_PHRASE 'the of and' ORDER BY id",
                "SniiCommonGramsGramPlans")
        order_qt_cg_authoritative_empty """
            SELECT id FROM test_common_grams_snii
            WHERE body MATCH_PHRASE 'the nonexistent phrase' ORDER BY id
        """
        assertProfileCounterPositive("authoritative_empty_exact",
                "SELECT id FROM test_common_grams_snii "
                        + "WHERE body MATCH_PHRASE 'the nonexistent phrase' ORDER BY id",
                "SniiCommonGramsAuthoritativeEmpty")
        def gramAuthoritativeEmpty = sql """
            SELECT id FROM test_common_grams_snii
            WHERE body MATCH_PHRASE 'the nonexistent phrase' ORDER BY id
        """
        order_qt_cg_authoritative_empty_prefix """
            SELECT id FROM test_common_grams_snii
            WHERE body MATCH_PHRASE_PREFIX 'the nonex' ORDER BY id
        """
        assertProfileCounterPositive("authoritative_empty_prefix",
                "SELECT id FROM test_common_grams_snii "
                        + "WHERE body MATCH_PHRASE_PREFIX 'the nonex' ORDER BY id",
                "SniiCommonGramsAuthoritativeEmpty")
        def gramAuthoritativeEmptyPrefix = sql """
            SELECT id FROM test_common_grams_snii
            WHERE body MATCH_PHRASE_PREFIX 'the nonex' ORDER BY id
        """
        setBeConfig("enable_common_grams_query_plan", "false")
        order_qt_plain_authoritative_empty """
            SELECT id FROM test_common_grams_snii
            WHERE body MATCH_PHRASE 'the nonexistent phrase' ORDER BY id
        """
        def plainAuthoritativeEmpty = sql """
            SELECT id FROM test_common_grams_snii
            WHERE body MATCH_PHRASE 'the nonexistent phrase' ORDER BY id
        """
        order_qt_plain_authoritative_empty_prefix """
            SELECT id FROM test_common_grams_snii
            WHERE body MATCH_PHRASE_PREFIX 'the nonex' ORDER BY id
        """
        def plainAuthoritativeEmptyPrefix = sql """
            SELECT id FROM test_common_grams_snii
            WHERE body MATCH_PHRASE_PREFIX 'the nonex' ORDER BY id
        """
        assertEquals(gramAuthoritativeEmpty, plainAuthoritativeEmpty)
        assertEquals(gramAuthoritativeEmptyPrefix, plainAuthoritativeEmptyPrefix)
        setBeConfig("enable_common_grams_query_plan", "true")

        def rankedIds = { String table, String predicate, int limit ->
            sql """
                SELECT id FROM (
                    SELECT id, score() AS s FROM ${table}
                    WHERE ${predicate}
                    ORDER BY s DESC LIMIT ${limit}
                ) ranked ORDER BY s DESC, id
            """
        }
        def orderedIds = { String table, String predicate, int limit ->
            sql """
                SELECT id FROM ${table}
                WHERE ${predicate}
                ORDER BY id LIMIT ${limit}
            """
        }
        [10, 100].each { limit ->
            assertEquals(
                    rankedIds("test_common_grams_v3_plain", "body MATCH_PHRASE 'alpha beta'", limit),
                    rankedIds("test_common_grams_snii", "body MATCH_PHRASE 'alpha beta'", limit))
            assertEquals(
                    orderedIds("test_common_grams_v3_plain", "body MATCH_PHRASE_PREFIX 'alpha be'", limit),
                    orderedIds("test_common_grams_snii", "body MATCH_PHRASE_PREFIX 'alpha be'", limit))
        }

        qt_score_snii_exact_limit_10 """
            SELECT id, ROUND(s, 6) FROM (
                SELECT id, score() AS s FROM test_common_grams_snii
                WHERE body MATCH_PHRASE 'alpha beta'
                ORDER BY s DESC LIMIT 10
            ) ranked ORDER BY s DESC, id
        """
        qt_score_v3_exact_limit_10 """
            SELECT id, ROUND(s, 6) FROM (
                SELECT id, score() AS s FROM test_common_grams_v3_plain
                WHERE body MATCH_PHRASE 'alpha beta'
                ORDER BY s DESC LIMIT 10
            ) ranked ORDER BY s DESC, id
        """
        qt_score_snii_exact_limit_100 """
            SELECT id, ROUND(s, 6) FROM (
                SELECT id, score() AS s FROM test_common_grams_snii
                WHERE body MATCH_PHRASE 'alpha beta'
                ORDER BY s DESC LIMIT 100
            ) ranked ORDER BY s DESC, id
        """
        qt_score_v3_exact_limit_100 """
            SELECT id, ROUND(s, 6) FROM (
                SELECT id, score() AS s FROM test_common_grams_v3_plain
                WHERE body MATCH_PHRASE 'alpha beta'
                ORDER BY s DESC LIMIT 100
            ) ranked ORDER BY s DESC, id
        """
        qt_score_snii_prefix_limit_10 """
            SELECT id, ROUND(s, 6) FROM (
                SELECT id, score() AS s FROM test_common_grams_snii
                WHERE body MATCH_PHRASE_PREFIX 'alpha be'
                ORDER BY s DESC LIMIT 10
            ) ranked ORDER BY s DESC, id
        """
        qt_score_snii_prefix_limit_100 """
            SELECT id, ROUND(s, 6) FROM (
                SELECT id, score() AS s FROM test_common_grams_snii
                WHERE body MATCH_PHRASE_PREFIX 'alpha be'
                ORDER BY s DESC LIMIT 100
            ) ranked ORDER BY s DESC, id
        """
        order_qt_v3_prefix_limit_10 """
            SELECT id FROM test_common_grams_v3_plain
            WHERE body MATCH_PHRASE_PREFIX 'alpha be'
            ORDER BY id LIMIT 10
        """
        order_qt_v3_prefix_limit_100 """
            SELECT id FROM test_common_grams_v3_plain
            WHERE body MATCH_PHRASE_PREFIX 'alpha be'
            ORDER BY id LIMIT 100
        """
        setBeConfig("enable_common_grams_index_build", "false")
        sql """
            CREATE TABLE test_common_grams_missing (
                id INT NOT NULL,
                body STRING NULL,
                INDEX idx_body (body) USING INVERTED PROPERTIES (
                    "analyzer" = "cg_default_analyzer",
                    "support_phrase" = "true"
                )
            ) ENGINE=OLAP
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "disable_auto_compaction" = "true",
                "inverted_index_storage_format" = "SNII"
            )
        """
        sql "INSERT INTO test_common_grams_missing VALUES (1, 'alpha beta the world')"
        sql "INSERT INTO test_common_grams_mixed_v3_plain VALUES (1, 'alpha beta the world')"
        qt_score_v3_missing_control """
            SELECT id FROM (
                SELECT id, score() AS s FROM test_common_grams_mixed_v3_plain
                WHERE body MATCH_PHRASE 'alpha beta'
                ORDER BY s DESC LIMIT 100
            ) ranked ORDER BY s DESC, id
        """
        order_qt_missing_plain_fallback """
            SELECT id FROM test_common_grams_missing
            WHERE body MATCH_PHRASE 'alpha beta the' ORDER BY id
        """
        order_qt_missing_prefix_plain_fallback """
            SELECT id FROM test_common_grams_missing
            WHERE body MATCH_PHRASE_PREFIX 'the wo' ORDER BY id
        """
        assertProfileCounterPositive("missing_prefix_fallback",
                "SELECT id FROM test_common_grams_missing "
                        + "WHERE body MATCH_PHRASE_PREFIX 'the wo' ORDER BY id",
                "SniiCommonGramsFallbackIncompatible")
        test {
            sql """
                SELECT id, score() AS s FROM test_common_grams_missing
                WHERE body MATCH_PHRASE 'alpha beta' ORDER BY s DESC LIMIT 100
            """
            exception "SNII semantic scoring metadata is missing"
        }

        setBeConfig("enable_common_grams_index_build", "true")
        sql """
            CREATE TABLE test_common_grams_mixed (
                id INT NOT NULL,
                body STRING NULL,
                INDEX idx_body (body) USING INVERTED PROPERTIES (
                    "analyzer" = "cg_default_analyzer",
                    "support_phrase" = "true"
                )
            ) ENGINE=OLAP
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "disable_auto_compaction" = "true",
                "inverted_index_storage_format" = "SNII"
            )
        """
        sql "INSERT INTO test_common_grams_mixed VALUES (1, 'alpha beta the world')"
        setBeConfig("enable_common_grams_index_build", "false")
        sql "INSERT INTO test_common_grams_mixed VALUES (2, 'alpha beta the worker')"
        sql "INSERT INTO test_common_grams_mixed_v3_plain VALUES (2, 'alpha beta the worker')"
        qt_score_v3_mixed_control """
            SELECT id FROM (
                SELECT id, score() AS s FROM test_common_grams_mixed_v3_plain
                WHERE body MATCH_PHRASE 'alpha beta'
                ORDER BY s DESC LIMIT 100
            ) ranked ORDER BY s DESC, id
        """
        order_qt_mixed_plain_fallback """
            SELECT id FROM test_common_grams_mixed WHERE body MATCH_PHRASE 'alpha beta' ORDER BY id
        """
        order_qt_mixed_prefix_plain_fallback """
            SELECT id FROM test_common_grams_mixed
            WHERE body MATCH_PHRASE_PREFIX 'the wo' ORDER BY id
        """
        assertProfileCounterPositive("mixed_prefix_fallback",
                "SELECT id FROM test_common_grams_mixed "
                        + "WHERE body MATCH_PHRASE_PREFIX 'the wo' ORDER BY id",
                "SniiCommonGramsFallbackIncompatible")
        test {
            sql """
                SELECT id, score() AS s FROM test_common_grams_mixed
                WHERE body MATCH_PHRASE 'alpha beta' ORDER BY s DESC LIMIT 100
            """
            exception "SNII semantic scoring metadata is missing"
        }

        setBeConfig("enable_common_grams_index_build", "true")
        trigger_and_wait_compaction("test_common_grams_mixed", "full", 300)
        order_qt_mixed_recovered_phrase """
            SELECT id FROM test_common_grams_mixed WHERE body MATCH_PHRASE 'alpha beta' ORDER BY id
        """
        order_qt_mixed_recovered_prefix """
            SELECT id FROM test_common_grams_mixed
            WHERE body MATCH_PHRASE_PREFIX 'the wo' ORDER BY id
        """
        // Same structural rejection as prefix_common: the phrase-prefix hybrid
        // keeps the plain stopword clause, so the gram plan cannot be cheaper.
        assertProfileCounterPositive("mixed_recovered_prefix",
                "SELECT id FROM test_common_grams_mixed "
                        + "WHERE body MATCH_PHRASE_PREFIX 'the wo' ORDER BY id",
                "SniiCommonGramsFallbackCost")
        qt_mixed_recovered_score """
            SELECT id, ROUND(s, 6) FROM (
                SELECT id, score() AS s FROM test_common_grams_mixed
                WHERE body MATCH_PHRASE 'alpha beta'
                ORDER BY s DESC LIMIT 100
            ) ranked ORDER BY s DESC, id
        """
        assertEquals(
                rankedIds("test_common_grams_mixed_v3_plain", "body MATCH_PHRASE 'alpha beta'", 100),
                rankedIds("test_common_grams_mixed", "body MATCH_PHRASE 'alpha beta'", 100))

        trigger_and_wait_compaction("test_common_grams_snii", "full", 300)
        order_qt_after_full_compaction_phrase """
            SELECT id FROM test_common_grams_snii
            WHERE body MATCH_PHRASE 'alpha the beta of gamma and' ORDER BY id
        """
        qt_after_full_compaction_score """
            SELECT id, ROUND(s, 6) FROM (
                SELECT id, score() AS s FROM test_common_grams_snii
                WHERE body MATCH_PHRASE 'alpha beta'
                ORDER BY s DESC LIMIT 100
            ) ranked ORDER BY s DESC, id
        """
        assertEquals(
                rankedIds("test_common_grams_v3_plain", "body MATCH_PHRASE 'alpha beta'", 100),
                rankedIds("test_common_grams_snii", "body MATCH_PHRASE 'alpha beta'", 100))
    } finally {
        originalBeConfigs.each { backendId, values ->
            values.each { key, value ->
                def (code, out, err) = update_be_config(
                        backendIdToIp.get(backendId), backendIdToHttpPort.get(backendId),
                        key, value)
                assertEquals(0, code,
                        "restore ${key}=${value} failed on backend ${backendId}: ${out} ${err}")
            }
        }
    }
}
