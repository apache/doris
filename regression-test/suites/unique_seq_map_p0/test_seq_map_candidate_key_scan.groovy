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

suite("test_seq_map_candidate_key_scan", "nonConcurrent") {
    def backendIdToBackendIp = [:]
    def backendIdToBackendHttpPort = [:]
    getBackendIpHttpPort(backendIdToBackendIp, backendIdToBackendHttpPort)
    def originalScanRangeMaxMb = [:]
    backendIdToBackendIp.keySet().each { String backendId ->
        def (code, out, err) = show_be_config(
                backendIdToBackendIp.get(backendId),
                backendIdToBackendHttpPort.get(backendId))
        assertEquals(0, code, "show BE config failed: ${err}")
        def configList = parseJson(out.trim())
        def configEntry = configList.find { it[0] == "doris_scan_range_max_mb" }
        assertTrue(configEntry != null, "doris_scan_range_max_mb is absent")
        originalScanRangeMaxMb[backendId] = configEntry[2]
    }
    def changedScanRangeMaxMbBackendIds = [] as Set
    def setScanRangeMaxMb = { String value ->
        backendIdToBackendIp.keySet().each { String backendId ->
            if (originalScanRangeMaxMb[backendId] == value) {
                return
            }
            changedScanRangeMaxMbBackendIds.add(backendId)
            def (code, out, err) = update_be_config(
                    backendIdToBackendIp.get(backendId),
                    backendIdToBackendHttpPort.get(backendId),
                    "doris_scan_range_max_mb", value)
            assertEquals(0, code, "update BE config failed: ${err}")
        }
    }
    def restoreScanRangeMaxMb = {
        def restoreFailures = []
        changedScanRangeMaxMbBackendIds.toList().each { String backendId ->
            try {
                def (code, out, err) = update_be_config(
                        backendIdToBackendIp.get(backendId),
                        backendIdToBackendHttpPort.get(backendId),
                        "doris_scan_range_max_mb", originalScanRangeMaxMb[backendId])
                if (code == 0) {
                    changedScanRangeMaxMbBackendIds.remove(backendId)
                } else {
                    restoreFailures.add("${backendId}: ${err}")
                }
            } catch (Throwable t) {
                restoreFailures.add("${backendId}: ${t.message}")
            }
        }
        assertTrue(restoreFailures.isEmpty(),
                "restore BE config failed: ${restoreFailures.join('; ')}")
    }
    onFinish {
        restoreScanRangeMaxMb()
    }

    sql "DROP TABLE IF EXISTS test_seq_map_candidate_key_scan"
    sql "DROP TABLE IF EXISTS test_seq_map_candidate_key_scan_composite"
    sql "DROP TABLE IF EXISTS test_seq_map_candidate_key_scan_multi_tablet"
    sql "DROP TABLE IF EXISTS test_seq_map_candidate_key_scan_multi_scanner"
    sql "DROP TABLE IF EXISTS test_seq_map_candidate_key_scan_cost_fallback"
    sql "DROP TABLE IF EXISTS test_seq_map_candidate_key_scan_ngram"
    try {
        sql """
            CREATE TABLE test_seq_map_candidate_key_scan (
                `id` BIGINT NOT NULL,
                `c` INT NULL,
                `d` INT NULL,
                `e` INT NULL,
                `s1` BIGINT NULL,
                `s2` BIGINT NULL,
                INDEX idx_c (`c`) USING INVERTED,
                INDEX idx_d (`d`) USING INVERTED,
                INDEX idx_e (`e`) USING INVERTED
            ) ENGINE=OLAP
            UNIQUE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "false",
                "light_schema_change" = "true",
                "disable_auto_compaction" = "true",
                "inverted_index_storage_format" = "V3",
                "sequence_mapping.s1" = "c,d",
                "sequence_mapping.s2" = "e"
            )
        """

        // Group s1 and group s2 deliberately arrive in different physical rows.
        sql "INSERT INTO test_seq_map_candidate_key_scan(id, c, d, s1) VALUES (1, 20, 200, 20)"
        sql "INSERT INTO test_seq_map_candidate_key_scan(id, e, s2) VALUES (1, 300, 30)"

        // id=2 has a stale physical row matching c=20. Its latest s1 value is c=99,
        // so candidate collection may include it but the final residual must remove it.
        sql "INSERT INTO test_seq_map_candidate_key_scan(id, c, d, s1) VALUES (2, 20, 200, 10)"
        sql "INSERT INTO test_seq_map_candidate_key_scan(id, c, d, s1) VALUES (2, 99, 999, 20)"
        sql "INSERT INTO test_seq_map_candidate_key_scan(id, e, s2) VALUES (2, 300, 30)"

        sql "INSERT INTO test_seq_map_candidate_key_scan(id, c, d, s1) VALUES (3, 20, 201, 20)"
        sql "INSERT INTO test_seq_map_candidate_key_scan(id, e, s2) VALUES (3, 300, 30)"
        // Keep the table large enough that the weighted point-probe admission model can still
        // exercise selective candidate scans instead of conservatively falling back.
        sql """
            INSERT INTO test_seq_map_candidate_key_scan(id, c, d, e, s1, s2)
            SELECT number + 1000, 999, 999, 999, 1, 1 FROM numbers("number" = "10000")
        """

        def query = "SELECT id FROM test_seq_map_candidate_key_scan WHERE c = 20 AND e = 300 ORDER BY id"

        sql "SET enable_sql_cache = false"
        sql "SET enable_seq_map_candidate_key_scan = false"
        qt_candidate_disabled query

        sql "SET enable_seq_map_candidate_key_scan = true"
        sql "SET enable_inverted_index_query = true"
        sql "SET inverted_index_skip_threshold = 0"
        sql "SET enable_function_pushdown = true"
        sql "SET enable_profile = true"

        def counterValue = { String profileString, String counterName ->
            def parseCounters = { String profileSection ->
                def matcher = Pattern.compile(
                        "(?m)^\\s*-\\s*${Pattern.quote(counterName)}:\\s*"
                                + "(?:sum\\s+)?([^\\r\\n,]+)")
                        .matcher(profileSection)
                long total = 0L
                int matches = 0
                while (matcher.find()) {
                    def renderedValue = matcher.group(1)
                    def exactMatcher = Pattern.compile("\\((-?\\d+)\\)").matcher(renderedValue)
                    if (exactMatcher.find()) {
                        total += Long.parseLong(exactMatcher.group(1))
                    } else {
                        def plainMatcher = Pattern.compile("^\\s*(-?\\d+)").matcher(renderedValue)
                        assertTrue(plainMatcher.find(),
                                "Cannot parse ${counterName} value: ${renderedValue}")
                        total += Long.parseLong(plainMatcher.group(1))
                    }
                    ++matches
                }
                return [total, matches]
            }

            def mergedProfileStart = profileString.indexOf("MergedProfile:")
            assertTrue(mergedProfileStart >= 0, "MergedProfile is absent from profile")
            int mergedProfileEnd = profileString.length()
            ["DetailProfile(", "Execution Profile:", "Appendix:"].each { String sectionName ->
                int sectionIndex = profileString.indexOf(sectionName, mergedProfileStart)
                if (sectionIndex > 0) {
                    mergedProfileEnd = Math.min(mergedProfileEnd, sectionIndex)
                }
            }
            def counterResult =
                    parseCounters(profileString.substring(mergedProfileStart, mergedProfileEnd))
            if (counterResult[1] == 0) {
                def detailProfileStart = profileString.indexOf("DetailProfile(", mergedProfileStart)
                if (detailProfileStart >= 0) {
                    int detailProfileEnd = profileString.length()
                    ["Execution Profile:", "Appendix:"].each { String sectionName ->
                        int sectionIndex = profileString.indexOf(sectionName, detailProfileStart)
                        if (sectionIndex > 0) {
                            detailProfileEnd = Math.min(detailProfileEnd, sectionIndex)
                        }
                    }
                    counterResult = parseCounters(
                            profileString.substring(detailProfileStart, detailProfileEnd))
                }
            }
            assertTrue(counterResult[1] > 0, "${counterName} is absent from profile")
            return counterResult[0]
        }

        def profileAction = new ProfileAction(context)
        def runWithProfile = { String tag, String statement, Closure profileCheck ->
            def queryId = "${tag}_${System.currentTimeMillis()}"
            def rows = sql "/* ${queryId} */ ${statement}"
            def profileString =
                    profileAction.getProfileBySql(queryId, ["SeqMapCandidateFallbacks"])
            profileCheck.call(profileString)
            return rows
        }

        qt_candidate_two_groups query
        def profiledRows = runWithProfile("seq_map_candidate_two_groups", query) { profileString ->
            assertEquals(2L, counterValue(profileString, "SeqMapCandidateDriverGroups"))
            assertEquals(2L, counterValue(profileString, "SeqMapCandidateDriverPredicates"))
            assertEquals(3L, counterValue(profileString, "SeqMapCandidateKeysAfterIntersect"))
            assertTrue(counterValue(profileString, "SeqMapCandidateKeyBytes") > 0)
            assertTrue(counterValue(profileString, "SeqMapCandidateRows") > 0)
            assertTrue(counterValue(profileString, "SeqMapCandidateScanRows") > 0)
            assertTrue(counterValue(profileString, "RowsKeyRangeFiltered") > 0)
            assertEquals(0L, counterValue(profileString, "SeqMapCandidateIndexDowngrades"))
            assertEquals(0L, counterValue(profileString, "SeqMapCandidateFallbacks"))
        }
        assertEquals([[1L], [3L]], profiledRows)

        // Same-group predicates must be evaluated on the same physical group row.
        def sameGroupQuery =
                "SELECT id FROM test_seq_map_candidate_key_scan WHERE c = 20 AND d = 200 ORDER BY id"
        qt_candidate_same_group sameGroupQuery
        runWithProfile("seq_map_candidate_same_group", sameGroupQuery) { profileString ->
            assertEquals(1L, counterValue(profileString, "SeqMapCandidateDriverGroups"))
            assertEquals(2L, counterValue(profileString, "SeqMapCandidateDriverPredicates"))
            assertEquals(2L, counterValue(profileString, "SeqMapCandidateKeysAfterIntersect"))
        }

        // Empty candidates can short-circuit this tablet.
        def emptyCandidateQuery = "SELECT id FROM test_seq_map_candidate_key_scan WHERE c = 777"
        qt_candidate_empty emptyCandidateQuery
        runWithProfile("seq_map_candidate_empty", emptyCandidateQuery) { profileString ->
            assertEquals(0L, counterValue(profileString, "SeqMapCandidateKeysAfterIntersect"))
            assertEquals(1L, counterValue(profileString, "SeqMapCandidatePrunedScanners"))
            assertEquals(0L, counterValue(profileString, "SeqMapCandidateFallbacks"))
        }

        // Force fallback and prove that the candidate-key limit branch was taken.
        sql "SET seq_map_candidate_key_max_count = 1"
        try {
            def fallbackQuery =
                    "SELECT id FROM test_seq_map_candidate_key_scan WHERE c IN (20, 99) ORDER BY id"
            qt_candidate_limit_fallback fallbackQuery
            runWithProfile("seq_map_candidate_limit", fallbackQuery) { profileString ->
                assertTrue(counterValue(profileString, "SeqMapCandidateFallbacks") > 0)
                assertTrue(profileString.contains("candidate_key_limit"),
                        "candidate_key_limit fallback reason is absent from profile")
            }
        } finally {
            sql "SET seq_map_candidate_key_max_count = 100000"
        }

        // Existing FE key ranges use the normal scan because their exact baseline is already
        // selective and cannot be conservatively estimated from whole-rowset counts.
        def keyRangeQuery = """
            SELECT id FROM test_seq_map_candidate_key_scan
            WHERE id BETWEEN 2 AND 3 AND c = 20 AND e = 300
            ORDER BY id
        """
        qt_candidate_key_range_fallback keyRangeQuery
        runWithProfile("seq_map_candidate_key_range", keyRangeQuery) { profileString ->
            assertTrue(counterValue(profileString, "SeqMapCandidateFallbacks") > 0)
            assertTrue(profileString.contains("key_range_present"),
                    "key_range_present fallback reason is absent from profile")
        }

        def keyInQuery = """
            SELECT id FROM test_seq_map_candidate_key_scan
            WHERE id IN (1, 3) AND c = 20 AND e = 300
            ORDER BY id
        """
        qt_candidate_key_in_fallback keyInQuery
        runWithProfile("seq_map_candidate_key_in", keyInQuery) { profileString ->
            assertTrue(counterValue(profileString, "SeqMapCandidateFallbacks") > 0)
            assertTrue(profileString.contains("key_range_present"),
                    "key_range_present fallback reason is absent from profile")
        }

        sql """
            CREATE TABLE test_seq_map_candidate_key_scan_ngram (
                `advertiser_id` BIGINT NOT NULL,
                `id` BIGINT NOT NULL,
                `title` STRING NULL,
                `s1` BIGINT NULL,
                INDEX idx_title_ngram (`title`) USING NGRAM_BF
                    PROPERTIES("gram_size" = "3", "bf_size" = "1024")
            ) ENGINE=OLAP
            UNIQUE KEY(`advertiser_id`, `id`)
            DISTRIBUTED BY HASH(`advertiser_id`) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "false",
                "light_schema_change" = "true",
                "disable_auto_compaction" = "true",
                "sequence_mapping.s1" = "title"
            )
        """
        sql """
            INSERT INTO test_seq_map_candidate_key_scan_ngram(advertiser_id, id, title, s1)
            VALUES (100, 1, 'have a good day', 10)
        """
        // The old physical version matches, but the final merged row must not.
        sql """
            INSERT INTO test_seq_map_candidate_key_scan_ngram(advertiser_id, id, title, s1)
            VALUES (100, 2, 'have a good old version', 10)
        """
        sql """
            INSERT INTO test_seq_map_candidate_key_scan_ngram(advertiser_id, id, title, s1)
            VALUES (100, 2, 'unrelated latest value', 20)
        """
        sql """
            INSERT INTO test_seq_map_candidate_key_scan_ngram(advertiser_id, id, title, s1)
            VALUES (100, 3, 'Have a Good Day', 10),
                   (200, 1, 'have a good day', 10)
        """
        sql """
            INSERT INTO test_seq_map_candidate_key_scan_ngram(advertiser_id, id, title, s1)
            SELECT 100, number + 1000, CONCAT('irrelevant filler ', number), 1
            FROM numbers("number" = "5000")
        """

        def ngramQuery = """
            SELECT id FROM test_seq_map_candidate_key_scan_ngram
            WHERE advertiser_id = 100
              AND title LIKE LOWER('%HaVe A GoOd%')
            ORDER BY id
        """
        qt_candidate_ngram_like ngramQuery
        runWithProfile("seq_map_candidate_ngram_like", ngramQuery) { profileString ->
            assertEquals(1L, counterValue(profileString, "SeqMapCandidateDriverGroups"))
            assertEquals(1L, counterValue(profileString, "SeqMapCandidateDriverPredicates"))
            assertEquals(2L, counterValue(profileString, "SeqMapCandidateKeysAfterIntersect"))
            assertEquals(0L, counterValue(profileString, "SeqMapCandidateFallbacks"))
            assertTrue(counterValue(
                    profileString, "SeqMapCandidateBloomFilterFilteredRows") > 0)
        }

        // A LIKE pattern shorter than gram_size cannot use the NGRAM bloom filter.
        def shortNgramQuery = """
            SELECT id FROM test_seq_map_candidate_key_scan_ngram
            WHERE advertiser_id = 100 AND title LIKE '%ha%'
            ORDER BY id
        """
        qt_candidate_ngram_short_pattern shortNgramQuery
        runWithProfile("seq_map_candidate_ngram_short_pattern", shortNgramQuery) { profileString ->
            assertTrue(counterValue(profileString, "SeqMapCandidateFallbacks") > 0)
            assertTrue(profileString.contains("no_indexed_positive_driver"),
                    "no_indexed_positive_driver fallback reason is absent from profile")
        }

        // NOT LIKE cannot safely use a bloom filter as a candidate driver.
        def oppositeNgramQuery = """
            SELECT COUNT(*) FROM test_seq_map_candidate_key_scan_ngram
            WHERE advertiser_id = 100 AND title NOT LIKE '%have a good%'
        """
        qt_candidate_ngram_not_like oppositeNgramQuery
        runWithProfile("seq_map_candidate_ngram_not_like", oppositeNgramQuery) { profileString ->
            assertTrue(counterValue(profileString, "SeqMapCandidateFallbacks") > 0)
            assertTrue(profileString.contains("no_indexed_positive_driver"),
                    "no_indexed_positive_driver fallback reason is absent from profile")
        }

        sql "SET enable_inverted_index_query = false"
        try {
            qt_candidate_index_disabled query
            runWithProfile("seq_map_candidate_index_disabled", query) { profileString ->
                assertTrue(counterValue(profileString, "SeqMapCandidateFallbacks") > 0)
                assertTrue(profileString.contains("inverted_index_query_disabled"),
                        "inverted_index_query_disabled fallback reason is absent from profile")
            }
            qt_candidate_ngram_with_inverted_disabled ngramQuery
            runWithProfile("seq_map_candidate_ngram_with_inverted_disabled",
                    ngramQuery) { profileString ->
                assertEquals(1L, counterValue(profileString, "SeqMapCandidateDriverGroups"))
                assertEquals(0L, counterValue(profileString, "SeqMapCandidateFallbacks"))
            }
        } finally {
            sql "SET enable_inverted_index_query = true"
        }

        // Range predicates are intentionally residual-only in the first version.
        def noDriverQuery =
                "SELECT id FROM test_seq_map_candidate_key_scan WHERE c > 20 AND c < 100 ORDER BY id"
        qt_candidate_no_driver noDriverQuery
        runWithProfile("seq_map_candidate_no_driver", noDriverQuery) { profileString ->
            assertTrue(counterValue(profileString, "SeqMapCandidateFallbacks") > 0)
            assertTrue(profileString.contains("no_indexed_positive_driver"),
                    "no_indexed_positive_driver fallback reason is absent from profile")
        }

        test {
            sql "SET seq_map_candidate_key_max_count = 0"
            exception "seq_map_candidate_key_max_count should be greater than 0"
        }
        test {
            sql "SET seq_map_candidate_key_max_count = -1"
            exception "seq_map_candidate_key_max_count should be greater than 0"
        }

        sql """
            CREATE TABLE test_seq_map_candidate_key_scan_cost_fallback (
                `id` BIGINT NOT NULL,
                `c` INT NULL,
                `s1` BIGINT NULL,
                INDEX idx_c (`c`) USING INVERTED
            ) ENGINE=OLAP
            UNIQUE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "false",
                "light_schema_change" = "true",
                "disable_auto_compaction" = "true",
                "inverted_index_storage_format" = "V3",
                "sequence_mapping.s1" = "c"
            )
        """
        sql """
            INSERT INTO test_seq_map_candidate_key_scan_cost_fallback(id, c, s1)
            SELECT number, 20, 1 FROM numbers("number" = "6000")
        """
        sql """
            INSERT INTO test_seq_map_candidate_key_scan_cost_fallback(id, c, s1)
            SELECT number + 6000, 20, 1 FROM numbers("number" = "6000")
        """
        def limitFallbackQuery = """
            SELECT COUNT(*) FROM (
                SELECT id FROM test_seq_map_candidate_key_scan_cost_fallback
                WHERE c = 20 LIMIT 1
            ) limited
        """
        qt_candidate_limit_query_fallback limitFallbackQuery
        runWithProfile("seq_map_candidate_limit_query_fallback",
                limitFallbackQuery) { profileString ->
            assertTrue(counterValue(profileString, "SeqMapCandidateFallbacks") > 0)
            assertEquals(0L, counterValue(profileString, "SeqMapCandidateScanRows"))
            assertTrue(profileString.contains("scan_limit_present"),
                    "scan_limit_present fallback reason is absent from profile")
        }

        def costFallbackQuery = """
            SELECT COUNT(*) FROM test_seq_map_candidate_key_scan_cost_fallback
            WHERE c = 20
        """
        qt_candidate_cost_fallback costFallbackQuery
        runWithProfile("seq_map_candidate_cost_fallback", costFallbackQuery) { profileString ->
            assertTrue(counterValue(profileString, "SeqMapCandidateFallbacks") > 0)
            assertTrue(counterValue(profileString, "SeqMapCandidateScanRows") > 0)
            assertTrue(profileString.contains("candidate_cost_limit"),
                    "candidate_cost_limit fallback reason is absent from profile")
        }

        sql """
            INSERT INTO test_seq_map_candidate_key_scan_cost_fallback(id, c, s1)
            SELECT number + 12000, 20, 1 FROM numbers("number" = "60000")
        """
        def workFallbackQuery = """
            SELECT COUNT(*) FROM test_seq_map_candidate_key_scan_cost_fallback
            WHERE c = 20
        """
        qt_candidate_work_fallback workFallbackQuery
        runWithProfile("seq_map_candidate_work_fallback", workFallbackQuery) { profileString ->
            assertTrue(counterValue(profileString, "SeqMapCandidateFallbacks") > 0)
            def candidateScanRows = counterValue(profileString, "SeqMapCandidateScanRows")
            assertTrue(candidateScanRows > 0)
            assertTrue(candidateScanRows < 72000,
                    "candidate prepass scanned the full table before falling back")
            assertTrue(profileString.contains("candidate_work_limit"),
                    "candidate_work_limit fallback reason is absent from profile")
        }

        sql """
            CREATE TABLE test_seq_map_candidate_key_scan_composite (
                `k1` BIGINT NOT NULL,
                `k2` VARCHAR(32) NOT NULL,
                `c` INT NULL,
                `e` INT NULL,
                `s1` BIGINT NULL,
                `s2` BIGINT NULL,
                INDEX idx_c (`c`) USING INVERTED,
                INDEX idx_e (`e`) USING INVERTED
            ) ENGINE=OLAP
            UNIQUE KEY(`k1`, `k2`)
            DISTRIBUTED BY HASH(`k1`) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "false",
                "light_schema_change" = "true",
                "disable_auto_compaction" = "true",
                "inverted_index_storage_format" = "V3",
                "sequence_mapping.s1" = "c",
                "sequence_mapping.s2" = "e"
            )
        """
        sql """
            INSERT INTO test_seq_map_candidate_key_scan_composite(k1, k2, c, s1)
            VALUES (1, 'alpha|beta', 20, 10)
        """
        sql """
            INSERT INTO test_seq_map_candidate_key_scan_composite(k1, k2, c, s1)
            VALUES (2, 'trailing space ', 20, 10)
        """
        sql """
            INSERT INTO test_seq_map_candidate_key_scan_composite(k1, k2, c, s1)
            VALUES (2, 'trailing space ', 99, 20)
        """
        sql """
            INSERT INTO test_seq_map_candidate_key_scan_composite(k1, k2, e, s2)
            VALUES (1, 'alpha|beta', 300, 30),
                   (2, 'trailing space ', 300, 30)
        """
        sql """
            INSERT INTO test_seq_map_candidate_key_scan_composite(k1, k2, c, e, s1, s2)
            SELECT number + 1000, CONCAT('filler-', number), 999, 999, 1, 1
            FROM numbers("number" = "1000")
        """
        sql """
            INSERT INTO test_seq_map_candidate_key_scan_composite(k1, k2, c, e, s1, s2)
            SELECT 1, CONCAT('prefix-filler-', number), 999, 999, 1, 1
            FROM numbers("number" = "1000")
        """
        def compositeQuery =
                "SELECT k1 FROM test_seq_map_candidate_key_scan_composite WHERE c = 20 ORDER BY k1"
        qt_candidate_composite_key compositeQuery
        runWithProfile("seq_map_candidate_composite_key", compositeQuery) { profileString ->
            assertEquals(1L, counterValue(profileString, "SeqMapCandidateDriverGroups"))
            assertEquals(2L, counterValue(profileString, "SeqMapCandidateKeysAfterIntersect"))
            assertEquals(0L, counterValue(profileString, "SeqMapCandidateFallbacks"))
        }

        // A composite-key prefix range can safely constrain candidate collection.
        def compositePrefixRangeQuery = """
            SELECT k1, k2 FROM test_seq_map_candidate_key_scan_composite
            WHERE k1 BETWEEN 1 AND 1 AND c = 20 AND e = 300
            ORDER BY k1, k2
        """
        qt_candidate_composite_prefix_range compositePrefixRangeQuery
        runWithProfile("seq_map_candidate_composite_prefix_range",
                compositePrefixRangeQuery) { profileString ->
            assertEquals(0L, counterValue(profileString, "SeqMapCandidateFallbacks"))
            assertEquals(2L, counterValue(profileString, "SeqMapCandidateDriverGroups"))
            assertEquals(1L, counterValue(profileString, "SeqMapCandidateKeysAfterIntersect"))
            assertEquals(1002L, counterValue(profileString, "SeqMapCandidateFullScanRows"))
        }

        sql """
            CREATE TABLE test_seq_map_candidate_key_scan_multi_scanner (
                `k1` BIGINT NOT NULL,
                `k2` VARCHAR(128) NOT NULL,
                `c` INT NULL,
                `s1` BIGINT NULL,
                INDEX idx_c (`c`) USING INVERTED
            ) ENGINE=OLAP
            UNIQUE KEY(`k1`, `k2`)
            DISTRIBUTED BY HASH(`k1`) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "false",
                "light_schema_change" = "true",
                "disable_auto_compaction" = "true",
                "inverted_index_storage_format" = "V3",
                "sequence_mapping.s1" = "c"
            )
        """
        sql """
            INSERT INTO test_seq_map_candidate_key_scan_multi_scanner(k1, k2, c, s1)
            VALUES (1, 'a', 20, 10), (2, 'b', 20, 10)
        """
        sql """
            INSERT INTO test_seq_map_candidate_key_scan_multi_scanner(k1, k2, c, s1)
            SELECT IF(number % 2 = 0, 1, 2),
                   CONCAT(MD5(CAST(number AS STRING)),
                          MD5(CAST(number + 100000 AS STRING))),
                   999,
                   1
            FROM numbers("number" = "100000")
        """
        setScanRangeMaxMb("1")
        sql "SET seq_map_candidate_key_max_count = 1"
        try {
            def multiScannerQuery = """
                SELECT k1, k2 FROM test_seq_map_candidate_key_scan_multi_scanner
                WHERE k1 IN (1, 2) AND c = 20
                ORDER BY k1, k2
            """
            qt_candidate_multi_scanner_tablet_budget multiScannerQuery
            runWithProfile("seq_map_candidate_multi_scanner_tablet_budget",
                    multiScannerQuery) { profileString ->
                assertEquals(1L, counterValue(
                        profileString, "SeqMapCandidateKeysAfterIntersect"))
                assertTrue(counterValue(profileString, "SeqMapCandidateFallbacks") > 0)
                assertTrue(profileString.contains("candidate_key_limit"),
                        "shared tablet candidate-key limit was not enforced")
            }
        } finally {
            sql "SET seq_map_candidate_key_max_count = 100000"
            restoreScanRangeMaxMb()
        }

        sql """
            CREATE TABLE test_seq_map_candidate_key_scan_multi_tablet (
                `id` BIGINT NOT NULL,
                `c` INT NULL,
                `s1` BIGINT NULL,
                INDEX idx_c (`c`) USING INVERTED
            ) ENGINE=OLAP
            UNIQUE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 2
            PROPERTIES (
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "false",
                "light_schema_change" = "true",
                "disable_auto_compaction" = "true",
                "inverted_index_storage_format" = "V3",
                "sequence_mapping.s1" = "c"
            )
        """
        def nonMatchingValues =
                (1..200).collect { id -> "(${id}, 99, 1)" }.join(",")
        sql "INSERT INTO test_seq_map_candidate_key_scan_multi_tablet(id, c, s1) VALUES ${nonMatchingValues}"
        sql "INSERT INTO test_seq_map_candidate_key_scan_multi_tablet(id, c, s1) VALUES (100, 20, 1)"
        def multiTabletQuery =
                "SELECT id FROM test_seq_map_candidate_key_scan_multi_tablet WHERE c = 20 ORDER BY id"
        qt_candidate_multi_tablet multiTabletQuery
        runWithProfile("seq_map_candidate_multi_tablet", multiTabletQuery) { profileString ->
            assertTrue(counterValue(profileString, "SeqMapCandidatePrunedScanners") > 0)
            assertEquals(0L, counterValue(profileString, "SeqMapCandidateFallbacks"))
        }

        sql "SET enable_profile = false"
    } finally {
        sql "SET enable_profile = false"
        sql "SET enable_sql_cache = true"
        sql "SET enable_seq_map_candidate_key_scan = false"
        sql "SET enable_inverted_index_query = true"
        sql "SET inverted_index_skip_threshold = 50"
        sql "SET enable_function_pushdown = false"
        sql "SET seq_map_candidate_key_max_count = 100000"
    }
}
