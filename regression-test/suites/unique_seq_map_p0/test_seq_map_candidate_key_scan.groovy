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

suite("test_seq_map_candidate_key_scan") {
    sql "DROP TABLE IF EXISTS test_seq_map_candidate_key_scan"
    sql "DROP TABLE IF EXISTS test_seq_map_candidate_key_scan_composite"
    sql "DROP TABLE IF EXISTS test_seq_map_candidate_key_scan_multi_tablet"
    sql "DROP TABLE IF EXISTS test_seq_map_candidate_key_scan_cost_fallback"
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
            SELECT number + 1000, 999, 999, 999, 1, 1 FROM numbers("number" = "1000")
        """

        def query = "SELECT id FROM test_seq_map_candidate_key_scan WHERE c = 20 AND e = 300 ORDER BY id"

        sql "SET enable_sql_cache = false"
        sql "SET enable_seq_map_candidate_key_scan = false"
        qt_candidate_disabled query

        sql "SET enable_seq_map_candidate_key_scan = true"
        sql "SET enable_inverted_index_query = true"
        sql "SET enable_profile = true"

        def counterValue = { String profileString, String counterName ->
            def matcher = Pattern.compile("${counterName}:\\s*(\\d+)").matcher(profileString)
            assertTrue(matcher.find(), "${counterName} is absent from profile")
            return Long.parseLong(matcher.group(1))
        }

        def profileAction = new ProfileAction(context)
        def runWithProfile = { String tag, String statement, Closure profileCheck ->
            def queryId = "${tag}_${System.currentTimeMillis()}"
            sql "/* ${queryId} */ ${statement}"
            def profileString =
                    profileAction.getProfileBySql(queryId, ["SeqMapCandidateFallbacks"])
            profileCheck.call(profileString)
        }

        qt_candidate_two_groups query
        runWithProfile("seq_map_candidate_two_groups", query) { profileString ->
            assertEquals(2L, counterValue(profileString, "SeqMapCandidateDriverGroups"))
            assertEquals(2L, counterValue(profileString, "SeqMapCandidateDriverPredicates"))
            assertEquals(3L, counterValue(profileString, "SeqMapCandidateKeysAfterIntersect"))
            assertTrue(counterValue(profileString, "SeqMapCandidateKeyBytes") > 0)
            assertTrue(counterValue(profileString, "SeqMapCandidateScanRows") > 0)
            assertEquals(0L, counterValue(profileString, "SeqMapCandidateIndexDowngrades"))
        }

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
            assertEquals(1L, counterValue(profileString, "SeqMapCandidatePrunedTablets"))
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

        sql "SET enable_inverted_index_query = false"
        try {
            qt_candidate_index_disabled query
            runWithProfile("seq_map_candidate_index_disabled", query) { profileString ->
                assertTrue(counterValue(profileString, "SeqMapCandidateFallbacks") > 0)
                assertTrue(profileString.contains("inverted_index_query_disabled"),
                        "inverted_index_query_disabled fallback reason is absent from profile")
            }
        } finally {
            sql "SET enable_inverted_index_query = true"
        }

        // Range predicates are intentionally residual-only in the first version.
        def noDriverQuery = "SELECT id FROM test_seq_map_candidate_key_scan WHERE c > 20 ORDER BY id"
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
        def costFallbackQuery = """
            SELECT id FROM test_seq_map_candidate_key_scan_cost_fallback
            WHERE c = 20 ORDER BY id LIMIT 1
        """
        qt_candidate_cost_fallback costFallbackQuery
        runWithProfile("seq_map_candidate_cost_fallback", costFallbackQuery) { profileString ->
            assertTrue(counterValue(profileString, "SeqMapCandidateFallbacks") > 0)
            assertTrue(counterValue(profileString, "SeqMapCandidateScanRows") > 0)
            assertTrue(profileString.contains("candidate_cost_limit"),
                    "candidate_cost_limit fallback reason is absent from profile")
        }

        sql """
            CREATE TABLE test_seq_map_candidate_key_scan_composite (
                `k1` BIGINT NOT NULL,
                `k2` VARCHAR(32) NOT NULL,
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
            INSERT INTO test_seq_map_candidate_key_scan_composite(k1, k2, c, s1)
            SELECT number + 1000, CONCAT('filler-', number), 999, 1
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

        // Composite-key prefix ranges also keep the normal range-aware scan path.
        def compositePrefixRangeQuery = """
            SELECT k1, k2 FROM test_seq_map_candidate_key_scan_composite
            WHERE k1 BETWEEN 1 AND 1 AND c = 20
            ORDER BY k1, k2
        """
        qt_candidate_composite_prefix_range compositePrefixRangeQuery
        runWithProfile("seq_map_candidate_composite_prefix_range",
                compositePrefixRangeQuery) { profileString ->
            assertTrue(counterValue(profileString, "SeqMapCandidateFallbacks") > 0)
            assertTrue(profileString.contains("key_range_present"),
                    "key_range_present fallback reason is absent from profile")
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
            assertTrue(counterValue(profileString, "SeqMapCandidatePrunedTablets") > 0)
            assertEquals(0L, counterValue(profileString, "SeqMapCandidateFallbacks"))
        }

        sql "SET enable_profile = false"
    } finally {
        sql "SET enable_profile = false"
        sql "SET enable_sql_cache = true"
        sql "SET enable_seq_map_candidate_key_scan = false"
        sql "SET enable_inverted_index_query = true"
        sql "SET seq_map_candidate_key_max_count = 100000"
    }
}
