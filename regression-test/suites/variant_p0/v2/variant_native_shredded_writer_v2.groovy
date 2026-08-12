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

import org.apache.doris.regression.action.ProfileAction

suite("variant_native_shredded_writer_v2", "p0,nonConcurrent") {
    setFeConfigTemporary([enable_variant_v2: true]) {
        assertTrue(getFeConfig("enable_variant_v2").toBoolean())
        sql "SET default_variant_enable_doc_mode = false"
        sql "SET default_variant_enable_typed_paths_to_sparse = false"
        sql "SET default_variant_sparse_hash_shard_count = 1"

        def backendIdToIp = [:]
        def backendIdToHttpPort = [:]
        getBackendIpHttpPort(backendIdToIp, backendIdToHttpPort)
        assertFalse(backendIdToIp.isEmpty())
        def sumWriterMetric = { String type ->
            long total = 0
            for (String backendId : backendIdToIp.keySet()) {
                total += get_be_metric(backendIdToIp[backendId],
                        backendIdToHttpPort[backendId],
                        "variant_v2_shredded_writer_rows", type) as long
            }
            return total
        }

        sql "SET enable_profile = true"
        sql "SET profile_level = 2"
        def profileAction = new ProfileAction(context)
        def sumProfileCounter = { String profile, String counterName ->
            def matcher = (profile =~ /${counterName}:\s*([0-9,]+)/)
            long total = 0
            while (matcher.find()) {
                total += matcher.group(1).replace(",", "") as long
            }
            return total
        }
        def assertPathLayout = { String oracleName, String sourceTable, String path,
                long expectedLeaf, long expectedHierarchical, long expectedSparse,
                long expectedDefault ->
            String profileToken = "variant_native_layout_${oracleName}_" +
                    UUID.randomUUID().toString()
            sql """
                SELECT '${profileToken}', id, CAST(v['${path}'] AS STRING)
                FROM ${sourceTable}
                ORDER BY id
            """

            def counters = [
                    VariantSubtreeLeafIterCount: expectedLeaf,
                    VariantSubtreeHierarchicalIterCount: expectedHierarchical,
                    VariantSubtreeSparseIterCount: expectedSparse,
                    VariantSubtreeDefaultIterCount: expectedDefault
            ]
            String profile = profileAction.getProfileBySql(profileToken, counters.keySet().toList())
            counters.each { counterName, expectedValue ->
                assertEquals(expectedValue, sumProfileCounter(profile, counterName),
                        "${oracleName} ${path} physical layout mismatch for ${counterName}, " +
                        "profile:\n${profile}")
            }
        }
        def assertNativeInsertSelect = { String oracleName, String sourceTable,
                String destinationTable, String predicate, String orderByClause,
                long expectedShreddedRows, long expectedWrittenRows ->
            long nativeBefore = sumWriterMetric("native")
            long fallbackBefore = sumWriterMetric("encoded_fallback")
            String profileToken = "variant_native_s_writer_${oracleName}_" +
                    UUID.randomUUID().toString()
            sql """
                INSERT INTO ${destinationTable}
                SELECT id, v
                FROM ${sourceTable}
                WHERE '${profileToken}' = '${profileToken}'
                ${predicate}
                ${orderByClause}
            """

            String profile = profileAction.getProfileBySql(
                    profileToken, ["VariantV2ShreddedOutputRows"])
            long shreddedRows = sumProfileCounter(profile, "VariantV2ShreddedOutputRows")
            assertEquals(expectedShreddedRows, shreddedRows,
                    "${oracleName} scan must publish every row as S, profile:\n${profile}")

            long nativeAfter = sumWriterMetric("native")
            for (int attempt = 0;
                    attempt < 20 && nativeAfter - nativeBefore < expectedWrittenRows; ++attempt) {
                sleep(500)
                nativeAfter = sumWriterMetric("native")
            }
            long fallbackAfter = sumWriterMetric("encoded_fallback")
            assertEquals(expectedWrittenRows, nativeAfter - nativeBefore,
                    "${oracleName} writer must consume every selected row natively: " +
                    "before=${nativeBefore}, after=${nativeAfter}")
            assertEquals(0L, fallbackAfter - fallbackBefore,
                    "${oracleName} rows must not use the encoded slow lane")
        }

        def testCases =
                [[name: "full", maxSubcolumns: 16], [name: "partial", maxSubcolumns: 1]]
        testCases.each { testCase ->
            String sourceTable = "variant_native_shredded_writer_source_${testCase.name}"
            String destinationTable = "variant_native_shredded_writer_dest_${testCase.name}"
            String secondDestinationTable =
                    "variant_native_shredded_writer_dest2_${testCase.name}"
            String filteredDestinationTable =
                    "variant_native_shredded_writer_filtered_${testCase.name}"
            def tables = [sourceTable, destinationTable, secondDestinationTable,
                    filteredDestinationTable]
            tables.each { tableName ->
                sql "DROP TABLE IF EXISTS ${tableName}"
            }
            tables.each { tableName ->
                sql """
                    CREATE TABLE ${tableName} (
                        id INT,
                        v VARIANT<PROPERTIES(
                            "variant_max_subcolumns_count" = "${testCase.maxSubcolumns}")> NULL
                    )
                    DUPLICATE KEY(id)
                    DISTRIBUTED BY HASH(id) BUCKETS 1
                    PROPERTIES (
                        "replication_num" = "1",
                        "disable_auto_compaction" = "true"
                    )
                """
            }

            // Three committed inserts create typed a/b/a physical layouts when the dynamic-path
            // budget is one. Lower-frequency mixed/shared values stay in the sparse residual and
            // jointly cover scalar/object/array conflicts, missing paths, and outer SQL NULL.
            sql """
                INSERT INTO ${sourceTable} VALUES
                    (1, parse_to_variant(
                        '{"a":1,"mixed":1,"shared":"first"}')),
                    (2, parse_to_variant(
                        '{"a":2,"path_null":null}'))
            """
            sql """
                INSERT INTO ${sourceTable} VALUES
                    (3, parse_to_variant(
                        '{"b":"three","mixed":{"k":3},"shared":[3]}')),
                    (4, parse_to_variant('{"b":"four","object":{"x":4}}'))
            """
            sql """
                INSERT INTO ${sourceTable} VALUES
                    (5, parse_to_variant(
                        '{"a":5,"mixed":[5],"shared":"last"}')),
                    (6, parse_to_variant('{"a":6}')),
                    (7, NULL)
            """

            // The three committed inserts create three segments. With the full path budget,
            // shared is a direct physical leaf in every segment. With a budget of one, a/b/a
            // consume that slot and shared must be read from the encoded sparse residual.
            assertPathLayout("${testCase.name}_shared", sourceTable, "shared",
                    testCase.name == "full" ? 3L : 0L, 0L,
                    testCase.name == "partial" ? 3L : 0L, 0L)
            assertPathLayout("${testCase.name}_mixed", sourceTable, "mixed",
                    testCase.name == "full" ? 2L : 0L, 1L,
                    testCase.name == "partial" ? 2L : 0L, 0L)
            assertPathLayout("${testCase.name}_a", sourceTable, "a",
                    2L, 0L, 0L, 1L)
            assertPathLayout("${testCase.name}_b", sourceTable, "b",
                    1L, 0L, 0L, 2L)

            assertNativeInsertSelect("${testCase.name}_first_generation", sourceTable,
                    destinationTable, "", "", 7L, 7L)
            assertNativeInsertSelect("${testCase.name}_second_generation", destinationTable,
                    secondDestinationTable, "", "", 7L, 7L)
            // The id predicate is evaluated before lazy materialization, so v assembles only
            // the four selected rowids before ORDER BY permutes the block sent to the sink.
            assertNativeInsertSelect("${testCase.name}_filtered_permuted",
                    secondDestinationTable, filteredDestinationTable,
                    "AND id IN (1, 3, 5, 7)", "ORDER BY id DESC", 4L, 4L)
        }

        order_qt_native_shredded_insert_select_full """
            SELECT id,
                   CAST(v AS STRING),
                   CAST(v['a'] AS BIGINT),
                   CAST(v['b'] AS STRING),
                   CAST(v['mixed'] AS STRING),
                   CAST(v['path_null'] AS STRING),
                   v IS NULL
            FROM variant_native_shredded_writer_dest_full
            ORDER BY id
        """
        order_qt_native_shredded_insert_select_partial """
            SELECT id,
                   CAST(v AS STRING),
                   CAST(v['a'] AS BIGINT),
                   CAST(v['b'] AS STRING),
                   CAST(v['mixed'] AS STRING),
                   CAST(v['path_null'] AS STRING),
                   v IS NULL
            FROM variant_native_shredded_writer_dest_partial
            ORDER BY id
        """
        order_qt_native_shredded_insert_select_second_generation_full """
            SELECT id,
                   CAST(v AS STRING),
                   CAST(v['a'] AS BIGINT),
                   CAST(v['b'] AS STRING),
                   CAST(v['mixed'] AS STRING),
                   CAST(v['path_null'] AS STRING),
                   v IS NULL
            FROM variant_native_shredded_writer_dest2_full
            ORDER BY id
        """
        order_qt_native_shredded_insert_select_second_generation_partial """
            SELECT id,
                   CAST(v AS STRING),
                   CAST(v['a'] AS BIGINT),
                   CAST(v['b'] AS STRING),
                   CAST(v['mixed'] AS STRING),
                   CAST(v['path_null'] AS STRING),
                   v IS NULL
            FROM variant_native_shredded_writer_dest2_partial
            ORDER BY id
        """
        order_qt_native_shredded_insert_select_filtered_permuted_full """
            SELECT id, CAST(v AS STRING), v IS NULL
            FROM variant_native_shredded_writer_filtered_full
            ORDER BY id
        """
        order_qt_native_shredded_insert_select_filtered_permuted_partial """
            SELECT id, CAST(v AS STRING), v IS NULL
            FROM variant_native_shredded_writer_filtered_partial
            ORDER BY id
        """
    }
}
