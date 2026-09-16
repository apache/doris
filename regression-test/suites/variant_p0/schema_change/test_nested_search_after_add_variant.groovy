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

suite("test_nested_search_after_add_variant", "variant_type") {
    def variantV2Function = getFeConfig("enable_variant_v2").toBoolean() ? "parse_to_variant" : ""
    def aliceDocument = '{"name":"alice","profile":{"score":10},' +
            '"items":[{"name":"alice"},{"name":"bob"}]}'
    def bobDocument = '{"name":"bob","profile":{"score":20},' +
            '"items":[{"name":"carol"}]}'
    def waitForBuildIndex = { tableName ->
        for (int retry = 0; retry < 600; retry++) {
            def jobs = sql """
                SHOW BUILD INDEX
                WHERE TableName = '${tableName}'
                ORDER BY JobId DESC LIMIT 1
            """
            if (!jobs.isEmpty()) {
                assertNotEquals("CANCELLED", jobs[0][7], "build index job failed: ${jobs[0]}")
                if (jobs[0][7] == "FINISHED") {
                    return true
                }
            }
            sleep(1000)
        }
        return false
    }
    def buildIndex = { tableName, indexName ->
        if (isCloudMode()) {
            sql "BUILD INDEX ON ${tableName}"
        } else {
            sql "BUILD INDEX ${indexName} ON ${tableName}"
        }
        assertTrue(waitForBuildIndex(tableName), "build index timed out for ${tableName}")
    }

    sql "SET default_variant_enable_nested_group = false"
    sql "SET default_variant_enable_doc_mode = false"
    sql "SET default_variant_enable_typed_paths_to_sparse = false"
    sql "SET enable_add_index_for_new_data = true"
    sql "DROP TABLE IF EXISTS test_nested_search_after_add_variant"
    sql """
        CREATE TABLE test_nested_search_after_add_variant (
            id BIGINT
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "disable_auto_compaction" = "true",
            "light_schema_change" = "true"
        )
    """

    // Case 1: keep these rows in a segment whose physical schema predates the VARIANT root.
    sql "INSERT INTO test_nested_search_after_add_variant VALUES (1), (2)"
    sql """
        ALTER TABLE test_nested_search_after_add_variant
        ADD COLUMN v VARIANT NULL
    """
    waitForSchemaChangeDone {
        sql """
            SHOW ALTER TABLE COLUMN
            WHERE TableName = 'test_nested_search_after_add_variant'
            ORDER BY CreateTime DESC LIMIT 1
        """
        time 600
    }

    // Case 2: root, direct child, nested child, and an absent child must all be NULL for old rowsets.
    order_qt_old_variant_root_and_paths """
        SELECT id,
               v IS NULL,
               cast(v AS STRING),
               cast(v['name'] AS STRING),
               cast(v['profile']['score'] AS INT),
               cast(v['items'] AS STRING),
               cast(v['does_not_exist'] AS STRING)
        FROM test_nested_search_after_add_variant
        ORDER BY id
    """

    sql """
        ALTER TABLE test_nested_search_after_add_variant
        ADD INDEX idx_v(v) USING INVERTED PROPERTIES("parser" = "unicode")
    """
    waitForSchemaChangeDone {
        sql """
            SHOW ALTER TABLE COLUMN
            WHERE TableName = 'test_nested_search_after_add_variant'
            ORDER BY CreateTime DESC LIMIT 1
        """
        time 600
    }
    buildIndex("test_nested_search_after_add_variant", "idx_v")

    sql "SET enable_inverted_index_query = true"
    sql "SET enable_match_without_inverted_index = false"

    // Case 3: these rows use the post-ALTER physical schema and are scanned together with the two
    // constant-backed historical rows.
    sql """
        INSERT INTO test_nested_search_after_add_variant (id, v) VALUES
            (3, ${variantV2Function}('${aliceDocument}')),
            (4, ${variantV2Function}('${bobDocument}')),
            (5, NULL)
    """

    order_qt_mixed_variant_root_and_paths """
        SELECT id,
               v IS NULL,
               cast(v['name'] AS STRING),
               cast(v['profile']['score'] AS INT),
               cast(v['items'] AS STRING),
               cast(v['does_not_exist'] AS STRING)
        FROM test_nested_search_after_add_variant
        ORDER BY id
    """

    // Case 4: a predicate on a generated subcolumn must handle missing and physical VARIANT roots.
    order_qt_variant_subcolumn_predicate """
        SELECT id, cast(v['profile']['score'] AS INT)
        FROM test_nested_search_after_add_variant
        WHERE cast(v['profile']['score'] AS INT) >= 10
        ORDER BY id
    """

    // Case 5: the physical rowset can use idx_v while the historical rowset has neither the VARIANT root
    // nor an index iterator. The scan must combine both cases without falling back to MATCH.
    order_qt_variant_index_match """
        SELECT id, cast(v['name'] AS STRING)
        FROM test_nested_search_after_add_variant
        WHERE cast(v['name'] AS STRING) MATCH 'alice'
        ORDER BY id
    """

    // Case 6: reading the array path exercises the physical sparse/typed VARIANT path without requiring
    // the optional NestedGroup search provider.
    order_qt_nested_variant_physical_path """
        SELECT id, cast(v['items'] AS STRING)
        FROM test_nested_search_after_add_variant
        WHERE id IN (3, 4)
        ORDER BY id
    """

    sql "SET enable_match_without_inverted_index = true"
    sql "SET enable_add_index_for_new_data = false"
}
