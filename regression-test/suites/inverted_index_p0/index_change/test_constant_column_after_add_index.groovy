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

suite("test_constant_column_after_add_index") {
    def waitForBuildIndex = { String tableName ->
        for (int retry = 0; retry < 600; ++retry) {
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

    sql "SET enable_add_index_for_new_data = true"
    sql "DROP TABLE IF EXISTS test_constant_column_after_add_index"
    sql """
        CREATE TABLE test_constant_column_after_add_index (
            id INT NOT NULL,
            payload VARCHAR(32) NULL
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "light_schema_change" = "true",
            "disable_auto_compaction" = "true"
        )
    """

    // These segments have neither the added column nor its inverted index.
    sql """
        INSERT INTO test_constant_column_after_add_index VALUES
            (1, 'old-1'),
            (2, 'old-2')
    """

    sql """
        ALTER TABLE test_constant_column_after_add_index
        ADD COLUMN tag VARCHAR(32) NOT NULL DEFAULT 'legacy'
    """
    waitForSchemaChangeDone {
        sql """
            SHOW ALTER TABLE COLUMN
            WHERE TableName = 'test_constant_column_after_add_index'
            ORDER BY CreateTime DESC LIMIT 1
        """
        time 600
    }

    sql """
        ALTER TABLE test_constant_column_after_add_index
        ADD INDEX idx_tag(tag) USING INVERTED
    """
    waitForSchemaChangeDone {
        sql """
            SHOW ALTER TABLE COLUMN
            WHERE TableName = 'test_constant_column_after_add_index'
            ORDER BY CreateTime DESC LIMIT 1
        """
        time 600
    }

    // Keep physical-index and constant-reader segments in the same scan.
    sql """
        INSERT INTO test_constant_column_after_add_index VALUES
            (3, 'new-legacy', 'legacy'),
            (4, 'new-fresh', 'fresh'),
            (5, 'new-other', 'other')
    """

    if (!isCloudMode()) {
        build_index_on_table("idx_tag", "test_constant_column_after_add_index")
        assertTrue(waitForBuildIndex("test_constant_column_after_add_index"),
                "build index timed out for test_constant_column_after_add_index")
    }

    order_qt_index_matches_constant_and_physical """
        SELECT /*+SET_VAR(enable_fallback_on_missing_inverted_index=false) */ id, payload, tag
        FROM test_constant_column_after_add_index
        WHERE tag = 'legacy'
        ORDER BY id
    """

    order_qt_index_matches_only_physical """
        SELECT /*+SET_VAR(enable_fallback_on_missing_inverted_index=false) */ id, payload, tag
        FROM test_constant_column_after_add_index
        WHERE tag = 'fresh'
        ORDER BY id
    """

    order_qt_index_rejects_constant_segment """
        SELECT /*+SET_VAR(enable_fallback_on_missing_inverted_index=false) */ id, payload, tag
        FROM test_constant_column_after_add_index
        WHERE tag IN ('fresh', 'other')
        ORDER BY id
    """

    qt_count_with_index_and_constant_segments """
        SELECT /*+SET_VAR(enable_fallback_on_missing_inverted_index=false) */ COUNT(*)
        FROM test_constant_column_after_add_index
        WHERE tag = 'legacy'
    """

    sql "SET enable_add_index_for_new_data = false"
}
