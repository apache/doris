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

suite("test_constant_column_topn_lazy_rowid") {
    sql "DROP TABLE IF EXISTS test_constant_column_topn_lazy_rowid"
    sql """
        CREATE TABLE test_constant_column_topn_lazy_rowid (
            id INT NOT NULL,
            score INT NOT NULL
        )
        UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "light_schema_change" = "true",
            "disable_auto_compaction" = "true"
        )
    """

    // Case 1: prepare a TopN result containing old constant-backed rows and a new physical row.
    sql """
        INSERT INTO test_constant_column_topn_lazy_rowid VALUES
            (1, 100),
            (2, 80)
    """

    sql """
        ALTER TABLE test_constant_column_topn_lazy_rowid
        ADD COLUMN payload VARCHAR(32) NOT NULL DEFAULT 'old-default'
    """
    waitForSchemaChangeDone {
        sql """
            SHOW ALTER TABLE COLUMN
            WHERE TableName = 'test_constant_column_topn_lazy_rowid'
            ORDER BY CreateTime DESC LIMIT 1
        """
        time 600
    }

    sql """
        INSERT INTO test_constant_column_topn_lazy_rowid VALUES
            (3, 90, 'new-physical'),
            (4, 70, 'not-in-topn')
    """

    // Case 2: lazy rowid fetch must produce the same added-column and hidden VERSION values as a
    // normal scan when constant-backed and physical rows are mixed.
    sql "SET show_hidden_columns = true"
    sql "SET topn_lazy_materialization_threshold = -1"
    def normalRead = sql """
        SELECT id, payload, __DORIS_VERSION_COL__
        FROM test_constant_column_topn_lazy_rowid
        ORDER BY score DESC
        LIMIT 3
    """
    def normalHiddenOnlyRead = sql """
        SELECT __DORIS_VERSION_COL__
        FROM test_constant_column_topn_lazy_rowid
        ORDER BY score DESC
        LIMIT 3
    """

    sql "SET topn_lazy_materialization_threshold = 1024"
    explain {
        sql """
            SHAPE PLAN
            SELECT __DORIS_VERSION_COL__
            FROM test_constant_column_topn_lazy_rowid
            ORDER BY score DESC
            LIMIT 3
        """
        contains "PhysicalLazyMaterialize"
    }

    def lazyRead = sql """
        SELECT id, payload, __DORIS_VERSION_COL__
        FROM test_constant_column_topn_lazy_rowid
        ORDER BY score DESC
        LIMIT 3
    """
    assertEquals(normalRead, lazyRead)
    assertTrue(lazyRead.every { row -> (row[2] as Long) > 0 },
            "TopN rowid fetch returned a hidden version placeholder: ${lazyRead}")

    // Case 3: VERSION is the only projected value in this query. Seeing PhysicalLazyMaterialize above
    // therefore proves that the hidden column itself reaches the rowid-fetch phase.
    def lazyHiddenOnlyRead = sql """
        SELECT __DORIS_VERSION_COL__
        FROM test_constant_column_topn_lazy_rowid
        ORDER BY score DESC
        LIMIT 3
    """
    assertEquals(normalHiddenOnlyRead, lazyHiddenOnlyRead)
    assertTrue(lazyHiddenOnlyRead.every { row -> (row[0] as Long) > 0 },
            "TopN rowid fetch returned an invalid hidden version: ${lazyHiddenOnlyRead}")

    order_qt_topn_mixes_constant_and_physical_rows """
        SELECT id, payload
        FROM test_constant_column_topn_lazy_rowid
        ORDER BY score DESC
        LIMIT 3
    """

    sql "SET topn_lazy_materialization_threshold = 1024"
    sql "SET show_hidden_columns = false"
}
