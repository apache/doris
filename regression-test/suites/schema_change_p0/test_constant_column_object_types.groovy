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

suite("test_constant_column_object_types", "p0") {
    sql "DROP TABLE IF EXISTS test_constant_object_bitmap_dup FORCE"
    sql "DROP TABLE IF EXISTS test_constant_object_bitmap_unique FORCE"
    sql "DROP TABLE IF EXISTS test_constant_object_hll_agg FORCE"

    // BITMAP has a legal BITMAP_EMPTY identity default on DUPLICATE and UNIQUE MOW tables.
    // Start with a DUPLICATE table so old segments have no physical bitmap column at all.
    sql """
        CREATE TABLE test_constant_object_bitmap_dup (
            k INT NOT NULL,
            payload VARCHAR(32) NOT NULL
        )
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "light_schema_change" = "true",
            "disable_auto_compaction" = "true"
        )
    """
    sql """
        INSERT INTO test_constant_object_bitmap_dup VALUES
            (1, 'old-1'),
            (2, 'old-2')
    """
    sql "SYNC"

    sql """
        ALTER TABLE test_constant_object_bitmap_dup
        ADD COLUMN bm BITMAP NOT NULL DEFAULT BITMAP_EMPTY
    """
    waitForSchemaChangeDone({
        sql """
            SHOW ALTER TABLE COLUMN
            WHERE TableName = 'test_constant_object_bitmap_dup'
            ORDER BY CreateTime DESC LIMIT 1
        """
        time 600
    })

    order_qt_constant_object_bitmap_dup_after_add """
        SELECT k, payload, bitmap_count(bm)
        FROM test_constant_object_bitmap_dup
        ORDER BY k
    """

    sql """
        INSERT INTO test_constant_object_bitmap_dup VALUES
            (3, 'new-3', to_bitmap(30)),
            (4, 'new-4', bitmap_from_string('40,41'))
    """
    sql "INSERT INTO test_constant_object_bitmap_dup(k, payload) VALUES (5, 'new-default-5')"
    sql "SYNC"

    order_qt_constant_object_bitmap_dup_mixed """
        SELECT k, payload, bitmap_count(bm)
        FROM test_constant_object_bitmap_dup
        ORDER BY k
    """
    qt_constant_object_bitmap_dup_union """
        SELECT bitmap_count(bitmap_union(bm))
        FROM test_constant_object_bitmap_dup
    """

    // Run the same metadata-only ADD on UNIQUE MOW. The update of key 1 creates a physical
    // bitmap while key 2 continues to read BITMAP_EMPTY from its pre-ALTER segment.
    sql """
        CREATE TABLE test_constant_object_bitmap_unique (
            k INT NOT NULL,
            payload VARCHAR(32) NOT NULL
        )
        UNIQUE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "light_schema_change" = "true",
            "disable_auto_compaction" = "true"
        )
    """
    sql """
        INSERT INTO test_constant_object_bitmap_unique VALUES
            (1, 'old-1'),
            (2, 'old-2')
    """
    sql "SYNC"

    sql """
        ALTER TABLE test_constant_object_bitmap_unique
        ADD COLUMN bm BITMAP NOT NULL DEFAULT BITMAP_EMPTY
    """
    waitForSchemaChangeDone({
        sql """
            SHOW ALTER TABLE COLUMN
            WHERE TableName = 'test_constant_object_bitmap_unique'
            ORDER BY CreateTime DESC LIMIT 1
        """
        time 600
    })

    order_qt_constant_object_bitmap_unique_after_add """
        SELECT k, payload, bitmap_count(bm)
        FROM test_constant_object_bitmap_unique
        ORDER BY k
    """

    sql """
        INSERT INTO test_constant_object_bitmap_unique VALUES
            (3, 'new-3', to_bitmap(303))
    """
    sql """
        INSERT INTO test_constant_object_bitmap_unique VALUES
            (1, 'updated-1', bitmap_from_string('101,102'))
    """
    sql "SYNC"

    order_qt_constant_object_bitmap_unique_mixed """
        SELECT k, payload, bitmap_count(bm)
        FROM test_constant_object_bitmap_unique
        ORDER BY k
    """
    qt_constant_object_bitmap_unique_union """
        SELECT bitmap_count(bitmap_union(bm))
        FROM test_constant_object_bitmap_unique
    """

    // HLL does not accept an explicit SQL default. FE assigns its HLL_EMPTY identity default,
    // making a value-column ADD legal only on an AGGREGATE KEY table with HLL_UNION.
    sql """
        CREATE TABLE test_constant_object_hll_agg (
            k INT NOT NULL,
            metric BIGINT SUM DEFAULT '0'
        )
        AGGREGATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "light_schema_change" = "true",
            "disable_auto_compaction" = "true"
        )
    """
    sql """
        INSERT INTO test_constant_object_hll_agg VALUES
            (1, 10),
            (2, 20)
    """
    sql "SYNC"

    sql """
        ALTER TABLE test_constant_object_hll_agg
        ADD COLUMN hll_state HLL HLL_UNION
    """
    waitForSchemaChangeDone({
        sql """
            SHOW ALTER TABLE COLUMN
            WHERE TableName = 'test_constant_object_hll_agg'
            ORDER BY CreateTime DESC LIMIT 1
        """
        time 600
    })

    order_qt_constant_object_hll_after_add """
        SELECT k, metric, hll_cardinality(hll_state)
        FROM test_constant_object_hll_agg
        ORDER BY k
    """

    sql """
        INSERT INTO test_constant_object_hll_agg VALUES
            (3, 30, hll_hash('h3')),
            (4, 40, hll_hash('h4-a'))
    """
    sql "INSERT INTO test_constant_object_hll_agg VALUES (4, 2, hll_hash('h4-b'))"
    sql "INSERT INTO test_constant_object_hll_agg(k, metric) VALUES (5, 50)"
    sql "SYNC"

    order_qt_constant_object_hll_mixed """
        SELECT k, metric, hll_cardinality(hll_state)
        FROM test_constant_object_hll_agg
        ORDER BY k
    """
    qt_constant_object_hll_union """
        SELECT hll_cardinality(hll_union(hll_state))
        FROM test_constant_object_hll_agg
    """

    // QUANTILE_STATE has no FE-injected identity default corresponding to BITMAP_EMPTY/HLL_EMPTY,
    // and AGG_STATE has no function-independent identity value. They are intentionally excluded
    // instead of constructing a default that ALTER cannot reliably materialize for old segments.
}
