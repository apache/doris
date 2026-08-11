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

suite("test_row_binlog_object_types") {
    sql "DROP TABLE IF EXISTS test_row_binlog_object_types FORCE"

    sql """
        CREATE TABLE test_row_binlog_object_types (
            id BIGINT NOT NULL,
            bitmap_value BITMAP NOT NULL,
            hll_value HLL NOT NULL
        )
        UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "binlog.need_historical_value" = "true"
        )
    """

    sql """
        INSERT INTO test_row_binlog_object_types
        VALUES (1, BITMAP_FROM_STRING('1,2'), HLL_HASH('one'))
    """

    qt_object_types_after_insert """
        SELECT id, BITMAP_COUNT(bitmap_value), HLL_CARDINALITY(hll_value)
        FROM test_row_binlog_object_types
        ORDER BY id
    """

    sql """
        INSERT INTO test_row_binlog_object_types
        SELECT 1, BITMAP_FROM_STRING('3,4,5'), HLL_UNION(HLL_HASH(number))
        FROM numbers("number" = "3")
    """

    qt_object_types_after_update """
        SELECT id, BITMAP_COUNT(bitmap_value), HLL_CARDINALITY(hll_value)
        FROM test_row_binlog_object_types
        ORDER BY id
    """

    sql "DELETE FROM test_row_binlog_object_types WHERE id = 1"

    qt_object_types_after_delete """
        SELECT COUNT(*)
        FROM test_row_binlog_object_types
    """

    qt_object_types_row_binlog """
        SELECT __DORIS_BINLOG_OP__ AS op,
               id,
               BITMAP_COUNT(bitmap_value) AS after_bitmap_count,
               HLL_CARDINALITY(hll_value) AS after_hll_cardinality,
               BITMAP_COUNT(__BEFORE__bitmap_value__) AS before_bitmap_count,
               HLL_CARDINALITY(__BEFORE__hll_value__) AS before_hll_cardinality
        FROM binlog("table" = "test_row_binlog_object_types")
        ORDER BY __DORIS_BINLOG_TSO__, __DORIS_BINLOG_LSN__
    """
}
