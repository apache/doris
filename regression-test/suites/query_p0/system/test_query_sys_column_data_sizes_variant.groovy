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

suite("test_query_sys_column_data_sizes_variant", "query,p0") {
    // A variant column keeps its data in the root, the materialized subcolumns and the sparse or
    // doc value columns. Compaction writes them as columns of their own, and external column meta
    // (V3) embeds the sparse and doc value columns into the root meta. All of them count to the
    // variant column.
    def checkVariantDataSizes = { String tableName ->
        for (int i = 0; i < 3; i++) {
            sql """
                INSERT INTO ${tableName}
                SELECT number, parse_to_variant(concat('{"a":', number, ',"b":"', repeat('x', number % 32),
                        '","c":{"d":', number * 2, '},"k', number % 16, '":"', repeat('y', number % 64), '"}'))
                FROM numbers("number" = "10000")
            """
        }
        def tabletId = sql("SHOW TABLETS FROM ${tableName}")[0][0]
        def check = {
            qt_sizes """
                SELECT COLUMN_NAME, COLUMN_TYPE, SUM(COMPRESSED_DATA_BYTES) > 0,
                       SUM(UNCOMPRESSED_DATA_BYTES) > 0, SUM(RAW_DATA_BYTES) > 0
                FROM information_schema.column_data_sizes
                WHERE TABLET_ID = ${tabletId}
                GROUP BY COLUMN_NAME, COLUMN_TYPE
                ORDER BY COLUMN_NAME
            """
            // The data pages of all columns lie in the segment files and make up most of them, so
            // a part of the variant that is not counted, or counted twice, breaks the bounds.
            def bytes = sql """
                SELECT SUM(c.data_page_bytes), SUM(r.DATA_DISK_SIZE)
                FROM (
                    SELECT BACKEND_ID, ROWSET_ID, SUM(COMPRESSED_DATA_BYTES) AS data_page_bytes
                    FROM information_schema.column_data_sizes
                    WHERE TABLET_ID = ${tabletId}
                    GROUP BY BACKEND_ID, ROWSET_ID
                ) c
                JOIN information_schema.rowsets r
                    ON r.BACKEND_ID = c.BACKEND_ID AND r.ROWSET_ID = c.ROWSET_ID
            """
            long dataPageBytes = bytes[0][0] as long
            long segmentBytes = bytes[0][1] as long
            logger.info("${tableName}: ${dataPageBytes} data page bytes in ${segmentBytes} segment bytes")
            assertTrue(dataPageBytes <= segmentBytes && dataPageBytes * 2 > segmentBytes,
                    "${tableName}: ${dataPageBytes} data page bytes in ${segmentBytes} segment bytes")
        }

        check()
        trigger_and_wait_compaction(tableName, "full")
        check()
    }

    // Materialized subcolumns and bucketized sparse columns, with the column metas in the footer
    // (V2) and in the external column meta region (V3).
    sql "set default_variant_enable_doc_mode = false"
    sql "DROP TABLE IF EXISTS test_column_data_sizes_variant_v2"
    sql """
        CREATE TABLE test_column_data_sizes_variant_v2 (
            id BIGINT,
            v VARIANT<PROPERTIES("variant_max_subcolumns_count" = "1", "variant_sparse_hash_shard_count" = "2")>
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1", "disable_auto_compaction" = "true", "storage_format" = "V2")
    """
    checkVariantDataSizes("test_column_data_sizes_variant_v2")

    sql "DROP TABLE IF EXISTS test_column_data_sizes_variant_v3"
    sql """
        CREATE TABLE test_column_data_sizes_variant_v3 (
            id BIGINT,
            v VARIANT<PROPERTIES("variant_max_subcolumns_count" = "1", "variant_sparse_hash_shard_count" = "2")>
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1", "disable_auto_compaction" = "true", "storage_format" = "V3")
    """
    checkVariantDataSizes("test_column_data_sizes_variant_v3")

    // Doc value columns and materialized subcolumns in doc mode.
    sql "DROP TABLE IF EXISTS test_column_data_sizes_variant_doc_v2"
    sql """
        CREATE TABLE test_column_data_sizes_variant_doc_v2 (
            id BIGINT,
            v VARIANT<PROPERTIES("variant_enable_doc_mode" = "true", "variant_doc_materialization_min_rows" = "0",
                    "variant_doc_hash_shard_count" = "2")>
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1", "disable_auto_compaction" = "true", "storage_format" = "V2")
    """
    checkVariantDataSizes("test_column_data_sizes_variant_doc_v2")

    sql "DROP TABLE IF EXISTS test_column_data_sizes_variant_doc_v3"
    sql """
        CREATE TABLE test_column_data_sizes_variant_doc_v3 (
            id BIGINT,
            v VARIANT<PROPERTIES("variant_enable_doc_mode" = "true", "variant_doc_materialization_min_rows" = "0",
                    "variant_doc_hash_shard_count" = "2")>
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1", "disable_auto_compaction" = "true", "storage_format" = "V3")
    """
    checkVariantDataSizes("test_column_data_sizes_variant_doc_v3")
}
