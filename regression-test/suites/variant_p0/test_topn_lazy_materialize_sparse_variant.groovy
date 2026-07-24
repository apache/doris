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

suite("test_topn_lazy_materialize_sparse_variant", "p0") {
    sql "set default_variant_enable_doc_mode = false"
    sql "set default_variant_max_subcolumns_count = 1"
    sql "set default_variant_sparse_hash_shard_count = 2"
    sql "set use_v3_storage_format = true"
    sql "set enable_file_cache = true"
    sql "set enable_segment_limit_pushdown = false"
    sql "set topn_lazy_materialization_threshold = 1024"

    sql "DROP TABLE IF EXISTS test_topn_lazy_materialize_sparse_variant"
    sql """
        CREATE TABLE test_topn_lazy_materialize_sparse_variant (
            pk INT,
            col_bigint BIGINT NULL,
            col_json JSON NOT NULL,
            col_variant VARIANT NULL
        ) ENGINE = OLAP
        DUPLICATE KEY(pk)
        PARTITION BY RANGE(pk) (
            PARTITION p0 VALUES LESS THAN ('4'),
            PARTITION p1 VALUES LESS THAN ('64'),
            PARTITION p2 VALUES LESS THAN ('256'),
            PARTITION pmax VALUES LESS THAN ('2147483647')
        )
        DISTRIBUTED BY HASH(pk) BUCKETS 4
        PROPERTIES (
            "replication_num" = "1",
            "disable_auto_compaction" = "true",
            "storage_format" = "V3"
        )
    """

    sql """
        INSERT INTO test_topn_lazy_materialize_sparse_variant VALUES
            (19, -9223372036854775808, '{"word":"hot","n":7}', '[1,2,3]'),
            (18, -9223372036854775808, '{"word":"hot","n":7}', '{"word":"hot","n":7}'),
            (17, -1, '{"k":1}', '{}'),
            (16, -906638365906932436, '{"word":"hot","n":7}', '{"k":1}'),
            (14, -9223372036854775808, '{"word":"hot","n":7}', '[1,2,3]'),
            (13, 5777142737451291289, '{}', '{"word":"hot","n":7}'),
            (12, 1, '{"word":"hot","n":7}', NULL),
            (11, 42, '[1,2,3]', NULL),
            (10, 1, '{}', '{"word":"hot","n":7}'),
            (8, NULL, '{"k":1}', '{"k":1}'),
            (7, 0, '[1,2,3]', '[1,2,3]'),
            (6, 1, '[1,2,3]', '{}'),
            (5, -3610716764638269764, '{"k":1}', '[1,2,3]'),
            (4, -9223372036854775808, '[1,2,3]', '{"k":1}'),
            (3, 42, '{"k":1}', '{}'),
            (2, -9223372036854775808, '{}', '{}'),
            (1, -1, '{}', '[1,2,3]'),
            (0, 1, '{}', '{}')
    """

    def topnQuery = """
        SELECT pk, col_bigint, col_json, col_variant
        FROM test_topn_lazy_materialize_sparse_variant
        WHERE ABS(pk % 3) IN (0, 1, 3)
        ORDER BY pk ASC
        LIMIT 128
    """
    explain {
        sql topnQuery
        contains("MaterializeNode")
    }
    qt_topn_lazy_sparse_variant topnQuery
}
