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

suite("test_variant_typed_path_bloom_filter", "nonConcurrent") {
    sql """DROP TABLE IF EXISTS test_variant_typed_path_bloom_filter"""
    sql "set default_variant_enable_doc_mode = false"
    sql "set enable_common_expr_pushdown = true"
    sql """
        CREATE TABLE test_variant_typed_path_bloom_filter (
            k bigint,
            v variant<'int_1' : int, properties(
                "variant_max_subcolumns_count" = "2",
                "variant_enable_typed_paths_to_sparse" = "false"
            )>
        )
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        properties(
            "replication_num" = "1",
            "disable_auto_compaction" = "true",
            "bloom_filter_columns" = "v"
        );
    """
    sql """
        INSERT INTO test_variant_typed_path_bloom_filter VALUES
            (1, '{"int_1": 1}'),
            (2, '{"int_1": 2}'),
            (3, '{"int_1": 100}'),
            (4, '{"int_1": 101}');
    """
    sql """sync"""

    def matched = sql """
        select k, cast(v['int_1'] as int) from test_variant_typed_path_bloom_filter
        where cast(v['int_1'] as int) in (1, 101)
        order by k
    """
    assertEquals("[[1, 1], [4, 101]]", matched.toString())

    try {
        GetDebugPoint().enableDebugPointForAllBEs("bloom_filter_must_filter_data")
        def missing = sql """
            select k from test_variant_typed_path_bloom_filter
            where cast(v['int_1'] as int) = 999
        """
        assertEquals(0, missing.size())
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("bloom_filter_must_filter_data")
    }
}
