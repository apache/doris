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

suite("test_variant_sparse_sort_insert_from", "p0") {
    sql "set experimental_enable_nereids_planner=true"
    sql "set enable_fallback_to_original_planner=false"
    sql "set batch_size=4064"
    sql "set broker_load_batch_size=16352"
    sql "set enable_broadcast_join_force_passthrough=true"
    sql "set enable_distinct_streaming_aggregation=true"
    sql "set enable_distinct_streaming_agg_force_passthrough=false"
    sql "set enable_streaming_agg_hash_join_force_passthrough=false"
    sql "set experimental_parallel_scan_max_scanners_count=8"
    sql "set experimental_parallel_scan_min_rows_per_scanner=256"
    sql "set experimental_use_serial_exchange=true"
    sql "set enable_fold_constant_by_be=true"
    sql "set enable_force_spill=true"
    sql "set enable_reserve_memory=false"
    sql "set enable_runtime_filter_prune=false"
    sql "set enable_share_hash_table_for_broadcast_join=false"
    sql "set parallel_prepare_threshold=18"
    sql "set rewrite_or_to_in_predicate_threshold=100000"
    sql "set runtime_filter_type='BLOOM_FILTER,MIN_MAX'"
    sql "set short_circuit_evaluation=true"
    sql "set spill_min_revocable_mem=1048576"
    sql "set topn_opt_limit_threshold=1000"
    sql "set default_variant_enable_doc_mode=false"
    sql "set default_variant_doc_hash_shard_count=0"
    sql "set default_variant_max_subcolumns_count=1"
    sql "set default_variant_sparse_hash_shard_count=2"
    sql "set use_v3_storage_format=false"
    sql "set parallel_pipeline_task_num=2"
    sql "set enable_spill=true"

    sql "DROP TABLE IF EXISTS test_variant_sparse_sort_insert_from"
    sql """
        CREATE TABLE test_variant_sparse_sort_insert_from (
            k bigint,
            v variant
        )
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 6
        properties("replication_num" = "1", "disable_auto_compaction" = "false");
    """

    sql """
        INSERT INTO test_variant_sparse_sort_insert_from
        SELECT * FROM (
            SELECT 0, '{"a": 11245, "b": [123, {"xx": 1}], "c": {"c": 456, "d": null, "e": 7.111}}' AS json_str
            UNION ALL SELECT 1, '{"a": 1123}' AS json_str
            UNION ALL SELECT 2, '{"a": 1234, "xxxx": "kaana"}' AS json_str FROM numbers("number" = "4096")
        ) t ORDER BY 1 LIMIT 4096;
    """
    sql "sync"

    sql """
        SELECT v FROM test_variant_sparse_sort_insert_from
        WHERE json_extract_string(v, "\$") != "{}"
        ORDER BY cast(v AS string) LIMIT 10;
    """

    sql "TRUNCATE TABLE test_variant_sparse_sort_insert_from"
    sql """
        INSERT INTO test_variant_sparse_sort_insert_from
        SELECT * FROM (
            SELECT 0, '{"a": 1123, "b": [123, {"xx": 1}], "c": {"c": 456, "d": null, "e": 7.111}, "zzz": null, "oooo": {"akakaka": null, "xxxx": {"xxx": 123}}}' AS json_str
            UNION ALL SELECT 1, '{"a": 1234, "xxxx": "kaana", "ddd": {"aaa": 123, "mxmxm": [456, "789"]}}' AS json_str FROM numbers("number" = "4096")
        ) t ORDER BY 1 LIMIT 4096;
    """
    sql "sync"

    sql """
        SELECT v FROM test_variant_sparse_sort_insert_from
        WHERE json_extract_string(v, "\$") != "{}"
        ORDER BY cast(v AS string) LIMIT 10;
    """
}
