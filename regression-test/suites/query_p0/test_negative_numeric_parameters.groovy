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

suite("test_negative_numeric_parameters") {
    // No table or rows are needed. Each SET must fail before the invalid value reaches
    // an unsigned capacity, a buffer allocation, a batch loop, or a LIMIT expression.
    // The old code accepts the first SET and this test fails because no error is returned.
    [
        "set local_exchange_free_blocks_limit = -1",
        "set spill_streaming_agg_mem_limit = -1",
        "set max_scan_mem_ratio = -1",
        "set data_queue_max_blocks = -1",
        "set partition_topn_max_partitions = -1",
        "set partition_topn_pre_partition_rows = -1",
        "set cte_max_recursion_depth = -1",
        "set query_cache_entry_max_bytes = -1",
        "set query_cache_entry_max_rows = -1",
        "set runtime_filter_wait_time_ms = -1",
        "set max_initial_file_split_size = -1",
        "set max_file_split_size = -1",
        "set runtime_bloom_filter_size = -1",
        "set runtime_bloom_filter_min_size = -1",
        "set runtime_bloom_filter_max_size = -1",
        "set inverted_index_max_expansions = -1",
        "set parallel_scan_min_rows_per_scanner = -1",
        "set spill_min_revocable_mem = -1",
        "set spill_buffer_size_bytes = -1",
        "set spill_aggregation_partition_count = -1",
        "set low_memory_mode_buffer_limit = -1",
        "set spill_hash_join_partition_count = -1",
        "set spill_repartition_max_depth = -1",
        "set spill_join_build_sink_mem_limit_bytes = -1",
        "set spill_aggregation_sink_mem_limit_bytes = -1",
        "set spill_sort_sink_mem_limit_bytes = -1",
        "set spill_analytic_sink_mem_limit_bytes = -1",
        "set spill_sort_merge_mem_limit_bytes = -1",
        "set max_fetch_remote_schema_tablet_count = -1",
        "set fetch_remote_schema_timeout_seconds = -1",
        "set global huge_table_default_sample_rows = -1",
        "set hot_value_collect_count = -1",
        "set global audit_plugin_max_batch_bytes = -1",
        "set global audit_plugin_max_batch_interval_sec = -1",
        "set global audit_plugin_max_sql_length = -1",
        "set global audit_plugin_max_insert_stmt_length = -1",
        "set global partition_analyze_batch_size = -1",
        "set global huge_partition_lower_bound_rows = -1"
    ].each { statement ->
        test {
            sql statement
            exception "must be"
        }
    }

    // Zero cannot advance a file split or form a bloom filter. A ratio above one is
    // not a fraction of the query budget. NaN and huge exponents must also fail.
    ["set max_initial_file_split_size = 0", "set max_file_split_size = 0",
     "set runtime_bloom_filter_max_size = 0", "set max_scan_mem_ratio = 1.1",
     "set max_scan_mem_ratio = 'NaN'", "set max_scan_mem_ratio = '1e309'",
     "set global audit_plugin_load_timeout = -2"].each { statement ->
        test {
            sql statement
            exception "must be"
        }
    }

    // Keep valid boundary values: zero disables this free-block cap, while negative
    // scanner concurrency selects the existing automatic mode. One constant row is enough.
    try {
        sql "set local_exchange_free_blocks_limit = 0"
        sql "set runtime_bloom_filter_min_size = 0"
        sql "set max_scanners_concurrency = -1"
        sql "set file_cache_query_limit_bytes = -1"
        order_qt_valid_sentinels "select 1"
    } finally {
        sql "set local_exchange_free_blocks_limit = default"
        sql "set runtime_bloom_filter_min_size = default"
        sql "set max_scanners_concurrency = default"
        sql "set file_cache_query_limit_bytes = default"
    }
}
