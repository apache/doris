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

#include "runtime/query_options_validation.h"

#include <gtest/gtest.h>

#include <limits>

#include "common/status.h"
#include "gen_cpp/PaloInternalService_types.h"
#include "gen_cpp/QueryCache_types.h"

namespace doris {
TEST(QueryOptionsValidationTest, RejectNegativeCountsBeforeExecution) {
    // One request object is enough. No fragment, table, rows, or allocation is needed.
    TQueryOptions options;
    EXPECT_TRUE(validate_query_options(options).ok());
    options.__set_spill_streaming_agg_mem_limit(-1);
    EXPECT_FALSE(validate_query_options(options).ok()) << "spill_streaming_agg_mem_limit";
    options.__set_spill_streaming_agg_mem_limit(0);
    EXPECT_TRUE(validate_query_options(options).ok());
    options.__set_data_queue_max_blocks(-1);
    EXPECT_FALSE(validate_query_options(options).ok()) << "data_queue_max_blocks";
    options.__set_data_queue_max_blocks(0);
    EXPECT_TRUE(validate_query_options(options).ok());
    options.__set_partition_topn_max_partitions(-1);
    EXPECT_FALSE(validate_query_options(options).ok()) << "partition_topn_max_partitions";
    options.__set_partition_topn_max_partitions(0);
    EXPECT_TRUE(validate_query_options(options).ok());
    options.__set_partition_topn_pre_partition_rows(-1);
    EXPECT_FALSE(validate_query_options(options).ok()) << "partition_topn_pre_partition_rows";
    options.__set_partition_topn_pre_partition_rows(0);
    EXPECT_TRUE(validate_query_options(options).ok());
    options.__set_cte_max_recursion_depth(-1);
    EXPECT_FALSE(validate_query_options(options).ok()) << "cte_max_recursion_depth";
    options.__set_cte_max_recursion_depth(0);
    EXPECT_TRUE(validate_query_options(options).ok());
    options.__set_runtime_filter_wait_time_ms(-1);
    EXPECT_FALSE(validate_query_options(options).ok()) << "runtime_filter_wait_time_ms";
    options.__set_runtime_filter_wait_time_ms(0);
    EXPECT_TRUE(validate_query_options(options).ok());
    options.__set_scan_queue_mem_limit(-1);
    EXPECT_FALSE(validate_query_options(options).ok()) << "scan_queue_mem_limit";
    options.__set_scan_queue_mem_limit(0);
    EXPECT_TRUE(validate_query_options(options).ok());
    options.__set_min_revocable_mem(-1);
    EXPECT_FALSE(validate_query_options(options).ok()) << "min_revocable_mem";
    options.__set_min_revocable_mem(0);
    EXPECT_TRUE(validate_query_options(options).ok());
    options.__set_spill_buffer_size_bytes(-1);
    EXPECT_FALSE(validate_query_options(options).ok()) << "spill_buffer_size_bytes";
    options.__set_spill_buffer_size_bytes(0);
    EXPECT_TRUE(validate_query_options(options).ok());
    options.__set_spill_aggregation_partition_count(-1);
    EXPECT_FALSE(validate_query_options(options).ok()) << "spill_aggregation_partition_count";
    options.__set_spill_aggregation_partition_count(0);
    EXPECT_TRUE(validate_query_options(options).ok());
    options.__set_low_memory_mode_buffer_limit(-1);
    EXPECT_FALSE(validate_query_options(options).ok()) << "low_memory_mode_buffer_limit";
    options.__set_low_memory_mode_buffer_limit(0);
    EXPECT_TRUE(validate_query_options(options).ok());
    options.__set_spill_hash_join_partition_count(-1);
    EXPECT_FALSE(validate_query_options(options).ok()) << "spill_hash_join_partition_count";
    options.__set_spill_hash_join_partition_count(0);
    EXPECT_TRUE(validate_query_options(options).ok());
    options.__set_spill_repartition_max_depth(-1);
    EXPECT_FALSE(validate_query_options(options).ok()) << "spill_repartition_max_depth";
    options.__set_spill_repartition_max_depth(0);
    EXPECT_TRUE(validate_query_options(options).ok());
    options.__set_spill_join_build_sink_mem_limit_bytes(-1);
    EXPECT_FALSE(validate_query_options(options).ok()) << "spill_join_build_sink_mem_limit_bytes";
    options.__set_spill_join_build_sink_mem_limit_bytes(0);
    EXPECT_TRUE(validate_query_options(options).ok());
    options.__set_spill_aggregation_sink_mem_limit_bytes(-1);
    EXPECT_FALSE(validate_query_options(options).ok()) << "spill_aggregation_sink_mem_limit_bytes";
    options.__set_spill_aggregation_sink_mem_limit_bytes(0);
    EXPECT_TRUE(validate_query_options(options).ok());
    options.__set_spill_sort_sink_mem_limit_bytes(-1);
    EXPECT_FALSE(validate_query_options(options).ok()) << "spill_sort_sink_mem_limit_bytes";
    options.__set_spill_sort_sink_mem_limit_bytes(0);
    EXPECT_TRUE(validate_query_options(options).ok());
    options.__set_spill_analytic_sink_mem_limit_bytes(-1);
    EXPECT_FALSE(validate_query_options(options).ok()) << "spill_analytic_sink_mem_limit_bytes";
    options.__set_spill_analytic_sink_mem_limit_bytes(0);
    EXPECT_TRUE(validate_query_options(options).ok());
    options.__set_spill_sort_merge_mem_limit_bytes(-1);
    EXPECT_FALSE(validate_query_options(options).ok()) << "spill_sort_merge_mem_limit_bytes";
    options.__set_spill_sort_merge_mem_limit_bytes(0);
    EXPECT_TRUE(validate_query_options(options).ok());
    options.__set_inverted_index_max_expansions(-1);
    EXPECT_FALSE(validate_query_options(options).ok()) << "inverted_index_max_expansions";
    options.__set_inverted_index_max_expansions(0);
    EXPECT_TRUE(validate_query_options(options).ok());
    options.__set_parallel_scan_min_rows_per_scanner(-1);
    EXPECT_FALSE(validate_query_options(options).ok()) << "parallel_scan_min_rows_per_scanner";
    options.__set_parallel_scan_min_rows_per_scanner(0);
    EXPECT_TRUE(validate_query_options(options).ok());
}

TEST(QueryOptionsValidationTest, LocalExchangeAndScanRatioRanges) {
    // A single request covers narrowing and ratio errors without allocating the requested size.
    TQueryOptions options;
    for (int64_t value : {-1LL, 2147483648LL}) {
        options.__set_local_exchange_free_blocks_limit(value);
        EXPECT_FALSE(validate_query_options(options).ok());
    }
    options.__set_local_exchange_free_blocks_limit(std::numeric_limits<int>::max());
    EXPECT_TRUE(validate_query_options(options).ok());
    for (double value : {-0.1, 1.1, std::numeric_limits<double>::infinity(),
                         std::numeric_limits<double>::quiet_NaN()}) {
        options.__set_max_scan_mem_ratio(value);
        EXPECT_FALSE(validate_query_options(options).ok());
    }
    options.__set_max_scan_mem_ratio(0.0);
    EXPECT_TRUE(validate_query_options(options).ok());
    options.__set_max_scan_mem_ratio(1.0);
    EXPECT_TRUE(validate_query_options(options).ok());
}

TEST(QueryOptionsValidationTest, CacheAndBloomSentinels) {
    // Preserve the wire defaults and zero Bloom minimum while rejecting adjacent invalid values.
    TQueryOptions options;
    for (int value : {-2, 0, 101}) {
        options.__set_file_cache_query_limit_percent(value);
        EXPECT_FALSE(validate_query_options(options).ok());
    }
    options.__set_file_cache_query_limit_percent(-1);
    EXPECT_TRUE(validate_query_options(options).ok());
    options.__set_file_cache_query_limit_percent(1);
    EXPECT_TRUE(validate_query_options(options).ok());
    options.__set_file_cache_query_limit_percent(100);
    // These negative values keep their documented default or unlimited meanings.
    options.__set_mem_limit(-1);
    options.__set_file_cache_query_limit_bytes(-1);
    options.__set_max_scanners_concurrency(-1);
    EXPECT_TRUE(validate_query_options(options).ok());
    options.__set_runtime_bloom_filter_min_size(-1);
    EXPECT_FALSE(validate_query_options(options).ok());
    options.__set_runtime_bloom_filter_min_size(0);
    EXPECT_TRUE(validate_query_options(options).ok());
    options.__set_runtime_bloom_filter_max_size(-1);
    EXPECT_FALSE(validate_query_options(options).ok());
    options.__set_runtime_bloom_filter_max_size(0);
    EXPECT_FALSE(validate_query_options(options).ok());
    options.__set_runtime_bloom_filter_max_size(1);
    EXPECT_TRUE(validate_query_options(options).ok());
}

TEST(QueryOptionsValidationTest, QueryCacheEntryLimits) {
    TQueryCacheParam parameters;
    EXPECT_TRUE(validate_query_cache_parameters(parameters).ok());
    parameters.__set_entry_max_bytes(-1);
    EXPECT_FALSE(validate_query_cache_parameters(parameters).ok());
    parameters.__set_entry_max_bytes(0);
    parameters.__set_entry_max_rows(-1);
    EXPECT_FALSE(validate_query_cache_parameters(parameters).ok());
    parameters.__set_entry_max_rows(0);
    EXPECT_TRUE(validate_query_cache_parameters(parameters).ok());
}
} // namespace doris
