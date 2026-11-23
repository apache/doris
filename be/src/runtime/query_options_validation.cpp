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

#include <cmath>
#include <cstdint>
#include <limits>
#include <string_view>

#include "common/status.h"
#include "gen_cpp/PaloInternalService_types.h"
#include "gen_cpp/QueryCache_types.h"

namespace doris {
namespace {
Status validate_nonnegative(bool is_set, int64_t value, std::string_view name) {
    if (is_set && value < 0) {
        return Status::InvalidArgument("{} must be >= 0, but was {}", name, value);
    }
    return Status::OK();
}

Status validate_nonnegative_spill_options(const TQueryOptions& options) {
    RETURN_IF_ERROR(validate_nonnegative(options.__isset.spill_streaming_agg_mem_limit,
                                         options.spill_streaming_agg_mem_limit,
                                         "spill_streaming_agg_mem_limit"));
    RETURN_IF_ERROR(validate_nonnegative(options.__isset.min_revocable_mem,
                                         options.min_revocable_mem, "min_revocable_mem"));
    RETURN_IF_ERROR(validate_nonnegative(options.__isset.spill_buffer_size_bytes,
                                         options.spill_buffer_size_bytes,
                                         "spill_buffer_size_bytes"));
    RETURN_IF_ERROR(validate_nonnegative(options.__isset.spill_aggregation_partition_count,
                                         options.spill_aggregation_partition_count,
                                         "spill_aggregation_partition_count"));
    RETURN_IF_ERROR(validate_nonnegative(options.__isset.low_memory_mode_buffer_limit,
                                         options.low_memory_mode_buffer_limit,
                                         "low_memory_mode_buffer_limit"));
    RETURN_IF_ERROR(validate_nonnegative(options.__isset.spill_hash_join_partition_count,
                                         options.spill_hash_join_partition_count,
                                         "spill_hash_join_partition_count"));
    RETURN_IF_ERROR(validate_nonnegative(options.__isset.spill_repartition_max_depth,
                                         options.spill_repartition_max_depth,
                                         "spill_repartition_max_depth"));
    RETURN_IF_ERROR(validate_nonnegative(options.__isset.spill_join_build_sink_mem_limit_bytes,
                                         options.spill_join_build_sink_mem_limit_bytes,
                                         "spill_join_build_sink_mem_limit_bytes"));
    RETURN_IF_ERROR(validate_nonnegative(options.__isset.spill_aggregation_sink_mem_limit_bytes,
                                         options.spill_aggregation_sink_mem_limit_bytes,
                                         "spill_aggregation_sink_mem_limit_bytes"));
    RETURN_IF_ERROR(validate_nonnegative(options.__isset.spill_sort_sink_mem_limit_bytes,
                                         options.spill_sort_sink_mem_limit_bytes,
                                         "spill_sort_sink_mem_limit_bytes"));
    RETURN_IF_ERROR(validate_nonnegative(options.__isset.spill_analytic_sink_mem_limit_bytes,
                                         options.spill_analytic_sink_mem_limit_bytes,
                                         "spill_analytic_sink_mem_limit_bytes"));
    RETURN_IF_ERROR(validate_nonnegative(options.__isset.spill_sort_merge_mem_limit_bytes,
                                         options.spill_sort_merge_mem_limit_bytes,
                                         "spill_sort_merge_mem_limit_bytes"));
    return Status::OK();
}

Status validate_nonnegative_query_options(const TQueryOptions& options) {
    // Unset optional fields retain their existing defaults. Zero keeps its existing meaning.
    RETURN_IF_ERROR(validate_nonnegative(options.__isset.data_queue_max_blocks,
                                         options.data_queue_max_blocks, "data_queue_max_blocks"));
    RETURN_IF_ERROR(validate_nonnegative(options.__isset.partition_topn_max_partitions,
                                         options.partition_topn_max_partitions,
                                         "partition_topn_max_partitions"));
    RETURN_IF_ERROR(validate_nonnegative(options.__isset.partition_topn_pre_partition_rows,
                                         options.partition_topn_pre_partition_rows,
                                         "partition_topn_pre_partition_rows"));
    RETURN_IF_ERROR(validate_nonnegative(options.__isset.cte_max_recursion_depth,
                                         options.cte_max_recursion_depth,
                                         "cte_max_recursion_depth"));
    RETURN_IF_ERROR(validate_nonnegative(options.__isset.runtime_filter_wait_time_ms,
                                         options.runtime_filter_wait_time_ms,
                                         "runtime_filter_wait_time_ms"));
    RETURN_IF_ERROR(validate_nonnegative(options.__isset.scan_queue_mem_limit,
                                         options.scan_queue_mem_limit, "scan_queue_mem_limit"));
    RETURN_IF_ERROR(validate_nonnegative(options.__isset.inverted_index_max_expansions,
                                         options.inverted_index_max_expansions,
                                         "inverted_index_max_expansions"));
    RETURN_IF_ERROR(validate_nonnegative(options.__isset.parallel_scan_min_rows_per_scanner,
                                         options.parallel_scan_min_rows_per_scanner,
                                         "parallel_scan_min_rows_per_scanner"));
    return validate_nonnegative_spill_options(options);
}
} // namespace

Status validate_query_options(const TQueryOptions& options) {
    RETURN_IF_ERROR(validate_nonnegative_query_options(options));
    // An explicit 0 or -1 creates an immediate or negative execution RPC deadline.
    // An absent field retains the existing compatibility default.
    if (options.__isset.execution_timeout && options.execution_timeout <= 0) {
        return Status::InvalidArgument("execution_timeout must be > 0, but was {}",
                                       options.execution_timeout);
    }
    // RuntimeState and QueryContext use query_timeout when execution_timeout is absent.
    // For example, -1 would become a negative deadline of -1000 milliseconds.
    if (options.__isset.query_timeout && options.query_timeout <= 0) {
        return Status::InvalidArgument("query_timeout must be > 0, but was {}",
                                       options.query_timeout);
    }
    // The wire field is i64, but the exchanger stores this count as int.
    if (options.__isset.local_exchange_free_blocks_limit &&
        (options.local_exchange_free_blocks_limit < 0 ||
         options.local_exchange_free_blocks_limit > std::numeric_limits<int>::max())) {
        return Status::InvalidArgument("local_exchange_free_blocks_limit must be between 0 and {}",
                                       std::numeric_limits<int>::max());
    }
    if (options.__isset.max_scan_mem_ratio &&
        (!std::isfinite(options.max_scan_mem_ratio) || options.max_scan_mem_ratio < 0 ||
         options.max_scan_mem_ratio > 1)) {
        return Status::InvalidArgument("max_scan_mem_ratio must be between 0 and 1");
    }
    // -1 is the wire default. Keep it for requests that do not provide a percentage.
    if (options.__isset.file_cache_query_limit_percent &&
        (options.file_cache_query_limit_percent < -1 ||
         options.file_cache_query_limit_percent == 0 ||
         options.file_cache_query_limit_percent > 100)) {
        return Status::InvalidArgument(
                "file_cache_query_limit_percent must be -1 or between 1 and 100");
    }
    if ((options.__isset.runtime_bloom_filter_min_size &&
         options.runtime_bloom_filter_min_size < 0) ||
        (options.__isset.runtime_bloom_filter_max_size &&
         options.runtime_bloom_filter_max_size <= 0)) {
        return Status::InvalidArgument(
                "runtime bloom filter minimum must be >= 0 and maximum must be > 0");
    }
    return Status::OK();
}

Status validate_query_cache_parameters(const TQueryCacheParam& parameters) {
    // A negative limit rejects even an empty result. Zero remains an explicit zero-capacity limit.
    if ((parameters.__isset.entry_max_bytes && parameters.entry_max_bytes < 0) ||
        (parameters.__isset.entry_max_rows && parameters.entry_max_rows < 0)) {
        return Status::InvalidArgument("query cache entry limits must be >= 0");
    }
    return Status::OK();
}
} // namespace doris
