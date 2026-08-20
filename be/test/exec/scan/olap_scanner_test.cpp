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

#include "exec/scan/olap_scanner.h"

#include <gtest/gtest.h>

#include <limits>
#include <string>

#include "cloud/config.h"
#include "common/config.h"
#include "io/io_common.h"
#include "runtime/runtime_profile.h"
#include "testutil/mock/mock_runtime_state.h"

namespace doris {
namespace {

class CloudFileCacheConfigGuard {
public:
    CloudFileCacheConfigGuard()
            : _cloud_unique_id(config::cloud_unique_id),
              _enable_file_cache(config::enable_file_cache) {}

    ~CloudFileCacheConfigGuard() {
        config::cloud_unique_id = _cloud_unique_id;
        config::enable_file_cache = _enable_file_cache;
    }

private:
    std::string _cloud_unique_id;
    bool _enable_file_cache;
};

TUniqueId make_query_id() {
    TUniqueId query_id;
    query_id.hi = 100;
    query_id.lo = 200;
    return query_id;
}

} // namespace

TEST(OlapScannerTest, BuildScoreRuntimeCollectionIoContextPropagatesQueryLimiter) {
    CloudFileCacheConfigGuard config_guard;
    config::cloud_unique_id = "olap_scanner_score_runtime_io_context_ut";
    config::enable_file_cache = true;

    TQueryOptions query_options;
    query_options.__set_query_type(TQueryType::SELECT);
    query_options.__set_file_cache_query_limit_bytes(1024);

    auto query_id = make_query_id();
    TNetworkAddress fe_addr;
    fe_addr.hostname = "127.0.0.1";
    fe_addr.port = 9030;
    auto query_ctx = MockQueryContext::create(query_id, ExecEnv::GetInstance(), query_options,
                                              fe_addr, true, fe_addr);
    ASSERT_NE(query_ctx->remote_scan_cache_write_limiter(), nullptr);

    MockRuntimeState state;
    state._query_id = query_id;
    state._query_ctx_uptr = query_ctx;
    state._query_ctx = query_ctx.get();

    io::FileCacheStatistics stats;
    auto io_ctx = build_score_runtime_collection_io_context(&state, ReaderType::READER_QUERY, 3600,
                                                            &stats);

    EXPECT_EQ(io_ctx.reader_type, ReaderType::READER_QUERY);
    EXPECT_EQ(io_ctx.expiration_time, 3600);
    EXPECT_EQ(io_ctx.query_id, &state.query_id());
    EXPECT_EQ(io_ctx.file_cache_stats, &stats);
    EXPECT_TRUE(io_ctx.is_inverted_index);
    EXPECT_EQ(io_ctx.remote_scan_cache_write_limiter, query_ctx->remote_scan_cache_write_limiter());
}

TEST(OlapScannerTest, BuildScoreRuntimeCollectionIoContextAllowsMissingQueryContext) {
    auto query_id = make_query_id();
    MockRuntimeState state;
    state._query_id = query_id;
    state._query_ctx = nullptr;

    io::FileCacheStatistics stats;
    auto io_ctx = build_score_runtime_collection_io_context(&state, ReaderType::READER_QUERY, 3600,
                                                            &stats);

    EXPECT_EQ(io_ctx.query_id, &state.query_id());
    EXPECT_EQ(io_ctx.file_cache_stats, &stats);
    EXPECT_TRUE(io_ctx.is_inverted_index);
    EXPECT_EQ(io_ctx.remote_scan_cache_write_limiter, nullptr);
}

TEST(OlapScannerTest, CandidateKeyBudgetRejectsBeforeInsertion) {
    OlapScanner::CandidateKeyMap candidate_keys;
    size_t candidate_bytes = 0;
    OlapTuple key;
    key.add_field(Field::create_field<TYPE_STRING>(String("candidate-key")));
    auto encoded_key = OlapScanner::_encode_candidate_key(key);

    EXPECT_EQ(OlapScanner::CandidateKeyInsertResult::KEY_BYTES_LIMIT,
              OlapScanner::_try_add_seq_map_candidate_key(std::move(encoded_key), std::move(key), 1,
                                                          0, std::numeric_limits<size_t>::max(),
                                                          &candidate_keys, &candidate_bytes));
    EXPECT_TRUE(candidate_keys.empty());
    EXPECT_EQ(0, candidate_bytes);
}

TEST(OlapScannerTest, DuplicateCandidateKeyIsNotChargedTwice) {
    auto make_key = [] {
        OlapTuple key;
        key.add_field(Field::create_field<TYPE_STRING>(String("candidate-key")));
        return key;
    };

    OlapScanner::CandidateKeyMap candidate_keys;
    size_t candidate_bytes = 0;
    auto first_key = make_key();
    ASSERT_EQ(OlapScanner::CandidateKeyInsertResult::OK,
              OlapScanner::_try_add_seq_map_candidate_key(
                      OlapScanner::_encode_candidate_key(first_key), std::move(first_key), 1,
                      std::numeric_limits<size_t>::max(), std::numeric_limits<size_t>::max(),
                      &candidate_keys, &candidate_bytes));
    const size_t first_key_bytes = candidate_bytes;

    auto duplicate_key = make_key();
    EXPECT_EQ(OlapScanner::CandidateKeyInsertResult::OK,
              OlapScanner::_try_add_seq_map_candidate_key(
                      OlapScanner::_encode_candidate_key(duplicate_key), std::move(duplicate_key),
                      1, std::numeric_limits<size_t>::max(), 0, &candidate_keys, &candidate_bytes));
    EXPECT_EQ(1, candidate_keys.size());
    EXPECT_EQ(first_key_bytes, candidate_bytes);
}

TEST(OlapScannerTest, CandidateKeyBudgetPreservesReservationHeadroom) {
    OlapScanner::CandidateKeyMap candidate_keys;
    size_t candidate_bytes = 0;
    OlapTuple key;
    key.add_field(Field::create_field<TYPE_STRING>(String("candidate-key")));
    auto encoded_key = OlapScanner::_encode_candidate_key(key);

    EXPECT_EQ(OlapScanner::CandidateKeyInsertResult::RESERVATION_LIMIT,
              OlapScanner::_try_add_seq_map_candidate_key(std::move(encoded_key), std::move(key), 1,
                                                          std::numeric_limits<size_t>::max(), 0,
                                                          &candidate_keys, &candidate_bytes));
    EXPECT_TRUE(candidate_keys.empty());
    EXPECT_EQ(0, candidate_bytes);
}

TEST(OlapScannerTest, CandidateMemoryBudgetSeparatesKeysAndWorkspace) {
    constexpr size_t MiB = 1024 * 1024;

    auto max_budget = OlapScanner::_split_candidate_memory_budget(40 * MiB);
    EXPECT_EQ(40 * MiB, max_budget.reservation_bytes);
    EXPECT_EQ(32 * MiB, max_budget.key_bytes);
    EXPECT_EQ(8 * MiB, max_budget.workspace_bytes);

    auto constrained_budget = OlapScanner::_split_candidate_memory_budget(10 * MiB);
    EXPECT_EQ(10 * MiB, constrained_budget.reservation_bytes);
    EXPECT_EQ(8 * MiB, constrained_budget.key_bytes);
    EXPECT_EQ(2 * MiB, constrained_budget.workspace_bytes);

    auto insufficient_budget = OlapScanner::_split_candidate_memory_budget(MiB);
    EXPECT_EQ(0, insufficient_budget.reservation_bytes);
    EXPECT_EQ(0, insufficient_budget.key_bytes);
    EXPECT_EQ(0, insufficient_budget.workspace_bytes);
}

TEST(OlapScannerTest, CandidateCostPricesEverySegmentLookup) {
    OlapScanner::CandidateScanCostLimit cost_limit;
    OlapScanner::_add_seq_map_candidate_cost(600, 3, &cost_limit);
    OlapScanner::_add_seq_map_candidate_cost(400, 0, &cost_limit);
    cost_limit.enabled = true;

    EXPECT_EQ(1000, cost_limit.full_scan_rows);
    EXPECT_EQ(60, cost_limit.point_probe_cost_per_key);
    EXPECT_FALSE(cost_limit.exceeded(100, 0, 14));
    EXPECT_TRUE(cost_limit.exceeded(100, 0, 15));
}

TEST(OlapScannerTest, CandidateCostPricesSmallTablePointProbes) {
    OlapScanner::CandidateScanCostLimit cost_limit;
    OlapScanner::_add_seq_map_candidate_cost(4000, 100, &cost_limit);
    cost_limit.enabled = true;

    EXPECT_EQ(4000, cost_limit.full_scan_rows);
    EXPECT_EQ(2400, cost_limit.point_probe_cost_per_key);
    EXPECT_FALSE(cost_limit.exceeded(0, 0, 1));
    EXPECT_TRUE(cost_limit.exceeded(0, 0, 2));
}

TEST(OlapScannerTest, CandidateCostIncludesPreviousGroups) {
    OlapScanner::CandidateScanCostLimit cost_limit {
            .full_scan_rows = 1000,
            .point_probe_cost_per_key = 2,
            .enabled = true,
    };

    EXPECT_FALSE(cost_limit.exceeded(400, 599, 0));
    EXPECT_TRUE(cost_limit.exceeded(400, 600, 0));
}

TEST(OlapScannerTest, CandidateCostHonorsDisabledAndExhaustedBoundaries) {
    OlapScanner::CandidateScanCostLimit cost_limit {
            .full_scan_rows = 1000,
            .point_probe_cost_per_key = 2,
            .enabled = false,
    };

    EXPECT_FALSE(cost_limit.exceeded(1000, 0, 1000));
    cost_limit.enabled = true;
    EXPECT_TRUE(cost_limit.exceeded(1000, 0, 0));
    EXPECT_TRUE(cost_limit.exceeded(400, 600, 0));
}

TEST(OlapScannerTest, CandidateCostSaturatesOnOverflow) {
    OlapScanner::CandidateScanCostLimit cost_limit;
    OlapScanner::_add_seq_map_candidate_cost(std::numeric_limits<uint64_t>::max(),
                                             std::numeric_limits<size_t>::max(), &cost_limit);

    EXPECT_EQ(std::numeric_limits<int64_t>::max(), cost_limit.full_scan_rows);
    EXPECT_EQ(std::numeric_limits<size_t>::max(), cost_limit.point_probe_cost_per_key);
}

TEST(OlapScannerTest, CandidateMemoryFailuresAreClassifiedForFallback) {
    EXPECT_TRUE(OlapScanner::_is_candidate_memory_failure(
            Status::MemoryLimitExceeded("candidate memory exhausted")));
    EXPECT_TRUE(OlapScanner::_is_candidate_memory_failure(
            Status::Error<ErrorCode::QUERY_MEMORY_EXCEEDED>("query memory exhausted")));
    EXPECT_TRUE(OlapScanner::_is_candidate_memory_failure(
            Status::Error<ErrorCode::WORKLOAD_GROUP_MEMORY_EXCEEDED>(
                    "workload group memory exhausted")));
    EXPECT_TRUE(OlapScanner::_is_candidate_memory_failure(
            Status::Error<ErrorCode::PROCESS_MEMORY_EXCEEDED>("process memory exhausted")));
    EXPECT_FALSE(OlapScanner::_is_candidate_memory_failure(Status::Cancelled("query cancelled")));
}

TEST(OlapScannerTest, CandidateFallbackReasonsDoNotOverwriteEachOther) {
    RuntimeProfile profile("candidate fallback reasons");

    OlapScanner::_record_seq_map_candidate_fallback_reason(&profile, "candidate_key_limit");
    OlapScanner::_record_seq_map_candidate_fallback_reason(&profile, "candidate_cost_limit");

    const auto* key_limit =
            profile.get_info_string("SeqMapCandidateFallbackReason.candidate_key_limit");
    const auto* cost_limit =
            profile.get_info_string("SeqMapCandidateFallbackReason.candidate_cost_limit");
    ASSERT_NE(key_limit, nullptr);
    ASSERT_NE(cost_limit, nullptr);
    EXPECT_EQ(*key_limit, "candidate_key_limit");
    EXPECT_EQ(*cost_limit, "candidate_cost_limit");
}

TEST(OlapScannerTest, CandidateStatsMergeFullFileCacheAccounting) {
    OlapReaderStatistics candidate_stats;
    candidate_stats.raw_rows_read = 11;
    candidate_stats.uncompressed_bytes_read = 12;
    candidate_stats.rows_inverted_index_filtered = 13;
    candidate_stats.inverted_index_downgrade_count = 14;
    candidate_stats.inverted_index_lookup_timer = 15;
    candidate_stats.io_ns = 16;
    candidate_stats.compressed_bytes_read = 17;
    candidate_stats.decompress_ns = 18;
    candidate_stats.bytes_read = 19;
    candidate_stats.file_cache_stats.bytes_read_from_local = 20;
    candidate_stats.file_cache_stats.bytes_read_from_remote = 21;
    candidate_stats.file_cache_stats.bytes_read_from_peer = 22;
    candidate_stats.file_cache_stats.bytes_write_into_cache = 23;
    candidate_stats.file_cache_stats.inverted_index_bytes_read_from_remote = 24;
    candidate_stats.file_cache_stats.segment_footer_index_bytes_read_from_peer = 25;
    candidate_stats.file_cache_stats.bytes_read_from_cross_cg_peer = 26;
    candidate_stats.file_cache_stats.peer_hosts.emplace("peer-host");

    OlapReaderStatistics total_stats;
    total_stats.file_cache_stats.bytes_read_from_remote = 1;
    OlapScanner::_merge_seq_map_candidate_stats(candidate_stats, &total_stats);

    EXPECT_EQ(11, total_stats.seq_map_candidate_scan_rows);
    EXPECT_EQ(12, total_stats.seq_map_candidate_scan_bytes);
    EXPECT_EQ(13, total_stats.seq_map_candidate_index_filtered_rows);
    EXPECT_EQ(14, total_stats.seq_map_candidate_index_downgrades);
    EXPECT_EQ(15, total_stats.seq_map_candidate_index_lookup_ns);
    EXPECT_EQ(20, total_stats.seq_map_candidate_cache_local_bytes);
    EXPECT_EQ(21, total_stats.seq_map_candidate_cache_remote_bytes);
    EXPECT_EQ(20, total_stats.file_cache_stats.bytes_read_from_local);
    EXPECT_EQ(22, total_stats.file_cache_stats.bytes_read_from_remote);
    EXPECT_EQ(22, total_stats.file_cache_stats.bytes_read_from_peer);
    EXPECT_EQ(23, total_stats.file_cache_stats.bytes_write_into_cache);
    EXPECT_EQ(24, total_stats.file_cache_stats.inverted_index_bytes_read_from_remote);
    EXPECT_EQ(25, total_stats.file_cache_stats.segment_footer_index_bytes_read_from_peer);
    EXPECT_EQ(26, total_stats.file_cache_stats.bytes_read_from_cross_cg_peer);
    EXPECT_TRUE(total_stats.file_cache_stats.peer_hosts.contains("peer-host"));
    EXPECT_EQ(16, total_stats.io_ns);
    EXPECT_EQ(17, total_stats.compressed_bytes_read);
    EXPECT_EQ(18, total_stats.decompress_ns);
    EXPECT_EQ(12, total_stats.uncompressed_bytes_read);
    EXPECT_EQ(19, total_stats.bytes_read);
    EXPECT_EQ(11, total_stats.raw_rows_read);
}

} // namespace doris
