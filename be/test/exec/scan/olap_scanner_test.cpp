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

#include <string>

#include "cloud/config.h"
#include "common/config.h"
#include "io/io_common.h"
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

} // namespace doris
