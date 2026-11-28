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

#include <gtest/gtest.h>

#include <cstring>
#include <memory>

#include "cloud/config.h"
#include "common/config.h"
#include "io/fs/file_range_read_scheduler.h"
#include "io/fs/read_ahead_io_resources.h"
#include "io/fs/read_ahead_metrics.h"
#include "runtime/exec_env.h"
#include "runtime/query_context.h"
#include "storage/iterators.h"
#include "testutil/mock/mock_query_context.h"
#include "util/threadpool.h"

namespace doris {
namespace {

class ReadAheadTestReader final : public io::FileReader {
public:
    Status close() override { return Status::OK(); }
    const io::Path& path() const override { return _path; }
    size_t size() const override { return 8192; }
    bool closed() const override { return false; }
    int64_t mtime() const override { return 0; }

protected:
    Status read_at_impl(size_t, Slice result, size_t* bytes_read, const io::IOContext*) override {
        std::memset(result.data, 'a', result.size);
        *bytes_read = result.size;
        return Status::OK();
    }

private:
    io::Path _path {"read-ahead-query-test"};
};

class QueryContextReadAheadTest : public testing::Test {
protected:
    void SetUp() override {
        _saved_deploy_mode = config::deploy_mode;
        _saved_cloud_id = config::cloud_unique_id;
        _saved_async_write = config::enable_async_file_cache_write;
        _saved_window = config::read_ahead_eager_window_bytes;
        config::deploy_mode = "cloud";
        config::enable_async_file_cache_write = true;
        auto* env = ExecEnv::GetInstance();
        _saved_scheduler = std::move(env->_file_range_read_scheduler);
        ASSERT_TRUE(ThreadPoolBuilder("QueryReadAheadTest").set_max_threads(4).build(&_pool).ok());
        ASSERT_TRUE(io::FileRangeReadScheduler::create(
                            {.max_bytes_per_query = 4096, .max_bytes_per_be = 8192}, _pool.get(),
                            &env->_file_range_read_scheduler)
                            .ok());
    }

    void TearDown() override {
        _pool->wait();
        ExecEnv::GetInstance()->_file_range_read_scheduler = std::move(_saved_scheduler);
        config::deploy_mode = _saved_deploy_mode;
        config::cloud_unique_id = _saved_cloud_id;
        config::enable_async_file_cache_write = _saved_async_write;
        config::read_ahead_eager_window_bytes = _saved_window;
    }

    static std::shared_ptr<MockQueryContext> create_query(bool enabled = true) {
        auto options = create_fake_query_options();
        options.__set_enable_query_read_ahead(enabled);
        return MockQueryContext::create(TUniqueId {}, ExecEnv::GetInstance(), options);
    }

    std::unique_ptr<ThreadPool> _pool;
    std::unique_ptr<io::FileRangeReadScheduler> _saved_scheduler;
    std::string _saved_deploy_mode;
    std::string _saved_cloud_id;
    bool _saved_async_write = false;
    int64_t _saved_window = 0;
};

TEST_F(QueryContextReadAheadTest, SessionSwitchDefaultsOffAndRequiresAsyncCloudIO) {
    EXPECT_EQ(MockQueryContext::create()->io_context().query_read_ahead, nullptr);
    EXPECT_EQ(create_query(false)->io_context().query_read_ahead, nullptr);
    ASSERT_NE(create_query()->io_context().query_read_ahead, nullptr);
    config::enable_async_file_cache_write = false;
    EXPECT_EQ(create_query()->io_context().query_read_ahead, nullptr);
    config::enable_async_file_cache_write = true;
    config::deploy_mode.clear();
    config::cloud_unique_id.clear();
    EXPECT_EQ(create_query()->io_context().query_read_ahead, nullptr);
}

TEST_F(QueryContextReadAheadTest, ScanCopiesShareBudgetAndCancellationButQueriesAreIsolated) {
    auto query = create_query();
    auto other = create_query();
    auto first_scan = query->io_context();
    auto second_scan = query->io_context();
    ASSERT_NE(first_scan.query_read_ahead, nullptr);
    EXPECT_EQ(first_scan.query_read_ahead, second_scan.query_read_ahead);
    EXPECT_NE(first_scan.query_read_ahead, other->io_context().query_read_ahead);
    auto reader = std::make_shared<ReadAheadTestReader>();
    auto* scheduler = first_scan.query_read_ahead->scheduler;
    auto accepted = scheduler->try_submit({{.offset = 0, .size = 4096}}, reader,
                                          io::FileRangeReadIOContext::from_caller(first_scan),
                                          first_scan.query_read_ahead->context);
    ASSERT_TRUE(accepted.accepted());
    auto rejected = scheduler->try_submit({{.offset = 4096, .size = 4096}}, reader,
                                          io::FileRangeReadIOContext::from_caller(second_scan),
                                          second_scan.query_read_ahead->context);
    EXPECT_EQ(rejected.reject_reason, io::FileRangeReadRejectReason::QUERY_BYTE_LIMIT);
    auto independent =
            scheduler->try_submit({{.offset = 4096, .size = 4096}}, reader,
                                  io::FileRangeReadIOContext::from_caller(other->io_context()),
                                  other->io_context().query_read_ahead->context);
    EXPECT_TRUE(independent.accepted());
    query->cancel(Status::Cancelled("cancel all scan copies"));
    EXPECT_TRUE(second_scan.query_read_ahead->context->cancelled());
    EXPECT_FALSE(other->io_context().query_read_ahead->context->cancelled());
    auto late_copy = query->io_context();
    EXPECT_TRUE(late_copy.query_read_ahead->context->cancelled());
}

TEST_F(QueryContextReadAheadTest, QueryDestructionCancelsRetainedScanResources) {
    auto query = create_query();
    auto scan = query->io_context();
    query.reset();
    EXPECT_TRUE(scan.query_read_ahead->context->cancelled());
}

TEST_F(QueryContextReadAheadTest, InvalidWindowFallsBackToSynchronousReads) {
    config::read_ahead_eager_window_bytes = 0;
    EXPECT_EQ(create_query()->io_context().query_read_ahead, nullptr);
}

TEST_F(QueryContextReadAheadTest, FileBindingsAndAsyncSnapshotsDoNotRetainEachOther) {
    auto query = create_query();
    OlapReaderStatistics scan_stats;
    StorageReadOptions scan(scan_stats);
    scan.io_ctx = query->io_context();
    auto reader = std::make_shared<ReadAheadTestReader>();
    auto stats = std::make_shared<io::ReadAheadStatistics>();
    io::FileCacheStatistics cache_stats;
    scan.io_ctx.file_cache_stats = &cache_stats;
    scan.file_read_ahead = io::FileReadAheadResources::create(reader, scan.io_ctx, stats, {});
    std::weak_ptr<const io::FileReadAheadResources> file = scan.file_read_ahead;
    auto snapshot = io::FileRangeReadIOContext::from_caller(scan.io_ctx);
    EXPECT_EQ(snapshot.io_context.file_cache_stats, nullptr);
    EXPECT_EQ(snapshot.io_context.query_read_ahead, query->io_context().query_read_ahead);
    EXPECT_NE(snapshot.io_context.query_id, scan.io_ctx.query_id);
    EXPECT_EQ(*snapshot.io_context.query_id, *scan.io_ctx.query_id);
    OlapReaderStatistics another_scan_stats;
    StorageReadOptions another(another_scan_stats);
    another.io_ctx = query->io_context();
    EXPECT_EQ(another.file_read_ahead, nullptr);
    EXPECT_EQ(another.io_ctx.file_cache_stats, nullptr);
    auto another_stats = std::make_shared<io::ReadAheadStatistics>();
    another.file_read_ahead =
            io::FileReadAheadResources::create(reader, another.io_ctx, another_stats, {});
    EXPECT_NE(another.file_read_ahead, scan.file_read_ahead);
    EXPECT_EQ(another.file_read_ahead->query_resources().context,
              scan.file_read_ahead->query_resources().context);
    EXPECT_NE(another.file_read_ahead->statistics, scan.file_read_ahead->statistics);
    scan.file_read_ahead.reset();
    EXPECT_TRUE(file.expired());
}

} // namespace
} // namespace doris
