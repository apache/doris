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

#include <chrono>
#include <future>
#include <thread>

#include "load/channel/load_stream_mgr.h"
#include "load/stream_load/stream_load_context.h"
#include "load/stream_load/stream_load_executor.h"
#include "runtime/exec_env.h"
#include "util/countdown_latch.h"

namespace doris {

static POpenLoadStreamRequest make_open_stream_request() {
    POpenLoadStreamRequest request;
    *request.mutable_load_id() = UniqueId(1, 2).to_proto();
    request.set_txn_id(1);
    request.set_src_id(1);
    request.set_total_streams(1);
    auto* schema = request.mutable_schema();
    schema->set_db_id(1);
    schema->set_table_id(1);
    schema->set_version(0);
    schema->mutable_tuple_desc()->set_id(0);
    schema->mutable_tuple_desc()->set_byte_size(0);
    schema->mutable_tuple_desc()->set_num_null_bytes(0);
    schema->add_indexes()->set_id(1);
    schema->mutable_indexes(0)->set_schema_hash(0);
    return request;
}

TEST(GracefulShutdownTest, CountsDistinctLoadStreams) {
    LoadStreamMgr mgr(1);
    EXPECT_EQ(mgr.get_load_stream_num(), 0);
    auto request = make_open_stream_request();
    LoadStream* stream = nullptr;
    ASSERT_TRUE(mgr.open_load_stream(&request, stream).ok());
    EXPECT_EQ(mgr.get_load_stream_num(), 1);
    LoadStream* same_stream = nullptr;
    ASSERT_TRUE(mgr.open_load_stream(&request, same_stream).ok());
    EXPECT_EQ(stream, same_stream);
    EXPECT_EQ(mgr.get_load_stream_num(), 1);

    // A failed schema initialization must not reserve a stream count.
    request.mutable_load_id()->set_lo(3);
    request.mutable_schema()->set_unique_key_update_mode(
            UniqueKeyUpdateModePB::UPDATE_FIXED_COLUMNS);
    request.mutable_schema()->set_auto_increment_column("missing_column_id");
    ASSERT_FALSE(mgr.open_load_stream(&request, same_stream).ok());
    mgr.clear_load(UniqueId(1, 3));
    EXPECT_EQ(mgr.get_load_stream_num(), 1);
    mgr.clear_load(UniqueId(1, 2));
    EXPECT_EQ(mgr.get_load_stream_num(), 0);
    mgr.clear_load(UniqueId(1, 2));
    EXPECT_EQ(mgr.get_load_stream_num(), 0);
}

TEST(GracefulShutdownTest, CountsClosingLoadStreamWithoutBlockingOnFlush) {
    LoadStreamMgr mgr(1);
    auto request = make_open_stream_request();
    LoadStream* stream = nullptr;
    ASSERT_TRUE(mgr.open_load_stream(&request, stream).ok());
    auto tablet =
            std::make_shared<TabletStream>(request.load_id(), 1, 1, &mgr, stream->_profile.get());
    auto* token = tablet->_flush_token.get();
    // An abnormal close waits for the running write in token shutdown.
    tablet->_status.update(Status::InternalError("sender disconnected"));
    stream->_index_streams_map.at(1)->_tablet_streams_map.emplace(1, std::move(tablet));

    CountDownLatch flush_started(1);
    CountDownLatch finish_flush(1);
    ASSERT_TRUE(token->submit_func([&] {
                         flush_started.count_down();
                         finish_flush.wait();
                     }).ok());
    flush_started.wait();
    std::thread closing([&] {
        SCOPED_INIT_THREAD_CONTEXT();
        stream->on_closed(brpc::INVALID_STREAM_ID);
    });
    bool waiting_for_flush = false;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (std::chrono::steady_clock::now() < deadline) {
        {
            std::lock_guard lock(mgr._file_writer_thread_pool->_lock);
            waiting_for_flush = token->state() == ThreadPoolToken::State::QUIESCING;
        }
        if (waiting_for_flush) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    // Use a bounded wait so the old locking accessor fails instead of hanging the test.
    std::promise<size_t> census;
    auto result = census.get_future();
    std::thread polling([&] { census.set_value(mgr.get_load_stream_num()); });
    const auto census_status = result.wait_for(std::chrono::seconds(5));
    finish_flush.count_down();
    closing.join();
    polling.join();
    EXPECT_TRUE(waiting_for_flush);
    EXPECT_EQ(census_status, std::future_status::ready);
    EXPECT_EQ(result.get(), 1);
    EXPECT_EQ(mgr.get_load_stream_num(), 0);
}

TEST(GracefulShutdownTest, CountsLoadUntilLastOwnerAndRollbackFinish) {
    const auto baseline = StreamLoadContext::num_active_contexts();
    ExecEnv env;
    class RollbackExecutor : public StreamLoadExecutor {
    public:
        RollbackExecutor(ExecEnv* env, size_t baseline)
                : StreamLoadExecutor(env), _baseline(baseline) {}
        void rollback_txn(StreamLoadContext* ctx) override {
            // The load must still be counted during its final transaction RPC.
            EXPECT_EQ(StreamLoadContext::num_active_contexts(), _baseline + 1);
            rolled_back = true;
        }
        bool rolled_back = false;

    private:
        size_t _baseline;
    };
    auto executor = std::make_unique<RollbackExecutor>(&env, baseline);
    auto* observer = executor.get();
    env.set_stream_load_executor(std::move(executor));
    auto context = StreamLoadContext::create_shared(&env);
    context->need_rollback = true;
    EXPECT_EQ(StreamLoadContext::num_active_contexts(), baseline + 1);
    auto retained = context;
    context.reset();
    EXPECT_EQ(StreamLoadContext::num_active_contexts(), baseline + 1);
    EXPECT_FALSE(observer->rolled_back);
    retained.reset();
    EXPECT_TRUE(observer->rolled_back);
    EXPECT_EQ(StreamLoadContext::num_active_contexts(), baseline);
    env.clear_stream_load_executor();
}

} // namespace doris
