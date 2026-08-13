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

#include "load/channel/load_channel.h"

#include <gen_cpp/internal_service.pb.h>
#include <google/protobuf/stubs/callback.h>
#include <gtest/gtest.h>

#include "load/channel/load_channel_mgr.h"
#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"
#include "util/debug_points.h"

namespace doris {

class LoadChannelFinalTabletResultTest : public testing::Test {
protected:
    void SetUp() override {
        _exec_env = ExecEnv::GetInstance();
        _previous_fragment_mgr = _exec_env->_fragment_mgr;
        if (_previous_fragment_mgr == nullptr) {
            _fragment_mgr = std::make_unique<FragmentMgr>(_exec_env);
            _exec_env->_fragment_mgr = _fragment_mgr.get();
        }
    }

    void TearDown() override {
        if (_fragment_mgr != nullptr) {
            _fragment_mgr->stop();
            _exec_env->_fragment_mgr = _previous_fragment_mgr;
        }
    }

    class CountingClosure final : public google::protobuf::Closure {
    public:
        void Run() override { ++runs; }

        int runs = 0;
    };

    ExecEnv* _exec_env = nullptr;
    FragmentMgr* _previous_fragment_mgr = nullptr;
    std::unique_ptr<FragmentMgr> _fragment_mgr;
};

TEST_F(LoadChannelFinalTabletResultTest, PublishCompletesDeferredRpcOnce) {
    LoadChannel channel(UniqueId(1, 2), 60, false, "test", -1, false, -1);
    PTabletWriterAddBlockResult deferred_response;
    Status::OK().to_protobuf(deferred_response.mutable_status());
    auto* deferred_error = deferred_response.add_tablet_errors();
    deferred_error->set_tablet_id(102);
    deferred_error->set_msg("early failure");
    CountingClosure closure;
    google::protobuf::Closure* done = &closure;

    channel._defer_or_copy_final_tablet_result(10, 1, &deferred_response, &done);
    EXPECT_EQ(done, nullptr);
    EXPECT_EQ(closure.runs, 0);
    std::unordered_map<int64_t, LoadChannel::FinalTabletResult> final_results;
    bool oversized = false;
    size_t result_bytes = 0;
    EXPECT_FALSE(
            channel.copy_final_tablet_results(&final_results, 1024, &oversized, &result_bytes));

    PTabletWriterAddBlockResult final_response;
    Status::OK().to_protobuf(final_response.mutable_status());
    auto* tablet = final_response.add_tablet_vec();
    tablet->set_tablet_id(100);
    tablet->set_schema_hash(0);
    auto* error = final_response.add_tablet_errors();
    error->set_tablet_id(101);
    error->set_msg("failed");

    channel._publish_final_tablet_result(10, 2, final_response);
    EXPECT_EQ(closure.runs, 1);
    EXPECT_TRUE(Status::create(deferred_response.status()).ok());
    ASSERT_EQ(deferred_response.tablet_vec_size(), 1);
    EXPECT_EQ(deferred_response.tablet_vec(0).tablet_id(), 100);
    EXPECT_TRUE(deferred_response.final_tablet_result_fanout());
    ASSERT_EQ(deferred_response.tablet_errors_size(), 2);
    std::unordered_set<int64_t> error_tablet_ids;
    for (const auto& tablet_error : deferred_response.tablet_errors()) {
        error_tablet_ids.insert(tablet_error.tablet_id());
    }
    EXPECT_EQ(error_tablet_ids, (std::unordered_set<int64_t> {101, 102}));

    PTabletWriterAddBlockResult final_rpc_response = final_response;
    CountingClosure final_closure;
    google::protobuf::Closure* final_done = &final_closure;
    channel._defer_or_copy_final_tablet_result(10, 2, &final_rpc_response, &final_done);
    EXPECT_EQ(final_done, &final_closure);
    EXPECT_EQ(final_closure.runs, 0);
    EXPECT_EQ(final_rpc_response.tablet_errors_size(), 2);
    EXPECT_FALSE(final_rpc_response.final_tablet_result_fanout());

    channel._publish_final_tablet_result(10, 2, final_response);
    EXPECT_EQ(closure.runs, 1);
    EXPECT_TRUE(channel.copy_final_tablet_results(&final_results, 1024, &oversized, &result_bytes));
    EXPECT_FALSE(oversized);
    EXPECT_EQ(final_results.at(10).owner_sender_id, 2);
    EXPECT_TRUE(channel.copy_final_tablet_results(&final_results, 0, &oversized, &result_bytes));
    EXPECT_TRUE(oversized);
}

TEST_F(LoadChannelFinalTabletResultTest, CancelCompletesDeferredRpcOnce) {
    LoadChannel channel(UniqueId(3, 4), 60, false, "test", -1, false, -1);
    PTabletWriterAddBlockResult response;
    Status::OK().to_protobuf(response.mutable_status());
    CountingClosure closure;
    google::protobuf::Closure* done = &closure;

    channel._defer_or_copy_final_tablet_result(10, 1, &response, &done);
    ASSERT_TRUE(channel.cancel(Status::Cancelled("test cancellation")).ok());
    EXPECT_EQ(closure.runs, 1);
    EXPECT_TRUE(Status::create(response.status()).is<ErrorCode::CANCELLED>());

    PTabletWriterAddBlockResult late_result;
    Status::OK().to_protobuf(late_result.mutable_status());
    channel._publish_final_tablet_result(10, 2, late_result);
    ASSERT_TRUE(channel.cancel(Status::Cancelled("second cancellation")).ok());
    EXPECT_EQ(closure.runs, 1);
    EXPECT_TRUE(Status::create(response.status()).is<ErrorCode::CANCELLED>());
}

TEST_F(LoadChannelFinalTabletResultTest, CancelBeforeReserveDoesNotParkRpc) {
    LoadChannel channel(UniqueId(3, 5), 60, false, "test", -1, false, -1);
    ASSERT_TRUE(channel.cancel(Status::Cancelled("early cancellation")).ok());

    PTabletWriterAddBlockResult response;
    Status::OK().to_protobuf(response.mutable_status());
    CountingClosure closure;
    google::protobuf::Closure* done = &closure;
    channel._reserve_final_tablet_result(10);
    channel._defer_or_copy_final_tablet_result(10, 1, &response, &done);

    EXPECT_EQ(done, &closure);
    EXPECT_TRUE(Status::create(response.status()).is<ErrorCode::CANCELLED>());
}

TEST_F(LoadChannelFinalTabletResultTest, FinishPublishesTombstoneBeforeCopy) {
    LoadChannelMgr manager;
    manager._load_state_channels = std::make_unique<LoadChannelMgr::LoadStateChannelCache>(1024);
    manager._final_tablet_result_cache = std::make_unique<LoadChannelMgr::FinalTabletResultCache>();
    UniqueId load_id(6, 7);
    auto channel = std::make_shared<LoadChannel>(load_id, 60, false, "test", -1, false, -1);
    PTabletWriterAddBlockResult final_response;
    Status::OK().to_protobuf(final_response.mutable_status());
    channel->_publish_final_tablet_result(10, 1, final_response);
    manager._load_channels.emplace(load_id, channel);

    bool cancelled_during_copy = false;
    const bool old_enable_debug_points = config::enable_debug_points;
    config::enable_debug_points = true;
    DebugPoints::instance()->add_with_callback(
            "LoadChannelMgr.finish.before_copy", std::function<void()>([&] {
                cancelled_during_copy = true;
                PTabletWriterCancelRequest request;
                request.mutable_id()->CopyFrom(load_id.to_proto());
                ASSERT_TRUE(manager.cancel(request).ok());
            }));
    manager._finish_load_channel(load_id, channel);
    DebugPoints::instance()->remove("LoadChannelMgr.finish.before_copy");
    config::enable_debug_points = old_enable_debug_points;

    EXPECT_TRUE(cancelled_during_copy);
    auto* handle = manager._load_state_channels->lookup(load_id.to_string());
    ASSERT_NE(handle, nullptr);
    manager._load_state_channels->release(handle);
    manager.stop();
}

TEST_F(LoadChannelFinalTabletResultTest, ManagerReservesResultBeforeReturningChannel) {
    LoadChannelMgr manager;
    UniqueId load_id(5, 6);
    auto channel = std::make_shared<LoadChannel>(load_id, 60, false, "test", -1, false, -1);
    manager._load_channels.emplace(load_id, channel);
    PTabletWriterAddBlockRequest request;
    request.set_index_id(10);
    request.set_eos(true);
    request.set_need_final_tablet_result(true);
    std::shared_ptr<LoadChannel> returned_channel;
    bool is_eof = false;
    PTabletWriterAddBlockResult response;

    ASSERT_TRUE(
            manager._get_load_channel(returned_channel, is_eof, load_id, request, &response).ok());
    EXPECT_EQ(returned_channel, channel);
    EXPECT_FALSE(is_eof);
    EXPECT_TRUE(channel->need_final_tablet_result());
    std::unordered_map<int64_t, LoadChannel::FinalTabletResult> final_results;
    bool oversized = false;
    size_t result_bytes = 0;
    EXPECT_FALSE(
            channel->copy_final_tablet_results(&final_results, 1024, &oversized, &result_bytes));
    manager.stop();
}

TEST_F(LoadChannelFinalTabletResultTest, OrdinaryLoadDoesNotTouchFinalResultCache) {
    LoadChannelMgr manager;
    manager._load_state_channels = std::make_unique<LoadChannelMgr::LoadStateChannelCache>(1024);
    manager._final_tablet_result_cache = std::make_unique<LoadChannelMgr::FinalTabletResultCache>();

    UniqueId load_id(7, 8);
    auto channel = std::make_shared<LoadChannel>(load_id, 60, false, "test", -1, false, -1);
    manager._load_channels.emplace(load_id, channel);
    auto cache_value = std::make_unique<LoadChannelMgr::FinalTabletResultCache::CacheValue>();
    auto* inserted = manager._final_tablet_result_cache->insert(
            load_id.to_string(), cache_value.get(), sizeof(*cache_value), sizeof(*cache_value));
    cache_value.release();
    manager._final_tablet_result_cache->release(inserted);

    manager._finish_load_channel(load_id, channel);

    auto* cached = manager._final_tablet_result_cache->lookup(load_id.to_string());
    ASSERT_NE(cached, nullptr);
    manager._final_tablet_result_cache->release(cached);
    manager.stop();
}

TEST_F(LoadChannelFinalTabletResultTest, RetryReadsFinalResultFromSuccessCache) {
    LoadChannelMgr manager;
    manager._load_state_channels = std::make_unique<LoadChannelMgr::LoadStateChannelCache>(1024);
    manager._final_tablet_result_cache = std::make_unique<LoadChannelMgr::FinalTabletResultCache>();

    UniqueId load_id(7, 8);
    auto channel = std::make_shared<LoadChannel>(load_id, 60, false, "test", -1, false, -1);
    PTabletWriterAddBlockResult final_response;
    Status::OK().to_protobuf(final_response.mutable_status());
    auto* tablet = final_response.add_tablet_vec();
    tablet->set_tablet_id(100);
    tablet->set_schema_hash(0);
    final_response.add_tablet_errors()->set_tablet_id(101);
    channel->_publish_final_tablet_result(10, 2, final_response);
    manager._load_channels.emplace(load_id, channel);

    manager._finish_load_channel(load_id, channel);

    PTabletWriterAddBlockRequest retry;
    retry.mutable_id()->CopyFrom(load_id.to_proto());
    retry.set_index_id(10);
    retry.set_eos(true);
    retry.set_sender_id(2);
    retry.set_need_final_tablet_result(true);
    PTabletWriterAddBlockResult retry_response;
    Status retry_status = manager.add_batch(retry, &retry_response);

    retry.set_index_id(11);
    PTabletWriterAddBlockResult unavailable_response;
    Status unavailable_status = manager.add_batch(retry, &unavailable_response);

    retry.set_need_final_tablet_result(false);
    PTabletWriterAddBlockResult legacy_retry_response;
    Status legacy_retry_status = manager.add_batch(retry, &legacy_retry_response);

    ASSERT_TRUE(retry_status.ok());
    EXPECT_TRUE(unavailable_status.ok());
    EXPECT_TRUE(legacy_retry_status.ok());
    EXPECT_TRUE(Status::create(retry_response.status()).ok());
    ASSERT_EQ(retry_response.tablet_vec_size(), 1);
    EXPECT_EQ(retry_response.tablet_vec(0).tablet_id(), 100);
    ASSERT_EQ(retry_response.tablet_errors_size(), 1);
    EXPECT_EQ(retry_response.tablet_errors(0).tablet_id(), 101);
    EXPECT_FALSE(retry_response.final_tablet_result_fanout());

    retry.set_index_id(10);
    retry.set_sender_id(1);
    retry.set_need_final_tablet_result(true);
    PTabletWriterAddBlockResult fanout_retry_response;
    ASSERT_TRUE(manager.add_batch(retry, &fanout_retry_response).ok());
    EXPECT_TRUE(fanout_retry_response.final_tablet_result_fanout());

    manager._final_tablet_result_cache->erase(load_id.to_string());
    PTabletWriterAddBlockResult uncached_retry_response;
    ASSERT_TRUE(manager.add_batch(retry, &uncached_retry_response).ok());
    EXPECT_EQ(uncached_retry_response.tablet_vec_size(), 0);
    manager.stop();
}

} // namespace doris
