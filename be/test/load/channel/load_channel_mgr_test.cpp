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

#include "load/channel/load_channel_mgr.h"

#include <gtest/gtest.h>

#include <thread>

#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"
#include "util/countdown_latch.h"

namespace doris {

class LoadChannelMgrTest : public testing::Test {
protected:
    void SetUp() override {
        auto* exec_env = ExecEnv::GetInstance();
        if (exec_env->fragment_mgr() == nullptr) {
            _fragment_mgr = std::make_unique<FragmentMgr>(exec_env);
            exec_env->_fragment_mgr = _fragment_mgr.get();
        }
        _mgr = std::make_unique<LoadChannelMgr>();
        // Drive timeout cleanup explicitly so the interleavings are deterministic.
        _mgr->_load_state_channels = std::make_unique<LoadChannelMgr::LoadStateChannelCache>(1024);
    }

    void TearDown() override {
        _mgr->stop();
        _mgr.reset();
        if (_fragment_mgr != nullptr) {
            _fragment_mgr->stop();
            ExecEnv::GetInstance()->_fragment_mgr = nullptr;
            _fragment_mgr.reset();
        }
    }

    std::shared_ptr<LoadChannel> create_channel() {
        auto channel = std::make_shared<LoadChannel>(_load_id, 1, true, "127.0.0.1", 0, false, -1);
        std::lock_guard<std::mutex> lock(_mgr->_lock);
        _mgr->_load_channels[_load_id] = channel;
        return channel;
    }

    void expect_no_cached_state() {
        auto* handle = _mgr->_load_state_channels->lookup(_load_id.to_string());
        EXPECT_EQ(handle, nullptr);
        if (handle != nullptr) {
            _mgr->_load_state_channels->release(handle);
        }
    }

    UniqueId _load_id {1, 2};
    std::unique_ptr<FragmentMgr> _fragment_mgr;
    std::unique_ptr<LoadChannelMgr> _mgr;
};

TEST_F(LoadChannelMgrTest, FailedBatchCancelsRetainedChannelAndCachesReason) {
    auto channel = create_channel();
    PTabletWriterAddBlockRequest request;
    *request.mutable_id() = _load_id.to_proto();
    request.set_index_id(100);
    PTabletWriterAddBlockResult response;
    // No index was opened, so this exercises the actual add_batch failure path.
    auto st = _mgr->add_batch(request, &response);
    ASSERT_FALSE(st.ok());
    EXPECT_EQ(channel->cancel_status().to_string(), st.to_string());
    EXPECT_TRUE(_mgr->_load_channels.empty());

    request.set_eos(true);
    auto late_status = _mgr->add_batch(request, &response);
    EXPECT_TRUE(late_status.is<ErrorCode::CANCELLED>()) << late_status;
    EXPECT_NE(late_status.to_string().find(st.to_string()), std::string::npos);

    PTabletWriterOpenRequest open_request;
    *open_request.mutable_id() = _load_id.to_proto();
    EXPECT_TRUE(_mgr->open(open_request).is<ErrorCode::CANCELLED>());
}

TEST_F(LoadChannelMgrTest, PublicCancelCachesAlreadyPublishedFailure) {
    auto channel = create_channel();
    const auto first_failure = Status::InternalError("first writer failure");
    CountDownLatch published(1);
    CountDownLatch resume_failure(1);
    Status publish_status;
    Status failure_status;
    std::thread failed_request([&] {
        SCOPED_INIT_THREAD_CONTEXT();
        // Stage the publication separately to model _cancel_load_channel paused
        // before taking the manager lock. Its repeated publication is idempotent.
        publish_status = channel->cancel(first_failure);
        published.count_down();
        resume_failure.wait();
        failure_status = _mgr->_cancel_load_channel(channel, first_failure);
    });
    published.wait();

    PTabletWriterCancelRequest cancel_request;
    *cancel_request.mutable_id() = _load_id.to_proto();
    cancel_request.set_cancel_reason("later upstream cancellation");
    const auto cancel_status = _mgr->cancel(cancel_request);
    resume_failure.count_down();
    failed_request.join();

    ASSERT_TRUE(publish_status.ok()) << publish_status;
    ASSERT_TRUE(cancel_status.ok()) << cancel_status;
    ASSERT_TRUE(failure_status.ok()) << failure_status;
    EXPECT_EQ(channel->cancel_status().to_string(), first_failure.to_string());
    EXPECT_TRUE(_mgr->_load_channels.empty());

    auto* handle = _mgr->_load_state_channels->lookup(_load_id.to_string());
    ASSERT_NE(handle, nullptr);
    auto* value =
            static_cast<LoadChannelMgr::CacheValue*>(_mgr->_load_state_channels->value(handle));
    EXPECT_NE(value, nullptr);
    if (value != nullptr) {
        EXPECT_EQ(value->_cancel_reason, first_failure.to_string());
    }
    _mgr->_load_state_channels->release(handle);

    PTabletWriterOpenRequest open_request;
    *open_request.mutable_id() = _load_id.to_proto();
    const auto open_status = _mgr->open(open_request);
    EXPECT_TRUE(open_status.is<ErrorCode::CANCELLED>()) << open_status;
    EXPECT_NE(open_status.to_string().find(first_failure.to_string()), std::string::npos);

    PTabletWriterAddBlockRequest add_request;
    *add_request.mutable_id() = _load_id.to_proto();
    add_request.set_eos(true);
    PTabletWriterAddBlockResult response;
    const auto add_status = _mgr->add_batch(add_request, &response);
    EXPECT_TRUE(add_status.is<ErrorCode::CANCELLED>()) << add_status;
    EXPECT_NE(add_status.to_string().find(first_failure.to_string()), std::string::npos);
}

TEST_F(LoadChannelMgrTest, LateFailureDoesNotCancelReplacementAfterTimeout) {
    auto original = create_channel();
    CountDownLatch captured(1);
    CountDownLatch resume_failure(1);
    Status failure_status;
    std::thread failed_request([&, channel = original] {
        SCOPED_INIT_THREAD_CONTEXT();
        captured.count_down();
        resume_failure.wait();
        failure_status = _mgr->_cancel_load_channel(channel, Status::InternalError("old request"));
    });
    captured.wait();
    original->_last_updated_time.store(0);
    EXPECT_TRUE(_mgr->_start_load_channels_clean().ok());
    auto replacement = create_channel();
    resume_failure.count_down();
    failed_request.join();

    EXPECT_TRUE(failure_status.ok()) << failure_status;
    EXPECT_TRUE(original->is_cancelled());
    EXPECT_FALSE(replacement->is_cancelled());
    EXPECT_EQ(_mgr->_load_channels.at(_load_id), replacement);
    expect_no_cached_state();
}

TEST_F(LoadChannelMgrTest, LateFinishDoesNotRemoveReplacement) {
    auto original = create_channel();
    auto replacement = create_channel();
    _mgr->_finish_load_channel(original);
    EXPECT_EQ(_mgr->_load_channels.at(_load_id), replacement);
    EXPECT_FALSE(replacement->is_cancelled());
    expect_no_cached_state();
}

TEST_F(LoadChannelMgrTest, FinishDoesNotOverwritePublishedFailure) {
    auto channel = create_channel();
    auto reason = Status::InternalError("first failure");
    ASSERT_TRUE(channel->cancel(reason).ok());
    // EOS arrives between cancellation publication and removal from the manager.
    _mgr->_finish_load_channel(channel);
    EXPECT_EQ(_mgr->_load_channels.at(_load_id), channel);
    expect_no_cached_state();
    ASSERT_TRUE(_mgr->_cancel_load_channel(channel, Status::InternalError("later failure")).ok());
    _mgr->_finish_load_channel(channel);

    auto* handle = _mgr->_load_state_channels->lookup(_load_id.to_string());
    ASSERT_NE(handle, nullptr);
    auto* value =
            static_cast<LoadChannelMgr::CacheValue*>(_mgr->_load_state_channels->value(handle));
    EXPECT_NE(value, nullptr);
    if (value != nullptr) {
        EXPECT_EQ(value->_cancel_reason, reason.to_string());
    }
    _mgr->_load_state_channels->release(handle);
}

TEST_F(LoadChannelMgrTest, FailureStillCancelsOriginalAfterFinish) {
    auto channel = create_channel();
    _mgr->_finish_load_channel(channel);
    ASSERT_TRUE(_mgr->_load_channels.empty());
    auto reason = Status::InternalError("late failure");
    ASSERT_TRUE(_mgr->_cancel_load_channel(channel, reason).ok());
    EXPECT_EQ(channel->cancel_status().to_string(), reason.to_string());
    // A previously completed manager transition keeps its terminal state.
    auto* handle = _mgr->_load_state_channels->lookup(_load_id.to_string());
    ASSERT_NE(handle, nullptr);
    EXPECT_EQ(_mgr->_load_state_channels->value(handle), nullptr);
    _mgr->_load_state_channels->release(handle);
}

} // namespace doris
