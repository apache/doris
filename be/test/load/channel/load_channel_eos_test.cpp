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

#include <gen_cpp/internal_service.pb.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <ctime>
#include <functional>
#include <memory>
#include <thread>
#include <utility>

#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablets_channel.h"
#include "load/channel/eos_completion.h"
#include "load/channel/load_channel.h"
#include "load/channel/load_channel_mgr.h"
#include "load/channel/tablets_channel.h"
#include "load/delta_writer/delta_writer.h"
#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"
#include "storage/storage_engine.h"
#include "util/countdown_latch.h"

namespace doris {

class LoadChannelEosTest : public testing::TestWithParam<bool> {
protected:
    void SetUp() override {
        auto* env = ExecEnv::GetInstance();
        _previous_fragment_mgr = env->_fragment_mgr;
        _fragment_mgr = std::make_unique<FragmentMgr>(env);
        env->_fragment_mgr = _fragment_mgr.get();
        _load = std::make_shared<LoadChannel>(UniqueId(1, 2), 60, true, "", 0, false, 0);
    }

    void TearDown() override {
        _load.reset();
        _fragment_mgr->stop();
        ExecEnv::GetInstance()->_fragment_mgr = _previous_fragment_mgr;
    }

    std::shared_ptr<BaseTabletsChannel> make_channel(int senders) {
        PUniqueId id;
        id.set_hi(1);
        id.set_lo(2);
        std::shared_ptr<BaseTabletsChannel> channel;
        if (GetParam()) {
            _cloud_engine = std::make_unique<CloudStorageEngine>(EngineOptions {});
            channel = std::make_shared<CloudTabletsChannel>(
                    *_cloud_engine, TabletsChannelKey(id, 10), UniqueId(1, 2), true, nullptr);
        } else {
            _engine = std::make_unique<StorageEngine>(EngineOptions {});
            channel = std::make_shared<TabletsChannel>(*_engine, TabletsChannelKey(id, 10),
                                                       UniqueId(1, 2), true, nullptr);
        }
        // No writers are needed to exercise real sender accounting and close.
        channel->_state = BaseTabletsChannel::kOpened;
        channel->_num_remaining_senders = senders;
        channel->_closed_senders.Reset(senders);
        _load->_tablets_channels.emplace(10, channel);
        _load->_opened = true;
        return channel;
    }

    static PTabletWriterAddBlockRequest eos(int sender, bool hang_wait = true) {
        PTabletWriterAddBlockRequest request;
        request.mutable_id()->set_hi(1);
        request.mutable_id()->set_lo(2);
        request.set_index_id(10);
        request.set_sender_id(sender);
        request.set_backend_id(1);
        request.set_eos(true);
        request.set_hang_wait(hang_wait);
        return request;
    }

    FragmentMgr* _previous_fragment_mgr = nullptr;
    std::unique_ptr<FragmentMgr> _fragment_mgr;
    std::unique_ptr<StorageEngine> _engine;
    std::unique_ptr<CloudStorageEngine> _cloud_engine;
    std::shared_ptr<LoadChannel> _load;
};

TEST_P(LoadChannelEosTest, SingleWorkerProcessesOneHundredSendersAndDuplicateEos) {
    auto channel = make_channel(100);
    std::atomic<int> replies {0};
    CountDownLatch processed(1);
    std::thread worker([&] {
        // Retry sender zero before any other sender. It must not decrement the
        // remaining count twice, and both RPCs must wait for final close.
        for (int i = 0; i <= 100; ++i) {
            int sender = i == 0 ? 0 : i - 1;
            PTabletWriterAddBlockResult response;
            std::shared_ptr<EosCompletion> completion;
            EXPECT_TRUE(_load->add_batch(eos(sender), &response, &completion).ok());
            EXPECT_NE(completion, nullptr);
            completion->add_waiter([&](const Status& status) {
                EXPECT_TRUE(status.ok());
                EXPECT_EQ(channel->_num_remaining_senders, 0);
                ++replies;
            });
            if (sender < 99) {
                EXPECT_EQ(replies.load(), 0);
            }
        }
        processed.count_down();
    });
    bool finished = processed.wait_for(std::chrono::seconds(10));
    if (!finished) {
        // Also makes the old blocking implementation fail instead of hanging UT.
        EXPECT_TRUE(_load->cancel().ok());
    }
    worker.join();
    EXPECT_TRUE(finished);
    EXPECT_EQ(replies.load(), 101);
    EXPECT_TRUE(_load->is_finished());
    EXPECT_EQ(channel->_num_remaining_senders, 0);
}

TEST_P(LoadChannelEosTest, NonWaitingLastSenderReleasesEarlierWaiter) {
    auto channel = make_channel(2);
    PTabletWriterAddBlockResult response;
    std::shared_ptr<EosCompletion> first;
    ASSERT_TRUE(_load->add_batch(eos(0), &response, &first).ok());
    int replies = 0;
    first->add_waiter([&](const Status& status) {
        EXPECT_TRUE(status.ok());
        ++replies;
    });
    EXPECT_EQ(replies, 0);
    std::shared_ptr<EosCompletion> last;
    EXPECT_TRUE(_load->add_batch(eos(1, false), &response, &last).ok());
    EXPECT_EQ(last, nullptr);
    EXPECT_EQ(replies, 1);
}

TEST_P(LoadChannelEosTest, CancelReleasesRegisteredAndNotYetRegisteredRpcs) {
    auto channel = make_channel(3);
    PTabletWriterAddBlockResult response;
    std::shared_ptr<EosCompletion> first;
    std::shared_ptr<EosCompletion> second;
    ASSERT_TRUE(_load->add_batch(eos(0), &response, &first).ok());
    ASSERT_TRUE(_load->add_batch(eos(1), &response, &second).ok());
    int replies = 0;
    auto callback = [&](const Status& status) {
        EXPECT_TRUE(status.is<ErrorCode::CANCELLED>());
        // Re-enter the load lock; cancellation must invoke callbacks outside it.
        EXPECT_FALSE(_load->is_finished());
        ++replies;
    };
    first->add_waiter(callback);
    EXPECT_TRUE(_load->cancel().ok());
    second->add_waiter(callback);
    EXPECT_TRUE(_load->cancel().ok());
    EXPECT_EQ(replies, 2);
}

TEST_P(LoadChannelEosTest, TimeoutCleanerReleasesPendingRpc) {
    auto channel = make_channel(2);
    PTabletWriterAddBlockResult response;
    std::shared_ptr<EosCompletion> completion;
    ASSERT_TRUE(_load->add_batch(eos(0), &response, &completion).ok());
    LoadChannelMgr manager;
    manager._load_channels.emplace(_load->load_id(), _load);
    _load->_last_updated_time.store(time(nullptr) - 61);
    int replies = 0;
    completion->add_waiter([&](const Status& status) {
        EXPECT_TRUE(status.is<ErrorCode::CANCELLED>());
        EXPECT_TRUE(manager.get_all_load_channel_ids().empty());
        ++replies;
    });
    EXPECT_TRUE(manager._start_load_channels_clean().ok());
    EXPECT_EQ(replies, 1);
    manager.stop();
}

// Control final-close completion independently of the kFinished state, which
// both storage implementations set before flushing/committing writers.
class ControlledCloseChannel : public BaseTabletsChannel {
public:
    explicit ControlledCloseChannel(std::function<Status()> finalize)
            : BaseTabletsChannel(TabletsChannelKey(PUniqueId(), 10), UniqueId(1, 2), true, nullptr),
              _finalize(std::move(finalize)) {}

    std::unique_ptr<BaseDeltaWriter> create_delta_writer(const WriteRequest&) override {
        return nullptr;
    }
    Status add_batch(const PTabletWriterAddBlockRequest&, PTabletWriterAddBlockResult*) override {
        return Status::OK();
    }
    Status close(LoadChannel*, const PTabletWriterAddBlockRequest& request,
                 PTabletWriterAddBlockResult*, bool* finished) override {
        if (request.sender_id() == 0) {
            return Status::OK();
        }
        *finished = true;
        _state = kFinished;
        return _finalize();
    }

private:
    std::function<Status()> _finalize;
};

TEST_P(LoadChannelEosTest, FinishedStateDoesNotReleaseBarrierBeforeCloseReturns) {
    CountDownLatch closing(1);
    CountDownLatch finish_close(1);
    auto channel = std::make_shared<ControlledCloseChannel>([&] {
        closing.count_down();
        finish_close.wait();
        return Status::InternalError("final flush failed");
    });
    _load->_tablets_channels.emplace(10, channel);
    PTabletWriterAddBlockResult first_response;
    std::shared_ptr<EosCompletion> first;
    ASSERT_TRUE(_load->add_batch(eos(0), &first_response, &first).ok());
    std::atomic<int> replies {0};
    first->add_waiter([&](const Status& status) {
        EXPECT_NE(status.to_string().find("final flush failed"), std::string::npos);
        ++replies;
    });
    std::thread finalizer([&] {
        PTabletWriterAddBlockResult response;
        std::shared_ptr<EosCompletion> last;
        auto status = _load->add_batch(eos(1), &response, &last);
        EXPECT_FALSE(status.ok());
    });
    closing.wait();
    EXPECT_TRUE(channel->is_finished());
    EXPECT_EQ(replies.load(), 0);
    finish_close.count_down();
    finalizer.join();
    EXPECT_EQ(replies.load(), 1);
}

TEST_P(LoadChannelEosTest, CancelDuringFinalCloseDoesNotTouchItsResponse) {
    CountDownLatch closing(1);
    CountDownLatch finish_close(1);
    auto channel = std::make_shared<ControlledCloseChannel>([&] {
        closing.count_down();
        finish_close.wait();
        return Status::OK();
    });
    _load->_tablets_channels.emplace(10, channel);
    PTabletWriterAddBlockResult first_response;
    std::shared_ptr<EosCompletion> first;
    ASSERT_TRUE(_load->add_batch(eos(0), &first_response, &first).ok());
    std::atomic<int> replies {0};
    first->add_waiter([&](const Status& status) {
        EXPECT_TRUE(status.is<ErrorCode::CANCELLED>());
        ++replies;
    });
    std::thread finalizer([&] {
        auto response = std::make_unique<PTabletWriterAddBlockResult>();
        std::shared_ptr<EosCompletion> last;
        EXPECT_TRUE(_load->add_batch(eos(1), response.get(), &last).ok());
        // Same ownership boundary as the service: finish all synchronous writes
        // before registering a callback which can destroy the RPC inline.
        response->set_execution_time_us(123);
        last->add_waiter([&](const Status& status) {
            EXPECT_TRUE(status.is<ErrorCode::CANCELLED>());
            EXPECT_EQ(response->execution_time_us(), 123);
            response.reset();
            ++replies;
        });
    });
    closing.wait();
    EXPECT_TRUE(_load->cancel().ok());
    EXPECT_EQ(replies.load(), 1);
    finish_close.count_down();
    finalizer.join();
    EXPECT_EQ(replies.load(), 2);
}

INSTANTIATE_TEST_SUITE_P(LocalAndCloud, LoadChannelEosTest, testing::Bool());

} // namespace doris
