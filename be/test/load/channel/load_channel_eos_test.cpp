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
#include <memory>
#include <string>
#include <thread>
#include <utility>

#include "agent/be_exec_version_manager.h"
#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablets_channel.h"
#include "core/block/block.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "cpp/sync_point.h"
#include "load/channel/eos_completion.h"
#include "load/channel/load_channel.h"
#include "load/channel/load_channel_mgr.h"
#include "load/channel/tablets_channel.h"
#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"
#include "storage/storage_engine.h"
#include "util/countdown_latch.h"
#include "util/defer_op.h"

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
        channel->_next_seqs.resize(senders);
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

    static PTabletWriterAddBlockRequest eos_with_block(int sender) {
        auto request = eos(sender);
        request.set_packet_seq(7);
        request.add_tablet_ids(123);
        auto column = ColumnInt32::create();
        column->insert_value(42);
        Block block {{std::move(column), std::make_shared<DataTypeInt32>(), "value"}};
        size_t uncompressed_bytes = 0;
        size_t compressed_bytes = 0;
        int64_t compress_time = 0;
        EXPECT_TRUE(block.serialize(BeExecVersionManager::get_newest_version(),
                                    request.mutable_block(), &uncompressed_bytes, &compressed_bytes,
                                    &compress_time, segment_v2::CompressionTypePB::SNAPPY)
                            .ok());
        return request;
    }

    void check_final_sender_retry(bool with_block, bool close_fails) {
        auto channel = make_channel(2);
        auto request = with_block ? eos_with_block(1) : eos(1);
        // Model the packet already accepted by add_batch before entering EOS
        // accounting. No writer is installed: reaching the writer lookup on a
        // retry would fail with "unknown tablet" instead of succeeding silently.
        channel->_next_seqs[1] = 8;
        PTabletWriterAddBlockResult first_response;
        std::shared_ptr<EosCompletion> first;
        ASSERT_TRUE(_load->add_batch(eos(0), &first_response, &first).ok());

        CountDownLatch arrival_callback(1);
        CountDownLatch finish_callback(1);
        CountDownLatch retry_waiting(1);
        first->add_waiter([&](const Status& status) {
            EXPECT_TRUE(status.ok());
            arrival_callback.count_down();
            finish_callback.wait();
        });
        auto* sp = SyncPoint::get_instance();
        SyncPoint::CallbackGuard wait_guard;
        SyncPoint::CallbackGuard close_guard;
        sp->set_call_back(
                "BaseTabletsChannel::close.wait_for_final_result",
                [&](auto&&) { retry_waiting.count_down(); }, &wait_guard);
        int final_closes = 0;
        sp->set_call_back(
                before_flush_sync_point(),
                [&](auto&& args) {
                    ++final_closes;
                    auto* response = try_any_cast<PTabletWriterAddBlockResult*>(args[0]);
                    auto* tablet = response->add_tablet_vec();
                    tablet->set_tablet_id(123);
                    tablet->set_schema_hash(0);
                    tablet->set_received_rows(1);
                    if (close_fails) {
                        auto* ret = try_any_cast_ret<Status>(args);
                        ret->first = Status::InternalError("final commit failed");
                        ret->second = true;
                    }
                },
                &close_guard);
        sp->enable_processing();
        Defer disable_sync_points {[&] { sp->disable_processing(); }};

        Status final_status;
        PTabletWriterAddBlockResult final_response;
        std::thread finalizer([&] {
            std::shared_ptr<EosCompletion> completion;
            final_status = _load->add_batch(request, &final_response, &completion);
        });
        bool arrived = arrival_callback.wait_for(std::chrono::seconds(10));
        EXPECT_TRUE(arrived);
        Status retry_status;
        PTabletWriterAddBlockResult retry_response;
        std::atomic<bool> retry_returned {false};
        std::thread retry([&] {
            std::shared_ptr<EosCompletion> completion;
            retry_status = _load->add_batch(request, &retry_response, &completion);
            retry_returned = true;
        });
        // The finalizer is still inside an arrival callback, with _lock released.
        // Require the retry to actually enter this window before allowing close.
        EXPECT_TRUE(retry_waiting.wait_for(std::chrono::seconds(10)));
        EXPECT_FALSE(retry_returned.load());
        finish_callback.count_down();
        finalizer.join();
        retry.join();
        EXPECT_TRUE(retry_returned.load());
        EXPECT_EQ(final_closes, 1);
        EXPECT_EQ(channel->_num_remaining_senders, 0);
        EXPECT_EQ(channel->_next_seqs[1], 8);
        EXPECT_EQ(final_status.ok(), !close_fails);
        EXPECT_EQ(retry_status.to_string(), final_status.to_string());
        EXPECT_EQ(retry_response.tablet_errors_size(), 0);
        ASSERT_EQ(retry_response.tablet_vec_size(), 1);
        EXPECT_EQ(retry_response.tablet_vec(0).tablet_id(), 123);
        EXPECT_EQ(retry_response.tablet_vec(0).received_rows(), 1);
        if (close_fails) {
            EXPECT_NE(retry_status.to_string().find("final commit failed"), std::string::npos);
        }
    }

    const char* before_flush_sync_point() const {
        return GetParam() ? "CloudTabletsChannel::close.before_flush"
                          : "TabletsChannel::close.before_flush";
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
        // remaining count twice, and both RPCs must wait for all senders to arrive.
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
    bool reached_close = false;
    auto* sp = SyncPoint::get_instance();
    SyncPoint::CallbackGuard guard;
    sp->set_call_back(
            before_flush_sync_point(),
            [&](auto&&) {
                reached_close = true;
                EXPECT_EQ(replies, 1);
            },
            &guard);
    sp->enable_processing();
    Defer disable_sync_points {[&] { sp->disable_processing(); }};
    std::shared_ptr<EosCompletion> last;
    EXPECT_TRUE(_load->add_batch(eos(1, false), &response, &last).ok());
    EXPECT_TRUE(reached_close);
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

// Exercise the real local/cloud close paths. Hold the final closer immediately
// before writer flushing, after sender accounting and barrier publication.
TEST_P(LoadChannelEosTest, ArrivalReleasesWaitersBeforeFinalCloseFailure) {
    auto channel = make_channel(3);
    CountDownLatch closing(1);
    CountDownLatch finish_close(1);
    auto* sp = SyncPoint::get_instance();
    SyncPoint::CallbackGuard guard;
    sp->set_call_back(
            before_flush_sync_point(),
            [&](auto&& args) {
                closing.count_down();
                finish_close.wait();
                auto* ret = try_any_cast_ret<Status>(args);
                ret->first = Status::InternalError("final flush failed");
                ret->second = true;
            },
            &guard);
    sp->enable_processing();
    Defer disable_sync_points {[&] { sp->disable_processing(); }};

    PTabletWriterAddBlockResult first_response;
    PTabletWriterAddBlockResult second_response;
    std::shared_ptr<EosCompletion> first;
    std::shared_ptr<EosCompletion> second;
    ASSERT_TRUE(_load->add_batch(eos(0), &first_response, &first).ok());
    ASSERT_TRUE(_load->add_batch(eos(1), &second_response, &second).ok());
    std::atomic<int> replies {0};
    auto reply = [&](const Status& status) {
        EXPECT_TRUE(status.ok());
        ++replies;
    };
    first->add_waiter(reply);
    std::atomic<bool> final_rpc_returned {false};
    std::thread finalizer([&] {
        PTabletWriterAddBlockResult response;
        std::shared_ptr<EosCompletion> last;
        auto status = _load->add_batch(eos(2), &response, &last);
        EXPECT_FALSE(status.ok());
        EXPECT_NE(status.to_string().find("final flush failed"), std::string::npos);
        // The service returns this error directly instead of registering the
        // final RPC on the already successful arrival barrier.
        final_rpc_returned = true;
    });
    bool reached_close = closing.wait_for(std::chrono::seconds(10));
    EXPECT_TRUE(reached_close);
    if (reached_close) {
        EXPECT_TRUE(channel->is_finished());
        EXPECT_FALSE(_load->is_finished());
        EXPECT_FALSE(final_rpc_returned.load());
        EXPECT_EQ(replies.load(), 1);
        // A sender may finish its response writes after the last EOS arrived.
        second->add_waiter(reply);
        EXPECT_EQ(replies.load(), 2);
    }
    finish_close.count_down();
    finalizer.join();
    EXPECT_TRUE(final_rpc_returned.load());
    EXPECT_TRUE(_load->cancel().ok());
    // Neither the later close failure nor cancellation can revoke arrival.
    first->add_waiter(reply);
    EXPECT_EQ(replies.load(), 3);
}

TEST_P(LoadChannelEosTest, FinalRpcKeepsItsResponseUntilCloseReturns) {
    auto channel = make_channel(2);
    auto first_response = std::make_unique<PTabletWriterAddBlockResult>();
    std::shared_ptr<EosCompletion> first;
    ASSERT_TRUE(_load->add_batch(eos(0), first_response.get(), &first).ok());
    int replies = 0;
    first->add_waiter([&](const Status& status) {
        EXPECT_TRUE(status.ok());
        first_response.reset();
        ++replies;
    });

    bool reached_close = false;
    auto* sp = SyncPoint::get_instance();
    SyncPoint::CallbackGuard guard;
    sp->set_call_back(
            before_flush_sync_point(),
            [&](auto&& args) {
                reached_close = true;
                EXPECT_EQ(replies, 1);
                EXPECT_EQ(first_response, nullptr);
                EXPECT_FALSE(_load->is_finished());
                // Model tablet results written by the final closer after all
                // earlier RPCs have already been released.
                auto* response = try_any_cast<PTabletWriterAddBlockResult*>(args[0]);
                auto* tablet = response->add_tablet_vec();
                tablet->set_tablet_id(123);
                tablet->set_schema_hash(0);
                tablet->set_received_rows(456);
                tablet->set_num_rows_filtered(7);
            },
            &guard);
    sp->enable_processing();
    Defer disable_sync_points {[&] { sp->disable_processing(); }};

    auto response = std::make_unique<PTabletWriterAddBlockResult>();
    std::shared_ptr<EosCompletion> last;
    ASSERT_TRUE(_load->add_batch(eos(1), response.get(), &last).ok());
    EXPECT_TRUE(reached_close);
    EXPECT_TRUE(_load->is_finished());
    // Same ownership boundary as the service: finish synchronous writes before
    // registering the final callback, which can destroy the response inline.
    response->set_execution_time_us(123);
    last->add_waiter([&](const Status& status) {
        EXPECT_TRUE(status.ok());
        EXPECT_EQ(response->execution_time_us(), 123);
        ASSERT_EQ(response->tablet_vec_size(), 1);
        EXPECT_EQ(response->tablet_vec(0).tablet_id(), 123);
        EXPECT_EQ(response->tablet_vec(0).received_rows(), 456);
        EXPECT_EQ(response->tablet_vec(0).num_rows_filtered(), 7);
        response.reset();
        ++replies;
    });
    EXPECT_EQ(response, nullptr);
    EXPECT_EQ(replies, 2);
}

TEST_P(LoadChannelEosTest, ArrivalCallbacksCanReenterCloseAndCancel) {
    auto channel = make_channel(2);
    PTabletWriterAddBlockResult response;
    std::shared_ptr<EosCompletion> first;
    ASSERT_TRUE(_load->add_batch(eos(0), &response, &first).ok());
    int replies = 0;
    first->add_waiter([&](const Status& status) {
        EXPECT_TRUE(status.ok());
        EXPECT_FALSE(_load->is_finished());
        ++replies;
        // Re-enter both load/channel locks and register an inline waiter. The
        // final closer must already be elected, but must not hold either lock.
        PTabletWriterAddBlockResult duplicate_response;
        std::shared_ptr<EosCompletion> duplicate;
        EXPECT_TRUE(_load->add_batch(eos(0), &duplicate_response, &duplicate).ok());
        EXPECT_EQ(channel->_num_remaining_senders, 0);
        duplicate->add_waiter([&](const Status& duplicate_status) {
            EXPECT_TRUE(duplicate_status.ok());
            ++replies;
        });
        EXPECT_TRUE(_load->cancel().ok());
    });
    int final_closes = 0;
    auto* sp = SyncPoint::get_instance();
    SyncPoint::CallbackGuard guard;
    sp->set_call_back(
            before_flush_sync_point(), [&](auto&&) { ++final_closes; }, &guard);
    sp->enable_processing();
    Defer disable_sync_points {[&] { sp->disable_processing(); }};

    std::shared_ptr<EosCompletion> last;
    EXPECT_TRUE(_load->add_batch(eos(1), &response, &last).ok());
    last->add_waiter([&](const Status& status) {
        EXPECT_TRUE(status.ok());
        ++replies;
    });
    EXPECT_EQ(replies, 3);
    EXPECT_EQ(final_closes, 1);
    EXPECT_TRUE(_load->is_cancelled());
    EXPECT_TRUE(_load->is_finished());
}

TEST_P(LoadChannelEosTest, FinalSenderRetryWaitsForCommitFailure) {
    check_final_sender_retry(false, true);
}

TEST_P(LoadChannelEosTest, FinalSenderRetryReceivesTabletResults) {
    check_final_sender_retry(false, false);
}

TEST_P(LoadChannelEosTest, FinalSenderBlockRetryWaitsForCommitFailure) {
    check_final_sender_retry(true, true);
}

TEST_P(LoadChannelEosTest, FinalSenderBlockRetryReceivesTabletResults) {
    check_final_sender_retry(true, false);
}

TEST_P(LoadChannelEosTest, EarlierSenderBlockRetryDoesNotWriteDuringArrivalCallback) {
    auto channel = make_channel(2);
    channel->_next_seqs[0] = 8;
    auto request = eos_with_block(0);
    PTabletWriterAddBlockResult response;
    std::shared_ptr<EosCompletion> first;
    ASSERT_TRUE(_load->add_batch(request, &response, &first).ok());
    int replies = 0;
    first->add_waiter([&](const Status& status) {
        EXPECT_TRUE(status.ok());
        PTabletWriterAddBlockResult retry_response;
        std::shared_ptr<EosCompletion> retry;
        auto retry_status = _load->add_batch(request, &retry_response, &retry);
        EXPECT_TRUE(retry_status.ok()) << retry_status;
        EXPECT_EQ(retry_response.tablet_errors_size(), 0);
        EXPECT_EQ(channel->_next_seqs[0], 8);
        ASSERT_NE(retry, nullptr);
        retry->add_waiter([&](const Status& st) {
            EXPECT_TRUE(st.ok());
            ++replies;
        });
    });
    std::shared_ptr<EosCompletion> last;
    EXPECT_TRUE(_load->add_batch(eos(1), &response, &last).ok());
    EXPECT_EQ(replies, 1);
}

TEST_P(LoadChannelEosTest, PacketSequenceAdmission) {
    auto channel = make_channel(1);
    channel->_next_seqs[0] = 8;
    auto request = eos_with_block(0);
    int64_t current_seq = 0;
    bool should_write = true;
    ASSERT_TRUE(channel->_get_current_seq(current_seq, request, should_write).ok());
    EXPECT_FALSE(should_write);
    EXPECT_EQ(current_seq, 8);
    request.set_packet_seq(8);
    ASSERT_TRUE(channel->_get_current_seq(current_seq, request, should_write).ok());
    EXPECT_TRUE(should_write);
    request.set_packet_seq(9);
    EXPECT_FALSE(channel->_get_current_seq(current_seq, request, should_write).ok());
    EXPECT_FALSE(should_write);
    EXPECT_TRUE(channel->cancel().ok());
    request.set_packet_seq(8);
    ASSERT_TRUE(channel->_get_current_seq(current_seq, request, should_write).ok());
    EXPECT_FALSE(should_write);
}

INSTANTIATE_TEST_SUITE_P(LocalAndCloud, LoadChannelEosTest, testing::Bool());

} // namespace doris
