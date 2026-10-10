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

#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "cloud/cloud_delta_writer.h"
#include "cloud/cloud_rowset_builder.h"
#include "cloud/cloud_rowset_writer.h"
#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablets_channel.h"
#include "cpp/sync_point.h"
#include "load/channel/load_channel_mgr.h"
#include "load/channel/tablets_channel.h"
#include "load/delta_writer/delta_writer.h"
#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/thread_context.h"
#include "storage/delete/calc_delete_bitmap_executor.h"
#include "storage/options.h"
#include "storage/rowset/beta_rowset_writer.h"
#include "storage/rowset/group_rowset_writer.h"
#include "storage/rowset_builder.h"
#include "storage/storage_engine.h"
#include "util/countdown_latch.h"
#include "util/defer_op.h"

namespace doris {

// Exercise both local/cloud writers, with and without row binlog, without tablet I/O.
class DeltaWriterCancelTest : public testing::TestWithParam<int> {
protected:
    void SetUp() override {
        auto tracker = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::OTHER,
                                                        "DeltaWriterCancelTest");
        _attach_task = std::make_unique<AttachTask>(tracker);
        ASSERT_TRUE(ThreadPoolBuilder("DeltaWriterCancelTest")
                            .set_min_threads(1)
                            .set_max_threads(1)
                            .build(&_pool)
                            .ok());
        _previous_fragment_mgr = ExecEnv::GetInstance()->_fragment_mgr;
        _fragment_mgr = std::make_unique<FragmentMgr>(ExecEnv::GetInstance());
        ExecEnv::GetInstance()->_fragment_mgr = _fragment_mgr.get();
        _load_channel = std::make_shared<LoadChannel>(UniqueId {}, 60, false, "", 0, false, -1);
        WriteRequest data_req;
        data_req.delete_bitmap_cancellation = _load_channel->_delete_bitmap_cancellation;
        WriteRequest group_req = data_req;
        group_req.write_req_type = WriteRequestType::GROUP;
        WriteRequest binlog_req = data_req;
        binlog_req.write_req_type = WriteRequestType::ROW_BINLOG;
        if (is_cloud()) {
            _cloud_engine = std::make_unique<CloudStorageEngine>(EngineOptions {});
            if (is_group()) {
                _writer = std::make_unique<CloudDeltaWriter>(*_cloud_engine, group_req, data_req,
                                                             binlog_req, nullptr, UniqueId {});
                auto* group = static_cast<CloudGroupRowsetBuilder*>(_writer->_rowset_builder.get());
                _builders = {group->data_builder(), group->row_binlog_builder()};
            } else {
                _writer = std::make_unique<CloudDeltaWriter>(*_cloud_engine, data_req, nullptr,
                                                             UniqueId {});
            }
        } else {
            _local_engine = std::make_unique<StorageEngine>(EngineOptions {});
            if (is_group()) {
                _writer = std::make_unique<DeltaWriter>(*_local_engine, group_req, data_req,
                                                        binlog_req, nullptr, UniqueId {});
                auto* group = static_cast<GroupRowsetBuilder*>(_writer->_rowset_builder.get());
                _builders = {group->txn_rowset_builder(), group->row_binlog_builder()};
            } else {
                _writer = std::make_unique<DeltaWriter>(*_local_engine, data_req, nullptr,
                                                        UniqueId {});
            }
        }
        if (!is_group()) {
            _builders = {_writer->_rowset_builder.get()};
        }
    }

    void TearDown() override {
        // Release the unrelated task before destroying writers, even after a failed assertion.
        _release_worker.count_down();
        if (_pool) {
            _pool->wait();
        }
        _writer.reset();
        _tokens.clear();
        _pool.reset();
        _cloud_engine.reset();
        _local_engine.reset();
        _load_channel.reset();
        if (_fragment_mgr) {
            _fragment_mgr->stop();
            ExecEnv::GetInstance()->_fragment_mgr = _previous_fragment_mgr;
            _fragment_mgr.reset();
        }
        _attach_task.reset();
    }

    bool is_cloud() const { return GetParam() & 1; }
    bool is_group() const { return GetParam() & 2; }

    void install_tokens() {
        for (auto* builder : _builders) {
            std::shared_ptr<BaseBetaRowsetWriter> rowset_writer;
            if (is_cloud()) {
                rowset_writer = std::make_shared<CloudRowsetWriter>(*_cloud_engine);
            } else {
                rowset_writer = std::make_shared<BetaRowsetWriter>(*_local_engine);
            }
            // Set up only the state needed by cancellation and destruction. No files are created.
            rowset_writer->_rowset_meta = std::make_shared<RowsetMeta>();
            rowset_writer->_calc_delete_bitmap_token = make_token();
            builder->_calc_delete_bitmap_token = make_token();
            builder->_rowset_writer = rowset_writer;
            _tokens.push_back(rowset_writer->_calc_delete_bitmap_token.get());
            _tokens.push_back(builder->_calc_delete_bitmap_token.get());
        }
        if (is_group()) {
            auto group_writer = std::make_shared<GroupRowsetWriter>();
            group_writer->set_data_writer(_builders[0]->rowset_writer());
            group_writer->set_row_binlog_writer(_builders[1]->rowset_writer());
            _writer->_rowset_builder->_rowset_writer = std::move(group_writer);
        }
    }

    std::unique_ptr<CalcDeleteBitmapToken> make_token() {
        return std::make_unique<CalcDeleteBitmapToken>(
                _pool->new_token(ThreadPool::ExecutionMode::CONCURRENT),
                _load_channel->_delete_bitmap_cancellation);
    }

    FragmentMgr* _previous_fragment_mgr = nullptr;
    std::unique_ptr<FragmentMgr> _fragment_mgr;
    std::shared_ptr<LoadChannel> _load_channel;
    std::unique_ptr<AttachTask> _attach_task;
    std::unique_ptr<StorageEngine> _local_engine;
    std::unique_ptr<CloudStorageEngine> _cloud_engine;
    std::unique_ptr<ThreadPool> _pool;
    std::unique_ptr<BaseDeltaWriter> _writer;
    std::vector<BaseRowsetBuilder*> _builders;
    std::vector<CalcDeleteBitmapToken*> _tokens;
    CountDownLatch _worker_started {1};
    CountDownLatch _release_worker {1};
    std::atomic<int> _executed {0};
};

TEST_P(DeltaWriterCancelTest, CancelBeforeInit) {
    ASSERT_TRUE(_writer->cancel().ok());
    EXPECT_TRUE(_writer->_is_cancelled);
    EXPECT_TRUE(_writer->_memtable_writer->_is_cancelled);
    for (auto* builder : _builders) {
        EXPECT_TRUE(builder->_is_cancelled);
    }
    EXPECT_TRUE(_writer->cancel().ok());
}

TEST_P(DeltaWriterCancelTest, CancelRemovesBothPhasesBeforeDestruction) {
    install_tokens();
    ASSERT_TRUE(_pool->submit_func([this] {
                         _worker_started.count_down();
                         _release_worker.wait();
                     }).ok());
    ASSERT_TRUE(_worker_started.wait_for(std::chrono::seconds(10)));
    for (auto* token : _tokens) {
        ASSERT_TRUE(token->submit_func([this] {
                             ++_executed;
                             return Status::OK();
                         }).ok());
    }
    ASSERT_EQ(_pool->get_queue_size(), _tokens.size());

    const auto cancelled = Status::Cancelled("load cancelled by sender");
    ASSERT_TRUE(_writer->cancel_with_status(cancelled).ok());
    EXPECT_EQ(_pool->get_queue_size(), 0);
    EXPECT_EQ(_executed.load(), 0);
    EXPECT_EQ(_writer->_memtable_writer->_cancel_status, cancelled);
    for (auto* token : _tokens) {
        EXPECT_EQ(token->wait(), cancelled);
        EXPECT_EQ(token->submit_func([] { return Status::OK(); }), cancelled);
    }
    ASSERT_TRUE(_writer->cancel_with_status(Status::Cancelled("second cancel")).ok());
    for (auto* token : _tokens) {
        EXPECT_EQ(token->wait(), cancelled);
    }

    _release_worker.count_down();
    _pool->wait();
    EXPECT_EQ(_executed.load(), 0);
}

TEST_P(DeltaWriterCancelTest, PreserveEarlierCalculationFailure) {
    install_tokens();
    const auto failure = Status::InternalError("delete bitmap calculation failed");
    ASSERT_TRUE(_tokens.front()->submit_func([failure] { return failure; }).ok());
    ASSERT_EQ(_tokens.front()->wait(), failure);
    for (size_t i = 1; i < _tokens.size(); ++i) {
        EXPECT_TRUE(_tokens[i]->_get_status().ok());
    }
    ASSERT_TRUE(_load_channel->cancel().ok());
    ASSERT_TRUE(_writer->cancel().ok());
    EXPECT_EQ(_tokens.front()->wait(), failure);
    EXPECT_EQ(_tokens.front()->submit_func([] { return Status::OK(); }), failure);
    for (size_t i = 1; i < _tokens.size(); ++i) {
        EXPECT_TRUE(_tokens[i]->wait().is<ErrorCode::CANCELLED>());
    }
}

TEST_P(DeltaWriterCancelTest, PreserveCancellationBeforeRunningCallbackFailure) {
    install_tokens();
    auto* token = _tokens.front();
    const auto later_failure = Status::InternalError("callback failed after cancellation");
    CountDownLatch cancellation_published(1);
    auto* sync_point = SyncPoint::get_instance();
    SyncPoint::CallbackGuard guard;
    sync_point->set_call_back(
            "DeleteBitmapCancellation::cancel:before_shutdown",
            [&](auto&& args) {
                if (try_any_cast<DeleteBitmapCancellation*>(args[0]) ==
                    _load_channel->_delete_bitmap_cancellation.get()) {
                    cancellation_published.count_down();
                }
            },
            &guard);
    sync_point->enable_processing();
    std::future<Status> canceller;
    Defer cleanup {[&] {
        _release_worker.count_down();
        if (canceller.valid()) {
            EXPECT_TRUE(canceller.get().ok());
        }
        _pool->wait();
        sync_point->disable_processing();
    }};
    ASSERT_TRUE(token->submit_func([this, later_failure] {
                         _worker_started.count_down();
                         _release_worker.wait();
                         return later_failure;
                     }).ok());
    ASSERT_TRUE(_worker_started.wait_for(std::chrono::seconds(10)));
    canceller = std::async(std::launch::async, [&] { return _load_channel->cancel(); });
    ASSERT_TRUE(cancellation_published.wait_for(std::chrono::seconds(10)));
    EXPECT_EQ(canceller.wait_for(std::chrono::milliseconds(100)), std::future_status::timeout);
    _release_worker.count_down();
    ASSERT_TRUE(canceller.get().ok());
    const auto cancelled = Status::Cancelled("load channel cancelled");
    EXPECT_EQ(token->wait(), cancelled);
    EXPECT_EQ(token->submit_func([] { return Status::OK(); }), cancelled);
    EXPECT_TRUE(_writer->cancel_with_status(later_failure).ok());
    EXPECT_EQ(token->wait(), cancelled);
}

TEST_P(DeltaWriterCancelTest, PreserveLoadCancellationBeforeExplicitTokenFailure) {
    install_tokens();
    ASSERT_TRUE(_load_channel->cancel().ok());
    const auto later_failure = Status::InternalError("later explicit token failure");
    for (auto* token : _tokens) {
        token->cancel(later_failure);
        EXPECT_EQ(token->wait(), Status::Cancelled("load channel cancelled"));
        EXPECT_EQ(token->submit_func([] { return Status::OK(); }),
                  Status::Cancelled("load channel cancelled"));
    }
}

// Model close holding a tablets-channel/writer/builder lock while waiting for bitmap.
// Load cancellation keeps its own lock, drains bitmap, then cancels the tablets channel.
TEST_P(DeltaWriterCancelTest, LoadCancelDrainsBitmapBeforeTabletsChannelCancellation) {
    install_tokens();
    ASSERT_TRUE(_pool->submit_func([this] {
                         _worker_started.count_down();
                         _release_worker.wait();
                     }).ok());
    ASSERT_TRUE(_worker_started.wait_for(std::chrono::seconds(10)));
    for (auto* token : _tokens) {
        ASSERT_TRUE(token->submit_func([this] {
                             ++_executed;
                             return Status::OK();
                         }).ok());
    }

    PUniqueId id;
    TabletsChannelKey key(id, 1);
    std::shared_ptr<BaseTabletsChannel> tablets_channel;
    if (is_cloud()) {
        tablets_channel = std::make_shared<CloudTabletsChannel>(*_cloud_engine, key, UniqueId {},
                                                                false, nullptr);
    } else {
        tablets_channel =
                std::make_shared<TabletsChannel>(*_local_engine, key, UniqueId {}, false, nullptr);
    }
    tablets_channel->set_delete_bitmap_cancellation(_load_channel->_delete_bitmap_cancellation);
    _load_channel->_tablets_channels.emplace(1, tablets_channel);
    LoadChannelMgr manager;
    Defer stop_manager {[&] { manager.stop(); }};
    manager._load_state_channels = std::make_unique<LoadChannelMgr::LoadStateChannelCache>(16);
    manager._load_channels.emplace(UniqueId {}, _load_channel);

    CountDownLatch locks_held(1);
    auto waiter = std::async(std::launch::async, [&] {
        std::lock_guard channel_lock(tablets_channel->_lock);
        std::unique_lock<std::mutex> local_writer_lock;
        std::unique_lock<bthread::Mutex> cloud_writer_lock;
        if (is_cloud()) {
            cloud_writer_lock =
                    std::unique_lock(static_cast<CloudDeltaWriter*>(_writer.get())->_mtx);
        } else {
            local_writer_lock = std::unique_lock(static_cast<DeltaWriter*>(_writer.get())->_lock);
        }
        std::lock_guard builder_lock(_builders.front()->_lock);
        locks_held.count_down();
        std::vector<Status> statuses;
        for (auto* token : _tokens) {
            statuses.push_back(token->wait());
        }
        return statuses;
    });
    bool entered = locks_held.wait_for(std::chrono::seconds(10));
    EXPECT_TRUE(entered);
    PTabletWriterCancelRequest request;
    *request.mutable_id() = id;
    request.set_cancel_reason("sender cancelled while close waits");
    auto canceller = std::async(std::launch::async, [&] { return manager.cancel(request); });
    auto cancel_ready = canceller.wait_for(std::chrono::seconds(10));
    auto wait_ready = waiter.wait_for(std::chrono::seconds(10));
    // The unrelated worker remains blocked: queued bitmap work must not delay
    // either cancellation or the close waiter.
    EXPECT_EQ(_pool->get_queue_size(), 0);
    // Always unblock the pool before joining, including when the regression reappears.
    _release_worker.count_down();
    EXPECT_EQ(cancel_ready, std::future_status::ready);
    EXPECT_EQ(wait_ready, std::future_status::ready);
    EXPECT_TRUE(canceller.get().ok());
    const auto cancelled = Status::Cancelled("load channel cancelled");
    for (const auto& st : waiter.get()) {
        EXPECT_EQ(st, cancelled);
    }
    EXPECT_EQ(_executed.load(), 0);
    EXPECT_TRUE(manager._load_channels.empty());
    EXPECT_TRUE(_load_channel->cancel().ok());
    EXPECT_EQ(_load_channel->_delete_bitmap_cancellation->status(), cancelled);
    EXPECT_EQ(tablets_channel->_state, BaseTabletsChannel::kFinished);
}

TEST_P(DeltaWriterCancelTest, LoadCancelRemovesQueuedTasksWithoutWait) {
    install_tokens();
    ASSERT_TRUE(_pool->submit_func([this] {
                         _worker_started.count_down();
                         _release_worker.wait();
                     }).ok());
    ASSERT_TRUE(_worker_started.wait_for(std::chrono::seconds(10)));
    for (auto* token : _tokens) {
        ASSERT_TRUE(token->submit_func([this] {
                             ++_executed;
                             return Status::OK();
                         }).ok());
    }
    const auto cancelled = Status::Cancelled("load channel cancelled");
    ASSERT_TRUE(_load_channel->cancel().ok());
    // Cancellation removes queued tasks without waiting for the unrelated worker.
    EXPECT_EQ(_pool->get_queue_size(), 0);
    _release_worker.count_down();
    _pool->wait();
    EXPECT_EQ(_executed.load(), 0);
    auto late_token = make_token();
    EXPECT_EQ(late_token->submit_func([] { return Status::OK(); }), cancelled);
    _load_channel->_delete_bitmap_cancellation->cancel(Status::Cancelled("second cancellation"));
    EXPECT_EQ(_load_channel->_delete_bitmap_cancellation->status(), cancelled);
    for (auto* token : _tokens) {
        EXPECT_EQ(token->wait(), cancelled);
        EXPECT_EQ(token->submit_func([] { return Status::OK(); }), cancelled);
    }
}

TEST_P(DeltaWriterCancelTest, SkipDequeuedTaskAfterCancellationPublication) {
    auto token = make_token();
    ASSERT_TRUE(_pool->submit_func([this] {
                         _worker_started.count_down();
                         _release_worker.wait();
                     }).ok());
    ASSERT_TRUE(_worker_started.wait_for(std::chrono::seconds(10)));
    ASSERT_TRUE(token->submit_func([this] {
                         ++_executed;
                         return Status::OK();
                     }).ok());
    // Model the interval after publication and before this token's shutdown.
    const auto cancelled = Status::Cancelled("published before shutdown");
    _load_channel->_delete_bitmap_cancellation->_status.update(cancelled);
    _release_worker.count_down();
    _pool->wait();
    EXPECT_EQ(_executed.load(), 0);
    EXPECT_EQ(token->wait(), cancelled);
    EXPECT_EQ(token->submit_func([] { return Status::OK(); }), cancelled);
}

TEST_P(DeltaWriterCancelTest, LoadCancellationStillAcquiresLoadLock) {
    install_tokens();
    ASSERT_TRUE(_pool->submit_func([this] {
                         _worker_started.count_down();
                         _release_worker.wait();
                     }).ok());
    ASSERT_TRUE(_worker_started.wait_for(std::chrono::seconds(10)));
    for (auto* token : _tokens) {
        ASSERT_TRUE(token->submit_func([this] {
                             ++_executed;
                             return Status::OK();
                         }).ok());
    }
    std::unique_lock load_lock(_load_channel->_lock);
    auto canceller = std::async(std::launch::async, [&] { return _load_channel->cancel(); });
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (!_load_channel->is_cancelled() && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_TRUE(_load_channel->is_cancelled());
    EXPECT_TRUE(_load_channel->_delete_bitmap_cancellation->ok());
    EXPECT_EQ(_pool->get_queue_size(), _tokens.size());
    EXPECT_EQ(canceller.wait_for(std::chrono::milliseconds(0)), std::future_status::timeout);
    load_lock.unlock();
    const auto ready = canceller.wait_for(std::chrono::seconds(10));
    EXPECT_EQ(ready, std::future_status::ready);
    EXPECT_EQ(_pool->get_queue_size(), 0);
    _release_worker.count_down();
    EXPECT_TRUE(canceller.get().ok());
    EXPECT_EQ(_executed.load(), 0);
}

TEST_P(DeltaWriterCancelTest, RunningTaskFinishesBeforeCancelledWaitReturns) {
    install_tokens();
    ASSERT_TRUE(_tokens.front()
                        ->submit_func([this] {
                            _worker_started.count_down();
                            _release_worker.wait();
                            ++_executed;
                            return Status::OK();
                        })
                        .ok());
    ASSERT_TRUE(_worker_started.wait_for(std::chrono::seconds(10)));
    // This task must never run after the running task is released.
    ASSERT_TRUE(_tokens.front()
                        ->submit_func([this] {
                            ++_executed;
                            return Status::OK();
                        })
                        .ok());
    const auto cancelled = Status::Cancelled("load channel cancelled");
    auto canceller = std::async(std::launch::async, [&] { return _load_channel->cancel(); });
    auto waiter = std::async(std::launch::async, [&] { return _tokens.front()->wait(); });
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (_load_channel->_delete_bitmap_cancellation->ok() &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_FALSE(_load_channel->_delete_bitmap_cancellation->ok());
    EXPECT_EQ(canceller.wait_for(std::chrono::milliseconds(0)), std::future_status::timeout);
    EXPECT_EQ(waiter.wait_for(std::chrono::milliseconds(0)), std::future_status::timeout);
    auto late_submitter = std::async(std::launch::async, [&] {
        auto token = make_token();
        return token->submit_func([] { return Status::OK(); });
    });
    const auto late_ready = late_submitter.wait_for(std::chrono::seconds(10));
    _release_worker.count_down();
    EXPECT_EQ(late_ready, std::future_status::ready);
    EXPECT_EQ(late_submitter.get(), cancelled);
    EXPECT_TRUE(canceller.get().ok());
    EXPECT_EQ(waiter.get(), cancelled);
    EXPECT_EQ(_executed.load(), 1);
}

TEST_P(DeltaWriterCancelTest, TokenDestructionDuringLoadCancellation) {
    auto token = make_token();
    ASSERT_TRUE(token->submit_func([this] {
                         _worker_started.count_down();
                         _release_worker.wait();
                         ++_executed;
                         return Status::OK();
                     }).ok());
    ASSERT_TRUE(_worker_started.wait_for(std::chrono::seconds(10)));
    auto canceller = std::async(std::launch::async, [&] { return _load_channel->cancel(); });
    auto destroyer =
            std::async(std::launch::async, [token = std::move(token)]() mutable { token.reset(); });
    // Both shutdown paths must preserve the callback's state until it finishes.
    EXPECT_EQ(canceller.wait_for(std::chrono::milliseconds(100)), std::future_status::timeout);
    EXPECT_EQ(destroyer.wait_for(std::chrono::milliseconds(100)), std::future_status::timeout);
    _release_worker.count_down();
    EXPECT_TRUE(canceller.get().ok());
    destroyer.get();
    EXPECT_EQ(_executed.load(), 1);
    EXPECT_TRUE(_load_channel->cancel().ok());
}

TEST_P(DeltaWriterCancelTest, CancellationBetweenStatusCheckAndSubmit) {
    auto token = make_token();
    auto* sync_point = SyncPoint::get_instance();
    SyncPoint::CallbackGuard guard;
    sync_point->set_call_back(
            "CalcDeleteBitmapToken::submit_func:before_submit",
            [&](auto&& args) {
                if (try_any_cast<CalcDeleteBitmapToken*>(args[0]) == token.get()) {
                    _worker_started.count_down();
                    _release_worker.wait();
                }
            },
            &guard);
    sync_point->enable_processing();
    auto submitter = std::async(std::launch::async, [&] {
        return token->submit_func([this] {
            ++_executed;
            return Status::OK();
        });
    });
    const bool entered = _worker_started.wait_for(std::chrono::seconds(10));
    EXPECT_TRUE(entered);
    EXPECT_TRUE(_load_channel->cancel().ok());
    _release_worker.count_down();
    EXPECT_EQ(submitter.get(), Status::Cancelled("load channel cancelled"));
    EXPECT_EQ(_executed.load(), 0);
    sync_point->disable_processing();
}

TEST_P(DeltaWriterCancelTest, FailedSubmitPreservesEarlierWrapperFailure) {
    auto token = make_token();
    const auto failure = Status::InternalError("earlier bitmap failure");
    auto* sync_point = SyncPoint::get_instance();
    SyncPoint::CallbackGuard guard;
    sync_point->set_call_back(
            "CalcDeleteBitmapToken::submit_func:before_submit",
            [&](auto&& args) {
                if (try_any_cast<CalcDeleteBitmapToken*>(args[0]) == token.get()) {
                    token->cancel(failure);
                    EXPECT_TRUE(_load_channel->cancel().ok());
                }
            },
            &guard);
    sync_point->enable_processing();
    EXPECT_EQ(token->submit_func([] { return Status::OK(); }), failure);
    sync_point->disable_processing();
}

TEST_P(DeltaWriterCancelTest, FailedSubmitWithoutCancellationPreservesPoolError) {
    auto token = make_token();
    _pool->shutdown();
    const auto pool_error = _pool->submit_func([] {});
    EXPECT_TRUE(pool_error.is<ErrorCode::SERVICE_UNAVAILABLE>());
    EXPECT_EQ(token->submit_func([] { return Status::OK(); }), pool_error);
    EXPECT_TRUE(_load_channel->_delete_bitmap_cancellation->ok());
    EXPECT_TRUE(token->wait().ok());
}

INSTANTIATE_TEST_SUITE_P(LocalAndCloud, DeltaWriterCancelTest, testing::Values(0, 1, 2, 3));

} // namespace doris
