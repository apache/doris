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

#include <bthread/bthread.h>
#include <gtest/gtest.h>

#include <cerrno>
#include <functional>
#include <memory>
#include <thread>
#include <tuple>
#include <utility>

#include "cloud/cloud_delta_writer.h"
#include "cloud/cloud_rowset_builder.h"
#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablets_channel.h"
#include "common/signal_handler.h"
#include "cpp/sync_point.h"
#include "runtime/thread_context.h"
#include "runtime/workload_group/workload_group.h"
#include "runtime/workload_management/resource_context.h"
#include "storage/delete/calc_delete_bitmap_executor.h"
#include "util/threadpool.h"

namespace doris {
namespace {

// Exercise close -> commit_rowset -> _commit_empty_rowset without metadata RPCs.
// Stop after creating and exercising the same bitmap token as the real builder.
class EmptyRowsetContextProbe : public CloudRowsetBuilder {
public:
    using CloudRowsetBuilder::CloudRowsetBuilder;
    Status init() override { return on_init(); }
    std::function<Status()> on_init;
};

} // namespace

class CloudTabletsChannelContextTest : public testing::TestWithParam<std::tuple<bool, bool>> {};

TEST_P(CloudTabletsChannelContextTest, EmptyRowsetCommitInheritsLoadContext) {
    const auto [force_inline, bthread_caller] = GetParam();
    auto* sync_point = SyncPoint::get_instance();
    SyncPoint::CallbackGuard guard;
    if (force_inline) {
        sync_point->set_call_back(
                "bthread_fork_join::start_background",
                [](auto&& args) {
                    auto* result = try_any_cast<std::pair<int, bool>*>(args.back());
                    result->first = EAGAIN;
                    result->second = true;
                },
                &guard);
        sync_point->enable_processing();
    }

    int initialized = 0;
    std::function<void()> run = [&] {
        SCOPED_INIT_THREAD_CONTEXT();
        EXPECT_FALSE(thread_context()->is_attach_task());
        auto ctx = ResourceContext::create_shared();
        auto tracker = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::LOAD,
                                                        "empty_rowset_context_test");
        ctx->memory_context()->set_mem_tracker(tracker);
        TUniqueId task_id;
        task_id.hi = 68385;
        task_id.lo = 67674;
        ctx->task_controller()->set_task_id(task_id);
        auto wg = std::make_shared<WorkloadGroup>(
                WorkloadGroupInfo {.id = 68385, .name = "empty_rowset_context_test"});
        ASSERT_TRUE(ThreadPoolBuilder("empty_rowset_workload_group_test")
                            .set_max_threads(1)
                            .build(&wg->_memtable_flush_pool)
                            .ok());
        ctx->set_workload_group(wg);
        {
            SCOPED_ATTACH_TASK(ctx);
            CloudStorageEngine engine {EngineOptions {}};
            engine.init_calc_delete_bitmap_executor_for_UT();
            WriteRequest req;
            req.txn_id = 123;
            req.tablet_id = 10;
            req.partition_id = 20;
            req.index_id = 30;
            PUniqueId load_id;
            load_id.set_hi(task_id.hi);
            load_id.set_lo(task_id.lo);
            CloudTabletsChannel channel(engine, TabletsChannelKey(load_id, req.index_id),
                                        UniqueId(load_id), false, nullptr);
            channel._num_remaining_senders = 1;
            auto writer =
                    std::make_unique<CloudDeltaWriter>(engine, req, nullptr, UniqueId(load_id));
            ASSERT_FALSE(writer->is_init());
            auto builder = std::make_unique<EmptyRowsetContextProbe>(engine, req, nullptr);
            const auto caller_id = bthread_self();
            const auto expected_error = Status::InternalError("stop after empty rowset token init");
            builder->on_init = [&]() -> Status {
                ++initialized;
                EXPECT_EQ(bthread_self() == caller_id, force_inline);
                // Fail cleanly on the unfixed code even when a fresh bthread has no TLS.
                SCOPED_INIT_THREAD_CONTEXT();
                EXPECT_TRUE(thread_context()->is_attach_task());
                if (!thread_context()->is_attach_task()) {
                    return Status::InternalError("empty rowset commit lost its load context");
                }
                EXPECT_EQ(thread_context()->resource_ctx(), ctx);
                EXPECT_EQ(thread_context()->thread_mem_tracker_mgr->limiter_mem_tracker(),
                          tracker.get());
                EXPECT_EQ(signal::query_id_hi, task_id.hi);
                EXPECT_EQ(signal::query_id_lo, task_id.lo);
                auto token = engine.calc_delete_bitmap_executor()->create_load_token(
                        req.txn_id, LoadTaskPriority::HIGH, LoadTaskType::LEAF);
                EXPECT_EQ(token->_thread_token->_pool, wg->get_memtable_flush_pool());
                EXPECT_EQ(token->_thread_token->_load_id, req.txn_id);
                RETURN_IF_ERROR(token->submit_func([&] {
                    EXPECT_EQ(thread_context()->resource_ctx(), ctx);
                    EXPECT_EQ(thread_context()->thread_mem_tracker_mgr->limiter_mem_tracker(),
                              tracker.get());
                    return Status::OK();
                }));
                RETURN_IF_ERROR(token->wait());
                return expected_error;
            };
            writer->_rowset_builder = std::move(builder);
            channel._tablet_writers.emplace(req.tablet_id, std::move(writer));
            ++BaseTabletsChannel::_s_tablet_writer_count;

            PTabletWriterAddBlockRequest close_req;
            close_req.set_sender_id(0);
            close_req.add_partition_ids(req.partition_id);
            PTabletWriterAddBlockResult result;
            bool finished = false;
            EXPECT_EQ(channel.close(nullptr, close_req, &result, &finished), expected_error);
            EXPECT_TRUE(finished);
            // The synchronous fallback must not detach or replace the parent's context.
            EXPECT_EQ(thread_context()->resource_ctx(), ctx);
            EXPECT_EQ(thread_context()->thread_mem_tracker_mgr->limiter_mem_tracker(),
                      tracker.get());
            EXPECT_EQ(signal::query_id_hi, task_id.hi);
            EXPECT_EQ(signal::query_id_lo, task_id.lo);
        }
        EXPECT_FALSE(thread_context()->is_attach_task());
    };
    if (bthread_caller) {
        bthread_t id;
        const int start_result = bthread_start_background(
                &id, nullptr,
                [](void* arg) -> void* {
                    (*static_cast<std::function<void()>*>(arg))();
                    return nullptr;
                },
                &run);
        EXPECT_EQ(start_result, 0);
        if (start_result == 0) {
            EXPECT_EQ(bthread_join(id, nullptr), 0);
        }
    } else {
        std::thread caller(run);
        caller.join();
    }
    if (force_inline) {
        sync_point->disable_processing();
    }
    EXPECT_EQ(initialized, 1);
}

INSTANTIATE_TEST_SUITE_P(AsyncAndFallback, CloudTabletsChannelContextTest,
                         testing::Combine(testing::Bool(), testing::Bool()));

TEST(CloudBitmapTokenLifetimeTest, CancellationRetainsWorkloadGroupUntilTokenRelease) {
    auto wg = std::make_shared<WorkloadGroup>(
            WorkloadGroupInfo {.id = 67674, .name = "cancelled_bitmap_pool_lifetime"});
    ASSERT_TRUE(ThreadPoolBuilder("cancelled_bitmap_pool_lifetime")
                        .set_max_threads(1)
                        .build(&wg->_memtable_flush_pool)
                        .ok());
    auto cancellation = std::make_shared<DeleteBitmapCancellation>();
    auto token = std::make_unique<CalcDeleteBitmapToken>(
            wg->_memtable_flush_pool->new_load_token(1, LoadTaskPriority::MID, LoadTaskType::LEAF),
            wg, false, cancellation);
    // Model cancellation retaining the token while its writer is destroyed.
    auto retained = cancellation->_tokens.front().lock();
    ASSERT_NE(retained, nullptr);
    std::weak_ptr<WorkloadGroup> weak_wg = wg;
    wg.reset();
    token.reset();
    EXPECT_FALSE(weak_wg.expired());
    retained->shutdown();
    retained.reset();
    // The cancellation registry still contains a weak token reference.
    EXPECT_TRUE(weak_wg.expired());
}

} // namespace doris
