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
#include <string>
#include <vector>

#include "common/signal_handler.h"
#include "runtime/thread_context.h"
#include "runtime/workload_management/resource_context.h"
#include "storage/delete/calc_delete_bitmap_executor.h"
#include "util/countdown_latch.h"
#include "util/defer_op.h"
#include "util/threadpool.h"

namespace doris {
using namespace std::chrono_literals;

TEST(LoadThreadPoolTest, GlobalPriorityAndFifoAcrossTokens) {
    std::unique_ptr<ThreadPool> pool;
    ASSERT_TRUE(ThreadPoolBuilder("load_priority_test").set_max_threads(1).build(&pool).ok());
    auto flush = pool->new_load_token(LoadTaskPriority::LOW);
    auto bitmap = pool->new_load_token(LoadTaskPriority::HIGHEST);
    auto dup = pool->new_load_token(LoadTaskPriority::HIGH);
    auto write_end = pool->new_load_token(LoadTaskPriority::HIGH);
    auto write_time = pool->new_load_token(LoadTaskPriority::MID);
    CountDownLatch entered(1), release(1);
    std::vector<int> order;
    Defer unblock = [&] { release.count_down(); };
    EXPECT_TRUE(pool->submit_func([&] {
                        entered.count_down();
                        release.wait();
                    }).ok());
    EXPECT_TRUE(entered.wait_for(5s));
    EXPECT_TRUE(flush->submit_func([&] { order.push_back(3); }).ok());
    EXPECT_TRUE(bitmap->submit_func([&] { order.push_back(0); }).ok());
    EXPECT_TRUE(write_time->submit_func([&] { order.push_back(2); }).ok());
    EXPECT_TRUE(dup->submit_func([&] { order.push_back(10); }).ok());
    EXPECT_TRUE(dup->submit_func([&] { order.push_back(11); }).ok());
    EXPECT_TRUE(write_end->submit_func([&] { order.push_back(12); }).ok());
    release.count_down();
    pool->wait();
    EXPECT_EQ(order, (std::vector<int> {0, 10, 11, 12, 2, 3}));
}

TEST(LoadThreadPoolTest, OneTokenCanUseAllWorkers) {
    std::unique_ptr<ThreadPool> pool;
    ASSERT_TRUE(ThreadPoolBuilder("load_parallel_test").set_max_threads(2).build(&pool).ok());
    auto token = pool->new_load_token(LoadTaskPriority::LOW);
    CountDownLatch entered(2), release(1);
    Defer unblock = [&] { release.count_down(); };
    for (int i = 0; i < 2; ++i) {
        EXPECT_TRUE(token->submit_func([&] {
                             entered.count_down();
                             release.wait();
                         }).ok());
    }
    EXPECT_TRUE(entered.wait_for(5s));
    release.count_down();
    token->wait();
}

TEST(LoadThreadPoolTest, CancelOnlyRemovesItsOwnTasks) {
    std::unique_ptr<ThreadPool> pool;
    ASSERT_TRUE(ThreadPoolBuilder("load_cancel_test").set_max_threads(1).build(&pool).ok());
    auto cancelled = pool->new_load_token(LoadTaskPriority::MID);
    auto kept = pool->new_load_token(LoadTaskPriority::MID);
    CountDownLatch entered(1), release(1);
    int completed = 0;
    Defer unblock = [&] { release.count_down(); };
    EXPECT_TRUE(pool->submit_func([&] {
                        entered.count_down();
                        release.wait();
                    }).ok());
    EXPECT_TRUE(entered.wait_for(5s));
    EXPECT_TRUE(cancelled->submit_func([&] { ADD_FAILURE() << "cancelled task ran"; }).ok());
    EXPECT_TRUE(kept->submit_func([&] { ++completed; }).ok());
    cancelled->shutdown();
    EXPECT_FALSE(cancelled->submit_func([] {}).ok());
    release.count_down();
    kept->wait();
    EXPECT_EQ(completed, 1);
}

TEST(LoadThreadPoolTest, NestedBitmapRunsInlineWithOneWorker) {
    std::unique_ptr<ThreadPool> pool;
    ASSERT_TRUE(ThreadPoolBuilder("load_nested_test").set_max_threads(1).build(&pool).ok());
    CalcDeleteBitmapExecutor executor;
    executor.init("background_bitmap_test", 1, pool.get());
    auto resource_ctx = ResourceContext::create_shared();
    auto request_tracker =
            MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::OTHER, "load_nested_request");
    auto tablet_tracker =
            MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::OTHER, "load_nested_tablet");
    resource_ctx->memory_context()->set_mem_tracker(request_tracker);
    TUniqueId task_id;
    task_id.hi = 1;
    task_id.lo = 2;
    resource_ctx->task_controller()->set_task_id(task_id);
    SCOPED_ATTACH_TASK(resource_ctx);
    auto parent = executor.create_load_token(LoadTaskPriority::HIGHEST, nullptr, nullptr);
    std::atomic<int> completed = 0;
    EXPECT_TRUE(
            parent->submit_func([&] {
                      EXPECT_EQ(thread_context()->resource_ctx(), resource_ctx);
                      EXPECT_EQ(thread_context()->thread_mem_tracker_mgr->limiter_mem_tracker(),
                                request_tracker.get());
                      // Match the cloud tablet handler's tracker switch inside an
                      // already attached bitmap callback.
                      SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(tablet_tracker);
                      auto check_context = [&] {
                          EXPECT_TRUE(thread_context()->is_attach_task());
                          EXPECT_EQ(thread_context()->resource_ctx(), resource_ctx);
                          EXPECT_EQ(signal::query_id_hi, task_id.hi);
                          EXPECT_EQ(signal::query_id_lo, task_id.lo);
                          EXPECT_EQ(thread_context()->thread_mem_tracker_mgr->limiter_mem_tracker(),
                                    tablet_tracker.get());
                      };
                      auto child = executor.create_load_token(LoadTaskPriority::HIGHEST, nullptr,
                                                              nullptr);
                      for (int i = 0; i < 2; ++i) {
                          EXPECT_TRUE(child->submit_func([&] {
                                               check_context();
                                               ++completed;
                                               return Status::OK();
                                           }).ok());
                          EXPECT_EQ(completed.load(), i + 1);
                          check_context();
                      }
                      EXPECT_TRUE(child->submit_func([&] {
                                           check_context();
                                           ++completed;
                                           return Status::InternalError("test bitmap failure");
                                       }).ok());
                      EXPECT_FALSE(child->wait().ok());
                      check_context();
                      return Status::OK();
                  }).ok());
    EXPECT_TRUE(parent->wait().ok());
    EXPECT_EQ(completed.load(), 3);
}

TEST(LoadThreadPoolTest, InlineBitmapObservesLoadCancellation) {
    std::unique_ptr<ThreadPool> pool;
    ASSERT_TRUE(ThreadPoolBuilder("load_inline_cancel_test").set_max_threads(1).build(&pool).ok());
    CalcDeleteBitmapExecutor executor;
    executor.init("background_inline_cancel_test", 1, pool.get());
    auto cancellation = std::make_shared<DeleteBitmapCancellation>();
    SCOPED_ATTACH_TASK(MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::OTHER,
                                                        "load_inline_cancel_test"));
    auto parent = executor.create_load_token(LoadTaskPriority::HIGHEST, nullptr, nullptr);
    const auto reason = Status::Cancelled("cancel inline bitmap work");
    EXPECT_TRUE(parent->submit_func([&] {
                          auto child =
                                  executor.create_load_token(LoadTaskPriority::MID, cancellation);
                          EXPECT_TRUE(child->submit_func([] { return Status::OK(); }).ok());
                          cancellation->cancel(reason);
                          EXPECT_EQ(child->submit_func([] {
                              ADD_FAILURE() << "cancelled inline bitmap callback ran";
                              return Status::OK();
                          }),
                                    reason);
                          EXPECT_EQ(child->wait(), reason);
                          auto late_child =
                                  executor.create_load_token(LoadTaskPriority::MID, cancellation);
                          EXPECT_EQ(late_child->wait(), reason);
                          return Status::OK();
                      }).ok());
    EXPECT_TRUE(parent->wait().ok());
}

TEST(LoadThreadPoolTest, LoadCancellationRemovesPrioritizedBitmapWork) {
    std::unique_ptr<ThreadPool> pool;
    ASSERT_TRUE(ThreadPoolBuilder("load_bitmap_cancel_test").set_max_threads(1).build(&pool).ok());
    CalcDeleteBitmapExecutor executor;
    executor.init("background_bitmap_cancel_test", 1, pool.get());
    auto cancellation = std::make_shared<DeleteBitmapCancellation>();
    SCOPED_ATTACH_TASK(MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::OTHER,
                                                        "load_bitmap_cancel_test"));
    auto token = executor.create_load_token(LoadTaskPriority::MID, nullptr, cancellation);
    CountDownLatch entered(1), release(1);
    Defer unblock = [&] { release.count_down(); };
    EXPECT_TRUE(pool->submit_func([&] {
                        entered.count_down();
                        release.wait();
                    }).ok());
    EXPECT_TRUE(entered.wait_for(5s));
    EXPECT_TRUE(token->submit_func([] {
                         ADD_FAILURE() << "cancelled prioritized bitmap callback ran";
                         return Status::OK();
                     }).ok());
    const auto reason = Status::Cancelled("cancel queued bitmap work");
    cancellation->cancel(reason);
    EXPECT_EQ(token->wait(), reason);
    EXPECT_EQ(token->submit_func([] { return Status::OK(); }), reason);
    release.count_down();
    pool->wait();
}

TEST(LoadThreadPoolTest, CancelledBitmapIsNotReportedAsComplete) {
    std::unique_ptr<ThreadPool> pool;
    ASSERT_TRUE(ThreadPoolBuilder("load_shutdown_test").set_max_threads(1).build(&pool).ok());
    CalcDeleteBitmapExecutor executor;
    executor.init("background_shutdown_test", 1, pool.get());
    auto token = executor.create_load_token(LoadTaskPriority::MID, nullptr, nullptr);
    CountDownLatch entered(1), release(1);
    Defer unblock = [&] { release.count_down(); };
    EXPECT_TRUE(pool->submit_func([&] {
                        entered.count_down();
                        release.wait();
                    }).ok());
    EXPECT_TRUE(entered.wait_for(5s));
    EXPECT_TRUE(token->submit_func([] { return Status::OK(); }).ok());
    token->cancel();
    EXPECT_TRUE(token->wait().is<ErrorCode::CANCELLED>());
    release.count_down();
    pool->shutdown();
    EXPECT_FALSE(token->submit_func([] { return Status::OK(); }).ok());
}

TEST(LoadThreadPoolTest, BitmapSubmissionFailureSurvivesWait) {
    std::unique_ptr<ThreadPool> pool;
    ASSERT_TRUE(ThreadPoolBuilder("bitmap_capacity_test")
                        .set_max_threads(1)
                        .set_max_queue_size(1)
                        .build(&pool)
                        .ok());
    CalcDeleteBitmapToken token(pool->new_load_token(LoadTaskPriority::MID));
    SCOPED_ATTACH_TASK(MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::OTHER,
                                                        "bitmap_capacity_test"));
    CountDownLatch entered(1), release(1);
    std::atomic<int> completed = 0;
    Defer unblock = [&] { release.count_down(); };
    EXPECT_TRUE(pool->submit_func([&] {
                        entered.count_down();
                        release.wait();
                    }).ok());
    EXPECT_TRUE(entered.wait_for(5s));
    EXPECT_TRUE(token.submit_func([&] {
                         ++completed;
                         return Status::OK();
                     }).ok());
    auto rejected = token.submit_func([] {
        ADD_FAILURE() << "rejected bitmap callback ran";
        return Status::OK();
    });
    EXPECT_TRUE(rejected.is<ErrorCode::SERVICE_UNAVAILABLE>());
    EXPECT_NE(rejected.to_string().find("at capacity"), std::string::npos);
    release.count_down();
    EXPECT_EQ(token.wait().to_string(), rejected.to_string());
    // The existing cancellation wrapper skips queued callbacks after a token failure.
    EXPECT_EQ(completed.load(), 0);
    // The token remains failed even after the queue has drained.
    EXPECT_EQ(token.submit_func([] { return Status::OK(); }).to_string(), rejected.to_string());
}

TEST(LoadThreadPoolTest, BitmapSubmissionAfterShutdownPreservesReason) {
    std::unique_ptr<ThreadPool> pool;
    ASSERT_TRUE(ThreadPoolBuilder("bitmap_rejected_test").set_max_threads(1).build(&pool).ok());
    CalcDeleteBitmapToken token(pool->new_load_token(LoadTaskPriority::HIGHEST));
    SCOPED_ATTACH_TASK(MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::OTHER,
                                                        "bitmap_rejected_test"));
    pool->shutdown();
    auto rejected = token.submit_func([] {
        ADD_FAILURE() << "shutdown pool accepted bitmap callback";
        return Status::OK();
    });
    EXPECT_TRUE(rejected.is<ErrorCode::SERVICE_UNAVAILABLE>());
    EXPECT_NE(rejected.to_string().find("shut down"), std::string::npos);
    EXPECT_EQ(token.wait().to_string(), rejected.to_string());
}

TEST(LoadThreadPoolTest, FlushCleanupCanJoinRunningBitmapLeaves) {
    std::unique_ptr<ThreadPool> pool;
    ASSERT_TRUE(ThreadPoolBuilder("load_cleanup_test").set_max_threads(2).build(&pool).ok());
    auto leaf = pool->new_load_token(LoadTaskPriority::MID);
    auto parent = pool->new_load_token(LoadTaskPriority::LOW);
    CountDownLatch leaf_entered(1), parent_entered(1), release(1);
    Defer unblock = [&] { release.count_down(); };
    EXPECT_TRUE(leaf->submit_func([&] {
                        leaf_entered.count_down();
                        release.wait();
                    }).ok());
    EXPECT_TRUE(leaf_entered.wait_for(5s));
    EXPECT_TRUE(parent->submit_func([&] {
                          parent_entered.count_down();
                          leaf->shutdown();
                      }).ok());
    EXPECT_TRUE(parent_entered.wait_for(5s));
    release.count_down();
    parent->wait();
    EXPECT_FALSE(leaf->submit_func([] {}).ok());
}

} // namespace doris
