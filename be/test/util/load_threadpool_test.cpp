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
#include <stdexcept>
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

TEST(LoadThreadPoolTest, MultipleTokensShareOneLoadTurn) {
    std::unique_ptr<ThreadPool> pool;
    ASSERT_TRUE(ThreadPoolBuilder("load_fifo_test").set_max_threads(1).build(&pool).ok());
    auto flush = pool->new_load_token(1, LoadTaskPriority::LOW);
    auto bitmap = pool->new_load_token(1, LoadTaskPriority::HIGHEST);
    auto dup = pool->new_load_token(2, LoadTaskPriority::LOW);
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
    EXPECT_TRUE(dup->submit_func([&] { order.push_back(2); }).ok());
    release.count_down();
    pool->wait();
    EXPECT_EQ(order, (std::vector<int> {0, 2, 3}));
}

TEST(LoadThreadPoolTest, TokenlessTasksKeepTheirLoadAndPriority) {
    class RecordTask : public Runnable {
    public:
        RecordTask(std::vector<int>* order, int value) : _order(order), _value(value) {}
        void run() override { _order->push_back(_value); }

    private:
        std::vector<int>* _order;
        int _value;
    };
    std::unique_ptr<ThreadPool> pool;
    ASSERT_TRUE(ThreadPoolBuilder("load_tokenless_order").set_max_threads(1).build(&pool).ok());
    CountDownLatch entered(1), release(1);
    std::vector<int> order;
    Defer unblock = [&] { release.count_down(); };
    EXPECT_TRUE(pool->submit_func([&] {
                        entered.count_down();
                        release.wait();
                    }).ok());
    EXPECT_TRUE(entered.wait_for(5s));
    EXPECT_TRUE(
            pool->submit_load(std::make_shared<RecordTask>(&order, 13), 1, LoadTaskPriority::LOW)
                    .ok());
    EXPECT_TRUE(
            pool->submit_load(std::make_shared<RecordTask>(&order, 23), 2, LoadTaskPriority::LOW)
                    .ok());
    EXPECT_TRUE(pool->submit_load(std::make_shared<RecordTask>(&order, 10), 1,
                                  LoadTaskPriority::HIGHEST)
                        .ok());
    release.count_down();
    pool->wait();
    EXPECT_EQ(order, (std::vector<int> {10, 23, 13}));
}

TEST(LoadThreadPoolTest, OneLoadCanUseAllWorkers) {
    std::unique_ptr<ThreadPool> pool;
    ASSERT_TRUE(ThreadPoolBuilder("load_parallel_test").set_max_threads(2).build(&pool).ok());
    auto token = pool->new_load_token(1, LoadTaskPriority::LOW);
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
    auto cancelled = pool->new_load_token(1, LoadTaskPriority::MID);
    auto kept = pool->new_load_token(1, LoadTaskPriority::LOW);
    auto other_load = pool->new_load_token(2, LoadTaskPriority::HIGHEST);
    CountDownLatch entered(1), release(1);
    std::vector<int> order;
    Defer unblock = [&] { release.count_down(); };
    EXPECT_TRUE(pool->submit_func([&] {
                        entered.count_down();
                        release.wait();
                    }).ok());
    EXPECT_TRUE(entered.wait_for(5s));
    EXPECT_TRUE(cancelled->submit_func([&] { ADD_FAILURE() << "cancelled task ran"; }).ok());
    EXPECT_TRUE(kept->submit_func([&] { order.push_back(1); }).ok());
    EXPECT_TRUE(other_load->submit_func([&] { order.push_back(2); }).ok());
    cancelled->shutdown();
    cancelled->shutdown();
    EXPECT_FALSE(cancelled->submit_func([] {}).ok());
    release.count_down();
    pool->wait();
    EXPECT_EQ(order, (std::vector<int> {1, 2}));
}

TEST(LoadThreadPoolTest, NestedBitmapHelpsOnlyOwnTokenWithOneWorker) {
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
    auto parent = executor.create_load_token(1, LoadTaskPriority::HIGHEST, nullptr);
    std::atomic<int> completed = 0;
    std::atomic<bool> unrelated_ran = false;
    auto unrelated = pool->new_load_token(2, LoadTaskPriority::HIGHEST);
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
                      EXPECT_TRUE(unrelated->submit_func([&] { unrelated_ran = true; }).ok());
                      auto child =
                              executor.create_load_token(1, LoadTaskPriority::HIGHEST, nullptr);
                      for (int i = 0; i < 2; ++i) {
                          EXPECT_TRUE(child->submit_func([&] {
                                               check_context();
                                               EXPECT_TRUE(ThreadPool::is_helping_load_task());
                                               EXPECT_FALSE(unrelated_ran.load());
                                               EXPECT_EQ(pool->num_active_threads(), 1);
                                               ++completed;
                                               return Status::OK();
                                           }).ok());
                          EXPECT_EQ(completed.load(), 0);
                          check_context();
                      }
                      EXPECT_TRUE(child->submit_func([&] {
                                           check_context();
                                           ++completed;
                                           return Status::InternalError("test bitmap failure");
                                       }).ok());
                      EXPECT_FALSE(child->wait().ok());
                      EXPECT_FALSE(unrelated_ran.load());
                      EXPECT_FALSE(ThreadPool::is_helping_load_task());
                      child.reset(); // No scheduler reference may survive this destruction.
                      auto next = executor.create_load_token(1, LoadTaskPriority::HIGHEST, nullptr);
                      EXPECT_TRUE(next->submit_func([&] {
                                          check_context();
                                          ++completed;
                                          return Status::OK();
                                      }).ok());
                      EXPECT_TRUE(next->wait().ok());
                      check_context();
                      return Status::OK();
                  }).ok());
    EXPECT_TRUE(parent->wait().ok());
    EXPECT_EQ(completed.load(), 4);
    pool->wait();
    EXPECT_TRUE(unrelated_ran.load());
}

TEST(LoadThreadPoolTest, NestedBitmapUsesSpareWorkerAndParentPool) {
    std::unique_ptr<ThreadPool> pool, default_pool;
    ASSERT_TRUE(ThreadPoolBuilder("bitmap_parallel_parent").set_max_threads(2).build(&pool).ok());
    ASSERT_TRUE(
            ThreadPoolBuilder("bitmap_other_domain").set_max_threads(1).build(&default_pool).ok());
    CalcDeleteBitmapExecutor executor;
    executor.init("bitmap_parallel_background", 1, default_pool.get());
    auto resource_ctx = ResourceContext::create_shared();
    auto request_tracker = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::OTHER,
                                                            "bitmap_parallel_request");
    auto tablet_tracker = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::OTHER,
                                                           "bitmap_parallel_tablet");
    resource_ctx->memory_context()->set_mem_tracker(request_tracker);
    SCOPED_ATTACH_TASK(resource_ctx);
    CalcDeleteBitmapToken parent(pool->new_load_token(1, LoadTaskPriority::HIGHEST));
    CountDownLatch worker_entered(1), helper_entered(1), release(1);
    Defer unblock = [&] { release.count_down(); };
    EXPECT_TRUE(
            parent.submit_func([&] {
                      SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(tablet_tracker);
                      auto child =
                              executor.create_load_token(1, LoadTaskPriority::HIGHEST, nullptr);
                      auto check_context = [&] {
                          EXPECT_EQ(ThreadPool::current_load_pool(), pool.get());
                          EXPECT_EQ(thread_context()->resource_ctx(), resource_ctx);
                          EXPECT_EQ(thread_context()->thread_mem_tracker_mgr->limiter_mem_tracker(),
                                    tablet_tracker.get());
                      };
                      EXPECT_TRUE(child->submit_func([&] {
                                           check_context();
                                           EXPECT_FALSE(ThreadPool::is_helping_load_task());
                                           worker_entered.count_down();
                                           release.wait();
                                           return Status::OK();
                                       }).ok());
                      EXPECT_TRUE(worker_entered.wait_for(5s));
                      EXPECT_TRUE(child->submit_func([&] {
                                           check_context();
                                           EXPECT_TRUE(ThreadPool::is_helping_load_task());
                                           helper_entered.count_down();
                                           release.wait();
                                           return Status::OK();
                                       }).ok());
                      auto st = child->wait();
                      check_context();
                      EXPECT_FALSE(ThreadPool::is_helping_load_task());
                      return st;
                  }).ok());
    EXPECT_TRUE(helper_entered.wait_for(5s));
    EXPECT_EQ(pool->num_active_threads(), 2); // Helping does not create a third physical worker.
    release.count_down();
    EXPECT_TRUE(parent.wait().ok());
}

TEST(LoadThreadPoolTest, AllWorkersCanHelpTheirOwnChildren) {
    std::unique_ptr<ThreadPool> pool;
    ASSERT_TRUE(ThreadPoolBuilder("bitmap_saturated").set_max_threads(2).build(&pool).ok());
    CalcDeleteBitmapExecutor executor;
    executor.init("bitmap_saturated_background", 1, pool.get());
    SCOPED_ATTACH_TASK(
            MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::OTHER, "bitmap_saturated"));
    auto first = executor.create_load_token(1, LoadTaskPriority::HIGHEST, nullptr);
    auto second = executor.create_load_token(2, LoadTaskPriority::HIGHEST, nullptr);
    CountDownLatch parents_entered(2), children_entered(2), release(1);
    std::atomic<int> completed = 0;
    Defer unblock = [&] { release.count_down(); };
    auto run_parent = [&](int64_t load_id) {
        parents_entered.count_down();
        EXPECT_TRUE(parents_entered.wait_for(5s));
        auto child = executor.create_load_token(load_id, LoadTaskPriority::HIGHEST, nullptr);
        EXPECT_TRUE(child->submit_func([&] {
                             EXPECT_TRUE(ThreadPool::is_helping_load_task());
                             children_entered.count_down();
                             release.wait();
                             ++completed;
                             return Status::OK();
                         }).ok());
        return child->wait();
    };
    EXPECT_TRUE(first->submit_func([&] { return run_parent(1); }).ok());
    EXPECT_TRUE(second->submit_func([&] { return run_parent(2); }).ok());
    EXPECT_TRUE(children_entered.wait_for(5s));
    EXPECT_EQ(pool->num_active_threads(), 2);
    release.count_down();
    EXPECT_TRUE(first->wait().ok());
    EXPECT_TRUE(second->wait().ok());
    EXPECT_EQ(completed.load(), 2);
}

TEST(LoadThreadPoolTest, ParentCanCancelQueuedPublishChildren) {
    std::unique_ptr<ThreadPool> pool;
    ASSERT_TRUE(ThreadPoolBuilder("bitmap_cancel_children").set_max_threads(1).build(&pool).ok());
    CalcDeleteBitmapExecutor executor;
    executor.init("bitmap_cancel_background", 1, pool.get());
    SCOPED_ATTACH_TASK(MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::OTHER,
                                                        "bitmap_cancel_children"));
    auto parent = executor.create_load_token(1, LoadTaskPriority::HIGHEST, nullptr);
    EXPECT_TRUE(parent->submit_func([&] {
                          auto child =
                                  executor.create_load_token(1, LoadTaskPriority::HIGHEST, nullptr);
                          EXPECT_TRUE(child->submit_func([] {
                                               ADD_FAILURE() << "cancelled child ran";
                                               return Status::OK();
                                           }).ok());
                          child->cancel();
                          EXPECT_TRUE(child->wait().is<ErrorCode::CANCELLED>());
                          return Status::OK();
                      }).ok());
    EXPECT_TRUE(parent->wait().ok());
}

TEST(LoadThreadPoolTest, HelpingCallbackExceptionRetiresTask) {
    std::unique_ptr<ThreadPool> pool;
    ASSERT_TRUE(ThreadPoolBuilder("bitmap_help_exception").set_max_threads(1).build(&pool).ok());
    auto parent = pool->new_load_token(1, LoadTaskPriority::HIGHEST);
    EXPECT_TRUE(parent->submit_func([&] {
                          auto child = pool->new_load_token(1, LoadTaskPriority::HIGHEST, true);
                          EXPECT_TRUE(child->submit_func([] {
                                               throw std::runtime_error("child failure");
                                           }).ok());
                          EXPECT_THROW(child->wait_and_help(), std::runtime_error);
                          EXPECT_FALSE(ThreadPool::is_helping_load_task());
                          EXPECT_EQ(ThreadPool::current_load_pool(), pool.get());
                          EXPECT_EQ(child->num_tasks(), 0);
                          child->wait_and_help(); // Failed callback must not leave an active task.
                      }).ok());
    parent->wait();
    EXPECT_EQ(pool->get_queue_size(), 0);
}

TEST(LoadThreadPoolTest, CancelledBitmapIsNotReportedAsComplete) {
    std::unique_ptr<ThreadPool> pool;
    ASSERT_TRUE(ThreadPoolBuilder("load_shutdown_test").set_max_threads(1).build(&pool).ok());
    CalcDeleteBitmapExecutor executor;
    executor.init("background_shutdown_test", 1, pool.get());
    auto token = executor.create_load_token(1, LoadTaskPriority::MID, nullptr);
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
    CalcDeleteBitmapToken token(pool->new_load_token(1, LoadTaskPriority::MID));
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
    EXPECT_EQ(completed.load(), 1);
    // The token remains failed even after the queue has drained.
    EXPECT_EQ(token.submit_func([] { return Status::OK(); }).to_string(), rejected.to_string());
}

TEST(LoadThreadPoolTest, BitmapSubmissionAfterShutdownPreservesReason) {
    std::unique_ptr<ThreadPool> pool;
    ASSERT_TRUE(ThreadPoolBuilder("bitmap_rejected_test").set_max_threads(1).build(&pool).ok());
    CalcDeleteBitmapToken token(pool->new_load_token(1, LoadTaskPriority::HIGHEST));
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

TEST(LoadThreadPoolTest, LoadCleanupCanJoinRunningBitmapLeaves) {
    for (auto priority : {LoadTaskPriority::MID, LoadTaskPriority::HIGHEST}) {
        std::unique_ptr<ThreadPool> pool;
        ASSERT_TRUE(ThreadPoolBuilder("load_cleanup_test").set_max_threads(2).build(&pool).ok());
        auto leaf = pool->new_load_token(1, priority, true);
        auto parent = pool->new_load_token(1, LoadTaskPriority::LOW);
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
}

} // namespace doris
