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
#include <vector>

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
    auto parent = pool->new_load_token(1, LoadTaskPriority::HIGHEST);
    std::atomic<bool> completed = false;
    EXPECT_TRUE(parent->submit_func([&] {
                          auto child =
                                  executor.create_load_token(1, LoadTaskPriority::HIGHEST, nullptr);
                          EXPECT_TRUE(child->submit_func([&] {
                                               completed = true;
                                               return Status::InternalError("test bitmap failure");
                                           }).ok());
                          EXPECT_FALSE(child->wait().ok());
                      }).ok());
    parent->wait();
    EXPECT_TRUE(completed);
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
    EXPECT_FALSE(token->wait().ok());
    release.count_down();
    pool->shutdown();
    EXPECT_FALSE(token->submit_func([] { return Status::OK(); }).ok());
}

TEST(LoadThreadPoolTest, FlushCleanupCanJoinRunningBitmapLeaves) {
    std::unique_ptr<ThreadPool> pool;
    ASSERT_TRUE(ThreadPoolBuilder("load_cleanup_test").set_max_threads(2).build(&pool).ok());
    auto leaf = pool->new_load_token(1, LoadTaskPriority::MID);
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

} // namespace doris
