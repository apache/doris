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

#include "storage/delete/calc_delete_bitmap_executor.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>

#include "runtime/thread_context.h"
#include "util/countdown_latch.h"
#include "util/defer_op.h"

namespace doris {

TEST(CalcDeleteBitmapTokenTest, SharedCancellationSkipsQueuedTasks) {
    SCOPED_INIT_THREAD_CONTEXT();
    std::unique_ptr<ThreadPool> pool;
    ASSERT_TRUE(ThreadPoolBuilder("BitmapCancellationTest")
                        .set_min_threads(1)
                        .set_max_threads(1)
                        .build(&pool)
                        .ok());
    auto status = std::make_shared<AtomicStatus>();
    CalcDeleteBitmapToken token(pool->new_token(ThreadPool::ExecutionMode::CONCURRENT), status);
    CountDownLatch entered(1);
    CountDownLatch release(1);
    Defer cleanup {[&] {
        release.count_down();
        pool->wait();
    }};
    ASSERT_TRUE(pool->submit_func([&] {
                        entered.count_down();
                        release.wait();
                    }).ok());
    ASSERT_TRUE(entered.wait_for(std::chrono::seconds(10)));

    std::atomic<int> executed = 0;
    ASSERT_TRUE(token.submit_func([&] {
                         ++executed;
                         return Status::OK();
                     }).ok());
    // These inputs must never be dereferenced: both typed entry points are queued
    // behind the barrier and cancelled before execution.
    ASSERT_TRUE(token.submit(nullptr, nullptr, nullptr, {}, 0, nullptr, nullptr, nullptr).ok());
    ASSERT_TRUE(token.submit(nullptr, nullptr, RowsetId(), {}, nullptr).ok());
    const auto reason = Status::Cancelled("load cancelled while bitmap tasks were queued");
    status->update(reason);
    EXPECT_EQ(token.submit_func([] { return Status::OK(); }).to_string(), reason.to_string());
    release.count_down();
    EXPECT_EQ(token.wait().to_string(), reason.to_string());
    EXPECT_EQ(executed.load(), 0);
}

TEST(CalcDeleteBitmapTokenTest, SharedCancellationStillWaitsForRunningCallback) {
    SCOPED_INIT_THREAD_CONTEXT();
    CalcDeleteBitmapExecutor executor;
    executor.init("BitmapRunningCancellationTest", 1);
    auto status = std::make_shared<AtomicStatus>();
    auto token = executor.create_token(status);
    CountDownLatch entered(1);
    CountDownLatch release(1);
    std::atomic<bool> finished = false;
    Defer cleanup {[&] {
        release.count_down();
        token->cancel();
    }};
    ASSERT_TRUE(token->submit_func([&] {
                         entered.count_down();
                         release.wait();
                         finished = true;
                         return Status::OK();
                     }).ok());
    ASSERT_TRUE(entered.wait_for(std::chrono::seconds(10)));
    status->update(Status::Cancelled("cancel running bitmap task"));
    auto waiter = std::async(std::launch::async, [&] { return token->wait(); });
    EXPECT_EQ(waiter.wait_for(std::chrono::milliseconds(50)), std::future_status::timeout);
    EXPECT_FALSE(finished.load());
    release.count_down();
    EXPECT_TRUE(waiter.get().is<ErrorCode::CANCELLED>());
    EXPECT_TRUE(finished.load());
}

TEST(CalcDeleteBitmapTokenTest, TokenWithoutLoadStatusPreservesTaskFailure) {
    SCOPED_INIT_THREAD_CONTEXT();
    CalcDeleteBitmapExecutor executor;
    executor.init("BitmapFailureTest", 1);
    auto token = executor.create_token();
    const auto failure = Status::InternalError("bitmap calculation failed");
    ASSERT_TRUE(token->submit_func([&] { return failure; }).ok());
    EXPECT_EQ(token->wait().to_string(), failure.to_string());
    EXPECT_EQ(token->submit_func([] { return Status::OK(); }).to_string(), failure.to_string());
}

TEST(CalcDeleteBitmapTokenTest, DestructionWaitsForRunningCallbackAfterCancellation) {
    SCOPED_INIT_THREAD_CONTEXT();
    CalcDeleteBitmapExecutor executor;
    executor.init("BitmapDestructionTest", 1);
    auto status = std::make_shared<AtomicStatus>();
    auto token = executor.create_token(status);
    CountDownLatch entered(1);
    CountDownLatch release(1);
    CountDownLatch destroying(1);
    Defer cleanup {[&] {
        release.count_down();
        if (token) {
            token->cancel();
        }
    }};
    ASSERT_TRUE(token->submit_func([&] {
                         entered.count_down();
                         release.wait();
                         // The wrapper still accesses the token's status after this callback.
                         return Status::InternalError("callback failed during destruction");
                     }).ok());
    ASSERT_TRUE(entered.wait_for(std::chrono::seconds(10)));
    status->update(Status::Cancelled("cancel before releasing the token"));
    auto destructor = std::async(std::launch::async, [&, owned = std::move(token)]() mutable {
        destroying.count_down();
        owned.reset();
    });
    EXPECT_TRUE(destroying.wait_for(std::chrono::seconds(10)));
    EXPECT_EQ(destructor.wait_for(std::chrono::milliseconds(50)), std::future_status::timeout);
    release.count_down();
    destructor.get();
}

} // namespace doris
