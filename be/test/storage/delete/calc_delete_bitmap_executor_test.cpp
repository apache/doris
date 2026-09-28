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

#include <chrono>
#include <future>

#include "runtime/thread_context.h"
#include "util/countdown_latch.h"
#include "util/defer_op.h"

namespace doris {

TEST(CalcDeleteBitmapTokenTest, PreservesTaskFailure) {
    SCOPED_INIT_THREAD_CONTEXT();
    CalcDeleteBitmapExecutor executor;
    executor.init("BitmapFailureTest", 1);
    auto token = executor.create_token();
    const auto failure = Status::InternalError("bitmap calculation failed");
    ASSERT_TRUE(token->submit_func([&] { return failure; }).ok());
    EXPECT_EQ(token->wait().to_string(), failure.to_string());
    EXPECT_EQ(token->submit_func([] { return Status::OK(); }).to_string(), failure.to_string());
}

TEST(CalcDeleteBitmapTokenTest, DestructionWaitsForRunningCallback) {
    SCOPED_INIT_THREAD_CONTEXT();
    CalcDeleteBitmapExecutor executor;
    executor.init("BitmapDestructionTest", 1);
    auto token = executor.create_token();
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
