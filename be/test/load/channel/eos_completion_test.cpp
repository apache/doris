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

#include "load/channel/eos_completion.h"

#include <gtest/gtest.h>

#include <atomic>
#include <string>
#include <thread>
#include <vector>

#include "util/countdown_latch.h"

namespace doris {

TEST(EosCompletionTest, MoreSendersThanWorkers) {
    EosCompletion completion;
    int replies = 0;
    // A single worker can process all 100 EOS tasks: registering a waiter must
    // return without waiting for any other sender or sending a successful RPC.
    for (int sender = 0; sender < 100; ++sender) {
        completion.add_waiter([&](const Status& status) {
            EXPECT_TRUE(status.ok());
            ++replies;
        });
        EXPECT_EQ(replies, 0);
    }
    completion.complete(Status::OK());
    EXPECT_EQ(replies, 100);
}

TEST(EosCompletionTest, FailureAndLateRegistration) {
    EosCompletion completion;
    int replies = 0;
    auto callback = [&](const Status& status) {
        EXPECT_FALSE(status.ok());
        EXPECT_NE(status.to_string().find("flush failed"), std::string::npos);
        ++replies;
    };
    completion.add_waiter(callback);
    completion.complete(Status::InternalError("flush failed"));
    // Models an EOS task finishing its response/profile writes after failure.
    completion.add_waiter(callback);
    completion.complete(Status::OK());
    EXPECT_EQ(replies, 2);
}

TEST(EosCompletionTest, ArrivalSuccessSurvivesLaterCloseFailure) {
    EosCompletion completion;
    int replies = 0;
    auto reply = [&](const Status& status) {
        EXPECT_TRUE(status.ok());
        ++replies;
    };
    completion.add_waiter(reply);
    completion.complete(Status::OK());
    completion.complete(Status::InternalError("final flush failed"));
    completion.complete(Status::Cancelled("late cancellation"));
    completion.add_waiter(reply);
    EXPECT_EQ(replies, 2);
}

TEST(EosCompletionTest, CancellationWinsBeforeRegistration) {
    EosCompletion completion;
    completion.complete(Status::Cancelled("load expired"));
    completion.complete(Status::OK());
    int replies = 0;
    completion.add_waiter([&](const Status& status) {
        EXPECT_TRUE(status.is<ErrorCode::CANCELLED>());
        ++replies;
    });
    EXPECT_EQ(replies, 1);
}

TEST(EosCompletionTest, DestroyingUnfinishedChannelReleasesWaiters) {
    int replies = 0;
    {
        EosCompletion completion;
        completion.add_waiter([&](const Status& status) {
            EXPECT_TRUE(status.is<ErrorCode::CANCELLED>());
            ++replies;
        });
    }
    EXPECT_EQ(replies, 1);
}

TEST(EosCompletionTest, CallbacksRunOutsideLock) {
    EosCompletion completion;
    int replies = 0;
    completion.add_waiter([&](const Status& status) {
        EXPECT_TRUE(status.ok());
        ++replies;
        completion.add_waiter([&](const Status& result) {
            EXPECT_TRUE(result.ok());
            ++replies;
        });
        completion.complete(Status::Cancelled("late cancellation"));
    });
    completion.complete(Status::OK());
    EXPECT_EQ(replies, 2);
}

TEST(EosCompletionTest, ConcurrentRegistrationArrivalAndCancellationCompleteOnce) {
    EosCompletion completion;
    CountDownLatch start(1);
    std::atomic<int> replies {0};
    std::atomic<int> successes {0};
    std::vector<std::thread> tasks;
    for (int i = 0; i < 100; ++i) {
        tasks.emplace_back([&] {
            start.wait();
            completion.add_waiter([&](const Status& status) {
                if (status.ok()) {
                    ++successes;
                }
                ++replies;
            });
        });
    }
    tasks.emplace_back([&] {
        start.wait();
        completion.complete(Status::OK());
    });
    tasks.emplace_back([&] {
        start.wait();
        completion.complete(Status::Cancelled("load expired"));
    });
    start.count_down();
    for (auto& task : tasks) {
        task.join();
    }
    EXPECT_EQ(replies.load(), 100);
    EXPECT_TRUE(successes.load() == 0 || successes.load() == 100);
}

} // namespace doris
