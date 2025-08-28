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

#include "cloud/cloud_meta_mgr.h"

#include <gtest/gtest.h>

#include <chrono>
#include <memory>

namespace doris {
using namespace cloud;
using namespace std::chrono;

class CloudMetaMgrTest : public testing::Test {
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(CloudMetaMgrTest, bthread_fork_join_test) {
    // clang-format off
    std::vector<std::function<Status()>> tasks {
        []{ bthread_usleep(20000); return Status::OK(); },
        []{ bthread_usleep(20000); return Status::OK(); },
        []{ bthread_usleep(20000); return Status::OK(); },
        []{ bthread_usleep(20000); return Status::OK(); },
        []{ bthread_usleep(20000); return Status::OK(); },
        []{ bthread_usleep(20000); return Status::OK(); },
        []{ bthread_usleep(20000); return Status::OK(); },
    };
    {
        auto start = steady_clock::now();
        EXPECT_TRUE(bthread_fork_join(tasks, 3).ok());
        auto end = steady_clock::now();
        auto elapsed = duration_cast<milliseconds>(end - start).count();
        EXPECT_GT(elapsed, 40); // at least 2 rounds running for 7 tasks
    }
    {
        std::future<Status> fut;
        auto start = steady_clock::now();
        EXPECT_TRUE(bthread_fork_join(tasks, 3, &fut).ok()); // return immediately
        auto end = steady_clock::now();
        auto elapsed = duration_cast<milliseconds>(end - start).count();
        EXPECT_LE(elapsed, 40); // async
        EXPECT_TRUE(fut.get().ok());
        end = steady_clock::now();
        elapsed = duration_cast<milliseconds>(end - start).count();
        EXPECT_GT(elapsed, 40); // at least 2 rounds running for 7 tasks
    }

    // make the first batch fail fast
    tasks.insert(tasks.begin(), []{ bthread_usleep(20000); return Status::InternalError<false>("error"); });
    {
        auto start = steady_clock::now();
        EXPECT_FALSE(bthread_fork_join(tasks, 3).ok());
        auto end = steady_clock::now();
        auto elapsed = duration_cast<milliseconds>(end - start).count();
        EXPECT_LE(elapsed, 40); // at most 1 round running for 7 tasks
    }
    {
        std::future<Status> fut;
        auto start = steady_clock::now();
        EXPECT_TRUE(bthread_fork_join(tasks, 3, &fut).ok()); // return immediately
        auto end = steady_clock::now();
        auto elapsed = duration_cast<milliseconds>(end - start).count();
        EXPECT_LE(elapsed, 40); // async
        EXPECT_FALSE(fut.get().ok());
        end = steady_clock::now();
        elapsed = duration_cast<milliseconds>(end - start).count();
        EXPECT_LE(elapsed, 40); // at most 1 round running for 7 tasks
    }
    // clang-format on
}

} // namespace doris
