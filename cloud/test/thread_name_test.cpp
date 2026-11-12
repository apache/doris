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

#include "common/thread_name.h"

#include <gtest/gtest.h>
#include <pthread.h>

#include <array>
#include <chrono>
#include <future>
#include <string>
#include <thread>

#include "common/simple_thread_pool.h"

namespace doris::cloud {

TEST(ThreadNameTest, NamesWorkerWithoutRenamingCreator) {
    std::array<char, 64> creator_name {};
    ASSERT_EQ(0, pthread_getname_np(pthread_self(), creator_name.data(), creator_name.size()));

    std::array<char, 64> worker_name {};
    int set_result = -1;
    int get_result = -1;
    std::thread worker([&] {
        set_result = set_current_thread_name("cloud_worker_long_name");
        get_result = pthread_getname_np(pthread_self(), worker_name.data(), worker_name.size());
    });
    worker.join();

    EXPECT_EQ(0, set_result);
    EXPECT_EQ(0, get_result);
#ifdef __APPLE__
    EXPECT_STREQ("cloud_worker_long_name", worker_name.data());
#else
    EXPECT_STREQ("cloud_worker_lo", worker_name.data());
#endif

    std::array<char, 64> creator_after {};
    ASSERT_EQ(0, pthread_getname_np(pthread_self(), creator_after.data(), creator_after.size()));
    EXPECT_STREQ(creator_name.data(), creator_after.data());
}

TEST(ThreadNameTest, PoolWorkerSetsItsOwnName) {
    std::array<char, 64> creator_name {};
    ASSERT_EQ(0, pthread_getname_np(pthread_self(), creator_name.data(), creator_name.size()));

    SimpleThreadPool pool(1, "cloudpool");
    ASSERT_EQ(0, pool.start());

    std::promise<std::string> result;
    auto name = result.get_future();
    ASSERT_EQ(0, pool.submit([&result] {
        std::array<char, 64> current_name {};
        pthread_getname_np(pthread_self(), current_name.data(), current_name.size());
        result.set_value(current_name.data());
    }));

    auto ready = name.wait_for(std::chrono::seconds(5));
    EXPECT_EQ(0, pool.stop());
    ASSERT_EQ(std::future_status::ready, ready);
    EXPECT_EQ("cloudpool_0", name.get());

    std::array<char, 64> creator_after {};
    ASSERT_EQ(0, pthread_getname_np(pthread_self(), creator_after.data(), creator_after.size()));
    EXPECT_STREQ(creator_name.data(), creator_after.data());
}

} // namespace doris::cloud
