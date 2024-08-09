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

#include <cpp/s3_rate_limiter.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

#include "common/configbase.h"
#include "common/logging.h"

using namespace doris::cloud;

int main(int argc, char** argv) {
    auto conf_file = "doris_cloud.conf";
    if (!doris::cloud::config::init(conf_file, true)) {
        std::cerr << "failed to init config file, conf=" << conf_file << std::endl;
        return -1;
    }
    if (!doris::cloud::init_glog("util")) {
        std::cerr << "failed to init glog" << std::endl;
        return -1;
    }

    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

#define S3RateLimiterTest DISABLED_S3RateLimiterTest
TEST(S3RateLimiterTest, normal) {
    auto rate_limiter = doris::S3RateLimiter(1, 5, 10);
    std::atomic_int64_t failed;
    std::atomic_int64_t succ;
    std::atomic_int64_t sleep_thread_num;
    std::atomic_int64_t sleep;
    auto request_thread = [&]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        auto ms = rate_limiter.add(1);
        if (ms < 0) {
            failed++;
        } else if (ms == 0) {
            succ++;
        } else {
            sleep += ms;
            sleep_thread_num++;
        }
    };
    {
        std::vector<std::thread> threads;
        for (size_t i = 0; i < 20; i++) {
            threads.emplace_back(request_thread);
        }
        for (auto& t : threads) {
            if (t.joinable()) {
                t.join();
            }
        }
    }
    ASSERT_EQ(failed, 10);
    ASSERT_EQ(succ, 6);
    ASSERT_EQ(sleep_thread_num, 4);
}