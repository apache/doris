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

#include <bvar/bvar.h>
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

// #define S3RateLimiterTest DISABLED_S3RateLimiterTest
TEST(S3RateLimiterTest, normal) {
    auto rate_limiter = doris::S3RateLimiter(1, 5, 10);
    std::atomic_int64_t failed;
    std::atomic_int64_t succ;
    std::atomic_int64_t sleep_thread_num;
    std::atomic_int64_t sleep;
    auto request_thread = [&]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
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
    EXPECT_EQ(failed, 10);
    EXPECT_EQ(succ, 5);
    EXPECT_EQ(sleep_thread_num, 5);
}

TEST(S3RateLimiterTest, BasicRateLimit) {
    auto rate_limiter = doris::S3RateLimiter(100, 100, 200);
    int64_t sleep_time = rate_limiter.add(50);
    EXPECT_EQ(sleep_time, 0);

    sleep_time = rate_limiter.add(60);
    EXPECT_GT(sleep_time, 0);
}

TEST(S3RateLimiterTest, BurstCapacity) {
    auto rate_limiter = doris::S3RateLimiter(125, 250, 1000);
    int64_t sleep_time = rate_limiter.add(250);
    EXPECT_EQ(sleep_time, 0);

    sleep_time = rate_limiter.add(1);
    EXPECT_GT(sleep_time, 0);
}

TEST(S3RateLimiterTest, ExceedLimit) {
    auto rate_limiter = doris::S3RateLimiter(125, 250, 250);
    int64_t sleep_time = rate_limiter.add(250);
    EXPECT_EQ(sleep_time, 0);

    sleep_time = rate_limiter.add(1);
    EXPECT_EQ(sleep_time, -1);
}

TEST(S3RateLimiterHolderTest, BvarMetric) {
    bvar::Adder<int64_t> rate_limit_ns("rate_limit_ns");
    bvar::Adder<int64_t> rate_limit_exceed_req_num("rate_limit_exceed_req_num");

    auto rate_limiter_holder = doris::S3RateLimiterHolder(
            125, 250, 500, doris::metric_func_factory(rate_limit_ns, rate_limit_exceed_req_num));
    int64_t sleep_time = rate_limiter_holder.add(250);
    EXPECT_EQ(sleep_time, 0);
    EXPECT_EQ(rate_limit_ns.get_value(), 0);
    EXPECT_EQ(rate_limit_exceed_req_num.get_value(), 0);

    sleep_time = rate_limiter_holder.add(1);
    EXPECT_GT(rate_limit_ns.get_value(), 0);
    EXPECT_EQ(rate_limit_exceed_req_num.get_value(), 1);
    EXPECT_GT(sleep_time, 0);
}