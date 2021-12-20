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

#include "util/thread_pool.hpp"

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <mutex>
#include <thread>

#include "util/logging.h"

namespace doris {

const int NUM_THREADS = 5;
int g_thread_counters[NUM_THREADS];

// Per-thread mutex to ensure visibility of counters after thread pool terminates
std::mutex g_thread_mutexes[NUM_THREADS];

void count(int thread_id, const int& i) {
    std::lock_guard<std::mutex> l(g_thread_mutexes[thread_id]);
    g_thread_counters[thread_id] += i;
}

TEST(ThreadPoolTest, BasicTest) {
    const int OFFERED_RANGE = 10000;

    for (int i = 0; i < NUM_THREADS; ++i) {
        g_thread_counters[i] = 0;
    }

    ThreadPool thread_pool(5, 250, count);

    for (int i = 0; i <= OFFERED_RANGE; ++i) {
        ASSERT_TRUE(thread_pool.offer(i));
    }

    thread_pool.drain_and_shutdown();

    // Check that Offer() after Shutdown() will return false
    ASSERT_FALSE(thread_pool.offer(-1));
    EXPECT_EQ(0, thread_pool.get_queue_size());

    int expected_count = (OFFERED_RANGE * (OFFERED_RANGE + 1)) / 2;
    int count = 0;

    for (int i = 0; i < NUM_THREADS; ++i) {
        std::lock_guard<std::mutex> l(g_thread_mutexes[i]);
        LOG(INFO) << "Counter " << i << ": " << g_thread_counters[i];
        count += g_thread_counters[i];
    }

    EXPECT_EQ(expected_count, count);
}

} // namespace doris

int main(int argc, char** argv) {
    std::string conffile = std::string(getenv("DORIS_HOME")) + "/conf/be.conf";
    if (!doris::config::init(conffile.c_str(), false)) {
        fprintf(stderr, "error read config file. \n");
        return -1;
    }
    init_glog("be-test");
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
