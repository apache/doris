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

#include "util/core_local.h"

#include <gtest/gtest.h>

#include <atomic>
#include <thread>

#include "common/logging.h"
#include "testutil/test_util.h"
#include "time.h"
#include "util/stopwatch.hpp"

namespace doris {

// Fixture for testing class Decompressor
class CoreLocalTest : public ::testing::Test {
protected:
    CoreLocalTest() {}
    ~CoreLocalTest() {}
};

void updater(int64_t loop, CoreLocalValue<int64_t>* value, int64_t* used_ns) {
    usleep(100);
    MonotonicStopWatch stopwatch;
    stopwatch.start();
    for (int i = 0; i < loop; ++i) {
        __sync_fetch_and_add(value->access(), 1);
    }
    *used_ns = stopwatch.elapsed_time();
}

TEST_F(CoreLocalTest, CoreLocalValue) {
    int64_t loop = LOOP_LESS_OR_MORE(1000, 1000000L);
    CoreLocalValue<int64_t> value;
    std::vector<int64_t> used_ns;
    used_ns.resize(8);
    std::vector<std::thread> workers;
    for (int i = 0; i < 8; ++i) {
        workers.emplace_back(updater, loop, &value, &used_ns[i]);
    }
    int64_t sum_ns = 0;
    for (int i = 0; i < 8; ++i) {
        workers[i].join();
        sum_ns += used_ns[i];
    }
    int64_t sum = 0;
    for (int i = 0; i < value.size(); ++i) {
        sum += __sync_fetch_and_add(value.access_at_core(i), 0);
    }
    EXPECT_EQ(8 * loop, sum);
    LOG(INFO) << "time:" << sum_ns / sum << "ns/op";
}

TEST_F(CoreLocalTest, CoreDataAllocator) {
    CoreDataAllocatorFactory factory;
    auto allocator1 = factory.get_allocator(1, 8);
    auto ptr = allocator1->get_or_create(0);
    EXPECT_TRUE(ptr != nullptr);
    {
        auto ptr2 = allocator1->get_or_create(0);
        EXPECT_TRUE(ptr == ptr2);
    }
    {
        auto ptr2 = allocator1->get_or_create(4096);
        EXPECT_TRUE(ptr2 != nullptr);
    }
    {
        auto allocator2 = factory.get_allocator(2, 8);
        EXPECT_TRUE(allocator2 != allocator1);
    }
}

TEST_F(CoreLocalTest, CoreLocalValueController) {
    CoreLocalValueController<int64_t> controller;
    auto id = controller.get_id();
    EXPECT_EQ(0, id);
    controller.reclaim_id(id);
    id = controller.get_id();
    EXPECT_EQ(0, id);
    id = controller.get_id();
    EXPECT_EQ(1, id);
}

TEST_F(CoreLocalTest, CoreLocalValueNormal) {
    CoreLocalValue<int64_t> value;
    for (int i = 0; i < value.size(); ++i) {
        EXPECT_EQ(0, *value.access_at_core(i));
        *value.access_at_core(i) += 1;
    }
    for (int i = 0; i < value.size(); ++i) {
        EXPECT_EQ(1, *value.access_at_core(i));
    }
    for (int i = 0; i < 10000; ++i) {
        *value.access() += 1;
    }
    int64_t sum = 0;
    for (int i = 0; i < value.size(); ++i) {
        sum += *value.access_at_core(i);
    }
    EXPECT_EQ(10000 + value.size(), sum);
}
} // namespace doris
