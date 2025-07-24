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

#include <gen_cpp/Types_types.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <stddef.h>

// IWYU pragma: no_include <bits/chrono.h>
#include <gtest/gtest.h>

#include <list>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <ranges>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "common/config.h"
#include "cpp/sync_point.h"
#include "gtest/gtest_pred_impl.h"
#include "io/cache/block_file_cache.h"
#include "util/time.h"

using namespace doris::io;

class LRUQueueTest : public ::testing::Test {
protected:
    void SetUp() override {
        queue1 = std::make_shared<LRUQueue>();
        queue2 = std::make_shared<LRUQueue>();
    }

    std::shared_ptr<LRUQueue> queue1;
    std::shared_ptr<LRUQueue> queue2;
};

TEST_F(LRUQueueTest, SameQueueDistance) {
    std::mutex mutex;
    std::lock_guard lock(mutex);

    queue1->add(UInt128Wrapper(123), 0, 1024, lock);
    queue1->add(UInt128Wrapper(456), 0, 1024, lock);

    EXPECT_EQ(queue1->levenshtein_distance_from(*queue1, lock), 0);
}

TEST_F(LRUQueueTest, DifferentQueueDistance) {
    std::mutex mutex;
    std::lock_guard lock(mutex);

    queue1->add(UInt128Wrapper(123), 0, 1024, lock);
    queue1->add(UInt128Wrapper(456), 0, 1024, lock);

    queue2->add(UInt128Wrapper(123), 0, 1024, lock);
    queue2->add(UInt128Wrapper(789), 0, 1024, lock);

    EXPECT_EQ(queue1->levenshtein_distance_from(*queue2, lock), 1);
}

TEST_F(LRUQueueTest, EmptyQueueDistance) {
    std::mutex mutex;
    std::lock_guard lock(mutex);

    queue1->add(UInt128Wrapper(123), 0, 1024, lock);
    queue1->add(UInt128Wrapper(456), 0, 1024, lock);

    EXPECT_EQ(queue1->levenshtein_distance_from(*queue2, lock), 2);
}

TEST_F(LRUQueueTest, PartialMatchDistance) {
    std::mutex mutex;
    std::lock_guard lock(mutex);

    queue1->add(UInt128Wrapper(123), 0, 1024, lock);
    queue1->add(UInt128Wrapper(456), 0, 1024, lock);
    queue1->add(UInt128Wrapper(789), 0, 1024, lock);

    queue2->add(UInt128Wrapper(123), 0, 1024, lock);
    queue2->add(UInt128Wrapper(101), 0, 1024, lock);
    queue2->add(UInt128Wrapper(789), 0, 1024, lock);

    EXPECT_EQ(queue1->levenshtein_distance_from(*queue2, lock), 1);
}

TEST_F(LRUQueueTest, SameElementsDifferentOrder) {
    std::mutex mutex;
    std::lock_guard lock(mutex);

    queue1->add(UInt128Wrapper(123), 0, 1024, lock);
    queue1->add(UInt128Wrapper(456), 0, 1024, lock);
    queue1->add(UInt128Wrapper(789), 0, 1024, lock);

    queue2->add(UInt128Wrapper(789), 0, 1024, lock);
    queue2->add(UInt128Wrapper(456), 0, 1024, lock);
    queue2->add(UInt128Wrapper(123), 0, 1024, lock);

    EXPECT_EQ(queue1->levenshtein_distance_from(*queue2, lock), 2);
}
