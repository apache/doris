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

TEST_F(LRUQueueTest, ResizeUpdatesCacheSize) {
    std::mutex mutex;
    std::lock_guard lock(mutex);

    auto iter = queue1->add(UInt128Wrapper(123), 0, 1024, lock);
    EXPECT_EQ(queue1->get_capacity(lock), 1024);

    queue1->resize(iter, 2048, lock);
    EXPECT_EQ(queue1->get_capacity(lock), 2048);
}

// The queue sweep keeps its cursor across releases of the cache lock, so an entry that leaves its
// place has to move the cursor past itself first, or the cursor would dangle.
TEST_F(LRUQueueTest, SweepCursorMovesPastEntriesThatLeave) {
    std::mutex mutex;
    std::lock_guard lock(mutex);

    auto a = queue1->add(UInt128Wrapper(1), 0, 1, lock);
    auto b = queue1->add(UInt128Wrapper(2), 0, 1, lock);
    auto c = queue1->add(UInt128Wrapper(3), 0, 1, lock);
    auto d = queue1->add(UInt128Wrapper(4), 0, 1, lock);
    auto e = queue1->add(UInt128Wrapper(5), 0, 1, lock);
    queue1->sweep_cursor = a;

    queue1->remove(a, lock);
    EXPECT_TRUE(*queue1->sweep_cursor == b);
    queue1->move_to_end(b, lock); // c d e b
    EXPECT_TRUE(*queue1->sweep_cursor == c);
    queue1->pop_front(lock); // d e b
    EXPECT_TRUE(*queue1->sweep_cursor == d);

    // Entries the cursor does not point at leave it where it is.
    queue1->remove(e, lock); // d b
    queue1->move_to_end(b, lock);
    queue1->add_front(UInt128Wrapper(6), 0, 1, lock); // f d b
    EXPECT_TRUE(*queue1->sweep_cursor == d);

    // The last entry leaving puts it at the end.
    queue1->remove(b, lock); // f d
    queue1->move_to_end(d, lock);
    EXPECT_TRUE(*queue1->sweep_cursor == queue1->end());

    queue1->remove_all(lock);
    EXPECT_FALSE(queue1->sweep_cursor.has_value());
}
