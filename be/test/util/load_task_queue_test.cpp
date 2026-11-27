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

#include "util/load_task_queue.h"

#include <gtest/gtest.h>

#include <memory>
#include <vector>

namespace doris {

TEST(LoadTaskQueueTest, PriorityAcrossLoads) {
    LoadTaskQueue<int> queue;
    queue.push(1, 3, 13);
    queue.push(2, 2, 22);
    queue.push(3, 1, 31);
    queue.push(4, 0, 40);
    queue.push(5, 3, 53); // All memtable flushes share P3 across loads.
    std::vector<int> actual;
    while (!queue.empty()) {
        actual.push_back(queue.pop());
    }
    EXPECT_EQ(actual, (std::vector<int> {40, 31, 22, 13, 53}));
}

TEST(LoadTaskQueueTest, SamePriorityIsGlobalFifoWithoutTransactionTurns) {
    LoadTaskQueue<int> queue;
    queue.push(1, 1, 1);
    queue.push(1, 1, 2);
    queue.push(2, 1, 3);
    queue.push(1, 1, 4);
    for (int expected : {1, 2, 3, 4}) {
        EXPECT_EQ(queue.pop(), expected);
    }
    EXPECT_TRUE(queue.empty());
}

TEST(LoadTaskQueueTest, NewHigherPriorityWorkPrecedesQueuedLowerPriorityWork) {
    LoadTaskQueue<int> queue;
    queue.push(1, 3, 1);
    queue.push(2, 3, 2);
    EXPECT_EQ(queue.pop(), 1);
    queue.push(3, 1, 3);
    queue.push(4, 0, 4);
    for (int expected : {4, 3, 2}) {
        EXPECT_EQ(queue.pop(), expected);
    }
    EXPECT_TRUE(queue.empty());
    queue.push(1, 2, 5);
    queue.push(2, 1, 6);
    EXPECT_EQ(queue.pop(), 6);
    EXPECT_EQ(queue.pop(), 5);
    EXPECT_TRUE(queue.empty());
}

TEST(LoadTaskQueueTest, CancelPreservesOtherTasksAndGlobalFifo) {
    LoadTaskQueue<int> queue;
    queue.push(1, 0, 1);
    queue.push(1, 2, 2);
    queue.push(2, 3, 3);
    queue.push(1, 3, 4);
    EXPECT_EQ(queue.remove_if(1, [](int task) { return task < 3; }), (std::vector<int> {1, 2}));
    EXPECT_EQ(queue.size(), 2);
    EXPECT_EQ(queue.pop(), 3);
    EXPECT_EQ(queue.pop(), 4);
    queue.push(1, 3, 5);
    queue.push(2, 0, 6);
    EXPECT_EQ(queue.remove_if(1, [](int) { return true; }), (std::vector<int> {5}));
    queue.push(1, 0, 7);
    EXPECT_EQ(queue.pop(), 6);
    EXPECT_EQ(queue.pop(), 7);
    EXPECT_TRUE(queue.remove_if(99, [](int) { return true; }).empty());
    EXPECT_TRUE(queue.empty());
}

TEST(LoadTaskQueueTest, MoveOnlyTasksAndDeferredDestruction) {
    LoadTaskQueue<std::unique_ptr<int>> queue;
    queue.push(1, 0, std::make_unique<int>(1));
    queue.push(1, 0, std::make_unique<int>(2));
    auto removed = queue.remove_if(1, [](const auto& task) { return *task == 1; });
    ASSERT_EQ(removed.size(), 1);
    EXPECT_EQ(*removed.front(), 1);
    EXPECT_EQ(*queue.pop(), 2);
    EXPECT_TRUE(queue.empty());
}

TEST(LoadTaskQueueTest, BitmapBatchDrainsBeforeOtherTransactionsFlushes) {
    LoadTaskQueue<int> queue;
    queue.push(2, 2, -2);
    queue.push(3, 3, -3);
    for (int i = 0; i < 532; ++i) {
        queue.push(1, 1, i);
    }
    for (int i = 0; i < 532; ++i) {
        EXPECT_EQ(queue.pop(), i);
    }
    EXPECT_EQ(queue.pop(), -2);
    EXPECT_EQ(queue.pop(), -3);
    EXPECT_TRUE(queue.empty());
}

TEST(LoadTaskQueueTest, CancelOnlyAppliesPredicateToMatchingLoad) {
    LoadTaskQueue<int> queue;
    queue.push(1, 3, 10);
    queue.push(2, 0, 20);
    queue.push(2, 3, 21);
    queue.push(3, 0, 30);
    int inspected = 0;
    auto removed = queue.remove_if(2, [&](int task) {
        ++inspected;
        EXPECT_TRUE(task == 20 || task == 21);
        return true;
    });
    EXPECT_EQ(inspected, 2);
    EXPECT_EQ(removed, (std::vector<int> {20, 21}));
    EXPECT_TRUE(queue.remove_if(2, [&](int) {
                         ADD_FAILURE() << "an empty load inspected another load's tasks";
                         return true;
                     }).empty());
    queue.push(2, 0, 22);
    for (int expected : {30, 22, 10}) {
        EXPECT_EQ(queue.pop(), expected);
    }
    EXPECT_TRUE(queue.empty());
}

TEST(LoadTaskQueueTest, EraseHandlePreservesOtherHandlesAndGlobalFifo) {
    LoadTaskQueue<int> queue;
    queue.push(1, 0, 10);
    queue.push(1, 0, 11);
    auto middle = queue.push(1, 0, 12);
    auto last = queue.push(1, 0, 13);
    auto other_load = queue.push(2, 0, 20);
    queue.push(3, 0, 30);
    EXPECT_EQ(queue.pop(), 10);
    queue.erase(middle);
    EXPECT_EQ(queue.remove_if(1, [](int task) { return task == 11; }), (std::vector<int> {11}));
    queue.erase(last); // remove_if must not invalidate another entry's handle.
    queue.erase(other_load);
    queue.push(1, 0, 14);
    EXPECT_EQ(queue.size(), 2);
    EXPECT_EQ(queue.pop(), 30);
    EXPECT_EQ(queue.pop(), 14);
    EXPECT_TRUE(queue.empty());
}

} // namespace doris
