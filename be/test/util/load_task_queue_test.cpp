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

TEST(LoadTaskQueueTest, LoadFifoThenPriority) {
    LoadTaskQueue<int> queue;
    queue.push(1, 3, 13);
    queue.push(1, 2, 12);
    queue.push(1, 1, 11);
    queue.push(1, 0, 10);
    queue.push(2, 3, 23); // DUP load gets a turn despite load 1's bitmap work.
    std::vector<int> actual;
    while (!queue.empty()) {
        actual.push_back(queue.pop());
    }
    EXPECT_EQ(actual, (std::vector<int> {10, 23, 11, 12, 13}));
}

TEST(LoadTaskQueueTest, PriorityDoesNotCrossLoadsAndSamePriorityIsFifo) {
    LoadTaskQueue<int> queue;
    queue.push(1, 3, 1);
    queue.push(2, 0, 2);
    queue.push(1, 3, 3);
    queue.push(2, 0, 4);
    for (int expected : {1, 2, 3, 4}) {
        EXPECT_EQ(queue.pop(), expected);
    }
    EXPECT_TRUE(queue.empty());
}

TEST(LoadTaskQueueTest, RequeueBeforeExecutionAndReactivateEmptyLoad) {
    LoadTaskQueue<int> queue;
    queue.push(1, 3, 1);
    queue.push(1, 3, 2);
    queue.push(2, 3, 3);
    EXPECT_EQ(queue.pop(), 1); // Task 1 need not finish before the next turn.
    EXPECT_EQ(queue.pop(), 3);
    EXPECT_EQ(queue.pop(), 2);
    queue.push(2, 3, 4);
    queue.push(1, 0, 5); // Empty -> nonempty puts load 1 at the tail exactly once.
    queue.push(1, 0, 6);
    for (int expected : {4, 5, 6}) {
        EXPECT_EQ(queue.pop(), expected);
    }
    EXPECT_TRUE(queue.empty());
}

TEST(LoadTaskQueueTest, CancelOneTokenPreservesOtherTasksAndLoadOrder) {
    LoadTaskQueue<int> queue;
    queue.push(1, 0, 1);
    queue.push(1, 2, 2);
    queue.push(2, 3, 3);
    queue.push(1, 3, 4);
    EXPECT_EQ(queue.remove_if(1, [](int task) { return task < 3; }), (std::vector<int> {1, 2}));
    EXPECT_EQ(queue.size(), 2);
    EXPECT_EQ(queue.pop(), 4);
    EXPECT_EQ(queue.pop(), 3);
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

} // namespace doris
