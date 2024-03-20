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

#include "util/indexed_priority_queue.hpp"

#include <gtest/gtest.h>

namespace doris {

class IndexedPriorityQueueTest : public testing::Test {
public:
    IndexedPriorityQueueTest() = default;
    virtual ~IndexedPriorityQueueTest() = default;
};

TEST_F(IndexedPriorityQueueTest, test_high_to_low) {
    IndexedPriorityQueue<int, IndexedPriorityQueuePriorityOrdering::HIGH_TO_LOW> pq;

    pq.add_or_update(3, 10);
    pq.add_or_update(1, 5);
    pq.add_or_update(4, 15);
    pq.add_or_update(2, 8);
    pq.add_or_update(5, 5);
    pq.add_or_update(6, 5);

    std::vector<int> expected_elements = {4, 3, 2, 1, 5, 6};
    std::vector<int> actual_elements;
    for (auto& elem : pq) {
        actual_elements.push_back(elem);
    }
    EXPECT_EQ(expected_elements, actual_elements);

    int removed = 2;
    pq.remove(removed);

    expected_elements = {4, 3, 1, 5, 6};
    actual_elements.clear();
    for (auto& elem : pq) {
        actual_elements.push_back(elem);
    }
    EXPECT_EQ(expected_elements, actual_elements);

    pq.add_or_update(4, 1);

    expected_elements = {3, 1, 5, 6, 4};
    actual_elements.clear();
    for (auto& elem : pq) {
        actual_elements.push_back(elem);
    }
    EXPECT_EQ(expected_elements, actual_elements);
}

TEST_F(IndexedPriorityQueueTest, test_low_to_high) {
    IndexedPriorityQueue<int, IndexedPriorityQueuePriorityOrdering::LOW_TO_HIGH> pq;

    pq.add_or_update(3, 10);
    pq.add_or_update(1, 5);
    pq.add_or_update(4, 15);
    pq.add_or_update(2, 8);
    pq.add_or_update(5, 5);
    pq.add_or_update(6, 5);

    std::vector<int> expected_elements = {1, 5, 6, 2, 3, 4};
    std::vector<int> actual_elements;
    for (auto& elem : pq) {
        actual_elements.push_back(elem);
    }
    EXPECT_EQ(expected_elements, actual_elements);

    int removed = 2;
    pq.remove(removed);

    expected_elements = {1, 5, 6, 3, 4};
    actual_elements.clear();
    for (auto& elem : pq) {
        actual_elements.push_back(elem);
    }
    EXPECT_EQ(expected_elements, actual_elements);

    pq.add_or_update(4, 1);

    expected_elements = {4, 1, 5, 6, 3};
    actual_elements.clear();
    for (auto& elem : pq) {
        actual_elements.push_back(elem);
    }
    EXPECT_EQ(expected_elements, actual_elements);
}

} // namespace doris
