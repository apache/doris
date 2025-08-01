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

#include "olap/rowset/segment_v2/inverted_index/util/priority_queue.h"

#include <gtest/gtest.h>

#include <vector>

namespace doris::segment_v2::inverted_index {

class MinHeapComparator : public PriorityQueue<int32_t> {
public:
    MinHeapComparator(size_t max_size, std::function<int32_t()> sentinel_object_supplier = nullptr)
            : PriorityQueue(max_size, sentinel_object_supplier) {}
    bool less_than(int32_t a, int32_t b) const override { return a < b; }
};

class PriorityQueueTest : public testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(PriorityQueueTest, TestConstructor) {
    MinHeapComparator pq(5);
    ASSERT_EQ(pq.size(), 0);

    MinHeapComparator pq2(0);
    ASSERT_EQ(pq2.size(), 0);
}

TEST_F(PriorityQueueTest, TestAddAndTop) {
    MinHeapComparator pq(3);
    pq.add(3);
    ASSERT_EQ(pq.top(), 3);

    pq.add(1);
    ASSERT_EQ(pq.top(), 1);

    pq.add(2);
    ASSERT_EQ(pq.top(), 1);
}

TEST_F(PriorityQueueTest, TestInsertWithOverflow) {
    MinHeapComparator pq(3);

    ASSERT_EQ(pq.insert_with_overflow(3), 0);
    ASSERT_EQ(pq.insert_with_overflow(2), 0);
    ASSERT_EQ(pq.insert_with_overflow(1), 0);
    ASSERT_EQ(pq.top(), 1);

    int32_t rejected = pq.insert_with_overflow(0);
    ASSERT_EQ(rejected, 0);
    ASSERT_EQ(pq.top(), 1);

    rejected = pq.insert_with_overflow(2);
    ASSERT_EQ(rejected, 1);
    ASSERT_EQ(pq.top(), 2);
}

TEST_F(PriorityQueueTest, TestPop) {
    MinHeapComparator pq(3);
    pq.add(3);
    pq.add(1);
    pq.add(2);

    ASSERT_EQ(pq.pop(), 1);
    ASSERT_EQ(pq.pop(), 2);
    ASSERT_EQ(pq.pop(), 3);
    ASSERT_EQ(pq.size(), 0);
}

TEST_F(PriorityQueueTest, TestUpdateTop) {
    MinHeapComparator pq(3);
    pq.add(3);
    pq.add(1);
    pq.add(2);

    pq.update_top();
    ASSERT_EQ(pq.top(), 1);

    pq.update_top(4);
    ASSERT_EQ(pq.top(), 2);
}

TEST_F(PriorityQueueTest, TestRemove) {
    MinHeapComparator pq(5);
    pq.add(5);
    pq.add(3);
    pq.add(1);
    pq.add(4);
    pq.add(2);

    ASSERT_TRUE(pq.remove(3));
    ASSERT_EQ(pq.size(), 4);

    std::vector<int32_t> elements;
    while (pq.size() > 0) {
        elements.push_back(pq.pop());
    }

    ASSERT_EQ(elements, (std::vector<int32_t> {1, 2, 4, 5}));
}

TEST_F(PriorityQueueTest, TestClear) {
    MinHeapComparator pq(5);
    pq.add(1);
    pq.add(2);
    pq.add(3);

    pq.clear();
    ASSERT_EQ(pq.size(), 0);
    ASSERT_EQ(pq.insert_with_overflow(0), 0);
}

TEST_F(PriorityQueueTest, TestSentinelInitialization) {
    auto sentinel_supplier = []() { return INT32_MAX; };
    MinHeapComparator pq(3, sentinel_supplier);

    ASSERT_EQ(pq.size(), 3);
    ASSERT_EQ(pq.top(), INT32_MAX);

    pq.insert_with_overflow(1);
    ASSERT_EQ(pq.top(), INT32_MAX);
}

TEST_F(PriorityQueueTest, TestBoundaryCases) {
    MinHeapComparator pq(0);
    ASSERT_EQ(pq.pop(), 0);

    MinHeapComparator pq2(1);
    pq2.add(5);
    ASSERT_EQ(pq2.insert_with_overflow(3), 3);
    ASSERT_EQ(pq2.top(), 5);

    ASSERT_EQ(pq2.insert_with_overflow(7), 5);
    ASSERT_EQ(pq2.top(), 7);
}

} // namespace doris::segment_v2::inverted_index